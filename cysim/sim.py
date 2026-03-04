"""Discrete-event simulation engine for Cyphal v1.1 epidemic gossip protocol."""

from __future__ import annotations

import heapq
import math
import random
from collections import deque
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Protocol constants (matching cy.c)
# ---------------------------------------------------------------------------
GOSSIP_PERIOD: int = 3_000_000          # 3 s in microseconds
GOSSIP_DITHER: int = 375_000            # ±375 ms
GOSSIP_TTL: int = 16
GOSSIP_OUTDEGREE: int = 2
GOSSIP_PEER_COUNT: int = 8
GOSSIP_DEDUP_CAP: int = 16
GOSSIP_DEDUP_TIMEOUT: int = 1_000_000   # 1 s
GOSSIP_PEER_STALE: int = 2 * GOSSIP_PERIOD  # 6 s
GOSSIP_PEER_ELIGIBLE: int = 3 * GOSSIP_PERIOD  # 9 s
PEER_REPLACE_PROB: float = 1.0 / 8
SUBJECT_ID_PINNED_MAX: int = 0x1FFF     # 8191
SUBJECT_ID_MODULUS: int = 8192           # default modulus
LAGE_MIN: int = -1
LAGE_MAX: int = 35

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class NetworkConfig:
    delay_us: tuple[int, int] = (1_000, 10_000)
    loss_probability: float = 0.0


@dataclass
class Topic:
    name: str
    hash: int              # rapidhash-style 64-bit
    evictions: int = 0
    ts_created_us: int = 0

    def lage(self, now_us: int) -> int:
        age_s = max(0, now_us - self.ts_created_us) // 1_000_000
        if age_s <= 0:
            return LAGE_MIN
        return min(int(math.log2(age_s)), LAGE_MAX)

    def subject_id(self) -> int:
        return subject_id(self.hash, self.evictions, SUBJECT_ID_MODULUS)


@dataclass
class GossipPeer:
    node_id: int
    last_seen_us: int = 0


@dataclass
class DedupEntry:
    hash: int = 0
    last_seen_us: int = 0


@dataclass
class Node:
    node_id: int
    online: bool = False
    topics: dict[int, Topic] = field(default_factory=dict)          # keyed by topic hash
    gossip_queue: deque[int] = field(default_factory=deque)         # topic hashes, round-robin
    gossip_urgent: deque[int] = field(default_factory=deque)        # priority queue for repairs
    peers: list[GossipPeer | None] = field(default_factory=lambda: [None] * GOSSIP_PEER_COUNT)
    dedup: list[DedupEntry] = field(default_factory=lambda: [DedupEntry() for _ in range(GOSSIP_DEDUP_CAP)])
    next_broadcast_us: int = 0
    peer_replacement_moratorium_until: int = 0

    def add_topic(self, topic: Topic) -> None:
        self.topics[topic.hash] = topic
        if topic.hash not in self.gossip_queue:
            self.gossip_queue.append(topic.hash)

    def find_topic_by_subject_id(self, sid: int) -> Topic | None:
        for t in self.topics.values():
            if t.subject_id() == sid:
                return t
        return None


@dataclass
class EventRecord:
    time_us: int
    event: str       # "broadcast", "unicast", "forward", "conflict", "resolved", "join", "dedup"
    src: int
    dst: int | None
    topic_hash: int
    details: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def subject_id(topic_hash: int, evictions: int, modulus: int) -> int:
    if topic_hash <= SUBJECT_ID_PINNED_MAX:
        return topic_hash
    return SUBJECT_ID_PINNED_MAX + 1 + ((topic_hash + evictions * evictions) % modulus)


def left_wins(l_lage: int, l_hash: int, r_lage: int, r_hash: int) -> bool:
    """Older topic wins; on tie, lower hash wins."""
    if l_lage != r_lage:
        return l_lage > r_lage
    return l_hash < r_hash


def gossip_dedup_hash(topic_hash: int, evictions: int, lage: int) -> int:
    lage_clamped = max(LAGE_MIN, min(lage, LAGE_MAX))
    other = (evictions << 16) | ((lage_clamped - LAGE_MIN) << 56)
    return topic_hash ^ other


# ---------------------------------------------------------------------------
# Lightweight snapshot for visualization replay
# ---------------------------------------------------------------------------

@dataclass
class TopicSnap:
    name: str
    hash: int
    evictions: int
    subject_id: int


@dataclass
class PeerSnap:
    node_id: int
    last_seen_us: int


@dataclass
class NodeSnapshot:
    node_id: int
    online: bool
    topics: list[TopicSnap]
    peers: list[PeerSnap | None]
    gossip_queue_front: int | None    # topic hash of next round-robin topic
    gossip_urgent_front: int | None   # topic hash of next urgent topic
    next_broadcast_us: int


# ---------------------------------------------------------------------------
# Simulation engine
# ---------------------------------------------------------------------------

class Simulation:
    def __init__(self, net: NetworkConfig, rng_seed: int = 42):
        self.net = net
        self.rng = random.Random(rng_seed)
        self.nodes: dict[int, Node] = {}
        self._queue: list[tuple[int, int, str, dict]] = []  # heapq
        self._seq: int = 0
        self.log: list[EventRecord] = []
        self.now_us: int = 0
        self.snapshots: dict[int, dict[int, NodeSnapshot]] = {}  # frame_index -> {node_id -> snap}

    # -- node management --

    def add_node(self, node_id: int, online_at_us: int = 0) -> Node:
        node = Node(node_id=node_id)
        self.nodes[node_id] = node
        self._push_event(online_at_us, "NODE_JOIN", {"node_id": node_id})
        return node

    def add_topic_to_node(self, node_id: int, topic: Topic) -> None:
        self.nodes[node_id].add_topic(topic)

    # -- event queue --

    def _push_event(self, time_us: int, etype: str, payload: dict) -> None:
        heapq.heappush(self._queue, (time_us, self._seq, etype, payload))
        self._seq += 1

    def _dithered_period(self) -> int:
        return GOSSIP_PERIOD + self.rng.randint(-GOSSIP_DITHER, GOSSIP_DITHER)

    def _rand_delay(self) -> int:
        lo, hi = self.net.delay_us
        return self.rng.randint(lo, hi)

    # -- message sending --

    def _online_nodes(self) -> list[Node]:
        return [n for n in self.nodes.values() if n.online]

    def _send_broadcast(self, sender: Node, topic_hash: int, evictions: int, lage: int, name: str) -> None:
        for dest in self._online_nodes():
            if dest.node_id == sender.node_id:
                continue
            if self.rng.random() < self.net.loss_probability:
                continue
            delay = self._rand_delay()
            self._push_event(self.now_us + delay, "MSG_ARRIVE", {
                "src": sender.node_id,
                "dst": dest.node_id,
                "topic_hash": topic_hash,
                "evictions": evictions,
                "lage": lage,
                "name": name,
                "ttl": 0,
                "msg_type": "broadcast",
            })
        self.log.append(EventRecord(
            time_us=self.now_us, event="broadcast", src=sender.node_id, dst=None,
            topic_hash=topic_hash, details={"evictions": evictions, "lage": lage, "name": name},
        ))

    def _send_unicast(self, sender: Node, dest_id: int, topic_hash: int,
                      evictions: int, lage: int, name: str, ttl: int, msg_type: str) -> None:
        if self.rng.random() < self.net.loss_probability:
            return
        delay = self._rand_delay()
        self._push_event(self.now_us + delay, "MSG_ARRIVE", {
            "src": sender.node_id,
            "dst": dest_id,
            "topic_hash": topic_hash,
            "evictions": evictions,
            "lage": lage,
            "name": name,
            "ttl": ttl,
            "msg_type": msg_type,
        })
        self.log.append(EventRecord(
            time_us=self.now_us, event=msg_type, src=sender.node_id, dst=dest_id,
            topic_hash=topic_hash, details={"evictions": evictions, "lage": lage, "ttl": ttl},
        ))

    # -- epidemic forwarding --

    def _epidemic_forward(self, node: Node, sender_id: int, topic_hash: int,
                          evictions: int, lage: int, name: str, ttl: int) -> None:
        if ttl <= 0:
            return
        new_ttl = ttl - 1
        blacklist = {sender_id}
        for _ in range(GOSSIP_OUTDEGREE):
            peer = self._random_eligible_peer(node, blacklist)
            if peer is None:
                break
            blacklist.add(peer.node_id)
            self._send_unicast(node, peer.node_id, topic_hash, evictions, lage, name, new_ttl, "forward")

    def _random_eligible_peer(self, node: Node, blacklist: set[int]) -> GossipPeer | None:
        eligible = [p for p in node.peers
                    if p is not None
                    and p.node_id not in blacklist
                    and (self.now_us - p.last_seen_us) < GOSSIP_PEER_ELIGIBLE]
        if not eligible:
            return None
        return self.rng.choice(eligible)

    # -- dedup --

    def _dedup_match_or_lru(self, node: Node, dhash: int) -> DedupEntry:
        oldest = node.dedup[0]
        for entry in node.dedup:
            if entry.hash == dhash:
                return entry
            if entry.last_seen_us < oldest.last_seen_us:
                oldest = entry
        return oldest

    def _dedup_is_fresh(self, entry: DedupEntry, dhash: int) -> bool:
        """Returns True if the entry does NOT suppress this gossip (i.e. we should process it)."""
        return (entry.hash != dhash) or (entry.last_seen_us < (self.now_us - GOSSIP_DEDUP_TIMEOUT))

    # -- peer refresh (mirrors cy.c:2065-2092) --

    def _peer_update(self, node: Node, sender_id: int) -> None:
        # Check if sender already in peer list
        for p in node.peers:
            if p is not None and p.node_id == sender_id:
                p.last_seen_us = self.now_us
                return

        # Not found — look for empty or stale slot
        stale_threshold = self.now_us - GOSSIP_PEER_STALE
        oldest_idx = 0
        oldest_seen = self.now_us + 1
        for i, p in enumerate(node.peers):
            seen = p.last_seen_us if p is not None else 0
            if seen < oldest_seen:
                oldest_seen = seen
                oldest_idx = i

        if oldest_seen < stale_threshold:
            node.peers[oldest_idx] = GossipPeer(node_id=sender_id, last_seen_us=self.now_us)
            return

        # All fresh — probabilistic replacement
        if (self.now_us >= node.peer_replacement_moratorium_until
                and self.rng.random() < PEER_REPLACE_PROB):
            idx = self.rng.randrange(GOSSIP_PEER_COUNT)
            node.peers[idx] = GossipPeer(node_id=sender_id, last_seen_us=self.now_us)
            moratorium = GOSSIP_PERIOD // 2
            node.peer_replacement_moratorium_until = self.now_us + self.rng.randint(0, moratorium)

    # -- broadcast tick handler --

    def _handle_broadcast_tick(self, node: Node) -> None:
        if not node.online:
            return

        # Pick topic: urgent first, then round-robin
        topic_hash: int | None = None
        if node.gossip_urgent:
            topic_hash = node.gossip_urgent.popleft()
        elif node.gossip_queue:
            topic_hash = node.gossip_queue[0]
            node.gossip_queue.rotate(-1)  # round-robin

        if topic_hash is not None and topic_hash in node.topics:
            topic = node.topics[topic_hash]
            lage = topic.lage(self.now_us)

            if topic_hash in node.gossip_urgent:
                # Was urgent — send unicast epidemic to peers
                blacklist: set[int] = set()
                for _ in range(GOSSIP_OUTDEGREE):
                    peer = self._random_eligible_peer(node, blacklist)
                    if peer is None:
                        break
                    blacklist.add(peer.node_id)
                    dhash = gossip_dedup_hash(topic.hash, topic.evictions, lage)
                    dedup = self._dedup_match_or_lru(node, dhash)
                    if self._dedup_is_fresh(dedup, dhash):
                        self._send_unicast(
                            node, peer.node_id, topic.hash,
                            topic.evictions, lage, topic.name, GOSSIP_TTL, "unicast",
                        )
                        dedup.hash = dhash
                        dedup.last_seen_us = self.now_us
            else:
                # Normal broadcast
                self._send_broadcast(node, topic.hash, topic.evictions, lage, topic.name)
                dhash = gossip_dedup_hash(topic.hash, topic.evictions, lage)
                dedup = self._dedup_match_or_lru(node, dhash)
                dedup.hash = dhash
                dedup.last_seen_us = self.now_us

        # Process urgent queue items (unicast epidemic) between broadcast ticks
        self._drain_urgent(node)

        # Schedule next broadcast tick
        node.next_broadcast_us = self.now_us + self._dithered_period()
        self._push_event(node.next_broadcast_us, "BROADCAST_TICK", {"node_id": node.node_id})

    def _drain_urgent(self, node: Node) -> None:
        """Send any remaining urgent gossips as unicast epidemics."""
        while node.gossip_urgent:
            th = node.gossip_urgent.popleft()
            if th not in node.topics:
                continue
            topic = node.topics[th]
            lage = topic.lage(self.now_us)
            dhash = gossip_dedup_hash(topic.hash, topic.evictions, lage)
            dedup = self._dedup_match_or_lru(node, dhash)
            if not self._dedup_is_fresh(dedup, dhash):
                continue
            blacklist: set[int] = set()
            sent = False
            for _ in range(GOSSIP_OUTDEGREE):
                peer = self._random_eligible_peer(node, blacklist)
                if peer is None:
                    break
                blacklist.add(peer.node_id)
                self._send_unicast(
                    node, peer.node_id, topic.hash,
                    topic.evictions, lage, topic.name, GOSSIP_TTL, "unicast",
                )
                sent = True
            if sent:
                dedup.hash = dhash
                dedup.last_seen_us = self.now_us

    # -- message arrival handler (mirrors cy.c on_gossip) --

    def _handle_msg_arrive(self, payload: dict) -> None:
        dst_id = payload["dst"]
        node = self.nodes.get(dst_id)
        if node is None or not node.online:
            return

        src_id = payload["src"]
        topic_hash = payload["topic_hash"]
        evictions = payload["evictions"]
        lage = payload["lage"]
        name = payload["name"]
        ttl = payload["ttl"]
        msg_type = payload["msg_type"]

        # Update peer set
        self._peer_update(node, src_id)

        # Dedup check
        dhash = gossip_dedup_hash(topic_hash, evictions, lage)
        dedup = self._dedup_match_or_lru(node, dhash)
        should_forward = self._dedup_is_fresh(dedup, dhash) and (ttl > 0)
        dedup.hash = dhash
        dedup.last_seen_us = self.now_us

        mine = node.topics.get(topic_hash)
        if mine is not None:
            # Known topic — check for divergence
            local_won = self._on_gossip_known_topic(node, mine, evictions, lage)
            if should_forward and not local_won:
                self._epidemic_forward(
                    node, src_id, topic_hash, mine.evictions, mine.lage(self.now_us), name, ttl,
                )
        else:
            # Unknown topic — check for subject-ID collision
            local_won = self._on_gossip_unknown_topic(node, topic_hash, evictions, lage)
            if not local_won and name:
                # Learn the new topic
                new_topic = Topic(
                    name=name, hash=topic_hash, evictions=evictions, ts_created_us=self._ts_from_lage(lage),
                )
                node.add_topic(new_topic)
                self.log.append(EventRecord(
                    time_us=self.now_us, event="learned", src=src_id, dst=dst_id,
                    topic_hash=topic_hash, details={"name": name, "evictions": evictions},
                ))
            if should_forward and not local_won:
                self._epidemic_forward(node, src_id, topic_hash, evictions, lage, name, ttl)

    def _ts_from_lage(self, lage: int) -> int:
        """Reconstruct approximate ts_created from log-age."""
        if lage <= LAGE_MIN:
            return self.now_us
        return self.now_us - (2 ** lage) * 1_000_000

    # -- on_gossip_known_topic (mirrors cy.c:1850-1895) --

    def _on_gossip_known_topic(self, node: Node, mine: Topic, evictions: int, lage: int) -> bool:
        mine_lage = mine.lage(self.now_us)
        if mine.evictions != evictions:
            # Divergence — arbitrate by (lage, evictions): higher wins
            win = (mine_lage > lage) or (mine_lage == lage and mine.evictions > evictions)
            self.log.append(EventRecord(
                time_us=self.now_us, event="conflict", src=node.node_id, dst=None,
                topic_hash=mine.hash,
                details={
                    "type": "divergence", "local_won": win,
                    "local_evictions": mine.evictions, "remote_evictions": evictions,
                    "local_lage": mine_lage, "remote_lage": lage,
                },
            ))
            if win:
                self._schedule_urgent(node, mine.hash)
            else:
                # Accept remote state, merge lage
                mine.evictions = evictions
                mine.ts_created_us = min(mine.ts_created_us, self._ts_from_lage(lage))
                self.log.append(EventRecord(
                    time_us=self.now_us, event="resolved", src=node.node_id, dst=None,
                    topic_hash=mine.hash,
                    details={"accepted_evictions": evictions, "new_sid": mine.subject_id()},
                ))
            return win
        else:
            # Same evictions — no conflict, merge lage
            mine.ts_created_us = min(mine.ts_created_us, self._ts_from_lage(lage))
            return False

    # -- on_gossip_unknown_topic (mirrors cy.c:1900-1941) --

    def _on_gossip_unknown_topic(self, node: Node, remote_hash: int, evictions: int, lage: int) -> bool:
        remote_sid = subject_id(remote_hash, evictions, SUBJECT_ID_MODULUS)
        mine = node.find_topic_by_subject_id(remote_sid)
        if mine is None:
            return False  # No collision

        mine_lage = mine.lage(self.now_us)
        win = left_wins(mine_lage, mine.hash, lage, remote_hash)
        self.log.append(EventRecord(
            time_us=self.now_us, event="conflict", src=node.node_id, dst=None,
            topic_hash=mine.hash,
            details={
                "type": "collision", "local_won": win,
                "local_sid": mine.subject_id(), "remote_hash": remote_hash,
                "remote_evictions": evictions,
            },
        ))
        if win:
            self._schedule_urgent(node, mine.hash)
        else:
            mine.evictions += 1
            self.log.append(EventRecord(
                time_us=self.now_us, event="resolved", src=node.node_id, dst=None,
                topic_hash=mine.hash,
                details={"new_evictions": mine.evictions, "new_sid": mine.subject_id()},
            ))
        return win

    def _schedule_urgent(self, node: Node, topic_hash: int) -> None:
        if topic_hash not in node.gossip_urgent:
            node.gossip_urgent.append(topic_hash)

    # -- node join --

    def _handle_node_join(self, node_id: int) -> None:
        node = self.nodes[node_id]
        node.online = True
        # Schedule first broadcast tick
        node.next_broadcast_us = self.now_us + self.rng.randint(0, GOSSIP_PERIOD)
        self._push_event(node.next_broadcast_us, "BROADCAST_TICK", {"node_id": node_id})
        self.log.append(EventRecord(
            time_us=self.now_us, event="join", src=node_id, dst=None, topic_hash=0,
        ))

    # -- snapshots --

    def _snap_node(self, node: Node) -> NodeSnapshot:
        topics = [
            TopicSnap(name=t.name, hash=t.hash, evictions=t.evictions, subject_id=t.subject_id())
            for t in sorted(node.topics.values(), key=lambda t: t.name)
        ]
        peers = [
            PeerSnap(node_id=p.node_id, last_seen_us=p.last_seen_us) if p is not None else None
            for p in node.peers
        ]
        q_front = node.gossip_queue[0] if node.gossip_queue else None
        u_front = node.gossip_urgent[0] if node.gossip_urgent else None
        return NodeSnapshot(
            node_id=node.node_id, online=node.online,
            topics=topics, peers=peers,
            gossip_queue_front=q_front, gossip_urgent_front=u_front,
            next_broadcast_us=node.next_broadcast_us,
        )

    def _take_snapshot(self, frame_idx: int) -> None:
        self.snapshots[frame_idx] = {
            nid: self._snap_node(n) for nid, n in self.nodes.items()
        }

    # -- main loop --

    def run(self, until_us: int, snapshot_interval_us: int = 0) -> list[EventRecord]:
        next_snap_idx = 0
        do_snaps = snapshot_interval_us > 0

        while self._queue:
            time_us, _seq, etype, payload = heapq.heappop(self._queue)
            if time_us > until_us:
                break

            # Snapshot any frames that fall between the previous event and this one.
            # This captures state *after* all events at the previous timestamp.
            if do_snaps:
                while next_snap_idx * snapshot_interval_us < time_us:
                    self._take_snapshot(next_snap_idx)
                    next_snap_idx += 1

            self.now_us = time_us

            if etype == "NODE_JOIN":
                self._handle_node_join(payload["node_id"])
            elif etype == "BROADCAST_TICK":
                node = self.nodes.get(payload["node_id"])
                if node is not None and node.online:
                    self._handle_broadcast_tick(node)
            elif etype == "MSG_ARRIVE":
                self._handle_msg_arrive(payload)

        # Capture remaining snapshots (including the final state)
        if do_snaps:
            end_idx = until_us // snapshot_interval_us + 1
            while next_snap_idx <= end_idx:
                self._take_snapshot(next_snap_idx)
                next_snap_idx += 1

        return self.log

    # -- convergence check --

    def check_convergence(self) -> bool:
        """Return True if all online nodes agree on topic→subject-ID mapping."""
        online = self._online_nodes()
        if len(online) < 2:
            return True
        ref = self._topic_sid_map(online[0])
        return all(self._topic_sid_map(n) == ref for n in online[1:])

    @staticmethod
    def _topic_sid_map(node: Node) -> dict[int, int]:
        return {th: t.subject_id() for th, t in node.topics.items()}

    def convergence_details(self) -> dict[int, dict[int, int]]:
        """Return per-node topic→subject-ID maps for inspection."""
        return {n.node_id: Simulation._topic_sid_map(n) for n in self._online_nodes()}
