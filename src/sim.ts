// ---------------------------------------------------------------------------
// Simulation engine — faithful port of cysim/sim.py
// ---------------------------------------------------------------------------

import {
  NetworkConfig, Topic, GossipPeer, DedupEntry, Node,
  EventRecord, TopicSnap, PeerSnap, NodeSnapshot,
} from "./types.js";
import {
  GOSSIP_PERIOD, GOSSIP_DITHER, GOSSIP_TTL, GOSSIP_OUTDEGREE,
  GOSSIP_PEER_COUNT, GOSSIP_DEDUP_CAP, GOSSIP_DEDUP_TIMEOUT,
  GOSSIP_PEER_STALE, GOSSIP_PEER_ELIGIBLE, PEER_REPLACE_PROB,
  SUBJECT_ID_PINNED_MAX, SUBJECT_ID_MODULUS, LAGE_MIN, LAGE_MAX,
} from "./constants.js";

// ---------------------------------------------------------------------------
// Seeded RNG — Mulberry32
// ---------------------------------------------------------------------------

class Rng {
  private state: number;

  constructor(seed: number) {
    this.state = seed | 0;
  }

  random(): number {
    let t = (this.state += 0x6D2B79F5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  randint(lo: number, hi: number): number {
    return lo + Math.floor(this.random() * (hi - lo + 1));
  }

  randrange(n: number): number {
    return Math.floor(this.random() * n);
  }

  choice<T>(arr: T[]): T {
    return arr[Math.floor(this.random() * arr.length)];
  }
}

// ---------------------------------------------------------------------------
// Priority queue (min-heap) for events
// ---------------------------------------------------------------------------

interface SimEvent {
  timeUs: number;
  seq: number;
  type: string;
  payload: Record<string, unknown>;
}

class MinHeap {
  private data: SimEvent[] = [];

  get length(): number { return this.data.length; }

  push(ev: SimEvent): void {
    this.data.push(ev);
    this.siftUp(this.data.length - 1);
  }

  pop(): SimEvent | undefined {
    const d = this.data;
    if (d.length === 0) return undefined;
    const top = d[0];
    const last = d.pop()!;
    if (d.length > 0) {
      d[0] = last;
      this.siftDown(0);
    }
    return top;
  }

  peek(): SimEvent | undefined {
    return this.data[0];
  }

  private less(a: SimEvent, b: SimEvent): boolean {
    return a.timeUs < b.timeUs || (a.timeUs === b.timeUs && a.seq < b.seq);
  }

  private siftUp(i: number): void {
    const d = this.data;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (this.less(d[i], d[p])) {
        [d[i], d[p]] = [d[p], d[i]];
        i = p;
      } else break;
    }
  }

  private siftDown(i: number): void {
    const d = this.data;
    const n = d.length;
    while (true) {
      let smallest = i;
      const l = 2 * i + 1, r = 2 * i + 2;
      if (l < n && this.less(d[l], d[smallest])) smallest = l;
      if (r < n && this.less(d[r], d[smallest])) smallest = r;
      if (smallest === i) break;
      [d[i], d[smallest]] = [d[smallest], d[i]];
      i = smallest;
    }
  }
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

export function subjectId(topicHash: bigint, evictions: number, modulus: number): number {
  if (topicHash <= BigInt(SUBJECT_ID_PINNED_MAX)) {
    return Number(topicHash);
  }
  return SUBJECT_ID_PINNED_MAX + 1 +
    Number((topicHash + BigInt(evictions * evictions)) % BigInt(modulus));
}

export function leftWins(lLage: number, lHash: bigint, rLage: number, rHash: bigint): boolean {
  if (lLage !== rLage) return lLage > rLage;
  return lHash < rHash;
}

export function gossipDedupHash(topicHash: bigint, evictions: number, lage: number): bigint {
  const lageClamped = Math.max(LAGE_MIN, Math.min(lage, LAGE_MAX));
  const other = (BigInt(evictions) << 16n) | (BigInt(lageClamped - LAGE_MIN) << 56n);
  return topicHash ^ other;
}

export function topicLage(tsCreatedUs: number, nowUs: number): number {
  const ageS = Math.max(0, nowUs - tsCreatedUs) / 1_000_000;
  if (ageS <= 0) return LAGE_MIN;
  return Math.min(Math.floor(Math.log2(ageS)), LAGE_MAX);
}

function topicSubjectId(t: Topic): number {
  return subjectId(t.hash, t.evictions, SUBJECT_ID_MODULUS);
}

// FNV-1a 64-bit via BigInt
function fnv1a64(s: string): bigint {
  let h = 0xcbf29ce484222325n;
  for (let i = 0; i < s.length; i++) {
    h ^= BigInt(s.charCodeAt(i));
    h = BigInt.asUintN(64, h * 0x100000001b3n);
  }
  return h;
}

/** Parse hash override suffix like "sensors/temperature#1a2b" (matching cy.c:1302-1332). */
function parseHashOverride(name: string): bigint | null {
  const idx = name.lastIndexOf("#");
  if (idx < 0 || idx === name.length - 1) return null;
  const hexPart = name.slice(idx + 1);
  if (hexPart.length > 16) return null;
  if (!/^[0-9a-f]+$/.test(hexPart)) return null;
  return BigInt("0x" + hexPart);
}

export function topicHash(name: string): bigint {
  const override = parseHashOverride(name);
  if (override !== null) return override;
  return fnv1a64(name);
}

// ---------------------------------------------------------------------------
// Node helpers
// ---------------------------------------------------------------------------

function makeNode(nodeId: number): Node {
  const dedup: DedupEntry[] = [];
  for (let i = 0; i < GOSSIP_DEDUP_CAP; i++) {
    dedup.push({ hash: 0n, lastSeenUs: 0 });
  }
  const peers: (GossipPeer | null)[] = [];
  for (let i = 0; i < GOSSIP_PEER_COUNT; i++) peers.push(null);
  return {
    nodeId,
    online: false,
    topics: new Map(),
    gossipQueue: [],
    gossipUrgent: [],
    peers,
    dedup,
    nextBroadcastUs: 0,
    partitionSet: "A",
    peerReplacementMoratoriumUntil: 0,
  };
}

function nodeAddTopic(node: Node, topic: Topic): void {
  node.topics.set(topic.hash, topic);
  if (!node.gossipQueue.includes(topic.hash)) {
    node.gossipQueue.push(topic.hash);
  }
}

function nodeFindBySubjectId(node: Node, sid: number): Topic | null {
  for (const t of node.topics.values()) {
    if (topicSubjectId(t) === sid) return t;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Simulation class
// ---------------------------------------------------------------------------

export class Simulation {
  net: NetworkConfig;
  rng: Rng;
  nodes: Map<number, Node> = new Map();
  nowUs = 0;

  private queue = new MinHeap();
  private seq = 0;
  private nextAutoId = 0;
  private nextAutoTopicChar = 97; // 'a'
  private seed: number;

  // Cumulative event log (for event counts in the UI)
  eventCounts: Record<string, number> = {};

  constructor(net: NetworkConfig, rngSeed = 42) {
    this.net = net;
    this.seed = rngSeed;
    this.rng = new Rng(rngSeed);
  }

  // -- public interactive API --

  addNode(nodeId?: number): Node {
    if (nodeId === undefined) {
      while (this.nodes.has(this.nextAutoId)) this.nextAutoId++;
      nodeId = this.nextAutoId++;
    }
    if (nodeId >= this.nextAutoId) this.nextAutoId = nodeId + 1;
    const node = makeNode(nodeId);
    this.nodes.set(nodeId, node);
    this.pushEvent(this.nowUs, "NODE_JOIN", { node_id: nodeId });
    return node;
  }

  destroyNode(nodeId: number): void {
    const node = this.nodes.get(nodeId);
    if (node) {
      node.online = false;
      this.nodes.delete(nodeId);
    }
  }

  restartNode(nodeId: number): void {
    const node = this.nodes.get(nodeId);
    if (!node) return;
    node.online = false;
    node.topics.clear();
    node.gossipQueue.length = 0;
    node.gossipUrgent.length = 0;
    for (let i = 0; i < node.peers.length; i++) node.peers[i] = null;
    for (const d of node.dedup) { d.hash = 0n; d.lastSeenUs = 0; }
    node.nextBroadcastUs = 0;
    node.peerReplacementMoratoriumUntil = 0;
    this.pushEvent(this.nowUs, "NODE_JOIN", { node_id: nodeId });
  }

  setPartition(nodeId: number, set: "A" | "B"): void {
    const node = this.nodes.get(nodeId);
    if (node) node.partitionSet = set;
  }

  addTopicToNode(nodeId: number, name?: string): Topic | null {
    const node = this.nodes.get(nodeId);
    if (!node) return null;
    if (name === undefined) {
      name = "topic/" + String.fromCharCode(this.nextAutoTopicChar);
      this.nextAutoTopicChar++;
      if (this.nextAutoTopicChar > 122) this.nextAutoTopicChar = 97; // wrap
    }
    const hash = topicHash(name);
    const topic: Topic = { name, hash, evictions: 0, tsCreatedUs: this.nowUs };
    nodeAddTopic(node, topic);
    // Schedule urgent gossip so the topic is announced quickly
    if (!node.gossipUrgent.includes(hash)) {
      node.gossipUrgent.push(hash);
    }
    return topic;
  }

  destroyTopicOnNode(nodeId: number, hash: bigint): void {
    const node = this.nodes.get(nodeId);
    if (!node) return;
    node.topics.delete(hash);
    node.gossipQueue = node.gossipQueue.filter(h => h !== hash);
    node.gossipUrgent = node.gossipUrgent.filter(h => h !== hash);
  }

  /** Process events up to targetUs. Returns new EventRecords generated. */
  stepUntil(targetUs: number): EventRecord[] {
    const newEvents: EventRecord[] = [];
    const pushLog = (rec: EventRecord) => {
      newEvents.push(rec);
      this.eventCounts[rec.event] = (this.eventCounts[rec.event] || 0) + 1;
    };

    while (this.queue.length > 0) {
      const top = this.queue.peek()!;
      if (top.timeUs > targetUs) break;
      const ev = this.queue.pop()!;
      this.nowUs = ev.timeUs;

      if (ev.type === "NODE_JOIN") {
        this.handleNodeJoin(ev.payload["node_id"] as number, pushLog);
      } else if (ev.type === "BROADCAST_TICK") {
        const node = this.nodes.get(ev.payload["node_id"] as number);
        if (node && node.online) {
          this.handleBroadcastTick(node, pushLog);
        }
      } else if (ev.type === "MSG_ARRIVE") {
        this.handleMsgArrive(ev.payload, pushLog);
      }
    }

    if (targetUs > this.nowUs) this.nowUs = targetUs;
    return newEvents;
  }

  /** Capture current state of all nodes. */
  snapshot(): Map<number, NodeSnapshot> {
    const result = new Map<number, NodeSnapshot>();
    for (const [nid, node] of this.nodes) {
      result.set(nid, this.snapNode(node));
    }
    return result;
  }

  checkConvergence(): boolean {
    const online = this.onlineNodes();
    if (online.length < 2) return true;
    const ref = this.topicSidMap(online[0]);
    for (let i = 1; i < online.length; i++) {
      const m = this.topicSidMap(online[i]);
      if (m.size !== ref.size) return false;
      for (const [k, v] of ref) {
        if (m.get(k) !== v) return false;
      }
    }
    return true;
  }

  checkConvergenceFromSnaps(snaps: Map<number, NodeSnapshot>): boolean {
    const sidMaps: Map<bigint, number>[] = [];
    for (const s of snaps.values()) {
      if (!s.online) continue;
      const m = new Map<bigint, number>();
      for (const t of s.topics) m.set(t.hash, t.subjectId);
      sidMaps.push(m);
    }
    if (sidMaps.length < 2) return true;
    const ref = sidMaps[0];
    for (let i = 1; i < sidMaps.length; i++) {
      const m = sidMaps[i];
      if (m.size !== ref.size) return false;
      for (const [k, v] of ref) {
        if (m.get(k) !== v) return false;
      }
    }
    return true;
  }

  // -- private --

  private pushEvent(timeUs: number, type: string, payload: Record<string, unknown>): void {
    this.queue.push({ timeUs, seq: this.seq++, type, payload });
  }

  private ditheredPeriod(): number {
    return GOSSIP_PERIOD + this.rng.randint(-GOSSIP_DITHER, GOSSIP_DITHER);
  }

  private randDelay(): number {
    const [lo, hi] = this.net.delayUs;
    return this.rng.randint(lo, hi);
  }

  private onlineNodes(): Node[] {
    const result: Node[] = [];
    for (const n of this.nodes.values()) {
      if (n.online) result.push(n);
    }
    return result;
  }

  // -- message sending (partition-aware) --

  private sendBroadcast(
    sender: Node, hash: bigint, evictions: number, lage: number, name: string,
    pushLog: (r: EventRecord) => void,
  ): void {
    for (const dest of this.nodes.values()) {
      if (dest.nodeId === sender.nodeId) continue;
      if (!dest.online) continue;
      if (dest.partitionSet !== sender.partitionSet) continue;
      if (this.rng.random() < this.net.lossProbability) continue;
      const delay = this.randDelay();
      this.pushEvent(this.nowUs + delay, "MSG_ARRIVE", {
        src: sender.nodeId, dst: dest.nodeId,
        topic_hash: hash, evictions, lage, name, ttl: 0, msg_type: "broadcast",
      });
    }
    pushLog({
      timeUs: this.nowUs, event: "broadcast", src: sender.nodeId, dst: null,
      topicHash: hash, details: { evictions, lage, name },
    });
  }

  private sendUnicast(
    sender: Node, destId: number, hash: bigint,
    evictions: number, lage: number, name: string, ttl: number, msgType: string,
    pushLog: (r: EventRecord) => void,
  ): void {
    const dest = this.nodes.get(destId);
    if (dest && dest.partitionSet !== sender.partitionSet) return;
    if (this.rng.random() < this.net.lossProbability) return;
    const delay = this.randDelay();
    this.pushEvent(this.nowUs + delay, "MSG_ARRIVE", {
      src: sender.nodeId, dst: destId,
      topic_hash: hash, evictions, lage, name, ttl, msg_type: msgType,
    });
    pushLog({
      timeUs: this.nowUs, event: msgType, src: sender.nodeId, dst: destId,
      topicHash: hash, details: { evictions, lage, ttl },
    });
  }

  // -- epidemic forwarding --

  private epidemicForward(
    node: Node, senderId: number, hash: bigint,
    evictions: number, lage: number, name: string, ttl: number,
    pushLog: (r: EventRecord) => void,
  ): void {
    if (ttl <= 0) return;
    const newTtl = ttl - 1;
    const blacklist = new Set([senderId]);
    for (let i = 0; i < GOSSIP_OUTDEGREE; i++) {
      const peer = this.randomEligiblePeer(node, blacklist);
      if (!peer) break;
      blacklist.add(peer.nodeId);
      this.sendUnicast(node, peer.nodeId, hash, evictions, lage, name, newTtl, "forward", pushLog);
    }
  }

  private randomEligiblePeer(node: Node, blacklist: Set<number>): GossipPeer | null {
    const eligible: GossipPeer[] = [];
    for (const p of node.peers) {
      if (p !== null && !blacklist.has(p.nodeId) &&
          (this.nowUs - p.lastSeenUs) < GOSSIP_PEER_ELIGIBLE) {
        eligible.push(p);
      }
    }
    if (eligible.length === 0) return null;
    return this.rng.choice(eligible);
  }

  // -- dedup --

  private dedupMatchOrLru(node: Node, dhash: bigint): DedupEntry {
    let oldest = node.dedup[0];
    for (const entry of node.dedup) {
      if (entry.hash === dhash) return entry;
      if (entry.lastSeenUs < oldest.lastSeenUs) oldest = entry;
    }
    return oldest;
  }

  private dedupIsFresh(entry: DedupEntry, dhash: bigint): boolean {
    return (entry.hash !== dhash) || (entry.lastSeenUs < (this.nowUs - GOSSIP_DEDUP_TIMEOUT));
  }

  // -- peer refresh --

  private peerUpdate(node: Node, senderId: number): void {
    for (const p of node.peers) {
      if (p !== null && p.nodeId === senderId) {
        p.lastSeenUs = this.nowUs;
        return;
      }
    }
    const staleThreshold = this.nowUs - GOSSIP_PEER_STALE;
    let oldestIdx = 0;
    let oldestSeen = this.nowUs + 1;
    for (let i = 0; i < node.peers.length; i++) {
      const seen = node.peers[i] !== null ? node.peers[i]!.lastSeenUs : 0;
      if (seen < oldestSeen) {
        oldestSeen = seen;
        oldestIdx = i;
      }
    }
    if (oldestSeen < staleThreshold) {
      node.peers[oldestIdx] = { nodeId: senderId, lastSeenUs: this.nowUs };
      return;
    }
    if (this.nowUs >= node.peerReplacementMoratoriumUntil &&
        this.rng.random() < PEER_REPLACE_PROB) {
      const idx = this.rng.randrange(GOSSIP_PEER_COUNT);
      node.peers[idx] = { nodeId: senderId, lastSeenUs: this.nowUs };
      const moratorium = GOSSIP_PERIOD >> 1;
      node.peerReplacementMoratoriumUntil = this.nowUs + this.rng.randint(0, moratorium);
    }
  }

  // -- broadcast tick handler --

  private handleBroadcastTick(node: Node, pushLog: (r: EventRecord) => void): void {
    if (!node.online) return;

    let hash: bigint | null = null;
    if (node.gossipUrgent.length > 0) {
      hash = node.gossipUrgent.shift()!;
    } else if (node.gossipQueue.length > 0) {
      hash = node.gossipQueue[0];
      // rotate(-1): move front to back
      node.gossipQueue.push(node.gossipQueue.shift()!);
    }

    if (hash !== null && node.topics.has(hash)) {
      const topic = node.topics.get(hash)!;
      const lage = topicLage(topic.tsCreatedUs, this.nowUs);

      if (node.gossipUrgent.includes(hash)) {
        // Was urgent — send unicast epidemic to peers
        const blacklist = new Set<number>();
        for (let i = 0; i < GOSSIP_OUTDEGREE; i++) {
          const peer = this.randomEligiblePeer(node, blacklist);
          if (!peer) break;
          blacklist.add(peer.nodeId);
          const dhash = gossipDedupHash(topic.hash, topic.evictions, lage);
          const dedup = this.dedupMatchOrLru(node, dhash);
          if (this.dedupIsFresh(dedup, dhash)) {
            this.sendUnicast(
              node, peer.nodeId, topic.hash,
              topic.evictions, lage, topic.name, GOSSIP_TTL, "unicast", pushLog,
            );
            dedup.hash = dhash;
            dedup.lastSeenUs = this.nowUs;
          }
        }
      } else {
        // Normal broadcast
        this.sendBroadcast(node, topic.hash, topic.evictions, lage, topic.name, pushLog);
        const dhash = gossipDedupHash(topic.hash, topic.evictions, lage);
        const dedup = this.dedupMatchOrLru(node, dhash);
        dedup.hash = dhash;
        dedup.lastSeenUs = this.nowUs;
      }
    }

    // Drain urgent queue
    this.drainUrgent(node, pushLog);

    // Schedule next broadcast tick
    node.nextBroadcastUs = this.nowUs + this.ditheredPeriod();
    this.pushEvent(node.nextBroadcastUs, "BROADCAST_TICK", { node_id: node.nodeId });
  }

  private drainUrgent(node: Node, pushLog: (r: EventRecord) => void): void {
    while (node.gossipUrgent.length > 0) {
      const th = node.gossipUrgent.shift()!;
      if (!node.topics.has(th)) continue;
      const topic = node.topics.get(th)!;
      const lage = topicLage(topic.tsCreatedUs, this.nowUs);
      const dhash = gossipDedupHash(topic.hash, topic.evictions, lage);
      const dedup = this.dedupMatchOrLru(node, dhash);
      if (!this.dedupIsFresh(dedup, dhash)) continue;
      const blacklist = new Set<number>();
      let sent = false;
      for (let i = 0; i < GOSSIP_OUTDEGREE; i++) {
        const peer = this.randomEligiblePeer(node, blacklist);
        if (!peer) break;
        blacklist.add(peer.nodeId);
        this.sendUnicast(
          node, peer.nodeId, topic.hash,
          topic.evictions, lage, topic.name, GOSSIP_TTL, "unicast", pushLog,
        );
        sent = true;
      }
      if (sent) {
        dedup.hash = dhash;
        dedup.lastSeenUs = this.nowUs;
      }
    }
  }

  // -- message arrival handler --

  private handleMsgArrive(payload: Record<string, unknown>, pushLog: (r: EventRecord) => void): void {
    const dstId = payload["dst"] as number;
    const node = this.nodes.get(dstId);
    if (!node || !node.online) return;

    const srcId = payload["src"] as number;
    const hash = payload["topic_hash"] as bigint;
    const evictions = payload["evictions"] as number;
    const lage = payload["lage"] as number;
    const name = payload["name"] as string;
    const ttl = payload["ttl"] as number;

    // Update peer set
    this.peerUpdate(node, srcId);

    // Dedup check
    const dhash = gossipDedupHash(hash, evictions, lage);
    const dedup = this.dedupMatchOrLru(node, dhash);
    const shouldForward = this.dedupIsFresh(dedup, dhash) && (ttl > 0);
    dedup.hash = dhash;
    dedup.lastSeenUs = this.nowUs;

    const mine = node.topics.get(hash);
    if (mine !== undefined) {
      // Known topic — check for divergence
      const localWon = this.onGossipKnownTopic(node, mine, evictions, lage, pushLog);
      if (shouldForward && !localWon) {
        this.epidemicForward(
          node, srcId, hash, mine.evictions, topicLage(mine.tsCreatedUs, this.nowUs), name, ttl, pushLog,
        );
      }
    } else {
      // Unknown topic — check for subject-ID collision
      const localWon = this.onGossipUnknownTopic(node, hash, evictions, lage, pushLog);
      if (!localWon && name) {
        const newTopic: Topic = {
          name, hash, evictions, tsCreatedUs: this.tsFromLage(lage),
        };
        nodeAddTopic(node, newTopic);
        pushLog({
          timeUs: this.nowUs, event: "learned", src: srcId, dst: dstId,
          topicHash: hash, details: { name, evictions },
        });
      }
      if (shouldForward && !localWon) {
        this.epidemicForward(node, srcId, hash, evictions, lage, name, ttl, pushLog);
      }
    }
  }

  private tsFromLage(lage: number): number {
    if (lage <= LAGE_MIN) return this.nowUs;
    return this.nowUs - Math.pow(2, lage) * 1_000_000;
  }

  // -- on_gossip_known_topic --

  private onGossipKnownTopic(
    node: Node, mine: Topic, evictions: number, lage: number,
    pushLog: (r: EventRecord) => void,
  ): boolean {
    const mineLage = topicLage(mine.tsCreatedUs, this.nowUs);
    if (mine.evictions !== evictions) {
      const win = (mineLage > lage) || (mineLage === lage && mine.evictions > evictions);
      pushLog({
        timeUs: this.nowUs, event: "conflict", src: node.nodeId, dst: null,
        topicHash: mine.hash,
        details: {
          type: "divergence", local_won: win,
          local_evictions: mine.evictions, remote_evictions: evictions,
          local_lage: mineLage, remote_lage: lage,
        },
      });
      if (win) {
        this.scheduleUrgent(node, mine.hash);
      } else {
        mine.evictions = evictions;
        mine.tsCreatedUs = Math.min(mine.tsCreatedUs, this.tsFromLage(lage));
        pushLog({
          timeUs: this.nowUs, event: "resolved", src: node.nodeId, dst: null,
          topicHash: mine.hash,
          details: { accepted_evictions: evictions, new_sid: topicSubjectId(mine) },
        });
      }
      return win;
    } else {
      mine.tsCreatedUs = Math.min(mine.tsCreatedUs, this.tsFromLage(lage));
      return false;
    }
  }

  // -- on_gossip_unknown_topic --

  private onGossipUnknownTopic(
    node: Node, remoteHash: bigint, evictions: number, lage: number,
    pushLog: (r: EventRecord) => void,
  ): boolean {
    const remoteSid = subjectId(remoteHash, evictions, SUBJECT_ID_MODULUS);
    const mine = nodeFindBySubjectId(node, remoteSid);
    if (!mine) return false;

    const mineLage = topicLage(mine.tsCreatedUs, this.nowUs);
    const win = leftWins(mineLage, mine.hash, lage, remoteHash);
    pushLog({
      timeUs: this.nowUs, event: "conflict", src: node.nodeId, dst: null,
      topicHash: mine.hash,
      details: {
        type: "collision", local_won: win,
        local_sid: topicSubjectId(mine), remote_hash: remoteHash.toString(16),
        remote_evictions: evictions,
      },
    });
    if (win) {
      this.scheduleUrgent(node, mine.hash);
    } else {
      mine.evictions += 1;
      pushLog({
        timeUs: this.nowUs, event: "resolved", src: node.nodeId, dst: null,
        topicHash: mine.hash,
        details: { new_evictions: mine.evictions, new_sid: topicSubjectId(mine) },
      });
    }
    return win;
  }

  private scheduleUrgent(node: Node, hash: bigint): void {
    if (!node.gossipUrgent.includes(hash)) {
      node.gossipUrgent.push(hash);
    }
  }

  // -- node join --

  private handleNodeJoin(nodeId: number, pushLog: (r: EventRecord) => void): void {
    const node = this.nodes.get(nodeId);
    if (!node) return;
    node.online = true;
    node.nextBroadcastUs = this.nowUs + this.rng.randint(0, GOSSIP_PERIOD);
    this.pushEvent(node.nextBroadcastUs, "BROADCAST_TICK", { node_id: nodeId });
    pushLog({
      timeUs: this.nowUs, event: "join", src: nodeId, dst: null, topicHash: 0n, details: {},
    });
  }

  // -- snapshots --

  private snapNode(node: Node): NodeSnapshot {
    const topics: TopicSnap[] = [];
    const sorted = [...node.topics.values()].sort((a, b) => a.name.localeCompare(b.name));
    for (const t of sorted) {
      topics.push({
        name: t.name, hash: t.hash, evictions: t.evictions,
        subjectId: topicSubjectId(t),
      });
    }
    const peers: (PeerSnap | null)[] = node.peers.map(
      p => p ? { nodeId: p.nodeId, lastSeenUs: p.lastSeenUs } : null,
    );
    return {
      nodeId: node.nodeId,
      online: node.online,
      topics,
      peers,
      gossipQueueFront: node.gossipQueue.length > 0 ? node.gossipQueue[0] : null,
      gossipUrgentFront: node.gossipUrgent.length > 0 ? node.gossipUrgent[0] : null,
      nextBroadcastUs: node.nextBroadcastUs,
      partitionSet: node.partitionSet,
    };
  }

  private topicSidMap(node: Node): Map<bigint, number> {
    const m = new Map<bigint, number>();
    for (const [h, t] of node.topics) {
      m.set(h, topicSubjectId(t));
    }
    return m;
  }
}
