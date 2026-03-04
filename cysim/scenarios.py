"""Preset scenario builders for the gossip simulation."""

from __future__ import annotations

from .sim import (
    GOSSIP_PERIOD,
    SUBJECT_ID_MODULUS,
    SUBJECT_ID_PINNED_MAX,
    NetworkConfig,
    Simulation,
    Topic,
)


def _find_colliding_hashes(modulus: int = SUBJECT_ID_MODULUS) -> tuple[int, int]:
    """Find two distinct hashes > PINNED_MAX that map to the same subject-ID at evictions=0."""
    # subject_id = PINNED_MAX + 1 + (hash % modulus)
    # So any two hashes with the same (hash % modulus) will collide.
    base = SUBJECT_ID_PINNED_MAX + 1_000  # arbitrary, well above pinned range
    h1 = base
    h2 = base + modulus  # same remainder mod modulus
    assert (h1 % modulus) == (h2 % modulus)
    return h1, h2


def collision(num_nodes: int = 6, net: NetworkConfig | None = None,
              seed: int = 42) -> Simulation:
    """N nodes online from t=0. Node 0 creates "temperature", node 3 creates "pressure",
    both hashing to the same subject-ID. Watch collision → epidemic repair → convergence."""
    if net is None:
        net = NetworkConfig()
    sim = Simulation(net, rng_seed=seed)

    h1, h2 = _find_colliding_hashes()

    for i in range(num_nodes):
        sim.add_node(i, online_at_us=0)

    # Both topics created at t=0 — they will have the same lage initially
    sim.add_topic_to_node(0, Topic(name="temperature", hash=h1, evictions=0, ts_created_us=0))
    sim.add_topic_to_node(3, Topic(name="pressure", hash=h2, evictions=0, ts_created_us=0))

    return sim


def divergence(num_nodes: int = 6, net: NetworkConfig | None = None,
               seed: int = 42) -> Simulation:
    """Start with a collision scenario, let it partially converge, then partition the network
    briefly so the same topic evolves different eviction counters. Heal → divergence repair."""
    if net is None:
        net = NetworkConfig()
    sim = Simulation(net, rng_seed=seed)

    h1, h2 = _find_colliding_hashes()

    for i in range(num_nodes):
        sim.add_node(i, online_at_us=0)

    sim.add_topic_to_node(0, Topic(name="temperature", hash=h1, evictions=0, ts_created_us=0))
    sim.add_topic_to_node(3, Topic(name="pressure", hash=h2, evictions=0, ts_created_us=0))

    # Phase 1: run normally for 10s to get initial collision resolved
    sim.run(until_us=10_000_000)

    # Phase 2: partition — nodes 0-2 vs 3-5, simulate by taking group B offline
    group_b = [n for nid, n in sim.nodes.items() if nid >= num_nodes // 2]
    for n in group_b:
        n.online = False

    # Manually bump evictions on a topic in group A to create divergence
    if h1 in sim.nodes[0].topics:
        sim.nodes[0].topics[h1].evictions += 1
        # Propagate to group A nodes that have the topic
        for nid in range(num_nodes // 2):
            node = sim.nodes[nid]
            if h1 in node.topics:
                node.topics[h1].evictions = sim.nodes[0].topics[h1].evictions

    # Run partitioned for 5s
    sim.run(until_us=15_000_000)

    # Phase 3: heal partition
    for n in group_b:
        n.online = True
        n.next_broadcast_us = sim.now_us + sim.rng.randint(0, GOSSIP_PERIOD)
        sim._push_event(n.next_broadcast_us, "BROADCAST_TICK", {"node_id": n.node_id})

    return sim


def join(num_nodes: int = 6, net: NetworkConfig | None = None,
         seed: int = 42) -> Simulation:
    """N-1 nodes online from t=0 with topics in consensus. Node N-1 joins at t=10s.
    Watch it learn topics via gossip broadcasts."""
    if net is None:
        net = NetworkConfig()
    sim = Simulation(net, rng_seed=seed)

    # Create nodes — last one joins late
    for i in range(num_nodes - 1):
        sim.add_node(i, online_at_us=0)
    sim.add_node(num_nodes - 1, online_at_us=10_000_000)

    # Give several topics to existing nodes
    topics_data = [
        ("temperature", 0xDEAD_BEEF_1234_5678),
        ("pressure", 0xCAFE_BABE_8765_4321),
        ("altitude", 0x1234_5678_ABCD_EF01),
    ]
    for name, h in topics_data:
        topic = Topic(name=name, hash=h, evictions=0, ts_created_us=0)
        for i in range(num_nodes - 1):
            sim.add_topic_to_node(i, Topic(name=name, hash=h, evictions=0, ts_created_us=0))

    return sim


SCENARIOS = {
    "collision": collision,
    "divergence": divergence,
    "join": join,
}
