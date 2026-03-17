import { describe, it, expect } from "vitest";
import { Simulation } from "../../src/sim.js";
import type { EventRecord, NetworkConfig } from "../../src/types.js";
import {
  DEFAULT_SHARD_COUNT,
  DEFAULT_GOSSIP_PERIOD,
  DEFAULT_GOSSIP_BROADCAST_RATIO,
  DEFAULT_GOSSIP_URGENT_DELAY,
  SUBJECT_ID_MODULUS,
} from "../../src/constants.js";

function makeNet(
  overrides: {
    delay?: [number, number];
    lossProbability?: number;
    protocol?: Partial<NetworkConfig["protocol"]>;
  } = {},
): NetworkConfig {
  const protocol = {
    subjectIdModulus: SUBJECT_ID_MODULUS,
    shardCount: DEFAULT_SHARD_COUNT,
    gossipPeriod: DEFAULT_GOSSIP_PERIOD,
    gossipBroadcastRatio: DEFAULT_GOSSIP_BROADCAST_RATIO,
    gossipUrgentDelay: DEFAULT_GOSSIP_URGENT_DELAY,
    ...overrides.protocol,
  };
  return {
    delay: overrides.delay ?? [0.001, 0.005],
    lossProbability: overrides.lossProbability ?? 0,
    protocol,
  };
}

function makeSim(seed = 42, net = makeNet()): Simulation {
  return new Simulation(net, seed);
}

function stepSeconds(sim: Simulation, seconds: number): void {
  sim.stepUntil(sim.nowUs + seconds * 1_000_000);
}

function invokeMsgArrive(sim: Simulation, payload: Record<string, unknown>): EventRecord[] {
  const out: EventRecord[] = [];
  (sim as any).handleMsgArrive(payload, (r: EventRecord) => out.push(r));
  return out;
}

describe("simulation integration", () => {
  it("two-node gossip does not create foreign topics", () => {
    const sim = makeSim();
    sim.addNode(); // 0
    sim.addNode(); // 1
    sim.stepUntil(1);
    sim.addTopicToNode(0, "topic/a");
    stepSeconds(sim, 30);
    expect(sim.nodes.get(0)!.topics.size).toBe(1);
    expect(sim.nodes.get(1)!.topics.size).toBe(0);
  });

  it("three-node convergence: unique topics on each", () => {
    const sim = makeSim();
    sim.addNode();
    sim.addNode();
    sim.addNode();
    sim.stepUntil(1);
    sim.addTopicToNode(0, "topic/a");
    sim.addTopicToNode(1, "topic/b");
    sim.addTopicToNode(2, "topic/c");
    stepSeconds(sim, 60);
    expect(sim.checkConvergence()).toBe(true);
  });

  it("SID collision resolution with target SID converges", () => {
    const sim = makeSim(123);
    sim.addNode();
    sim.addNode();
    sim.stepUntil(1);
    sim.addTopicToNode(0, undefined, 900);
    sim.addTopicToNode(1, undefined, 900);
    stepSeconds(sim, 60);
    expect(sim.checkConvergence()).toBe(true);
  });

  it("local-win divergence emits urgent broadcast gossip before periodic slot", () => {
    const sim = makeSim(
      1,
      makeNet({
        delay: [0, 0],
      }),
    );
    sim.addNode(0);
    sim.addNode(1);
    sim.stepUntil(1);

    const local = sim.addTopicToNode(0, "topic/divergence", undefined, 3, 6)!;
    sim.addTopicToNode(1, "topic/divergence", undefined, 0, 0);
    sim.nodes.get(0)!.pendingUrgentByHash.clear();
    sim.nodes.get(0)!.topicScheduleByHash.get(local.hash)!.nextGossipUs = sim.nowUs + 1_000_000;
    const localFirstPeriodicUs = sim.nodes.get(0)!.topicScheduleByHash.get(local.hash)!.nextGossipUs;

    invokeMsgArrive(sim, {
      src: 1,
      dst: 0,
      topic_hash: local.hash,
      evictions: 0,
      lage: 0,
      name: local.name,
      msg_type: "shard",
      shard_index: 0,
      send_time_us: sim.nowUs,
    });

    const events = sim.stepUntil(localFirstPeriodicUs - 1);
    const urgentBroadcast = events.find(
      (e) => e.event === "broadcast" && e.src === 0 && e.topicHash === local.hash && e.timeUs < localFirstPeriodicUs,
    );
    expect(urgentBroadcast).toBeDefined();
  });

  it("local-win collision emits urgent broadcast gossip", () => {
    const sim = makeSim(
      2,
      makeNet({
        delay: [0, 0],
      }),
    );
    sim.addNode(0);
    sim.addNode(1);
    sim.stepUntil(1);

    const sid = 900;
    const local = sim.addTopicToNode(0, undefined, sid, 0, 6)!;
    const remote = sim.addTopicToNode(1, undefined, sid, 0, 0)!;
    sim.nodes.get(0)!.pendingUrgentByHash.clear();
    sim.nodes.get(0)!.topicScheduleByHash.get(local.hash)!.nextGossipUs = sim.nowUs + 1_000_000;
    const localFirstPeriodicUs = sim.nodes.get(0)!.topicScheduleByHash.get(local.hash)!.nextGossipUs;
    expect(local.hash).not.toBe(remote.hash);

    invokeMsgArrive(sim, {
      src: 1,
      dst: 0,
      topic_hash: remote.hash,
      evictions: remote.evictions,
      lage: 0,
      name: remote.name,
      msg_type: "shard",
      shard_index: 0,
      send_time_us: sim.nowUs,
    });

    const events = sim.stepUntil(localFirstPeriodicUs - 1);
    const urgentBroadcast = events.find(
      (e) => e.event === "broadcast" && e.src === 0 && e.topicHash === local.hash && e.timeUs < localFirstPeriodicUs,
    );
    expect(urgentBroadcast).toBeDefined();
  });

  it("network partition: node in B doesn't affect partition A convergence", () => {
    const sim = makeSim();
    sim.addNode();
    sim.addNode();
    sim.addNode();
    sim.stepUntil(1);
    sim.setPartition(2, "B");
    sim.addTopicToNode(0, "topic/a");
    sim.addTopicToNode(1, "topic/b");
    sim.addTopicToNode(2, "topic/c");
    stepSeconds(sim, 60);
    expect(sim.checkConvergence()).toBe(true);
  });

  it("deterministic replay: same seed + same ops = identical snapshots", () => {
    function run() {
      const sim = makeSim(999);
      sim.addNode();
      sim.addNode();
      sim.stepUntil(1);
      sim.addTopicToNode(0, "topic/a");
      sim.addTopicToNode(1, "topic/b");
      stepSeconds(sim, 30);
      return sim.snapshot();
    }
    const snap1 = run();
    const snap2 = run();
    for (const [nid, ns1] of snap1) {
      const ns2 = snap2.get(nid)!;
      expect(ns1.online).toBe(ns2.online);
      expect(ns1.topics.length).toBe(ns2.topics.length);
      for (let i = 0; i < ns1.topics.length; i++) {
        expect(ns1.topics[i].name).toBe(ns2.topics[i].name);
        expect(ns1.topics[i].evictions).toBe(ns2.topics[i].evictions);
        expect(ns1.topics[i].subjectId).toBe(ns2.topics[i].subjectId);
      }
    }
  });

  it("node restart: keeps topics, node comes back online", () => {
    const sim = makeSim();
    sim.addNode();
    sim.stepUntil(1);
    sim.addTopicToNode(0, "topic/a");
    expect(sim.nodes.get(0)!.topics.size).toBe(1);
    sim.restartNode(0);
    expect(sim.nodes.get(0)!.topics.size).toBe(1);
    expect(sim.nodes.get(0)!.online).toBe(false);
    stepSeconds(sim, 5);
    expect(sim.nodes.get(0)!.online).toBe(true);
  });

  it("packet loss: convergence still reached with longer timeout", () => {
    const sim = makeSim(
      42,
      makeNet({
        lossProbability: 0.5,
      }),
    );
    sim.addNode();
    sim.addNode();
    sim.addNode();
    sim.stepUntil(1);
    sim.addTopicToNode(0, "topic/a");
    sim.addTopicToNode(1, "topic/b");
    sim.addTopicToNode(2, "topic/c");
    stepSeconds(sim, 120);
    expect(sim.checkConvergence()).toBe(true);
  });
});
