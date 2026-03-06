import { describe, it, expect } from "vitest";
import { Simulation } from "../../src/sim.js";
import type { NetworkConfig } from "../../src/types.js";

const NET: NetworkConfig = { delayUs: [1000, 5000], lossProbability: 0 };

function makeSim(seed = 42, net = NET): Simulation {
  return new Simulation(net, seed);
}

function stepSeconds(sim: Simulation, seconds: number): void {
  sim.stepUntil(sim.nowUs + seconds * 1_000_000);
}

describe("simulation integration", () => {
  it("two-node gossip: node 1 discovers topic via gossip", () => {
    const sim = makeSim();
    sim.addNode(); // 0
    sim.addNode(); // 1
    sim.stepUntil(1); // join both
    sim.addTopicToNode(0, "topic/a");
    stepSeconds(sim, 30);
    // node 1 should NOT learn it — nodes never learn foreign topics
    // But node 0 should still have it
    expect(sim.nodes.get(0)!.topics.size).toBe(1);
  });

  it("three-node convergence: unique topics on each", () => {
    const sim = makeSim();
    sim.addNode(); // 0
    sim.addNode(); // 1
    sim.addNode(); // 2
    sim.stepUntil(1);
    sim.addTopicToNode(0, "topic/a");
    sim.addTopicToNode(1, "topic/b");
    sim.addTopicToNode(2, "topic/c");
    stepSeconds(sim, 60);
    expect(sim.checkConvergence()).toBe(true);
  });

  it("SID collision resolution: force collision via targetSid", () => {
    const sim = makeSim(123);
    sim.addNode(); // 0
    sim.addNode(); // 1
    sim.stepUntil(1);
    sim.addTopicToNode(0, undefined, 9000);
    sim.addTopicToNode(1, undefined, 9000);
    stepSeconds(sim, 60);
    expect(sim.checkConvergence()).toBe(true);
  });

  it("local-win divergence emits urgent unicast gossip", () => {
    const sim = makeSim(1);
    sim.addNode(0);
    sim.addNode(1);
    sim.stepUntil(1);

    sim.addTopicToNode(0, "topic/divergence", undefined, 3, 6);
    const localFirstBroadcastUs = sim.nodes.get(0)!.gossipNextUs;
    sim.addTopicToNode(1, "topic/divergence", undefined, 0, 0);

    // Verify urgent repair happens before the local node's first scheduled broadcast slot.
    const events = sim.stepUntil(localFirstBroadcastUs - 1);
    const conflict = events.find(
      e =>
        e.event === "conflict" &&
        e.src === 0 &&
        (e.details as Record<string, unknown>)["type"] === "divergence" &&
        (e.details as Record<string, unknown>)["local_won"] === true,
    );
    expect(conflict).toBeDefined();

    const unicast = events.find(
      e =>
        e.event === "unicast" &&
        e.src === 0 &&
        e.topicHash === conflict!.topicHash &&
        e.timeUs >= conflict!.timeUs,
    );
    expect(unicast).toBeDefined();
    expect(unicast!.timeUs).toBeLessThan(localFirstBroadcastUs);
  });

  it("local-win collision emits urgent unicast gossip", () => {
    const sim = makeSim(1);
    sim.addNode(0);
    sim.addNode(1);
    sim.stepUntil(1);

    const sid = 9000;
    const local = sim.addTopicToNode(0, undefined, sid, 0, 6)!;
    const localFirstBroadcastUs = sim.nodes.get(0)!.gossipNextUs;
    const remote = sim.addTopicToNode(1, undefined, sid, 0, 0)!;
    expect(local.hash).not.toBe(remote.hash);

    // Verify urgent repair happens before the local node's first scheduled broadcast slot.
    const events = sim.stepUntil(localFirstBroadcastUs - 1);
    const conflict = events.find(
      e =>
        e.event === "conflict" &&
        e.src === 0 &&
        (e.details as Record<string, unknown>)["type"] === "collision" &&
        (e.details as Record<string, unknown>)["local_won"] === true,
    );
    expect(conflict).toBeDefined();

    const unicast = events.find(
      e =>
        e.event === "unicast" &&
        e.src === 0 &&
        e.topicHash === conflict!.topicHash &&
        e.timeUs >= conflict!.timeUs,
    );
    expect(unicast).toBeDefined();
    expect(unicast!.timeUs).toBeLessThan(localFirstBroadcastUs);
  });

  it("network partition: node in B doesn't affect partition A convergence", () => {
    const sim = makeSim();
    sim.addNode(); // 0
    sim.addNode(); // 1
    sim.addNode(); // 2
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
    // Compare serializable parts
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

  it("node restart: clears topics, node comes back online", () => {
    const sim = makeSim();
    sim.addNode();
    sim.stepUntil(1);
    sim.addTopicToNode(0, "topic/a");
    expect(sim.nodes.get(0)!.topics.size).toBe(1);
    sim.restartNode(0);
    expect(sim.nodes.get(0)!.topics.size).toBe(0);
    expect(sim.nodes.get(0)!.online).toBe(false);
    stepSeconds(sim, 5);
    expect(sim.nodes.get(0)!.online).toBe(true);
  });

  it("time travel: save checkpoint, step, load restores state", () => {
    const sim = makeSim();
    sim.addNode();
    sim.stepUntil(1);
    sim.addTopicToNode(0, "topic/a");
    stepSeconds(sim, 5);
    const state = sim.saveState();
    const savedNowUs = sim.nowUs;
    stepSeconds(sim, 30);
    expect(sim.nowUs).toBeGreaterThan(savedNowUs);
    sim.loadState(state);
    expect(sim.nowUs).toBe(savedNowUs);
    expect(sim.nodes.get(0)!.topics.size).toBe(1);
  });

  it("packet loss: convergence still reached with longer timeout", () => {
    const lossyNet: NetworkConfig = { delayUs: [1000, 5000], lossProbability: 0.5 };
    const sim = makeSim(42, lossyNet);
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

  it("10-node network: all topics converge, no SID collisions", () => {
    const sim = makeSim(7);
    for (let i = 0; i < 10; i++) sim.addNode();
    sim.stepUntil(1);
    for (let i = 0; i < 10; i++) {
      sim.addTopicToNode(i, `topic/${String.fromCharCode(97 + i)}`);
    }
    stepSeconds(sim, 120);
    expect(sim.checkConvergence()).toBe(true);
  });
});
