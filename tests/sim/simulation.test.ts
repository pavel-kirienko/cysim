import { describe, it, expect } from "vitest";
import { Simulation } from "../../src/sim.js";
import type { EventRecord, NetworkConfig } from "../../src/types.js";
import { GOSSIP_PERIOD } from "../../src/constants.js";

const NET: NetworkConfig = { delayUs: [1000, 5000], lossProbability: 0 };

function makeSim(seed = 42): Simulation {
  return new Simulation(NET, seed);
}

function invokeMsgArrive(sim: Simulation, payload: Record<string, unknown>): EventRecord[] {
  const out: EventRecord[] = [];
  (sim as any).handleMsgArrive(payload, (r: EventRecord) => out.push(r));
  return out;
}

describe("Simulation", () => {
  describe("addNode", () => {
    it("auto-increments IDs", () => {
      const sim = makeSim();
      const n0 = sim.addNode();
      const n1 = sim.addNode();
      expect(n0.nodeId).toBe(0);
      expect(n1.nodeId).toBe(1);
    });

    it("accepts explicit ID", () => {
      const sim = makeSim();
      const n = sim.addNode(10);
      expect(n.nodeId).toBe(10);
    });

    it("node starts offline", () => {
      const sim = makeSim();
      const n = sim.addNode();
      expect(n.online).toBe(false);
    });
  });

  describe("node join", () => {
    it("node goes online after stepping past join time", () => {
      const sim = makeSim();
      const n = sim.addNode();
      sim.stepUntil(1);
      expect(n.online).toBe(true);
    });

    it("gossip stays disabled after join until commencement", () => {
      const sim = makeSim();
      sim.addNode();
      sim.stepUntil(1);
      expect(sim.nodes.get(0)!.gossipNextUs).toBe(Number.MAX_SAFE_INTEGER);
      const events = sim.stepUntil(30_000_000);
      expect(events.filter(e => e.event === "broadcast").length).toBe(0);
    });
  });

  describe("destroyNode", () => {
    it("removes node and generates pending event", () => {
      const sim = makeSim();
      sim.addNode();
      sim.destroyNode(0);
      expect(sim.nodes.has(0)).toBe(false);
      const events = sim.drainPendingEvents();
      expect(events.some(e => e.event === "node_expunged")).toBe(true);
    });
  });

  describe("addTopicToNode", () => {
    it("returns topic with correct fields", () => {
      const sim = makeSim();
      sim.addNode();
      sim.stepUntil(1);
      const topic = sim.addTopicToNode(0, "my/topic");
      expect(topic).not.toBeNull();
      expect(topic!.name).toBe("my/topic");
      expect(topic!.evictions).toBe(0);
    });

    it("auto-names topics (topic/a, topic/b...)", () => {
      const sim = makeSim();
      sim.addNode();
      sim.stepUntil(1);
      const t1 = sim.addTopicToNode(0);
      const t2 = sim.addTopicToNode(0);
      expect(t1!.name).toBe("topic/a");
      expect(t2!.name).toBe("topic/b");
    });

    it("returns null for nonexistent node", () => {
      const sim = makeSim();
      expect(sim.addTopicToNode(999)).toBeNull();
    });

    it("starts gossip once a local topic is created", () => {
      const sim = makeSim();
      sim.addNode();
      sim.stepUntil(1);
      sim.addTopicToNode(0, "my/topic");
      expect(sim.nodes.get(0)!.gossipNextUs).not.toBe(Number.MAX_SAFE_INTEGER);
      const events = sim.stepUntil(sim.nowUs + 10_000_000);
      expect(events.filter(e => e.event === "broadcast").length).toBeGreaterThan(0);
    });

    it("initial gossip is dithered within period/8", () => {
      const sim = makeSim(42);
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);
      const t0 = sim.nowUs;

      sim.addTopicToNode(0, "a/topic");
      sim.addTopicToNode(1, "b/topic");

      const n0 = sim.nodes.get(0)!;
      const n1 = sim.nodes.get(1)!;
      const maxOffset = Math.floor(GOSSIP_PERIOD / 8);
      expect(n0.gossipNextUs).toBeGreaterThanOrEqual(t0);
      expect(n1.gossipNextUs).toBeGreaterThanOrEqual(t0);
      expect(n0.gossipNextUs).toBeLessThanOrEqual(t0 + maxOffset);
      expect(n1.gossipNextUs).toBeLessThanOrEqual(t0 + maxOffset);
      expect(n0.gossipNextUs).not.toBe(n1.gossipNextUs);
    });

    it("topics added before join still gossip after node joins", () => {
      const sim = makeSim();
      sim.addNode();
      sim.addTopicToNode(0, "offline/topic");
      sim.stepUntil(1);
      const events = sim.stepUntil(sim.nowUs + 10_000_000);
      expect(events.filter(e => e.event === "broadcast").length).toBeGreaterThan(0);
    });
  });

  describe("stepUntil", () => {
    it("processes NODE_JOIN and returns EventRecords", () => {
      const sim = makeSim();
      sim.addNode();
      const events = sim.stepUntil(1);
      expect(events.some(e => e.event === "join")).toBe(true);
    });

    it("advances nowUs", () => {
      const sim = makeSim();
      sim.stepUntil(5_000_000);
      expect(sim.nowUs).toBe(5_000_000);
    });
  });

  describe("gossip_xterminated", () => {
    it("logs GX for epidemic unicast dropped due to TTL=0", () => {
      const sim = makeSim();
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);
      const events = invokeMsgArrive(sim, {
        src: 1,
        dst: 0,
        topic_hash: 0x1ab2cd3ef45n,
        evictions: 0,
        lage: 0,
        name: "topic/x",
        ttl: 0,
        msg_type: "unicast",
        send_time_us: sim.nowUs,
      });
      const gx = events.find(e => e.event === "gossip_xterminated");
      expect(gx).toBeDefined();
      expect(gx!.details["drop_reason"]).toBe("ttl");
    });

    it("logs GX for epidemic unicast dropped due to dedup", () => {
      const sim = makeSim();
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);
      const payload = {
        src: 1,
        dst: 0,
        topic_hash: 0x0fedcba98765n,
        evictions: 0,
        lage: 0,
        name: "topic/y",
        ttl: 3,
        msg_type: "forward",
        send_time_us: sim.nowUs,
      };
      invokeMsgArrive(sim, payload);
      const events = invokeMsgArrive(sim, payload);
      const gx = events.find(e => e.event === "gossip_xterminated");
      expect(gx).toBeDefined();
      expect(gx!.details["drop_reason"]).toBe("dedup");
    });

    it("does not log GX for broadcast gossip", () => {
      const sim = makeSim();
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);
      const events = invokeMsgArrive(sim, {
        src: 1,
        dst: 0,
        topic_hash: 0x123456789abcn,
        evictions: 0,
        lage: 0,
        name: "topic/z",
        ttl: 0,
        msg_type: "broadcast",
        send_time_us: sim.nowUs,
      });
      expect(events.some(e => e.event === "gossip_xterminated")).toBe(false);
    });
  });

  describe("snapshot", () => {
    it("returns map with entries per node", () => {
      const sim = makeSim();
      sim.addNode();
      sim.addNode();
      sim.stepUntil(1);
      const snap = sim.snapshot();
      expect(snap.size).toBe(2);
      expect(snap.has(0)).toBe(true);
      expect(snap.has(1)).toBe(true);
    });

    it("TopicSnap has correct subjectId and lage", () => {
      const sim = makeSim();
      sim.addNode();
      sim.stepUntil(1);
      sim.addTopicToNode(0, "topic/a");
      sim.stepUntil(2_000_000);
      const snap = sim.snapshot();
      const topics = snap.get(0)!.topics;
      expect(topics.length).toBe(1);
      expect(typeof topics[0].subjectId).toBe("number");
      expect(typeof topics[0].lage).toBe("number");
    });
  });

  describe("saveState/loadState", () => {
    it("round-trips state", () => {
      const sim = makeSim();
      sim.addNode();
      sim.stepUntil(1);
      sim.addTopicToNode(0, "topic/a");
      sim.stepUntil(5_000_000);
      const state = sim.saveState();
      const sim2 = makeSim();
      sim2.loadState(state);
      expect(sim2.nowUs).toBe(sim.nowUs);
      expect(sim2.nodes.size).toBe(sim.nodes.size);
    });

    it("deep clones — mutating loaded state doesn't affect saved", () => {
      const sim = makeSim();
      sim.addNode();
      sim.stepUntil(1);
      sim.addTopicToNode(0, "topic/a");
      const state = sim.saveState();
      sim.addTopicToNode(0, "topic/b");
      const sim2 = makeSim();
      sim2.loadState(state);
      expect(sim2.nodes.get(0)!.topics.size).toBe(1);
    });
  });

  describe("drainPendingEvents", () => {
    it("returns and clears pending events", () => {
      const sim = makeSim();
      sim.addNode();
      sim.stepUntil(1);
      sim.addTopicToNode(0, "topic/a");
      const first = sim.drainPendingEvents();
      expect(first.length).toBeGreaterThan(0);
      const second = sim.drainPendingEvents();
      expect(second.length).toBe(0);
    });

    it("drainPendingEvents returns topic_new event", () => {
      const sim = makeSim();
      sim.addNode();
      sim.stepUntil(1);
      sim.addTopicToNode(0, "topic/a");
      const events = sim.drainPendingEvents();
      expect(events.filter(e => e.event === "topic_new").length).toBe(1);
    });
  });
});
