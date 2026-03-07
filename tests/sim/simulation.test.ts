import { describe, it, expect } from "vitest";
import { Simulation } from "../../src/sim.js";
import type { EventRecord, NetworkConfig } from "../../src/types.js";
import { GOSSIP_PERIOD, GOSSIP_PERIODIC_UNICAST_PERIOD, GOSSIP_PERIODIC_UNICAST_TTL } from "../../src/constants.js";

const NET: NetworkConfig = { delayUs: [1000, 5000], lossProbability: 0, periodicUnicastEnabled: true };

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

    it("logs GX for periodic unicast dropped due to TTL=0", () => {
      const sim = makeSim();
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);
      const events = invokeMsgArrive(sim, {
        src: 1,
        dst: 0,
        topic_hash: 0x123456789abdn,
        evictions: 0,
        lage: 0,
        name: "topic/p",
        ttl: 0,
        msg_type: "periodic_unicast",
        send_time_us: sim.nowUs,
      });
      const gx = events.find(e => e.event === "gossip_xterminated");
      expect(gx).toBeDefined();
      expect(gx!.details["drop_reason"]).toBe("ttl");
    });
  });

  describe("consensus-forwarding semantics", () => {
    it("known-topic local-loss suppresses epidemic forwarding", () => {
      const sim = makeSim(7);
      sim.addNode(0);
      sim.addNode(1);
      sim.addNode(2);
      sim.stepUntil(1);

      const local = sim.addTopicToNode(0, "topic/known-loss", undefined, 0, 0)!;
      sim.nodes.get(0)!.peers[0] = { nodeId: 2, lastSeenUs: sim.nowUs };

      const events = invokeMsgArrive(sim, {
        src: 1,
        dst: 0,
        topic_hash: local.hash,
        evictions: 1,
        lage: 0,
        name: local.name,
        ttl: 3,
        msg_type: "forward",
        send_time_us: sim.nowUs,
      });

      expect(
        events.some(
          e =>
            e.event === "conflict" &&
            (e.details as Record<string, unknown>)["type"] === "divergence" &&
            (e.details as Record<string, unknown>)["local_won"] === false,
        ),
      ).toBe(true);
      expect(events.some(e => e.event === "forward" && e.src === 0)).toBe(false);
      expect(events.some(e => e.event === "gossip_xterminated")).toBe(false);
    });

    it("unknown-topic local-loss still forwards epidemic gossip", () => {
      const sim = makeSim(9);
      sim.addNode(0);
      sim.addNode(1);
      sim.addNode(2);
      sim.stepUntil(1);

      const sid = 9000;
      const local = sim.addTopicToNode(0, undefined, sid, 0, 0)!;
      const remote = sim.addTopicToNode(1, undefined, sid, 0, 6)!;
      expect(local.hash).not.toBe(remote.hash);

      sim.nodes.get(0)!.peers[0] = { nodeId: 2, lastSeenUs: sim.nowUs };
      const events = invokeMsgArrive(sim, {
        src: 1,
        dst: 0,
        topic_hash: remote.hash,
        evictions: remote.evictions,
        lage: 6,
        name: remote.name,
        ttl: 3,
        msg_type: "forward",
        send_time_us: sim.nowUs,
      });

      expect(
        events.some(
          e =>
            e.event === "conflict" &&
            (e.details as Record<string, unknown>)["type"] === "collision" &&
            (e.details as Record<string, unknown>)["local_won"] === false,
        ),
      ).toBe(true);
      expect(
        events.some(e => e.event === "forward" && e.src === 0 && e.dst === 2 && e.topicHash === remote.hash),
      ).toBe(true);
    });
  });

  describe("urgent gossip scheduling", () => {
    it("re-scheduling an already urgent topic does not perturb FIFO", () => {
      const sim = makeSim(11);
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);

      const a = sim.addTopicToNode(0, "topic/a", undefined, 0, 0)!;
      const b = sim.addTopicToNode(0, "topic/b", undefined, 0, 0)!;
      const node0 = sim.nodes.get(0)!;
      node0.gossipUrgent.length = 0;
      node0.gossipQueue.length = 0;
      node0.gossipNextUs = sim.nowUs + 1_000_000;
      node0.peers[0] = { nodeId: 1, lastSeenUs: sim.nowUs };

      const out: EventRecord[] = [];
      (sim as any).scheduleGossipUrgent(node0, a.hash);
      (sim as any).scheduleGossipUrgent(node0, b.hash);
      (sim as any).scheduleGossipUrgent(node0, a.hash);
      (sim as any).gossipPoll(node0, (r: EventRecord) => out.push(r));
      (sim as any).gossipPoll(node0, (r: EventRecord) => out.push(r));

      const unicasts = out.filter(e => e.event === "unicast" && e.src === 0);
      expect(unicasts.length).toBe(2);
      expect(unicasts[0].topicHash).toBe(a.hash);
      expect(unicasts[1].topicHash).toBe(b.hash);
    });

    it("own urgent gossips bypass dedup freshness checks", () => {
      const sim = makeSim(13);
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);

      const topic = sim.addTopicToNode(0, "topic/repair", undefined, 0, 0)!;
      const node0 = sim.nodes.get(0)!;
      node0.gossipUrgent.length = 0;
      node0.gossipQueue.length = 0;
      node0.gossipNextUs = sim.nowUs + 1_000_000;
      node0.peers[0] = { nodeId: 1, lastSeenUs: sim.nowUs };

      const out: EventRecord[] = [];
      (sim as any).scheduleGossipUrgent(node0, topic.hash);
      (sim as any).gossipPoll(node0, (r: EventRecord) => out.push(r));
      (sim as any).scheduleGossipUrgent(node0, topic.hash);
      (sim as any).gossipPoll(node0, (r: EventRecord) => out.push(r));

      const unicasts = out.filter(e => e.event === "unicast" && e.src === 0 && e.topicHash === topic.hash);
      expect(unicasts.length).toBe(2);
    });
  });

  describe("periodic unicast scheduling", () => {
    it("activates only when a peer exists and first send is dithered around one second", () => {
      const sim = makeSim(21);
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);

      const topic = sim.addTopicToNode(0, "topic/p0")!;
      const node0 = sim.nodes.get(0)!;
      node0.gossipUrgent.length = 0;
      node0.gossipPeriodicNextUs = Number.MAX_SAFE_INTEGER;
      for (let i = 0; i < node0.peers.length; i++) node0.peers[i] = null;

      (sim as any).ensurePollScheduled(node0);
      expect(node0.gossipPeriodicNextUs).toBe(Number.MAX_SAFE_INTEGER);

      node0.peers[0] = { nodeId: 1, lastSeenUs: sim.nowUs };
      (sim as any).ensurePollScheduled(node0);
      const minDelay = GOSSIP_PERIODIC_UNICAST_PERIOD - Math.floor(GOSSIP_PERIODIC_UNICAST_PERIOD / 8);
      const maxDelay = GOSSIP_PERIODIC_UNICAST_PERIOD + Math.floor(GOSSIP_PERIODIC_UNICAST_PERIOD / 8);
      expect(node0.gossipPeriodicNextUs).toBeGreaterThanOrEqual(sim.nowUs + minDelay);
      expect(node0.gossipPeriodicNextUs).toBeLessThanOrEqual(sim.nowUs + maxDelay);

      const before = sim.stepUntil(sim.nowUs + minDelay - 1);
      expect(before.some(e => e.event === "periodic_unicast" && e.src === 0)).toBe(false);

      const at = sim.stepUntil(sim.nowUs + maxDelay);
      const periodic = at.find(e => e.event === "periodic_unicast" && e.src === 0);
      expect(periodic).toBeDefined();
      expect(periodic!.topicHash).toBe(topic.hash);
      expect(periodic!.details["ttl"]).toBe(GOSSIP_PERIODIC_UNICAST_TTL);
    });

    it("does not emit periodic unicasts when disabled in network config", () => {
      const sim = new Simulation(
        { delayUs: [1000, 5000], lossProbability: 0, periodicUnicastEnabled: false },
        24,
      );
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);

      const topic = sim.addTopicToNode(0, "topic/no-periodic")!;
      const node0 = sim.nodes.get(0)!;
      node0.gossipUrgent.length = 0;
      node0.gossipNextUs = Number.MAX_SAFE_INTEGER;
      node0.gossipPeriodicNextUs = sim.nowUs;
      node0.peers[0] = { nodeId: 1, lastSeenUs: sim.nowUs };

      const out: EventRecord[] = [];
      (sim as any).gossipPoll(node0, (r: EventRecord) => out.push(r));

      expect(out.some(e => e.event === "periodic_unicast" && e.topicHash === topic.hash)).toBe(false);
      expect(node0.gossipPeriodicNextUs).toBe(Number.MAX_SAFE_INTEGER);
    });

    it("emits periodic unicast with dithered period and TTL=2", () => {
      const sim = makeSim(22);
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);

      const a = sim.addTopicToNode(0, "topic/p1")!;
      const b = sim.addTopicToNode(0, "topic/p2")!;
      const node0 = sim.nodes.get(0)!;
      node0.gossipUrgent.length = 0;
      node0.gossipNextUs = Number.MAX_SAFE_INTEGER;
      node0.gossipPeriodicNextUs = Number.MAX_SAFE_INTEGER;
      node0.peers[0] = { nodeId: 1, lastSeenUs: sim.nowUs };

      (sim as any).ensurePollScheduled(node0);
      const startUs = sim.nowUs;
      const events = sim.stepUntil(startUs + 6 * GOSSIP_PERIODIC_UNICAST_PERIOD);
      const periodic = events.filter(e => e.event === "periodic_unicast" && e.src === 0);

      expect(periodic.length).toBeGreaterThanOrEqual(5);
      expect(periodic.every(e => e.dst === 1)).toBe(true);
      expect(periodic.every(e => e.details["ttl"] === GOSSIP_PERIODIC_UNICAST_TTL)).toBe(true);
      expect(
        periodic.every(e => e.topicHash === a.hash || e.topicHash === b.hash),
      ).toBe(true);

      const minStep = GOSSIP_PERIODIC_UNICAST_PERIOD - Math.floor(GOSSIP_PERIODIC_UNICAST_PERIOD / 8);
      const maxStep = GOSSIP_PERIODIC_UNICAST_PERIOD + Math.floor(GOSSIP_PERIODIC_UNICAST_PERIOD / 8);
      for (let i = 1; i < periodic.length; i++) {
        const dt = periodic[i].timeUs - periodic[i - 1].timeUs;
        expect(dt).toBeGreaterThanOrEqual(minStep);
        expect(dt).toBeLessThanOrEqual(maxStep);
      }
    });

    it("emits broadcast, periodic, and urgent paths together when all are due", () => {
      const sim = makeSim(23);
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);

      const topic = sim.addTopicToNode(0, "topic/all-due")!;
      const node0 = sim.nodes.get(0)!;
      node0.gossipUrgent.length = 0;
      node0.peers[0] = { nodeId: 1, lastSeenUs: sim.nowUs };
      (sim as any).scheduleGossipUrgent(node0, topic.hash);
      node0.gossipNextUs = sim.nowUs;
      node0.gossipPeriodicNextUs = sim.nowUs;

      const out: EventRecord[] = [];
      (sim as any).gossipPoll(node0, (r: EventRecord) => out.push(r));

      expect(out.some(e => e.event === "broadcast" && e.src === 0)).toBe(true);
      expect(out.some(e => e.event === "periodic_unicast" && e.src === 0)).toBe(true);
      expect(out.some(e => e.event === "unicast" && e.src === 0)).toBe(true);
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
