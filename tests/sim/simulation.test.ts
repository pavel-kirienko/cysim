import { describe, it, expect } from "vitest";
import { Simulation, subjectId } from "../../src/sim.js";
import type { EventRecord, NetworkConfig } from "../../src/types.js";
import {
  DEFAULT_SHARD_COUNT,
  DEFAULT_GOSSIP_PERIOD,
  DEFAULT_GOSSIP_BROADCAST_RATIO,
  DEFAULT_GOSSIP_URGENT_DELAY,
  GOSSIP_PERIOD_DITHER_RATIO,
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
    delay: overrides.delay ?? [0, 0],
    lossProbability: overrides.lossProbability ?? 0,
    protocol,
  };
}

function makeSim(seed = 42, net = makeNet()): Simulation {
  return new Simulation(net, seed);
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

    it("node starts offline and joins after stepping", () => {
      const sim = makeSim();
      const n = sim.addNode();
      expect(n.online).toBe(false);
      sim.stepUntil(1);
      expect(n.online).toBe(true);
    });
  });

  describe("topic scheduling", () => {
    it("does not schedule gossip before first local topic", () => {
      const sim = makeSim();
      sim.addNode(0);
      sim.stepUntil(1);
      sim.stepUntil(30_000_000);
      const events = sim.stepUntil(sim.nowUs);
      expect(events.filter((e) => e.event === "broadcast" || e.event === "shard").length).toBe(0);
    });

    it("topic creation schedules urgent gossip in [0, urgent_delay)", () => {
      const sim = makeSim(
        123,
        makeNet({
          protocol: { gossipUrgentDelay: 1 },
        }),
      );
      sim.addNode(0);
      sim.stepUntil(1);
      const t0 = sim.nowUs;
      const topic = sim.addTopicToNode(0, "topic/a")!;
      const pending = sim.nodes.get(0)!.pendingUrgentByHash.get(topic.hash);
      expect(pending).toBeDefined();
      expect(pending!.deadlineUs).toBeGreaterThanOrEqual(t0);
      expect(pending!.deadlineUs).toBeLessThan(t0 + 1_000_000);
    });

    it("initial periodic emissions are broadcast before shard cadence begins", () => {
      const sim = makeSim(
        7,
        makeNet({
          protocol: {
            gossipPeriod: 0.000001,
            gossipBroadcastRatio: 10,
          },
        }),
      );
      sim.addNode(0);
      sim.stepUntil(1);
      const topic = sim.addTopicToNode(0, "topic/x#1")!;
      const node = sim.nodes.get(0)!;
      node.pendingUrgentByHash.clear();
      const schedule = node.topicScheduleByHash.get(topic.hash)!;
      schedule.nextGossipUs = sim.nowUs;
      schedule.gossipCounter = 0;
      sim.setPartition(0, "A");

      const events = sim.stepUntil(sim.nowUs + 30);
      const sends = events.filter((e) => e.src === 0 && (e.event === "broadcast" || e.event === "shard"));
      expect(sends.length).toBeGreaterThanOrEqual(12);
      expect(sends[0].event).toBe("broadcast");
      expect(sends[9].event).toBe("broadcast");
      expect(sends[10].event).toBe("broadcast");
      expect(sends[11].event).toBe("shard");
    });

    it("broadcast cadence follows counter rule after initial burst", () => {
      const sim = makeSim(
        11,
        makeNet({
          protocol: {
            gossipPeriod: 0.000001,
            gossipBroadcastRatio: 10,
          },
        }),
      );
      sim.addNode(0);
      sim.stepUntil(1);
      const topic = sim.addTopicToNode(0, "topic/x#2")!;
      const node = sim.nodes.get(0)!;
      node.pendingUrgentByHash.clear();
      const schedule = node.topicScheduleByHash.get(topic.hash)!;
      schedule.nextGossipUs = sim.nowUs;
      schedule.gossipCounter = 0;
      sim.setPartition(0, "A");

      const events = sim.stepUntil(sim.nowUs + 60);
      const sends = events.filter((e) => e.src === 0 && (e.event === "broadcast" || e.event === "shard"));
      expect(sends.length).toBeGreaterThanOrEqual(21);
      expect(events.some((e) => e.event === "unicast" || e.event === "forward" || e.event === "periodic_unicast")).toBe(
        false,
      );
      expect(sends[0].event).toBe("broadcast");
      expect(sends[9].event).toBe("broadcast");
      expect(sends[10].event).toBe("broadcast");
      expect(sends[11].event).toBe("shard");
      expect(sends[20].event).toBe("broadcast");
    });

    it("known-topic receive applies duplicate suppression [period+dither, period*3)", () => {
      const sim = makeSim(
        17,
        makeNet({
          protocol: {
            gossipPeriod: 5,
            gossipUrgentDelay: 0,
          },
        }),
      );
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);
      const topic = sim.addTopicToNode(0, "topic/shared")!;
      sim.addTopicToNode(1, "topic/shared");

      // Prevent node 1 from self-sending first; we want it to reschedule on receive.
      sim.nodes.get(1)!.topicScheduleByHash.get(topic.hash)!.nextGossipUs = Number.MAX_SAFE_INTEGER;
      sim.nodes.get(1)!.pendingUrgentByHash.delete(topic.hash);

      const events = sim.stepUntil(sim.nowUs + 10);
      const received = events.find((e) => e.event === "received" && e.src === 1 && e.topicHash === topic.hash);
      expect(received).toBeDefined();

      const n1Schedule = sim.nodes.get(1)!.topicScheduleByHash.get(topic.hash)!;
      const periodUs = Math.round(sim.net.protocol.gossipPeriod * 1_000_000);
      const ditherUs = Math.floor(periodUs / GOSSIP_PERIOD_DITHER_RATIO);
      expect(n1Schedule.nextGossipUs).toBeGreaterThanOrEqual(received!.timeUs + periodUs + ditherUs);
      expect(n1Schedule.nextGossipUs).toBeLessThan(received!.timeUs + periodUs * 3);
    });
  });

  describe("shard transport", () => {
    it("shard send targets listening nodes", () => {
      const sim = makeSim(
        19,
        makeNet({
          protocol: {
            gossipPeriod: 0.000001,
            gossipUrgentDelay: 0,
          },
        }),
      );
      sim.addNode(0);
      sim.addNode(1);
      sim.addNode(2);
      sim.stepUntil(1);
      const sender = sim.addTopicToNode(0, "topic/sender#1")!;
      sim.addTopicToNode(2, "topic/listener#7c1");
      const node0 = sim.nodes.get(0)!;
      node0.pendingUrgentByHash.clear();
      const schedule = node0.topicScheduleByHash.get(sender.hash)!;
      schedule.nextGossipUs = sim.nowUs;
      schedule.gossipCounter = sim.net.protocol.gossipBroadcastRatio + 1;
      sim.setPartition(0, "A");

      const events = sim.stepUntil(sim.nowUs + 5);
      const shard = events.find((e) => e.event === "shard" && e.src === 0);
      expect(shard).toBeDefined();
      expect(shard!.details.listeners as number[]).toEqual([2]);
    });

    it("shard send with no listeners is still logged", () => {
      const sim = makeSim(
        23,
        makeNet({
          protocol: {
            gossipPeriod: 0.000001,
            gossipUrgentDelay: 0,
          },
        }),
      );
      sim.addNode(0);
      sim.stepUntil(1);
      const sender = sim.addTopicToNode(0, "topic/sender#3")!;
      const node0 = sim.nodes.get(0)!;
      node0.pendingUrgentByHash.clear();
      const schedule = node0.topicScheduleByHash.get(sender.hash)!;
      schedule.nextGossipUs = sim.nowUs;
      schedule.gossipCounter = sim.net.protocol.gossipBroadcastRatio + 1;
      sim.setPartition(0, "A");

      const events = sim.stepUntil(sim.nowUs + 5);
      const shard = events.find((e) => e.event === "shard" && e.src === 0);
      expect(shard).toBeDefined();
      expect((shard!.details.listeners as number[]).length).toBe(0);
    });
  });

  describe("urgent repair scheduling", () => {
    it("known-topic local-win divergence schedules urgent broadcast gossip", () => {
      const sim = makeSim(29);
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);

      const local = sim.addTopicToNode(0, "topic/divergence", undefined, 3, 6)!;
      sim.nodes.get(0)!.pendingUrgentByHash.clear();
      const events = invokeMsgArrive(sim, {
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

      const conflict = events.find(
        (e) =>
          e.event === "conflict" &&
          (e.details as Record<string, unknown>)["type"] === "divergence" &&
          (e.details as Record<string, unknown>)["local_won"] === true,
      );
      expect(conflict).toBeDefined();
      const pending = sim.nodes.get(0)!.pendingUrgentByHash.get(local.hash);
      expect(pending).toBeDefined();
      expect(pending!.deadlineUs).toBeGreaterThanOrEqual(sim.nowUs);
      expect(pending!.deadlineUs).toBeLessThan(sim.nowUs + Math.round(DEFAULT_GOSSIP_URGENT_DELAY * 1_000_000));
    });

    it("unknown-topic local-win collision schedules urgent broadcast gossip", () => {
      const sim = makeSim(31);
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);

      const sid = 900;
      const local = sim.addTopicToNode(0, undefined, sid, 0, 6)!;
      const remote = sim.addTopicToNode(1, undefined, sid, 0, 0)!;
      expect(local.hash).not.toBe(remote.hash);
      sim.nodes.get(0)!.pendingUrgentByHash.clear();

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

      const pending = sim.nodes.get(0)!.pendingUrgentByHash.get(local.hash);
      expect(pending).toBeDefined();
    });

    it("known-topic local-loss with local collision schedules urgent broadcast for displaced local topic", () => {
      const sim = makeSim(37);
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);

      const mine = sim.addTopicToNode(0, "topic/known-loss", undefined, 0, 0)!;
      const collisionSid = subjectId(mine.hash, 1, SUBJECT_ID_MODULUS);
      const localOther = sim.addTopicToNode(0, undefined, collisionSid, 0, 0)!;
      sim.nodes.get(0)!.pendingUrgentByHash.clear();

      invokeMsgArrive(sim, {
        src: 1,
        dst: 0,
        topic_hash: mine.hash,
        evictions: 1,
        lage: 6,
        name: mine.name,
        msg_type: "shard",
        shard_index: 0,
        send_time_us: sim.nowUs,
      });

      const pending = sim.nodes.get(0)!.pendingUrgentByHash.get(localOther.hash);
      expect(pending).toBeDefined();
    });

    it("unknown-topic local-loss schedules urgent broadcast gossip", () => {
      const sim = makeSim(41);
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);

      const sid = 902;
      const local = sim.addTopicToNode(0, undefined, sid, 0, 0)!;
      const remote = sim.addTopicToNode(1, undefined, sid, 0, 6)!;
      expect(local.hash).not.toBe(remote.hash);
      sim.nodes.get(0)!.pendingUrgentByHash.clear();

      invokeMsgArrive(sim, {
        src: 1,
        dst: 0,
        topic_hash: remote.hash,
        evictions: remote.evictions,
        lage: 6,
        name: remote.name,
        msg_type: "shard",
        shard_index: 0,
        send_time_us: sim.nowUs,
      });

      const pending = sim.nodes.get(0)!.pendingUrgentByHash.get(local.hash);
      expect(pending).toBeDefined();
    });

    it("pending urgent is not canceled by stale-lage gossip", () => {
      const sim = makeSim(43);
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);

      const local = sim.addTopicToNode(0, "topic/stale-lage", undefined, 0, 3)!;
      sim.nodes.get(0)!.pendingUrgentByHash.set(local.hash, {
        deadlineUs: sim.nowUs + 10_000,
      });

      invokeMsgArrive(sim, {
        src: 1,
        dst: 0,
        topic_hash: local.hash,
        evictions: 0,
        lage: 1,
        name: local.name,
        msg_type: "shard",
        shard_index: 0,
        send_time_us: sim.nowUs,
      });

      expect(sim.nodes.get(0)!.pendingUrgentByHash.has(local.hash)).toBe(true);
    });

    it("pending urgent cancellation follows divergence arbitration", () => {
      const sim = makeSim(47);
      sim.addNode(0);
      sim.addNode(1);
      sim.stepUntil(1);

      const local = sim.addTopicToNode(0, "topic/cancel-arb", undefined, 1, 3)!;
      sim.nodes.get(0)!.pendingUrgentByHash.set(local.hash, {
        deadlineUs: sim.nowUs + 10_000,
      });

      // Same lage, higher remote evictions: local loses divergence arbitration, pending is canceled.
      invokeMsgArrive(sim, {
        src: 1,
        dst: 0,
        topic_hash: local.hash,
        evictions: 2,
        lage: 3,
        name: local.name,
        msg_type: "shard",
        shard_index: 0,
        send_time_us: sim.nowUs,
      });
      expect(sim.nodes.get(0)!.pendingUrgentByHash.has(local.hash)).toBe(false);

      sim.nodes.get(0)!.pendingUrgentByHash.set(local.hash, {
        deadlineUs: sim.nowUs + 10_000,
      });

      // Same lage, lower remote evictions: local wins divergence arbitration, pending is retained.
      invokeMsgArrive(sim, {
        src: 1,
        dst: 0,
        topic_hash: local.hash,
        evictions: 1,
        lage: 3,
        name: local.name,
        msg_type: "shard",
        shard_index: 0,
        send_time_us: sim.nowUs,
      });
      expect(sim.nodes.get(0)!.pendingUrgentByHash.has(local.hash)).toBe(true);
    });
  });

  describe("snapshot/state", () => {
    it("snapshot exposes shard IDs and next topic", () => {
      const sim = makeSim();
      sim.addNode(0);
      sim.stepUntil(1);
      const topic = sim.addTopicToNode(0, "topic/a#11")!;
      const snap = sim.snapshot().get(0)!;
      expect(snap.shardIds.length).toBe(1);
      expect(snap.nextTopicHash).toBe(topic.hash);
      expect(snap.pendingUrgentCount).toBe(1);
    });

    it("saveState/loadState round-trips", () => {
      const sim = makeSim();
      sim.addNode(0);
      sim.stepUntil(1);
      sim.addTopicToNode(0, "topic/a");
      sim.stepUntil(5_000_000);
      const state = sim.saveState();
      const sim2 = makeSim();
      sim2.loadState(state);
      expect(sim2.nowUs).toBe(sim.nowUs);
      expect(sim2.nodes.size).toBe(sim.nodes.size);
      expect(sim2.snapshot().get(0)?.topics.length).toBe(1);
    });
  });
});
