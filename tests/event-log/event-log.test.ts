import { describe, it, expect } from "vitest";
import { EventLog, mapCode } from "../../src/event-log.js";
import type { EventRecord } from "../../src/types.js";

function rec(event: string, src = 0, dst: number | null = null, topicHash = 0n, details: Record<string, unknown> = {}): EventRecord {
  return { timeUs: 1000, event, src, dst, topicHash, details };
}

describe("mapCode", () => {
  it("maps all event types correctly", () => {
    expect(mapCode(rec("broadcast"))).toBe("GB");
    expect(mapCode(rec("unicast"))).toBe("GU");
    expect(mapCode(rec("forward"))).toBe("GF");
    expect(mapCode(rec("received"))).toBe("GR");
    expect(mapCode(rec("gossip_xterminated"))).toBe("GX");
    expect(mapCode(rec("join"))).toBe("NN");
    expect(mapCode(rec("topic_new"))).toBe("TN");
    expect(mapCode(rec("topic_expunged"))).toBe("TX");
    expect(mapCode(rec("node_expunged"))).toBe("NX");
    expect(mapCode(rec("resolved"))).toBe("CR");
  });

  it("conflict subtypes: collision -> TC, divergence -> TD", () => {
    expect(mapCode(rec("conflict", 0, null, 0n, { type: "collision" }))).toBe("TC");
    expect(mapCode(rec("conflict", 0, null, 0n, { type: "divergence" }))).toBe("TD");
  });
});

describe("EventLog.ingest", () => {
  it("assigns incrementing IDs", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast")], 0);
    expect(log.events[0].id).toBe(0);
    expect(log.events[1].id).toBe(1);
  });

  it("sets correct fields", () => {
    const log = new EventLog();
    const r = rec("broadcast", 5, null, 123n);
    log.ingest([r], 3);
    const ev = log.events[0];
    expect(ev.code).toBe("GB");
    expect(ev.nodeId).toBe(5);
    expect(ev.topicHash).toBe(123n);
    expect(ev.historyIndex).toBe(3);
  });

  it("correlates send and receive events", () => {
    const log = new EventLog();
    const sendRec: EventRecord = {
      timeUs: 1000, event: "broadcast", src: 0, dst: null, topicHash: 42n, details: {},
    };
    const recvRec: EventRecord = {
      timeUs: 2000, event: "received", src: 1, dst: 0, topicHash: 42n,
      details: { originSrc: 0, sendTimeUs: 1000 },
    };
    log.ingest([sendRec, recvRec], 0);
    const sendEv = log.events[0];
    const recvEv = log.events[1];
    expect(recvEv.sendId).toBe(sendEv.id);
    expect(sendEv.receiveIds).toContain(recvEv.id);
  });

  it("multiple receives correlate to one send", () => {
    const log = new EventLog();
    const sendRec: EventRecord = {
      timeUs: 1000, event: "broadcast", src: 0, dst: null, topicHash: 42n, details: {},
    };
    const recv1: EventRecord = {
      timeUs: 2000, event: "received", src: 1, dst: 0, topicHash: 42n,
      details: { originSrc: 0, sendTimeUs: 1000 },
    };
    const recv2: EventRecord = {
      timeUs: 2500, event: "received", src: 2, dst: 0, topicHash: 42n,
      details: { originSrc: 0, sendTimeUs: 1000 },
    };
    log.ingest([sendRec, recv1, recv2], 0);
    expect(log.events[0].receiveIds.length).toBe(2);
  });

  it("no correlation for non-matching keys", () => {
    const log = new EventLog();
    const sendRec: EventRecord = {
      timeUs: 1000, event: "broadcast", src: 0, dst: null, topicHash: 42n, details: {},
    };
    const recvRec: EventRecord = {
      timeUs: 2000, event: "received", src: 1, dst: 0, topicHash: 99n,
      details: { originSrc: 0, sendTimeUs: 1000 },
    };
    log.ingest([sendRec, recvRec], 0);
    expect(log.events[1].sendId).toBeNull();
  });
});

describe("EventLog.truncateAfter", () => {
  it("removes events and cleans references", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast")], 0);
    log.ingest([rec("join"), rec("broadcast")], 1);
    expect(log.events.length).toBe(4);
    log.truncateAfter(0);
    expect(log.events.length).toBe(2);
    expect(log.events.every(e => e.historyIndex <= 0)).toBe(true);
  });

  it("resets nextId correctly", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast")], 0);
    log.ingest([rec("join")], 1);
    log.truncateAfter(0);
    // Next ingest should continue from the right ID
    log.ingest([rec("forward")], 1);
    expect(log.events[log.events.length - 1].id).toBe(2);
  });

  it("cleans sendId references on truncated receives", () => {
    const log = new EventLog();
    const sendRec: EventRecord = {
      timeUs: 1000, event: "broadcast", src: 0, dst: null, topicHash: 42n, details: {},
    };
    log.ingest([sendRec], 0);
    const recvRec: EventRecord = {
      timeUs: 2000, event: "received", src: 1, dst: 0, topicHash: 42n,
      details: { originSrc: 0, sendTimeUs: 1000 },
    };
    log.ingest([recvRec], 1);
    expect(log.events[0].receiveIds.length).toBe(1);
    log.truncateAfter(0);
    expect(log.events[0].receiveIds.length).toBe(0);
  });
});

describe("EventLog.clear", () => {
  it("empties everything", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast")], 0);
    log.clear();
    expect(log.events.length).toBe(0);
    // New ingest starts at id 0
    log.ingest([rec("join")], 0);
    expect(log.events[0].id).toBe(0);
  });
});
