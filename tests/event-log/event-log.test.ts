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
    expect(mapCode(rec("periodic_unicast"))).toBe("GP");
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

  it("correlates periodic send and receive events", () => {
    const log = new EventLog();
    const sendRec: EventRecord = {
      timeUs: 1000, event: "periodic_unicast", src: 0, dst: 2, topicHash: 77n, details: {},
    };
    const recvRec: EventRecord = {
      timeUs: 2000, event: "received", src: 2, dst: 0, topicHash: 77n,
      details: { originSrc: 0, sendTimeUs: 1000 },
    };
    log.ingest([sendRec, recvRec], 0);
    const sendEv = log.events[0];
    const recvEv = log.events[1];
    expect(sendEv.code).toBe("GP");
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

// ---------------------------------------------------------------------------
// Helpers for adversarial byId tests
// ---------------------------------------------------------------------------

function recAt(event: string, timeUs: number, src = 0, dst: number | null = null, topicHash = 0n, details: Record<string, unknown> = {}): EventRecord {
  return { timeUs, event, src, dst, topicHash, details };
}

/** Verify byId map is exactly in sync with events array */
function assertByIdSync(log: EventLog): void {
  for (const e of log.events) {
    expect(log.getById(e.id)).toBe(e); // referential identity
  }
  // No extra entries: check IDs 0..maxId+5
  const maxId = log.events.length > 0 ? Math.max(...log.events.map(e => e.id)) : -1;
  const validIds = new Set(log.events.map(e => e.id));
  for (let i = 0; i <= maxId + 5; i++) {
    if (!validIds.has(i)) expect(log.getById(i)).toBeUndefined();
  }
}

// ---------------------------------------------------------------------------
// EventLog.getById
// ---------------------------------------------------------------------------

describe("EventLog.getById", () => {
  it("returns event by id after ingest", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast"), rec("forward")], 0);
    expect(log.getById(0)).toBe(log.events[0]);
    expect(log.getById(1)).toBe(log.events[1]);
    expect(log.getById(2)).toBe(log.events[2]);
  });

  it("returns undefined for non-existent IDs", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast")], 0);
    expect(log.getById(999)).toBeUndefined();
    expect(log.getById(-1)).toBeUndefined();
  });

  it("returns undefined on empty log", () => {
    const log = new EventLog();
    expect(log.getById(0)).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// EventLog.byId consistency after truncateAfter
// ---------------------------------------------------------------------------

describe("EventLog.byId consistency after truncateAfter", () => {
  it("kept events findable, removed events undefined", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast")], 0);
    log.ingest([rec("forward"), rec("join")], 1);
    log.truncateAfter(0);
    expect(log.getById(0)).toBe(log.events[0]);
    expect(log.getById(1)).toBe(log.events[1]);
    expect(log.getById(2)).toBeUndefined();
    expect(log.getById(3)).toBeUndefined();
  });

  it("byId size matches events length", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast")], 0);
    log.ingest([rec("forward")], 1);
    log.truncateAfter(0);
    assertByIdSync(log);
  });

  it("truncateAfter keeping all events is a no-op", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast")], 0);
    log.ingest([rec("forward")], 1);
    log.truncateAfter(5);
    expect(log.events.length).toBe(3);
    assertByIdSync(log);
  });

  it("truncateAfter on empty log does not crash", () => {
    const log = new EventLog();
    log.truncateAfter(0);
    expect(log.events.length).toBe(0);
    expect(log.getById(0)).toBeUndefined();
  });

  it("no ghost references after truncation", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast")], 0);
    log.ingest([rec("forward")], 1);
    const ghost = log.getById(2)!;
    expect(ghost).toBeDefined();
    log.truncateAfter(0);
    // The JS object still exists, but the map should not reference it
    expect(log.getById(2)).toBeUndefined();
    expect(ghost.code).toBe("GF"); // object itself is fine, just not in the map
  });
});

// ---------------------------------------------------------------------------
// EventLog.byId after clear
// ---------------------------------------------------------------------------

describe("EventLog.byId after clear", () => {
  it("clear empties byId, re-ingest works", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast")], 0);
    log.clear();
    expect(log.getById(0)).toBeUndefined();
    expect(log.getById(1)).toBeUndefined();
    // Re-ingest reuses IDs starting from 0
    log.ingest([rec("forward")], 0);
    expect(log.getById(0)).toBeDefined();
    expect(log.getById(0)!.code).toBe("GF");
  });

  it("clear removes all ghost references", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast"), rec("forward")], 0);
    const ids = log.events.map(e => e.id);
    log.clear();
    for (const id of ids) {
      expect(log.getById(id)).toBeUndefined();
    }
  });
});

// ---------------------------------------------------------------------------
// EventLog.byId with ID reuse
// ---------------------------------------------------------------------------

describe("EventLog.byId with ID reuse", () => {
  it("full truncation + re-ingest: byId points to NEW events", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast")], 0);
    log.truncateAfter(-1); // removes everything, nextId resets to 0
    expect(log.events.length).toBe(0);
    log.ingest([rec("forward"), rec("unicast")], 0);
    expect(log.getById(0)!.code).toBe("GF"); // not "NN" from old ingest
    expect(log.getById(1)!.code).toBe("GU"); // not "GB" from old ingest
    assertByIdSync(log);
  });

  it("partial truncation + re-ingest: new events at recycled IDs", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast")], 0);     // ids 0, 1
    log.ingest([rec("forward"), rec("unicast")], 1);    // ids 2, 3
    log.truncateAfter(0);                                // removes ids 2, 3
    log.ingest([rec("topic_new"), rec("resolved")], 1); // new ids 2, 3
    expect(log.getById(2)!.code).toBe("TN");
    expect(log.getById(3)!.code).toBe("CR");
    assertByIdSync(log);
  });
});

// ---------------------------------------------------------------------------
// EventLog.byId across multiple truncation cycles
// ---------------------------------------------------------------------------

describe("EventLog.byId across multiple truncation cycles", () => {
  it("double truncate cycle", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast")], 0);
    assertByIdSync(log);
    log.truncateAfter(0);
    assertByIdSync(log);
    log.ingest([rec("forward"), rec("unicast")], 1);
    assertByIdSync(log);
    log.truncateAfter(0);
    assertByIdSync(log);
    expect(log.events.length).toBe(2);
  });

  it("stress: 10 rounds of ingest/truncate with full audit", () => {
    const log = new EventLog();
    for (let round = 0; round < 10; round++) {
      const events = Array.from({ length: 5 }, (_, i) => rec("join"));
      log.ingest(events, round);
      assertByIdSync(log);
      if (round > 0) {
        log.truncateAfter(round - 1);
        // Re-ingest for the current round after truncation
        log.ingest(Array.from({ length: 5 }, () => rec("broadcast")), round);
      }
      assertByIdSync(log);
    }
  });
});

// ---------------------------------------------------------------------------
// EventLog correlation via byId after truncation
// ---------------------------------------------------------------------------

describe("EventLog correlation via byId after truncation", () => {
  it("truncated send not re-correlated with new receive", () => {
    const log = new EventLog();
    // Send at historyIndex 0, receive at historyIndex 1
    const sendRec: EventRecord = {
      timeUs: 1000, event: "broadcast", src: 0, dst: null, topicHash: 42n, details: {},
    };
    const recvRec: EventRecord = {
      timeUs: 2000, event: "received", src: 1, dst: 0, topicHash: 42n,
      details: { originSrc: 0, sendTimeUs: 1000 },
    };
    log.ingest([sendRec], 0);
    log.ingest([recvRec], 1);
    // Remove receive
    log.truncateAfter(0);
    // Now remove send too
    log.truncateAfter(-1);
    // Ingest a new receive referencing the same key — should NOT correlate
    log.ingest([recvRec], 0);
    expect(log.events[0].sendId).toBeNull();
  });

  it("send survives truncation, new receive correlates", () => {
    const log = new EventLog();
    const sendRec: EventRecord = {
      timeUs: 1000, event: "broadcast", src: 0, dst: null, topicHash: 42n, details: {},
    };
    log.ingest([sendRec], 0);
    log.ingest([rec("join")], 1); // unrelated at historyIndex 1
    log.truncateAfter(0); // removes the unrelated event
    // Now ingest receive at historyIndex 1
    const recvRec: EventRecord = {
      timeUs: 2000, event: "received", src: 1, dst: 0, topicHash: 42n,
      details: { originSrc: 0, sendTimeUs: 1000 },
    };
    log.ingest([recvRec], 1);
    const recv = log.events[log.events.length - 1];
    expect(recv.sendId).toBe(0);
    expect(log.getById(0)!.receiveIds).toContain(recv.id);
  });

  it("cross-batch correlation: byId reference is same object", () => {
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
    const sendEv = log.getById(0)!;
    const recvEv = log.getById(1)!;
    expect(sendEv.receiveIds).toContain(recvEv.id);
    expect(recvEv.sendId).toBe(sendEv.id);
    // Referential identity: byId returns the same object as events array
    expect(sendEv).toBe(log.events[0]);
    expect(recvEv).toBe(log.events[1]);
  });

  it("pendingSends cleanup: truncated send cannot be re-correlated", () => {
    const log = new EventLog();
    log.ingest([rec("join")], 0); // id 0 at historyIndex 0
    const sendRec: EventRecord = {
      timeUs: 1000, event: "broadcast", src: 0, dst: null, topicHash: 42n, details: {},
    };
    log.ingest([sendRec], 1); // id 1 at historyIndex 1
    log.truncateAfter(0); // removes send (id 1) and its pendingSends key
    // Ingest receive referencing the same key — should NOT correlate
    const recvRec: EventRecord = {
      timeUs: 2000, event: "received", src: 1, dst: 0, topicHash: 42n,
      details: { originSrc: 0, sendTimeUs: 1000 },
    };
    log.ingest([recvRec], 1);
    expect(log.events[log.events.length - 1].sendId).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// EventLog edge cases
// ---------------------------------------------------------------------------

describe("EventLog edge cases", () => {
  it("ingest empty array is no-op", () => {
    const log = new EventLog();
    log.ingest([], 0);
    expect(log.events.length).toBe(0);
    assertByIdSync(log);
  });

  it("ingest empty between real ingests", () => {
    const log = new EventLog();
    log.ingest([rec("join"), rec("broadcast")], 0);
    log.ingest([], 1);
    log.ingest([rec("forward")], 2);
    expect(log.events.length).toBe(3);
    expect(log.getById(0)).toBeDefined();
    expect(log.getById(1)).toBeDefined();
    expect(log.getById(2)).toBeDefined();
    assertByIdSync(log);
  });
});
