// ---------------------------------------------------------------------------
// EventLog — maps EventRecords to TimelineEvents with send/receive correlation
// ---------------------------------------------------------------------------

import { EventRecord, TimelineCode, TimelineEvent } from "./types.js";

export function mapCode(rec: EventRecord): TimelineCode {
  switch (rec.event) {
    case "broadcast": return "GB";
    case "unicast":   return "GU";
    case "forward":   return "GF";
    case "received":  return "GR";
    case "gossip_xterminated": return "GX";
    case "join":      return "NN";
    case "topic_new": return "TN";
    case "topic_expunged": return "TX";
    case "node_expunged":  return "NX";
    case "resolved":  return "CR";
    case "peer_refresh": return "PR";
    case "conflict": {
      const t = rec.details?.type as string;
      return t === "collision" ? "TC" : "TD";
    }
    default: return "GB"; // fallback
  }
}

export class EventLog {
  events: TimelineEvent[] = [];
  private nextId = 0;
  // Correlation: key -> send event ID
  private pendingSends = new Map<string, number>();
  private byId = new Map<number, TimelineEvent>();

  getById(id: number): TimelineEvent | undefined {
    return this.byId.get(id);
  }

  ingest(records: EventRecord[], historyIndex: number): void {
    for (const rec of records) {
      const code = mapCode(rec);
      const te: TimelineEvent = {
        id: this.nextId++,
        timeUs: rec.timeUs,
        code,
        nodeId: rec.src,
        topicHash: rec.topicHash,
        details: { ...rec.details, dst: rec.dst },
        secondaryTopicHash: null,
        receiveIds: [],
        sendId: null,
        historyIndex,
      };

      // For TC events, store the remote topic hash as secondary
      if (code === "TC") {
        const rh = rec.details?.remote_hash;
        if (typeof rh === "bigint") te.secondaryTopicHash = rh;
      }

      this.events.push(te);
      this.byId.set(te.id, te);

      // Correlation for send/receive
      if (code === "GB" || code === "GU" || code === "GF") {
        const key = `${rec.src}:${rec.topicHash}:${rec.timeUs}`;
        this.pendingSends.set(key, te.id);
      } else if (code === "GR") {
        const originSrc = rec.details?.originSrc as number;
        const sendTimeUs = rec.details?.sendTimeUs as number;
        if (originSrc !== undefined && sendTimeUs !== undefined) {
          const key = `${originSrc}:${rec.topicHash}:${sendTimeUs}`;
          const sendId = this.pendingSends.get(key);
          if (sendId !== undefined) {
            te.sendId = sendId;
            const sendEv = this.byId.get(sendId);
            if (sendEv) sendEv.receiveIds.push(te.id);
          }
        }
      }
    }
  }

  truncateAfter(historyIndex: number): void {
    // Remove events with historyIndex > index
    const removedIds = new Set<number>();
    this.events = this.events.filter(e => {
      if (e.historyIndex > historyIndex) {
        removedIds.add(e.id);
        this.byId.delete(e.id);
        return false;
      }
      return true;
    });
    // Clean up correlation map
    for (const [key, id] of this.pendingSends) {
      if (removedIds.has(id)) this.pendingSends.delete(key);
    }
    // Clean up receiveIds references
    if (removedIds.size > 0) {
      for (const e of this.events) {
        if (e.receiveIds.length > 0) {
          e.receiveIds = e.receiveIds.filter(id => !removedIds.has(id));
        }
        if (e.sendId !== null && removedIds.has(e.sendId)) {
          e.sendId = null;
        }
      }
    }
    // Reset nextId
    this.nextId = this.events.length > 0 ? this.events[this.events.length - 1].id + 1 : 0;
  }

  clear(): void {
    this.events = [];
    this.nextId = 0;
    this.pendingSends.clear();
    this.byId.clear();
  }
}
