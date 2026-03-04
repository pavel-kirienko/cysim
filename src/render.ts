// ---------------------------------------------------------------------------
// Canvas 2D renderer — port of cysim/viz.py MatplotlibRenderer
// ---------------------------------------------------------------------------

import { EventRecord, NodeSnapshot, PeerSnap, TopicSnap } from "./types.js";
import {
  GOSSIP_PEER_ELIGIBLE, GOSSIP_PERIOD,
  MSG_PERSIST_US, CONFLICT_FLASH_US,
} from "./constants.js";

// Colors (matching viz.py)
const C_BG          = "#1e1e1e";
const C_ONLINE      = "#d5e8d4";
const C_OFFLINE     = "#555555";
const C_CONFLICT    = "#f8cecc";
const C_BORDER      = "#888888";
const C_BROADCAST   = "#888888";
const C_UNICAST     = "#e67e22";
const C_FORWARD     = "#9b59b6";
const C_PEER_FRESH  = "#27ae60";
const C_PEER_STALE  = "#95a5a6";
const C_PEER_EMPTY  = "#666666";
const C_TEXT        = "#e0e0e0";
const C_HEADER      = "#ffffff";
const C_SEPARATOR   = "#666666";

const FONT_SIZE     = 11;
const LINE_HEIGHT   = 15;
const BOX_WIDTH     = 210;
const BOX_PAD       = 8;
const BOX_RADIUS    = 6;

interface ActiveMessage {
  expireUs: number;
  event: EventRecord;
}

export class Renderer {
  private canvas: HTMLCanvasElement;
  private ctx: CanvasRenderingContext2D;
  nodePositions: Map<number, { x: number; y: number }> = new Map();
  private nodeBoxSizes: Map<number, { w: number; h: number }> = new Map();

  private activeMessages: ActiveMessage[] = [];
  private activeConflicts: Map<number, number> = new Map(); // nodeId -> flashUntilUs

  constructor(canvas: HTMLCanvasElement) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d")!;
  }

  layoutNodes(nodeIds: number[]): void {
    const n = nodeIds.length;
    if (n === 0) return;
    const cx = this.canvas.width / 2;
    const cy = this.canvas.height / 2;
    const radius = Math.min(cx, cy) * 0.55 + n * 15;
    this.nodePositions.clear();
    for (let i = 0; i < n; i++) {
      const angle = (2 * Math.PI * i) / n - Math.PI / 2;
      this.nodePositions.set(nodeIds[i], {
        x: cx + radius * Math.cos(angle),
        y: cy + radius * Math.sin(angle),
      });
    }
  }

  render(
    timeUs: number,
    snaps: Map<number, NodeSnapshot>,
    newEvents: EventRecord[],
  ): void {
    const ctx = this.ctx;
    const W = this.canvas.width;
    const H = this.canvas.height;

    // Clear
    ctx.fillStyle = C_BG;
    ctx.fillRect(0, 0, W, H);

    // Update active messages
    for (const ev of newEvents) {
      if (ev.event === "broadcast" || ev.event === "unicast" || ev.event === "forward") {
        this.activeMessages.push({ expireUs: timeUs + MSG_PERSIST_US, event: ev });
      }
      if (ev.event === "conflict") {
        this.activeConflicts.set(ev.src, timeUs + CONFLICT_FLASH_US);
      }
    }
    this.activeMessages = this.activeMessages.filter(m => m.expireUs > timeUs);

    // Compute node box sizes first (needed for edge points)
    this.nodeBoxSizes.clear();
    for (const [nid, snap] of snaps) {
      this.nodeBoxSizes.set(nid, this.computeBoxSize(snap));
    }

    // Draw message arrows (behind nodes)
    this.drawMessages(ctx, timeUs, snaps);

    // Draw node boxes
    for (const [nid, snap] of snaps) {
      const pos = this.nodePositions.get(nid);
      if (!pos) continue;
      this.drawNodeBox(ctx, timeUs, pos.x, pos.y, snap);
    }
  }

  private computeBoxSize(snap: NodeSnapshot): { w: number; h: number } {
    let rows = 3; // header + next HB + next tx
    rows += 1;    // separator
    rows += Math.max(snap.topics.length, 1); // topics or "(no topics)"
    rows += 1;    // separator
    rows += 1;    // "peers:"
    let nPeers = 0;
    let nEmpty = 0;
    for (const p of snap.peers) {
      if (p) nPeers++; else nEmpty++;
    }
    rows += nPeers;
    if (nEmpty > 0) rows += 1;
    return { w: BOX_WIDTH, h: rows * LINE_HEIGHT + BOX_PAD * 2 };
  }

  private drawNodeBox(
    ctx: CanvasRenderingContext2D, timeUs: number,
    cx: number, cy: number, snap: NodeSnapshot,
  ): void {
    const size = this.nodeBoxSizes.get(snap.nodeId)!;
    const w = size.w, h = size.h;
    const x = cx - w / 2, y = cy - h / 2;

    // Determine fill color
    const inConflict = this.activeConflicts.has(snap.nodeId) &&
                       this.activeConflicts.get(snap.nodeId)! >= timeUs;
    let bg: string;
    if (!snap.online) bg = C_OFFLINE;
    else if (inConflict) bg = C_CONFLICT;
    else bg = C_ONLINE;

    // Rounded rect
    ctx.beginPath();
    ctx.roundRect(x, y, w, h, BOX_RADIUS);
    ctx.fillStyle = bg;
    ctx.fill();
    ctx.strokeStyle = C_BORDER;
    ctx.lineWidth = 1.2;
    ctx.stroke();

    // Text content
    const textX = x + BOX_PAD;
    let row = 0;
    const textColor = snap.online ? "#222222" : C_TEXT;

    const putText = (text: string, opts?: { bold?: boolean; color?: string; size?: number }) => {
      const sz = opts?.size || FONT_SIZE;
      ctx.font = (opts?.bold ? "bold " : "") + sz + "px monospace";
      ctx.fillStyle = opts?.color || textColor;
      ctx.fillText(text, textX, y + BOX_PAD + row * LINE_HEIGHT + sz);
      row++;
    };

    // Row 0: header
    const status = snap.online ? "ONLINE" : "OFFLINE";
    const partLabel = ` [${snap.partitionSet}]`;
    putText(`N${snap.nodeId}  ${status}${partLabel}`, { bold: true, size: 12 });

    // Row 1: next heartbeat
    if (snap.online && snap.nextBroadcastUs > 0) {
      const dt = Math.max(0, snap.nextBroadcastUs - timeUs) / 1_000_000;
      putText(`next HB: ${dt.toFixed(2)}s`);
    } else {
      putText("next HB: --");
    }

    // Row 2: next topic to broadcast
    let nextTopicName = "--";
    const nxtH = snap.gossipUrgentFront ?? snap.gossipQueueFront;
    if (snap.online && nxtH !== null) {
      for (const ts of snap.topics) {
        if (ts.hash === nxtH) { nextTopicName = ts.name; break; }
      }
    }
    putText(`next tx: ${nextTopicName}`);

    // Separator
    putText("─".repeat(26), { color: C_SEPARATOR });

    // Topics
    if (snap.topics.length > 0) {
      for (const ts of snap.topics) {
        const nm = ts.name.length > 12 ? ts.name.slice(0, 12) : ts.name.padEnd(12);
        putText(`${nm} S${String(ts.subjectId).padEnd(5)} e${ts.evictions}`);
      }
    } else {
      putText("(no topics)", { color: C_PEER_EMPTY });
    }

    // Separator
    putText("─".repeat(26), { color: C_SEPARATOR });

    // Peers
    putText("peers:", { bold: true });
    let nEmpty = 0;
    for (const p of snap.peers) {
      if (p === null) { nEmpty++; continue; }
      const age = (timeUs - p.lastSeenUs) / 1_000_000;
      const fresh = (timeUs - p.lastSeenUs) < GOSSIP_PEER_ELIGIBLE;
      const c = fresh ? C_PEER_FRESH : C_PEER_STALE;
      putText(`  N${p.nodeId} ${age.toFixed(1)}s ago`, { color: c });
    }
    if (nEmpty > 0) {
      putText(`  (${nEmpty} empty)`, { color: C_PEER_EMPTY });
    }
  }

  private drawMessages(
    ctx: CanvasRenderingContext2D, timeUs: number,
    snaps: Map<number, NodeSnapshot>,
  ): void {
    for (const msg of this.activeMessages) {
      const ev = msg.event;
      const ageFrac = 1.0 - (msg.expireUs - timeUs) / MSG_PERSIST_US;
      const alpha = Math.max(0.15, 0.9 - 0.75 * ageFrac);

      let color: string, lineWidth: number, dashed: boolean;
      if (ev.event === "broadcast") {
        color = C_BROADCAST; lineWidth = 0.8; dashed = false;
      } else if (ev.event === "unicast") {
        color = C_UNICAST; lineWidth = 2.0; dashed = false;
      } else {
        color = C_FORWARD; lineWidth = 1.5; dashed = true;
      }

      const srcPos = this.nodePositions.get(ev.src);
      if (!srcPos) continue;

      const dsts: number[] = [];
      if (ev.dst !== null) {
        dsts.push(ev.dst);
      } else {
        // Broadcast — draw to all other nodes
        for (const nid of snaps.keys()) {
          if (nid !== ev.src) dsts.push(nid);
        }
      }

      for (const did of dsts) {
        const dstPos = this.nodePositions.get(did);
        if (!dstPos) continue;

        const srcBox = this.nodeBoxSizes.get(ev.src) || { w: BOX_WIDTH, h: 100 };
        const dstBox = this.nodeBoxSizes.get(did) || { w: BOX_WIDTH, h: 100 };
        const [x0, y0] = this.edgePoint(srcPos.x, srcPos.y, srcBox.w, srcBox.h, dstPos.x, dstPos.y);
        const [x1, y1] = this.edgePoint(dstPos.x, dstPos.y, dstBox.w, dstBox.h, srcPos.x, srcPos.y);

        this.drawArrow(ctx, x0, y0, x1, y1, color, lineWidth, alpha, dashed);
      }
    }
  }

  /** Compute intersection of line from box center to target with box border. */
  private edgePoint(
    cx: number, cy: number, bw: number, bh: number,
    tx: number, ty: number,
  ): [number, number] {
    const dx = tx - cx, dy = ty - cy;
    if (dx === 0 && dy === 0) return [cx, cy];
    const hw = bw / 2, hh = bh / 2;
    const sx = dx !== 0 ? hw / Math.abs(dx) : 1e9;
    const sy = dy !== 0 ? hh / Math.abs(dy) : 1e9;
    const s = Math.min(sx, sy);
    return [cx + dx * s, cy + dy * s];
  }

  private drawArrow(
    ctx: CanvasRenderingContext2D,
    x0: number, y0: number, x1: number, y1: number,
    color: string, lineWidth: number, alpha: number, dashed: boolean,
  ): void {
    ctx.save();
    ctx.globalAlpha = alpha;
    ctx.strokeStyle = color;
    ctx.lineWidth = lineWidth;
    ctx.setLineDash(dashed ? [6, 4] : []);

    // Quadratic bezier for slight curve
    const mx = (x0 + x1) / 2, my = (y0 + y1) / 2;
    const dx = x1 - x0, dy = y1 - y0;
    const len = Math.sqrt(dx * dx + dy * dy);
    const off = len * 0.08;
    const nx = -dy / (len || 1), ny = dx / (len || 1);
    const cpx = mx + nx * off, cpy = my + ny * off;

    ctx.beginPath();
    ctx.moveTo(x0, y0);
    ctx.quadraticCurveTo(cpx, cpy, x1, y1);
    ctx.stroke();

    // Arrowhead
    const headLen = 8;
    // Tangent at endpoint of quadratic bezier: derivative at t=1 is 2*(P1-CP)
    const tdx = x1 - cpx, tdy = y1 - cpy;
    const tlen = Math.sqrt(tdx * tdx + tdy * tdy) || 1;
    const ux = tdx / tlen, uy = tdy / tlen;
    ctx.setLineDash([]);
    ctx.fillStyle = color;
    ctx.beginPath();
    ctx.moveTo(x1, y1);
    ctx.lineTo(x1 - ux * headLen - uy * headLen * 0.4,
               y1 - uy * headLen + ux * headLen * 0.4);
    ctx.lineTo(x1 - ux * headLen + uy * headLen * 0.4,
               y1 - uy * headLen - ux * headLen * 0.4);
    ctx.closePath();
    ctx.fill();

    ctx.restore();
  }
}
