// ---------------------------------------------------------------------------
// Timeline renderer — event chart with causal arrows and draggable cursor
// ---------------------------------------------------------------------------

import { TimelineCode, TimelineEvent } from "./types.js";
import { EventLog } from "./event-log.js";

const CODE_COLORS: Record<TimelineCode, string> = {
  GB: "#f1c40f",
  GU: "#e67e22",
  GF: "#9b59b6",
  GR: "#3498db",
  TN: "#27ae60",
  TC: "#e74c3c",
  TD: "#e74c3c",
  TX: "#95a5a6",
  NN: "#27ae60",
  NX: "#95a5a6",
  CR: "#2ecc71",
};

const CODE_NAMES: Record<TimelineCode, string> = {
  GB: "Gossip Broadcast",
  GU: "Gossip Unicast",
  GF: "Gossip Forward",
  GR: "Gossip Received",
  TN: "Topic New",
  TC: "Topic Collision",
  TD: "Topic Divergence",
  TX: "Topic eXpunged",
  NN: "Node New",
  NX: "Node eXpunged",
  CR: "Conflict Resolved",
};

const GUTTER_W = 40;
const ROW_H = 20;
const AXIS_H = 16;
const MARKER_FONT = "bold 7px monospace";

export class Timeline {
  private canvas: HTMLCanvasElement;
  private ctx: CanvasRenderingContext2D;
  private tooltip: HTMLElement;
  private eventLog: EventLog;

  private viewStartUs = 0;
  private viewEndUs = 10_000_000; // 10s default
  private nodeIds: number[] = [];

  // Navigation
  onNavigate: ((index: number) => void) | null = null;
  private historyTimes: number[] = [];
  private currentHistoryIndex = 0;

  // Interaction state
  private dragging = false;
  private hoveredEvent: TimelineEvent | null = null;
  private userHasManuallyScrolled = false;

  constructor(
    canvas: HTMLCanvasElement,
    tooltip: HTMLElement,
    eventLog: EventLog,
  ) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d")!;
    this.tooltip = tooltip;
    this.eventLog = eventLog;
    this.setupInteraction();
  }

  setNodeIds(ids: number[]): void {
    this.nodeIds = ids;
  }

  setHistoryTimes(times: number[]): void {
    this.historyTimes = times;
  }

  setCurrentIndex(index: number): void {
    this.currentHistoryIndex = index;
  }

  resize(): void {
    const container = this.canvas.parentElement!;
    this.canvas.width = container.clientWidth;
    this.canvas.height = container.clientHeight;
  }

  render(currentTimeUs: number): void {
    const ctx = this.ctx;
    const W = this.canvas.width;
    const H = this.canvas.height;
    if (W === 0 || H === 0) return;

    ctx.fillStyle = "#1a1a1a";
    ctx.fillRect(0, 0, W, H);

    const contentH = H - AXIS_H;
    const nRows = this.nodeIds.length;

    // Auto-scroll: if cursor > 90% of viewport, shift right
    const range = this.viewEndUs - this.viewStartUs;
    if (this.userHasManuallyScrolled) {
      // Reset flag once the cursor approaches the right edge again (live edge)
      if (currentTimeUs > this.viewEndUs - range * 0.2) {
        this.userHasManuallyScrolled = false;
      }
    }
    if (!this.userHasManuallyScrolled && currentTimeUs > this.viewStartUs + range * 0.9) {
      this.viewStartUs = currentTimeUs - range * 0.5;
      this.viewEndUs = this.viewStartUs + range;
    }

    // Ensure viewStart doesn't go negative
    if (this.viewStartUs < 0) {
      const range = this.viewEndUs - this.viewStartUs;
      this.viewStartUs = 0;
      this.viewEndUs = range;
    }

    // Node ID labels in left gutter
    ctx.font = "bold 10px monospace";
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    for (let i = 0; i < nRows; i++) {
      const y = i * ROW_H + ROW_H / 2;
      if (y > contentH) break;
      ctx.fillStyle = "#888";
      ctx.fillText(`N${this.nodeIds[i]}`, GUTTER_W - 4, y);
    }

    // Row grid lines
    ctx.strokeStyle = "#333";
    ctx.lineWidth = 0.5;
    for (let i = 0; i <= nRows; i++) {
      const y = i * ROW_H;
      if (y > contentH) break;
      ctx.beginPath();
      ctx.moveTo(GUTTER_W, y);
      ctx.lineTo(W, y);
      ctx.stroke();
    }

    // Causal arrows (behind markers)
    this.drawCausalArrows(ctx, contentH);

    // Event markers
    ctx.font = MARKER_FONT;
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    for (const ev of this.eventLog.events) {
      const x = this.timeToX(ev.timeUs);
      if (x < GUTTER_W - 10 || x > W + 10) continue;
      const rowIdx = this.nodeIds.indexOf(ev.nodeId);
      if (rowIdx < 0) continue;
      const y = rowIdx * ROW_H;
      if (y > contentH) continue;

      const color = CODE_COLORS[ev.code];
      ctx.fillStyle = color;
      // Two characters stacked vertically
      const top = ev.code[0];
      const bot = ev.code[1];
      ctx.fillText(top, x, y + 2);
      ctx.fillText(bot, x, y + 10);
    }

    // Time axis
    this.drawTimeAxis(ctx, W, H, contentH);

    // Cursor
    this.drawCursor(ctx, currentTimeUs, W, contentH, H);
  }

  private timeToX(timeUs: number): number {
    const range = this.viewEndUs - this.viewStartUs;
    if (range <= 0) return GUTTER_W;
    const frac = (timeUs - this.viewStartUs) / range;
    return GUTTER_W + frac * (this.canvas.width - GUTTER_W);
  }

  private xToTime(x: number): number {
    const plotW = this.canvas.width - GUTTER_W;
    if (plotW <= 0) return this.viewStartUs;
    const frac = (x - GUTTER_W) / plotW;
    return this.viewStartUs + frac * (this.viewEndUs - this.viewStartUs);
  }

  private drawCausalArrows(ctx: CanvasRenderingContext2D, contentH: number): void {
    const W = this.canvas.width;
    for (const ev of this.eventLog.events) {
      if (ev.receiveIds.length === 0) continue;
      const sx = this.timeToX(ev.timeUs);
      if (sx > W + 50) continue;
      const sRow = this.nodeIds.indexOf(ev.nodeId);
      if (sRow < 0) continue;
      const sy = sRow * ROW_H + ROW_H / 2;
      if (sy > contentH) continue;

      const color = CODE_COLORS[ev.code];
      ctx.save();
      ctx.strokeStyle = color;
      ctx.globalAlpha = 0.3;
      ctx.lineWidth = 1;

      for (const rid of ev.receiveIds) {
        const recv = this.eventLog.events.find(e => e.id === rid);
        if (!recv) continue;
        const rx = this.timeToX(recv.timeUs);
        if (rx < GUTTER_W - 50) continue;
        const rRow = this.nodeIds.indexOf(recv.nodeId);
        if (rRow < 0) continue;
        const ry = rRow * ROW_H + ROW_H / 2;
        if (ry > contentH) continue;

        ctx.beginPath();
        ctx.moveTo(sx, sy);
        ctx.lineTo(rx, ry);
        ctx.stroke();
      }
      ctx.restore();
    }
  }

  private drawTimeAxis(
    ctx: CanvasRenderingContext2D, W: number, H: number, contentH: number,
  ): void {
    ctx.fillStyle = "#222";
    ctx.fillRect(0, contentH, W, AXIS_H);
    ctx.strokeStyle = "#555";
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(0, contentH);
    ctx.lineTo(W, contentH);
    ctx.stroke();

    // Tick marks
    const rangeUs = this.viewEndUs - this.viewStartUs;
    const rangeS = rangeUs / 1_000_000;
    // Choose tick interval
    const intervals = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60];
    let tickS = 1;
    for (const iv of intervals) {
      if (rangeS / iv < 20) { tickS = iv; break; }
    }
    const startTick = Math.ceil(this.viewStartUs / (tickS * 1_000_000));
    const endTick = Math.floor(this.viewEndUs / (tickS * 1_000_000));

    const decimals = tickS < 0.01 ? 3 : tickS < 0.1 ? 2 : tickS < 1 ? 1 : 0;
    ctx.font = "9px monospace";
    ctx.fillStyle = "#aaa";
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    for (let i = startTick; i <= endTick; i++) {
      const tUs = i * tickS * 1_000_000;
      const x = this.timeToX(tUs);
      if (x < GUTTER_W || x > W) continue;
      ctx.beginPath();
      ctx.moveTo(x, contentH);
      ctx.lineTo(x, contentH + 4);
      ctx.stroke();
      ctx.fillText(`${(tUs / 1_000_000).toFixed(decimals)}s`, x, contentH + 4);
    }

    // Zoom indicator
    const zoomRangeMs = rangeUs / 1_000;
    const zoomLabel = zoomRangeMs < 1000
      ? `${zoomRangeMs.toFixed(zoomRangeMs < 10 ? 1 : 0)}ms`
      : `${(zoomRangeMs / 1000).toFixed(1)}s`;
    ctx.font = "9px monospace";
    ctx.fillStyle = "#666";
    ctx.textAlign = "right";
    ctx.textBaseline = "bottom";
    ctx.fillText(zoomLabel, W - 4, H - 2);
  }

  private drawCursor(
    ctx: CanvasRenderingContext2D, timeUs: number,
    W: number, contentH: number, H: number,
  ): void {
    const x = this.timeToX(timeUs);
    if (x < GUTTER_W || x > W) return;

    // Vertical dashed line
    ctx.save();
    ctx.strokeStyle = "rgba(255,255,255,0.6)";
    ctx.lineWidth = 1;
    ctx.setLineDash([4, 3]);
    ctx.beginPath();
    ctx.moveTo(x, 0);
    ctx.lineTo(x, contentH);
    ctx.stroke();
    ctx.restore();

    // Triangle at bottom
    ctx.save();
    ctx.fillStyle = "rgba(255,255,255,0.8)";
    ctx.beginPath();
    ctx.moveTo(x, contentH);
    ctx.lineTo(x - 5, H);
    ctx.lineTo(x + 5, H);
    ctx.closePath();
    ctx.fill();
    ctx.restore();
  }

  private setupInteraction(): void {
    const canvas = this.canvas;

    canvas.addEventListener("mousedown", (e) => {
      if (e.button !== 0) return;
      this.dragging = true;
      this.navigateToX(e.offsetX);
    });

    canvas.addEventListener("mousemove", (e) => {
      if (this.dragging) {
        this.navigateToX(e.offsetX);
      } else {
        this.handleHover(e.offsetX, e.offsetY);
      }
    });

    canvas.addEventListener("mouseup", () => {
      this.dragging = false;
    });

    canvas.addEventListener("mouseleave", () => {
      this.dragging = false;
      this.tooltip.style.display = "none";
      this.hoveredEvent = null;
    });

    // Wheel: zoom around mouse; shift+wheel: horizontal scroll
    canvas.addEventListener("wheel", (e) => {
      e.preventDefault();
      this.userHasManuallyScrolled = true;
      const range = this.viewEndUs - this.viewStartUs;
      if (e.shiftKey) {
        // Horizontal scroll
        const shift = range * 0.1 * (e.deltaY > 0 ? 1 : -1);
        this.viewStartUs += shift;
        this.viewEndUs += shift;
      } else {
        // Zoom around mouse position (plain wheel + ctrl/pinch)
        const mouseTime = this.xToTime(e.offsetX);
        const factor = e.deltaY > 0 ? 1.15 : 1 / 1.15;
        const newRange = Math.max(1_000, Math.min(range * factor, 600_000_000));
        const mouseFrac = (mouseTime - this.viewStartUs) / range;
        this.viewStartUs = mouseTime - mouseFrac * newRange;
        this.viewEndUs = this.viewStartUs + newRange;
      }
    }, { passive: false });
  }

  private navigateToX(x: number): void {
    const timeUs = this.xToTime(x);
    // Binary search for nearest history index
    const times = this.historyTimes;
    if (times.length === 0) return;
    let lo = 0, hi = times.length - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      if (times[mid] <= timeUs) lo = mid;
      else hi = mid - 1;
    }
    // lo is the largest index with times[lo] <= timeUs
    // Check if lo+1 is closer
    if (lo + 1 < times.length && Math.abs(times[lo + 1] - timeUs) < Math.abs(times[lo] - timeUs)) {
      lo = lo + 1;
    }
    if (lo !== this.currentHistoryIndex) {
      this.onNavigate?.(lo);
    }
  }

  private handleHover(x: number, y: number): void {
    const contentH = this.canvas.height - AXIS_H;
    if (y > contentH || x < GUTTER_W) {
      this.tooltip.style.display = "none";
      this.hoveredEvent = null;
      return;
    }

    // Hit-test events
    const hitRadius = 8;
    let closest: TimelineEvent | null = null;
    let closestDist = hitRadius;

    for (const ev of this.eventLog.events) {
      const ex = this.timeToX(ev.timeUs);
      const rowIdx = this.nodeIds.indexOf(ev.nodeId);
      if (rowIdx < 0) continue;
      const ey = rowIdx * ROW_H + ROW_H / 2;
      const dx = x - ex, dy = y - ey;
      const dist = Math.sqrt(dx * dx + dy * dy);
      if (dist < closestDist) {
        closestDist = dist;
        closest = ev;
      }
    }

    if (closest) {
      if (closest !== this.hoveredEvent) {
        this.hoveredEvent = closest;
        this.showTooltip(closest, x, y);
      }
    } else {
      // Check arrow hover
      const arrowHit = this.hitTestArrow(x, y);
      if (arrowHit) {
        if (arrowHit !== this.hoveredEvent) {
          this.hoveredEvent = arrowHit;
          this.showArrowTooltip(arrowHit, x, y);
        }
      } else {
        this.tooltip.style.display = "none";
        this.hoveredEvent = null;
      }
    }
  }

  private hitTestArrow(x: number, y: number): TimelineEvent | null {
    const threshold = 6;
    for (const ev of this.eventLog.events) {
      if (ev.receiveIds.length === 0) continue;
      const sx = this.timeToX(ev.timeUs);
      const sRow = this.nodeIds.indexOf(ev.nodeId);
      if (sRow < 0) continue;
      const sy = sRow * ROW_H + ROW_H / 2;

      for (const rid of ev.receiveIds) {
        const recv = this.eventLog.events.find(e => e.id === rid);
        if (!recv) continue;
        const rx = this.timeToX(recv.timeUs);
        const rRow = this.nodeIds.indexOf(recv.nodeId);
        if (rRow < 0) continue;
        const ry = rRow * ROW_H + ROW_H / 2;

        const dist = this.pointToSegmentDist(x, y, sx, sy, rx, ry);
        if (dist < threshold) return ev;
      }
    }
    return null;
  }

  private pointToSegmentDist(
    px: number, py: number, x0: number, y0: number, x1: number, y1: number,
  ): number {
    const dx = x1 - x0, dy = y1 - y0;
    const lenSq = dx * dx + dy * dy;
    if (lenSq === 0) return Math.sqrt((px - x0) ** 2 + (py - y0) ** 2);
    let t = ((px - x0) * dx + (py - y0) * dy) / lenSq;
    t = Math.max(0, Math.min(1, t));
    const cx = x0 + t * dx, cy = y0 + t * dy;
    return Math.sqrt((px - cx) ** 2 + (py - cy) ** 2);
  }

  private showTooltip(ev: TimelineEvent, x: number, y: number): void {
    const name = CODE_NAMES[ev.code];
    const d = ev.details;
    let text = `${ev.code} - ${name}\nNode: N${ev.nodeId}\nTime: ${(ev.timeUs / 1_000_000).toFixed(3)}s`;
    if (d.name) text += `\nTopic: ${d.name}`;
    if (d.evictions !== undefined) text += `\nEvictions: ${d.evictions}`;
    if (d.lage !== undefined) text += `\nLage: ${d.lage}`;
    if (d.dst !== null && d.dst !== undefined) text += `\nDst: N${d.dst}`;
    if (d.type) text += `\nType: ${d.type}`;
    if (d.local_won !== undefined) text += `\nLocal won: ${d.local_won}`;

    this.tooltip.textContent = text;
    this.tooltip.style.display = "block";
    const rect = this.canvas.parentElement!.getBoundingClientRect();
    this.tooltip.style.left = Math.min(x + 12, rect.width - 200) + "px";
    this.tooltip.style.top = Math.max(0, y - 40) + "px";
  }

  private showArrowTooltip(ev: TimelineEvent, x: number, y: number): void {
    const name = CODE_NAMES[ev.code];
    const d = ev.details;
    let text = `Arrow: ${ev.code} - ${name}\nFrom: N${ev.nodeId}`;
    if (d.name) text += `\nTopic: ${d.name}`;
    if (d.evictions !== undefined) text += `\nEvictions: ${d.evictions}`;
    if (d.lage !== undefined) text += `\nLage: ${d.lage}`;
    text += `\nReceivers: ${ev.receiveIds.length}`;

    this.tooltip.textContent = text;
    this.tooltip.style.display = "block";
    const rect = this.canvas.parentElement!.getBoundingClientRect();
    this.tooltip.style.left = Math.min(x + 12, rect.width - 200) + "px";
    this.tooltip.style.top = Math.max(0, y - 40) + "px";
  }
}
