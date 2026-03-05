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
const COLOCATED_SPACING = 8; // horizontal pixels between same-timestep markers
const NET_BIN_US = 100_000; // 0.1s bins for network utilization chart
const NET_MSG_CODES = new Set<TimelineCode>(["GB", "GU", "GF", "GR"]);

export class Timeline {
  private canvas: HTMLCanvasElement;
  private ctx: CanvasRenderingContext2D;
  private tooltip: HTMLElement;
  private eventLog: EventLog;

  private viewStartUs = 0;
  private viewEndUs = 10_000_000; // 10s default
  private nodeIds: number[] = [];       // all ever seen, sorted
  private activeNodeIds = new Set<number>();
  private convergenceHistory: { timeUs: number; converged: boolean }[] = [];

  // Navigation
  onNavigate: ((index: number) => void) | null = null;
  isPlaying: (() => boolean) | null = null;
  private historyTimes: number[] = [];
  private currentHistoryIndex = 0;

  // Interaction state
  private get logicalW(): number { return this.canvas.width / (window.devicePixelRatio || 1); }
  private get logicalH(): number { return this.canvas.height / (window.devicePixelRatio || 1); }

  private panning = false;
  private panLastX = 0;
  private panStartX = 0;
  private draggingCursor = false;
  private hoveredEvents: TimelineEvent[] = [];
  private userHasManuallyScrolled = false;
  private lastCursorX = 0; // cached cursor screen X for hit-testing

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
    // Accumulate all ever-seen node IDs, track which are currently active
    this.activeNodeIds = new Set(ids);
    for (const id of ids) {
      if (!this.nodeIds.includes(id)) {
        this.nodeIds.push(id);
        this.nodeIds.sort((a, b) => a - b);
      }
    }
  }

  resetNodeIds(): void {
    this.nodeIds = [];
    this.activeNodeIds.clear();
    this.convergenceHistory = [];
  }

  recordConvergence(timeUs: number, converged: boolean): void {
    const last = this.convergenceHistory[this.convergenceHistory.length - 1];
    if (!last || last.converged !== converged) {
      this.convergenceHistory.push({ timeUs, converged });
    }
  }

  truncateConvergenceAfter(timeUs: number): void {
    while (this.convergenceHistory.length > 0 &&
           this.convergenceHistory[this.convergenceHistory.length - 1].timeUs > timeUs) {
      this.convergenceHistory.pop();
    }
  }

  setHistoryTimes(times: number[]): void {
    this.historyTimes = times;
  }

  setCurrentIndex(index: number): void {
    this.currentHistoryIndex = index;
  }

  resize(): void {
    const container = this.canvas.parentElement!;
    const dpr = window.devicePixelRatio || 1;
    const w = container.clientWidth;
    const h = container.clientHeight;
    this.canvas.width = w * dpr;
    this.canvas.height = h * dpr;
    this.canvas.style.width = w + "px";
    this.canvas.style.height = h + "px";
    this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }

  render(currentTimeUs: number): void {
    const ctx = this.ctx;
    const W = this.logicalW;
    const H = this.logicalH;
    if (W === 0 || H === 0) return;

    ctx.fillStyle = "#1a1a1a";
    ctx.fillRect(0, 0, W, H);

    const contentH = H - AXIS_H;
    const nRows = this.nodeIds.length;

    // Auto-scroll: if cursor > 90% of viewport, shift right (only when playing)
    const range = this.viewEndUs - this.viewStartUs;
    const playing = this.isPlaying?.() ?? false;
    if (this.userHasManuallyScrolled) {
      // Reset flag once the cursor approaches the right edge again (live edge)
      if (playing && currentTimeUs > this.viewEndUs - range * 0.2) {
        this.userHasManuallyScrolled = false;
      }
    }
    if (!this.userHasManuallyScrolled && playing && currentTimeUs > this.viewStartUs + range * 0.9) {
      this.viewStartUs = currentTimeUs - range * 0.5;
      this.viewEndUs = this.viewStartUs + range;
    }

    // Ensure viewStart doesn't go negative
    if (this.viewStartUs < 0) {
      const range = this.viewEndUs - this.viewStartUs;
      this.viewStartUs = 0;
      this.viewEndUs = range;
    }

    // Net row (row 0): convergence background + msg/s sparkline
    this.drawNetRow(ctx, W, contentH);

    // Node ID labels in left gutter (offset by 1 row for convergence)
    ctx.font = "bold 10px monospace";
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    for (let i = 0; i < nRows; i++) {
      const y = (i + 1) * ROW_H + ROW_H / 2;
      if (y > contentH) break;
      const active = this.activeNodeIds.has(this.nodeIds[i]);
      ctx.fillStyle = active ? "#888" : "#444";
      ctx.fillText(`Node${this.nodeIds[i]}`, GUTTER_W - 4, y);
    }

    // Convergence row label
    ctx.fillStyle = "#888";
    ctx.fillText("Net", GUTTER_W - 4, ROW_H / 2);

    // Row grid lines
    ctx.strokeStyle = "#333";
    ctx.lineWidth = 0.5;
    for (let i = 0; i <= nRows + 1; i++) {
      const y = i * ROW_H;
      if (y > contentH) break;
      ctx.beginPath();
      ctx.moveTo(GUTTER_W, y);
      ctx.lineTo(W, y);
      ctx.stroke();
    }

    // Build co-located offsets before drawing
    this.buildColocatedOffsets();

    // Clip plot area so markers/arrows don't occlude gutter labels
    ctx.save();
    ctx.beginPath();
    ctx.rect(GUTTER_W, 0, W - GUTTER_W, contentH);
    ctx.clip();

    // Causal arrows (behind markers)
    this.drawCausalArrows(ctx, contentH);

    // Event markers
    ctx.font = MARKER_FONT;
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    for (const ev of this.eventLog.events) {
      const x = this.eventX(ev);
      if (x < GUTTER_W - 10 || x > W + 10) continue;
      const rowIdx = this.nodeIds.indexOf(ev.nodeId);
      if (rowIdx < 0) continue;
      const y = (rowIdx + 1) * ROW_H; // +1 for convergence row
      if (y > contentH) continue;

      const active = this.activeNodeIds.has(ev.nodeId);
      const color = CODE_COLORS[ev.code];
      ctx.globalAlpha = active ? 1.0 : 0.35;
      ctx.fillStyle = color;
      const top = ev.code[0];
      const bot = ev.code[1];
      ctx.fillText(top, x, y + 2);
      ctx.fillText(bot, x, y + 10);
    }
    ctx.globalAlpha = 1.0;

    ctx.restore();

    // Time axis
    this.drawTimeAxis(ctx, W, H, contentH);

    // Cursor
    this.drawCursor(ctx, currentTimeUs, W, contentH, H);
  }

  // Cache of per-event x-offsets for co-located events (same nodeId + timeUs)
  private colocatedOffsets = new Map<number, number>(); // event.id → pixel offset

  private buildColocatedOffsets(): void {
    this.colocatedOffsets.clear();
    // Group events by (nodeId, timeUs) preserving array order
    const groups = new Map<string, TimelineEvent[]>();
    for (const ev of this.eventLog.events) {
      const key = `${ev.nodeId}:${ev.timeUs}`;
      let group = groups.get(key);
      if (!group) { group = []; groups.set(key, group); }
      group.push(ev);
    }
    for (const group of groups.values()) {
      if (group.length <= 1) continue;
      const totalWidth = (group.length - 1) * COLOCATED_SPACING;
      for (let i = 0; i < group.length; i++) {
        this.colocatedOffsets.set(group[i].id, -totalWidth / 2 + i * COLOCATED_SPACING);
      }
    }
  }

  private eventX(ev: TimelineEvent): number {
    return this.timeToX(ev.timeUs) + (this.colocatedOffsets.get(ev.id) ?? 0);
  }

  private timeToX(timeUs: number): number {
    const range = this.viewEndUs - this.viewStartUs;
    if (range <= 0) return GUTTER_W;
    const frac = (timeUs - this.viewStartUs) / range;
    return GUTTER_W + frac * (this.logicalW - GUTTER_W);
  }

  private xToTime(x: number): number {
    const plotW = this.logicalW - GUTTER_W;
    if (plotW <= 0) return this.viewStartUs;
    const frac = (x - GUTTER_W) / plotW;
    return this.viewStartUs + frac * (this.viewEndUs - this.viewStartUs);
  }

  private drawCausalArrows(ctx: CanvasRenderingContext2D, contentH: number): void {
    const W = this.logicalW;
    for (const ev of this.eventLog.events) {
      if (ev.receiveIds.length === 0) continue;
      const sx = this.eventX(ev);
      if (sx > W + 50) continue;
      const sRow = this.nodeIds.indexOf(ev.nodeId);
      if (sRow < 0) continue;
      const sy = (sRow + 1) * ROW_H + ROW_H / 2;
      if (sy > contentH) continue;

      const color = CODE_COLORS[ev.code];
      ctx.save();
      ctx.strokeStyle = color;
      ctx.globalAlpha = 0.3;
      ctx.lineWidth = 1;

      for (const rid of ev.receiveIds) {
        const recv = this.eventLog.events.find(e => e.id === rid);
        if (!recv) continue;
        const rx = this.eventX(recv);
        if (rx < GUTTER_W - 50) continue;
        const rRow = this.nodeIds.indexOf(recv.nodeId);
        if (rRow < 0) continue;
        const ry = (rRow + 1) * ROW_H + ROW_H / 2;
        if (ry > contentH) continue;

        ctx.beginPath();
        ctx.moveTo(sx, sy);
        ctx.lineTo(rx, ry);
        ctx.stroke();
      }
      ctx.restore();
    }
  }

  private drawNetRow(ctx: CanvasRenderingContext2D, W: number, contentH: number): void {
    const y = 0;
    const h = ROW_H;

    // Convergence background
    for (let i = 0; i < this.convergenceHistory.length; i++) {
      const entry = this.convergenceHistory[i];
      const nextEntry = this.convergenceHistory[i + 1];
      const startX = Math.max(GUTTER_W, this.timeToX(entry.timeUs));
      const endX = nextEntry ? this.timeToX(nextEntry.timeUs) : W;
      if (endX < GUTTER_W || startX > W) continue;
      ctx.fillStyle = entry.converged ? "rgba(39, 174, 96, 0.25)" : "rgba(231, 76, 60, 0.25)";
      ctx.fillRect(Math.max(startX, GUTTER_W), y, Math.min(endX, W) - Math.max(startX, GUTTER_W), h);
    }

    // Network utilization sparkline (msg/s in 0.1s bins)
    const events = this.eventLog.events;
    if (events.length === 0) return;

    // Determine visible bin range
    const binStart = Math.floor(this.viewStartUs / NET_BIN_US);
    const binEnd = Math.ceil(this.viewEndUs / NET_BIN_US);

    // Count messages per bin using a sweep over sorted events
    const binCounts = new Map<number, number>();
    for (const ev of events) {
      if (!NET_MSG_CODES.has(ev.code)) continue;
      const bin = Math.floor(ev.timeUs / NET_BIN_US);
      if (bin < binStart - 1 || bin > binEnd + 1) continue;
      binCounts.set(bin, (binCounts.get(bin) || 0) + 1);
    }

    if (binCounts.size === 0) return;

    // Find max for scaling
    let maxCount = 0;
    for (const c of binCounts.values()) {
      if (c > maxCount) maxCount = c;
    }
    if (maxCount === 0) return;

    const padding = 2;
    const chartH = h - padding * 2;

    ctx.save();
    ctx.beginPath();
    ctx.rect(GUTTER_W, y, W - GUTTER_W, h);
    ctx.clip();

    ctx.fillStyle = "rgba(52, 152, 219, 0.5)";
    for (let bin = binStart; bin <= binEnd; bin++) {
      const count = binCounts.get(bin) || 0;
      if (count === 0) continue;
      const x0 = this.timeToX(bin * NET_BIN_US);
      const x1 = this.timeToX((bin + 1) * NET_BIN_US);
      const barH = (count / maxCount) * chartH;
      ctx.fillRect(x0, y + h - padding - barH, x1 - x0, barH);
    }

    ctx.restore();
  }

  private getNetMsgRate(timeUs: number): number {
    const binStart = timeUs - NET_BIN_US / 2;
    const binEnd = timeUs + NET_BIN_US / 2;
    let count = 0;
    for (const ev of this.eventLog.events) {
      if (!NET_MSG_CODES.has(ev.code)) continue;
      if (ev.timeUs >= binStart && ev.timeUs < binEnd) count++;
    }
    return count * (1_000_000 / NET_BIN_US); // scale to msg/s
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
    ctx.fillStyle = "#fff";
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
    this.lastCursorX = x;
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
      if (e.button === 0 || e.button === 1) {
        e.preventDefault();
        // Check if clicking on the cursor triangle (bottom axis area, near cursor X)
        const contentH = this.logicalH - AXIS_H;
        if (!this.isPlaying?.() && e.offsetY >= contentH && Math.abs(e.offsetX - this.lastCursorX) < 10) {
          this.draggingCursor = true;
          canvas.style.cursor = "ew-resize";
        } else {
          this.panning = true;
          this.panLastX = e.offsetX;
          this.panStartX = e.offsetX;
          canvas.style.cursor = "grabbing";
        }
      }
    });

    canvas.addEventListener("mousemove", (e) => {
      if (this.draggingCursor) {
        this.navigateToX(e.offsetX);
      } else if (this.panning) {
        const dx = e.offsetX - this.panLastX;
        const plotW = this.logicalW - GUTTER_W;
        if (plotW > 0) {
          const range = this.viewEndUs - this.viewStartUs;
          const shift = -(dx / plotW) * range;
          this.viewStartUs += shift;
          this.viewEndUs += shift;
          this.userHasManuallyScrolled = true;
        }
        this.panLastX = e.offsetX;
      } else {
        this.handleHover(e.offsetX, e.offsetY);
      }
    });

    canvas.addEventListener("mouseup", (e) => {
      if (this.draggingCursor) {
        this.draggingCursor = false;
        canvas.style.cursor = "";
      } else if (this.panning) {
        // If barely moved, treat as a click → navigate cursor
        if (Math.abs(e.offsetX - this.panStartX) < 3) {
          if (this.isPlaying?.()) {
            this.showWarning(e.offsetX);
          } else {
            this.navigateToX(e.offsetX);
          }
        }
        this.panning = false;
        canvas.style.cursor = "";
      }
    });

    canvas.addEventListener("mouseleave", () => {
      if (this.draggingCursor) {
        this.draggingCursor = false;
        canvas.style.cursor = "";
      } else if (this.panning) {
        this.panning = false;
        canvas.style.cursor = "";
      }
      this.tooltip.style.display = "none";
      this.hoveredEvents = [];
    });

    // Prevent context menu on middle-click
    canvas.addEventListener("auxclick", (e) => {
      if (e.button === 1) e.preventDefault();
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
    const contentH = this.logicalH - AXIS_H;
    if (y > contentH || x < GUTTER_W) {
      this.tooltip.style.display = "none";
      this.hoveredEvents = [];
      return;
    }

    // Net row hover: show msg/s
    if (y < ROW_H) {
      const timeUs = this.xToTime(x);
      const rate = this.getNetMsgRate(timeUs);
      this.tooltip.textContent = `${rate.toFixed(0)} msg/s`;
      this.tooltip.style.display = "block";
      const rect = this.canvas.parentElement!.getBoundingClientRect();
      this.tooltip.style.left = Math.min(x + 12, rect.width - 80) + "px";
      this.tooltip.style.top = Math.max(0, y - 20) + "px";
      this.hoveredEvents = [];
      return;
    }

    // Hit-test: collect all events within radius
    const hitRadius = 8;
    const hits: { ev: TimelineEvent; dist: number }[] = [];

    for (const ev of this.eventLog.events) {
      const ex = this.eventX(ev);
      const rowIdx = this.nodeIds.indexOf(ev.nodeId);
      if (rowIdx < 0) continue;
      const ey = (rowIdx + 1) * ROW_H + ROW_H / 2;
      const dx = x - ex, dy = y - ey;
      const dist = Math.sqrt(dx * dx + dy * dy);
      if (dist < hitRadius) {
        hits.push({ ev, dist });
      }
    }

    if (hits.length > 0) {
      hits.sort((a, b) => a.dist - b.dist);
      const evs = hits.map(h => h.ev);
      // Only update if the set of hovered events changed
      if (evs.length !== this.hoveredEvents.length || evs.some((e, i) => e !== this.hoveredEvents[i])) {
        this.hoveredEvents = evs;
        this.showTooltip(evs, x, y);
      }
    } else {
      // Check arrow hover
      const arrowHit = this.hitTestArrow(x, y);
      if (arrowHit) {
        if (this.hoveredEvents.length !== 1 || this.hoveredEvents[0] !== arrowHit) {
          this.hoveredEvents = [arrowHit];
          this.showArrowTooltip(arrowHit, x, y);
        }
      } else {
        this.tooltip.style.display = "none";
        this.hoveredEvents = [];
      }
    }
  }

  private hitTestArrow(x: number, y: number): TimelineEvent | null {
    const threshold = 6;
    for (const ev of this.eventLog.events) {
      if (ev.receiveIds.length === 0) continue;
      const sx = this.eventX(ev);
      const sRow = this.nodeIds.indexOf(ev.nodeId);
      if (sRow < 0) continue;
      const sy = (sRow + 1) * ROW_H + ROW_H / 2;

      for (const rid of ev.receiveIds) {
        const recv = this.eventLog.events.find(e => e.id === rid);
        if (!recv) continue;
        const rx = this.eventX(recv);
        const rRow = this.nodeIds.indexOf(recv.nodeId);
        if (rRow < 0) continue;
        const ry = (rRow + 1) * ROW_H + ROW_H / 2;

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

  private formatEvent(ev: TimelineEvent): string {
    const name = CODE_NAMES[ev.code];
    const d = ev.details;
    let text = `${ev.code} - ${name}  Node${ev.nodeId}  ${(ev.timeUs / 1_000_000).toFixed(3)}s`;
    if (d.name) text += `  Topic: ${d.name}`;
    if (d.evictions !== undefined) text += `  Evictions: ${d.evictions}`;
    if (d.lage !== undefined) text += `  Lage: ${d.lage}`;
    if (d.dst !== null && d.dst !== undefined) text += `  Dst: Node${d.dst}`;
    if (d.type) text += `  Type: ${d.type}`;
    if (d.local_won !== undefined) text += `  Local won: ${d.local_won}`;
    return text;
  }

  private showTooltip(evs: TimelineEvent[], x: number, y: number): void {
    const text = evs.map(ev => this.formatEvent(ev)).join("\n");
    this.tooltip.textContent = text;
    this.tooltip.style.display = "block";
    const rect = this.canvas.parentElement!.getBoundingClientRect();
    this.tooltip.style.left = Math.min(x + 12, rect.width - 200) + "px";
    this.tooltip.style.top = Math.max(0, y - 40) + "px";
  }

  private showArrowTooltip(ev: TimelineEvent, x: number, y: number): void {
    const name = CODE_NAMES[ev.code];
    const d = ev.details;
    let text = `Arrow: ${ev.code} - ${name}\nFrom: Node${ev.nodeId}`;
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

  private warningTimeout: ReturnType<typeof setTimeout> | null = null;

  private showWarning(x: number): void {
    this.tooltip.textContent = "Pause to rewind. Resuming from old state erases newer history.";
    this.tooltip.style.display = "block";
    const rect = this.canvas.parentElement!.getBoundingClientRect();
    this.tooltip.style.left = Math.min(x + 12, rect.width - 200) + "px";
    this.tooltip.style.top = "4px";
    if (this.warningTimeout) clearTimeout(this.warningTimeout);
    this.warningTimeout = setTimeout(() => {
      this.tooltip.style.display = "none";
      this.warningTimeout = null;
    }, 2000);
  }
}
