// ---------------------------------------------------------------------------
// Timeline renderer — event chart with causal arrows and draggable cursor
// ---------------------------------------------------------------------------

import { TimelineCode, TimelineEvent } from "./types.js";
import { EventLog } from "./event-log.js";

const CODE_COLORS: Record<TimelineCode, string> = {
  GB: "#f1c40f",
  GS: "#4dc3ff",
  GU: "#e67e22",
  GP: "#1abc9c",
  GF: "#9b59b6",
  GR: "#3498db",
  GX: "#8e44ad",
  TN: "#27ae60",
  TC: "#e74c3c",
  TD: "#e74c3c",
  TX: "#95a5a6",
  NN: "#27ae60",
  NX: "#95a5a6",
  CR: "#2ecc71",
  PR: "#ff00ff",
};

const CODE_NAMES: Record<TimelineCode, string> = {
  GB: "Gossip Broadcast",
  GS: "Gossip Shard",
  GU: "Gossip Unicast",
  GP: "Gossip Periodic",
  GF: "Gossip Forward",
  GR: "Gossip Received",
  GX: "Gossip eXterminated",
  TN: "Topic New",
  TC: "Topic Collision",
  TD: "Topic Divergence",
  TX: "Topic eXpunged",
  NN: "Node New",
  NX: "Node eXpunged",
  CR: "Conflict Resolved",
  PR: "Peer Refresh",
};

const GUTTER_W = 40;
const ROW_H = 20;
const AXIS_H = 16;
const MARKER_FONT = "bold 7px monospace";
const COLOCATED_SPACING = 8; // horizontal pixels between same-timestep markers
const NET_BIN_US = 100_000; // 0.1s bins for network utilization chart
const NET_MSG_CODES = new Set<TimelineCode>(["GB", "GS", "GU", "GP", "GF", "GR"]);
type TimelineMode = "node" | "topic";

export interface TopicTimelineRow {
  hash: bigint;
  name: string;
  conflictHue: number | null;
}

export class Timeline {
  private canvas: HTMLCanvasElement;
  private ctx: CanvasRenderingContext2D;
  private tooltip: HTMLElement;
  private eventLog: EventLog;
  private mode: TimelineMode;
  stickyTopicHash: bigint | null = null;
  hoverTopicHash: bigint | null = null;

  private viewStartUs = 0;
  private viewEndUs = 10_000_000; // 10s default
  private nodeIds: number[] = []; // all ever seen, sorted
  private nodeRowIndex = new Map<number, number>();
  private activeNodeIds = new Set<number>();
  private convergenceHistory: { timeUs: number; converged: boolean }[] = [];

  // Navigation
  onNavigate: ((index: number) => void) | null = null;
  isPlaying: (() => boolean) | null = null;
  private historyTimes: number[] = [];
  private currentHistoryIndex = 0;
  private lastCurrentTimeUs = 0;
  onViewChanged: ((startUs: number, endUs: number) => void) | null = null;

  // Interaction state
  private get logicalW(): number {
    return this.canvas.width / (window.devicePixelRatio || 1);
  }
  private get logicalH(): number {
    return this.canvas.height / (window.devicePixelRatio || 1);
  }

  private panning = false;
  private panLastX = 0;
  private panLastY = 0;
  private panStartX = 0;
  private panStartY = 0;
  private draggingCursor = false;
  private dragCursorX: number | null = null; // screen X while dragging cursor
  private hoveredEvents: TimelineEvent[] = [];
  private userHasManuallyScrolled = false;
  private lastCursorX = 0; // cached cursor screen X for hit-testing
  private nodeScrollPx = 0;
  private topicRows: TopicTimelineRow[] = [];
  private topicRowIndex = new Map<bigint, number>();
  private lastHoveredTopicRowHash: bigint | null = null;
  onTopicRowHover: ((hash: bigint | null) => void) | null = null;

  constructor(canvas: HTMLCanvasElement, tooltip: HTMLElement, eventLog: EventLog, options?: { mode?: TimelineMode }) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d")!;
    this.tooltip = tooltip;
    this.eventLog = eventLog;
    this.mode = options?.mode ?? "node";
    this.setupInteraction();
  }

  setNodeIds(ids: number[]): void {
    if (this.mode !== "node") {
      return;
    }
    // Accumulate all ever-seen node IDs, track which are currently active
    this.activeNodeIds = new Set(ids);
    for (const id of ids) {
      if (!this.nodeIds.includes(id)) {
        this.nodeIds.push(id);
        this.nodeIds.sort((a, b) => a - b);
      }
    }
    this.rebuildNodeRowIndex();
    this.clampNodeScroll(this.logicalH - AXIS_H);
  }

  setTopicRows(rows: TopicTimelineRow[]): void {
    if (this.mode !== "topic") {
      return;
    }
    this.topicRows = rows.slice();
    this.topicRowIndex.clear();
    for (let i = 0; i < this.topicRows.length; i++) {
      this.topicRowIndex.set(this.topicRows[i].hash, i);
    }
    if (this.lastHoveredTopicRowHash !== null && !this.topicRowIndex.has(this.lastHoveredTopicRowHash)) {
      this.emitTopicRowHover(null);
    }
    this.clampNodeScroll(this.logicalH - AXIS_H);
  }

  resetNodeIds(): void {
    this.nodeIds = [];
    this.nodeRowIndex.clear();
    this.activeNodeIds.clear();
    this.convergenceHistory = [];
    this.colocatedCacheLen = -1;
    this.nodeScrollPx = 0;
    this.topicRows = [];
    this.topicRowIndex.clear();
    this.emitTopicRowHover(null);
  }

  getViewRange(): { startUs: number; endUs: number } {
    return { startUs: this.viewStartUs, endUs: this.viewEndUs };
  }

  setViewRange(startUs: number, endUs: number, markUserScrolled = false, emit = false): void {
    const minRange = 1_000;
    const maxRange = 600_000_000;
    let start = Math.floor(startUs);
    let end = Math.floor(endUs);
    let range = end - start;
    if (range < minRange) {
      range = minRange;
    } else if (range > maxRange) {
      range = maxRange;
    }

    // Preserve the range when clamping at origin to avoid accidental zooming
    // while panning right into the zero-time boundary.
    if (start < 0) {
      end -= start;
      start = 0;
    } else {
      end = start + range;
    }
    if (end - start < minRange) {
      end = start + minRange;
    } else if (end - start > maxRange) {
      end = start + maxRange;
    }
    this.viewStartUs = start;
    this.viewEndUs = end;
    if (markUserScrolled) {
      this.userHasManuallyScrolled = true;
    }
    if (emit) {
      this.onViewChanged?.(this.viewStartUs, this.viewEndUs);
    }
  }

  private rebuildNodeRowIndex(): void {
    this.nodeRowIndex.clear();
    for (let i = 0; i < this.nodeIds.length; i++) {
      this.nodeRowIndex.set(this.nodeIds[i], i);
    }
  }

  recordConvergence(timeUs: number, converged: boolean): void {
    const last = this.convergenceHistory[this.convergenceHistory.length - 1];
    if (!last || last.converged !== converged) {
      this.convergenceHistory.push({ timeUs, converged });
    }
  }

  truncateConvergenceAfter(timeUs: number): void {
    while (
      this.convergenceHistory.length > 0 &&
      this.convergenceHistory[this.convergenceHistory.length - 1].timeUs > timeUs
    ) {
      this.convergenceHistory.pop();
    }
  }

  setHistoryTimes(times: number[]): void {
    this.historyTimes = times;
  }

  setCurrentIndex(index: number): void {
    this.currentHistoryIndex = index;
  }

  /** Navigate to the history index of the next (dir=1) or previous (dir=-1) event. */
  stepToEvent(dir: 1 | -1): void {
    const events = this.eventLog.events;
    if (events.length === 0) return;
    const cur = this.currentHistoryIndex;
    let best: number | null = null;
    for (const ev of events) {
      const hi = ev.historyIndex;
      if (dir === 1 && hi > cur) {
        best = hi;
        break;
      }
      if (dir === -1 && hi < cur) best = hi;
    }
    if (best !== null && best !== cur) {
      this.onNavigate?.(best);
    }
  }

  /** Pan the timeline view. dir=1 pans right, dir=-1 pans left. */
  pan(dir: 1 | -1): void {
    const shift = (this.viewEndUs - this.viewStartUs) * 0.2 * dir;
    this.setViewRange(this.viewStartUs + shift, this.viewEndUs + shift, true, true);
  }

  /** Zoom the timeline view. dir=1 zooms in, dir=-1 zooms out. Centers on cursor. */
  zoom(dir: 1 | -1): void {
    const range = this.viewEndUs - this.viewStartUs;
    const factor = dir === 1 ? 1 / 1.3 : 1.3;
    const newRange = Math.max(1_000, Math.min(range * factor, 600_000_000));
    const center = this.lastCurrentTimeUs;
    const centerFrac = (center - this.viewStartUs) / range;
    const start = center - centerFrac * newRange;
    this.setViewRange(start, start + newRange, true, true);
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
    this.lastCurrentTimeUs = currentTimeUs;
    const ctx = this.ctx;
    const W = this.logicalW;
    const H = this.logicalH;
    if (W === 0 || H === 0) return;

    ctx.fillStyle = "#1a1a1a";
    ctx.fillRect(0, 0, W, H);

    const contentH = H - AXIS_H;
    const nRows = this.scrollableRowCount();
    this.clampNodeScroll(contentH);

    const oldViewStart = this.viewStartUs;
    const oldViewEnd = this.viewEndUs;

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

    // When paused, keep cursor visible by panning (unless user manually scrolled)
    if (!playing && !this.userHasManuallyScrolled) {
      if (currentTimeUs > this.viewEndUs) {
        this.viewStartUs = currentTimeUs - range * 0.8;
        this.viewEndUs = this.viewStartUs + range;
      } else if (currentTimeUs < this.viewStartUs) {
        this.viewStartUs = currentTimeUs - range * 0.2;
        this.viewEndUs = this.viewStartUs + range;
      }
    }

    // Ensure viewStart doesn't go negative
    if (this.viewStartUs < 0) {
      const adjustedRange = this.viewEndUs - this.viewStartUs;
      this.viewStartUs = 0;
      this.viewEndUs = adjustedRange;
    }
    if (oldViewStart !== this.viewStartUs || oldViewEnd !== this.viewEndUs) {
      this.onViewChanged?.(this.viewStartUs, this.viewEndUs);
    }

    if (this.hasNetRow()) {
      // Net row (row 0): convergence background + msg/s sparkline
      this.drawNetRow(ctx, W, contentH);
    }

    // Row labels
    ctx.font = "bold 10px monospace";
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    if (this.mode === "node") {
      for (let i = 0; i < nRows; i++) {
        const y = this.rowCenter(i);
        if (y < ROW_H || y > contentH) continue;
        const active = this.activeNodeIds.has(this.nodeIds[i]);
        ctx.fillStyle = active ? "#fff" : "#555";
        ctx.fillText(`Node${this.nodeIds[i]}`, GUTTER_W - 4, y);
      }
      ctx.fillStyle = "#fff";
      ctx.fillText("Net", GUTTER_W - 4, ROW_H / 2);
    } else {
      for (let i = 0; i < nRows; i++) {
        const y = this.rowCenter(i);
        if (y < 0 || y > contentH) continue;
        const row = this.topicRows[i];
        if (!row) continue;
        const label = row.name.length > 10 ? row.name.slice(0, 10) : row.name;
        ctx.fillStyle = row.conflictHue === null ? "#ddd" : `hsl(${row.conflictHue}, 80%, 65%)`;
        ctx.fillText(label, GUTTER_W - 4, y);
      }
    }

    // Row grid lines
    ctx.strokeStyle = "#333";
    ctx.lineWidth = 0.5;
    if (this.hasNetRow()) {
      // Pinned net row top and bottom.
      for (const y of [0, ROW_H]) {
        ctx.beginPath();
        ctx.moveTo(GUTTER_W, y);
        ctx.lineTo(W, y);
        ctx.stroke();
      }
    }
    // Scrollable rows.
    for (let i = 0; i <= nRows; i++) {
      const y = this.scrollableRowOffset() + i * ROW_H - this.nodeScrollPx;
      if (y < this.scrollableRowOffset() || y > contentH) continue;
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

    // Topic-mode row conflict highlighting (mirrors table conflict hue mapping).
    if (this.mode === "topic") {
      for (let i = 0; i < this.topicRows.length; i++) {
        const row = this.topicRows[i];
        const y = this.rowTop(i);
        if (y + ROW_H < 0 || y > contentH) continue;
        if (row.conflictHue !== null) {
          ctx.fillStyle = `hsla(${row.conflictHue}, 65%, 35%, 0.35)`;
          ctx.fillRect(GUTTER_W, y, W - GUTTER_W, ROW_H);
        }
        if (row.hash === this.lastHoveredTopicRowHash) {
          ctx.fillStyle = "rgba(255, 255, 255, 0.08)";
          ctx.fillRect(GUTTER_W, y, W - GUTTER_W, ROW_H);
        }
      }
    }

    // Causal arrows (behind markers) are meaningful in per-node mode only.
    this.drawCausalArrows(ctx, contentH);

    // Event markers
    ctx.font = MARKER_FONT;
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    const sticky = this.stickyTopicHash;
    const hover = this.hoverTopicHash;
    const hasFocus = sticky !== null || hover !== null;
    for (const ev of this.eventLog.events) {
      const x = this.eventX(ev);
      if (x < GUTTER_W - 10 || x > W + 10) continue;
      const rowIndices = this.eventRowIndices(ev);
      if (rowIndices.length === 0) continue;

      for (const rowIdx of rowIndices) {
        const y = this.rowTop(rowIdx);
        if (y + ROW_H < this.scrollableRowOffset() || y > contentH) continue;

        let baseAlpha = 1.0;
        if (this.mode === "node") {
          const active = this.activeNodeIds.has(ev.nodeId);
          baseAlpha = active ? 1.0 : 0.35;
        }
        if (hasFocus) {
          const matchesHover = hover !== null && (ev.topicHash === hover || ev.secondaryTopicHash === hover);
          const matchesSticky = sticky !== null && (ev.topicHash === sticky || ev.secondaryTopicHash === sticky);
          if (matchesHover) {
            /* full */
          } else if (matchesSticky) {
            baseAlpha *= hover !== null ? 0.6 : 1.0;
          } else {
            baseAlpha *= hover !== null ? 0.15 : 0.3;
          }
        }
        const color = CODE_COLORS[ev.code];
        ctx.globalAlpha = baseAlpha;
        ctx.fillStyle = color;
        const top = ev.code[0];
        const bot = ev.code[1];
        ctx.fillText(top, x, y + 2);
        ctx.fillText(bot, x, y + 10);
      }
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
  private colocatedCacheLen = -1;

  private buildColocatedOffsets(): void {
    if (this.eventLog.events.length === this.colocatedCacheLen) return;
    this.colocatedCacheLen = this.eventLog.events.length;
    this.colocatedOffsets.clear();
    // Group events by (row key, timeUs) preserving array order
    const groups = new Map<string, TimelineEvent[]>();
    for (const ev of this.eventLog.events) {
      const key = this.colocatedGroupKey(ev);
      let group = groups.get(key);
      if (!group) {
        group = [];
        groups.set(key, group);
      }
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

  private hasNetRow(): boolean {
    return this.mode === "node";
  }

  private scrollableRowOffset(): number {
    return this.hasNetRow() ? ROW_H : 0;
  }

  private scrollableRowCount(): number {
    return this.mode === "topic" ? this.topicRows.length : this.nodeIds.length;
  }

  private rowViewportHeight(contentH: number): number {
    return Math.max(0, contentH - this.scrollableRowOffset());
  }

  private maxNodeScrollPx(contentH: number): number {
    return Math.max(0, this.scrollableRowCount() * ROW_H - this.rowViewportHeight(contentH));
  }

  private clampNodeScroll(contentH: number): void {
    const max = this.maxNodeScrollPx(contentH);
    if (this.nodeScrollPx < 0) {
      this.nodeScrollPx = 0;
    } else if (this.nodeScrollPx > max) {
      this.nodeScrollPx = max;
    }
  }

  private scrollNodesBy(deltaPx: number): void {
    const contentH = this.logicalH - AXIS_H;
    if (contentH <= ROW_H) {
      this.nodeScrollPx = 0;
      return;
    }
    this.nodeScrollPx += deltaPx;
    this.clampNodeScroll(contentH);
  }

  private rowTop(rowIdx: number): number {
    return this.scrollableRowOffset() + rowIdx * ROW_H - this.nodeScrollPx;
  }

  private rowCenter(rowIdx: number): number {
    return this.rowTop(rowIdx) + ROW_H / 2;
  }

  private rowIndexFromY(y: number): number {
    const relY = y - this.scrollableRowOffset() + this.nodeScrollPx;
    if (relY < 0) {
      return -1;
    }
    const out = Math.floor(relY / ROW_H);
    return out >= 0 && out < this.scrollableRowCount() ? out : -1;
  }

  private emitTopicRowHover(hash: bigint | null): void {
    if (this.mode !== "topic") {
      return;
    }
    if (hash === this.lastHoveredTopicRowHash) {
      return;
    }
    this.lastHoveredTopicRowHash = hash;
    this.onTopicRowHover?.(hash);
  }

  private updateTopicRowHover(y: number, contentH: number): void {
    if (this.mode !== "topic") {
      return;
    }
    if (y < 0 || y > contentH) {
      this.emitTopicRowHover(null);
      return;
    }
    const idx = this.rowIndexFromY(y);
    if (idx < 0 || idx >= this.topicRows.length) {
      this.emitTopicRowHover(null);
      return;
    }
    this.emitTopicRowHover(this.topicRows[idx].hash);
  }

  private eventRowIndices(ev: TimelineEvent): number[] {
    if (this.mode === "node") {
      const idx = this.nodeRowIndex.get(ev.nodeId);
      return idx === undefined ? [] : [idx];
    }
    const out: number[] = [];
    const primary = this.topicRowIndex.get(ev.topicHash);
    if (primary !== undefined) {
      out.push(primary);
    }
    if (ev.secondaryTopicHash !== null) {
      const secondary = this.topicRowIndex.get(ev.secondaryTopicHash);
      if (secondary !== undefined && secondary !== primary) {
        out.push(secondary);
      }
    }
    return out;
  }

  private colocatedGroupKey(ev: TimelineEvent): string {
    if (this.mode === "topic") {
      return `topic:${ev.topicHash}:${ev.timeUs}`;
    }
    return `node:${ev.nodeId}:${ev.timeUs}`;
  }

  private drawCausalArrows(ctx: CanvasRenderingContext2D, contentH: number): void {
    if (this.mode !== "node") {
      return;
    }
    const W = this.logicalW;
    const stickyA = this.stickyTopicHash;
    const hoverA = this.hoverTopicHash;
    const hasFocusA = stickyA !== null || hoverA !== null;
    for (const ev of this.eventLog.events) {
      if (ev.receiveIds.length === 0) continue;
      const sx = this.eventX(ev);
      if (sx > W + 50) continue;
      const sRow = this.nodeRowIndex.get(ev.nodeId) ?? -1;
      if (sRow < 0) continue;
      const sy = this.rowCenter(sRow);
      if (sy < ROW_H || sy > contentH) continue;

      let arrowAlpha = 0.5;
      if (hasFocusA) {
        const matchesHover = hoverA !== null && (ev.topicHash === hoverA || ev.secondaryTopicHash === hoverA);
        const matchesSticky = stickyA !== null && (ev.topicHash === stickyA || ev.secondaryTopicHash === stickyA);
        if (matchesHover) {
          /* full */
        } else if (matchesSticky) {
          arrowAlpha *= hoverA !== null ? 0.6 : 1.0;
        } else {
          arrowAlpha *= hoverA !== null ? 0.15 : 0.2;
        }
      }

      const color = CODE_COLORS[ev.code];
      ctx.save();
      ctx.strokeStyle = color;
      ctx.globalAlpha = arrowAlpha;
      ctx.lineWidth = 1;

      for (const rid of ev.receiveIds) {
        const recv = this.eventLog.getById(rid);
        if (!recv) continue;
        const rx = this.eventX(recv);
        if (rx < GUTTER_W - 50) continue;
        const rRow = this.nodeRowIndex.get(recv.nodeId) ?? -1;
        if (rRow < 0) continue;
        const ry = this.rowCenter(rRow);
        if (ry < ROW_H || ry > contentH) continue;

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

    // 1-second moving average line (centered window of 10 bins)
    const MA_BINS = 20;
    const half = 10;
    ctx.beginPath();
    ctx.strokeStyle = "rgba(255, 255, 255, 0.8)";
    ctx.lineWidth = 1.5;
    let started = false;
    for (let bin = binStart; bin <= binEnd; bin++) {
      let sum = 0;
      for (let j = bin - half; j < bin - half + MA_BINS; j++) {
        sum += binCounts.get(j) || 0;
      }
      const avg = sum / MA_BINS;
      const x = this.timeToX((bin + 0.5) * NET_BIN_US);
      const ly = y + h - padding - (avg / maxCount) * chartH;
      if (!started) {
        ctx.moveTo(x, ly);
        started = true;
      } else {
        ctx.lineTo(x, ly);
      }
    }
    ctx.stroke();

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

  private getNetMsgRateAvg(timeUs: number): number {
    const MA_BINS = 20;
    const half = 10;
    const centerBin = Math.floor(timeUs / NET_BIN_US);
    let total = 0;
    for (const ev of this.eventLog.events) {
      if (!NET_MSG_CODES.has(ev.code)) continue;
      const bin = Math.floor(ev.timeUs / NET_BIN_US);
      if (bin >= centerBin - half && bin < centerBin - half + MA_BINS) total++;
    }
    return (total / MA_BINS) * (1_000_000 / NET_BIN_US);
  }

  private drawTimeAxis(ctx: CanvasRenderingContext2D, W: number, H: number, contentH: number): void {
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
      if (rangeS / iv < 20) {
        tickS = iv;
        break;
      }
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
    const zoomLabel =
      zoomRangeMs < 1000 ? `${zoomRangeMs.toFixed(zoomRangeMs < 10 ? 1 : 0)}ms` : `${(zoomRangeMs / 1000).toFixed(1)}s`;
    ctx.font = "9px monospace";
    ctx.fillStyle = "#666";
    ctx.textAlign = "right";
    ctx.textBaseline = "bottom";
    ctx.fillText(zoomLabel, W - 4, H - 2);
  }

  private drawCursor(ctx: CanvasRenderingContext2D, timeUs: number, W: number, contentH: number, H: number): void {
    const x = this.timeToX(timeUs);
    this.lastCursorX = x;

    // Shadow cursor: shows unsnapped drag position
    if (this.dragCursorX !== null && Math.abs(this.dragCursorX - x) > 2) {
      const sx = this.dragCursorX;
      if (sx >= GUTTER_W && sx <= W) {
        ctx.save();
        ctx.strokeStyle = "rgba(255,255,255,0.25)";
        ctx.lineWidth = 1;
        ctx.setLineDash([2, 3]);
        ctx.beginPath();
        ctx.moveTo(sx, 0);
        ctx.lineTo(sx, contentH);
        ctx.stroke();
        ctx.restore();

        ctx.save();
        ctx.fillStyle = "rgba(255,255,255,0.3)";
        ctx.beginPath();
        ctx.moveTo(sx, contentH);
        ctx.lineTo(sx - 5, H);
        ctx.lineTo(sx + 5, H);
        ctx.closePath();
        ctx.fill();
        ctx.restore();
      }
    }

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

    canvas.addEventListener("pointerdown", (e) => {
      if (e.button === 0 || e.button === 1) {
        e.preventDefault();
        // Check if clicking on the cursor triangle (bottom axis area, near cursor X)
        const contentH = this.logicalH - AXIS_H;
        if (!this.isPlaying?.() && e.offsetY >= contentH && Math.abs(e.offsetX - this.lastCursorX) < 10) {
          this.draggingCursor = true;
          canvas.style.cursor = "ew-resize";
          canvas.setPointerCapture(e.pointerId);
        } else {
          this.panning = true;
          this.panLastX = e.offsetX;
          this.panLastY = e.offsetY;
          this.panStartX = e.offsetX;
          this.panStartY = e.offsetY;
          canvas.style.cursor = "grabbing";
          canvas.setPointerCapture(e.pointerId);
        }
      }
    });

    canvas.addEventListener("pointermove", (e) => {
      if (this.draggingCursor) {
        this.dragCursorX = e.offsetX;
        this.navigateToX(e.offsetX);
      } else if (this.panning) {
        const dx = e.offsetX - this.panLastX;
        const dy = e.offsetY - this.panLastY;
        const plotW = this.logicalW - GUTTER_W;
        if (plotW > 0 && dx !== 0) {
          const range = this.viewEndUs - this.viewStartUs;
          const shift = -(dx / plotW) * range;
          this.setViewRange(this.viewStartUs + shift, this.viewEndUs + shift, true, true);
        }
        this.scrollNodesBy(-dy);
        this.panLastX = e.offsetX;
        this.panLastY = e.offsetY;
      } else {
        this.handleHover(e.offsetX, e.offsetY);
      }
    });

    canvas.addEventListener("pointerup", (e) => {
      if (this.draggingCursor) {
        this.draggingCursor = false;
        if (this.dragCursorX !== null) {
          this.navigateToX(this.dragCursorX);
          this.dragCursorX = null;
        }
        canvas.style.cursor = "";
        canvas.releasePointerCapture(e.pointerId);
      } else if (this.panning) {
        // If barely moved, treat as a click → navigate cursor
        if (Math.abs(e.offsetX - this.panStartX) < 3 && Math.abs(e.offsetY - this.panStartY) < 3) {
          if (this.isPlaying?.()) {
            this.showWarning(e.offsetX);
          } else {
            this.navigateToX(e.offsetX);
          }
        }
        this.panning = false;
        canvas.style.cursor = "";
        canvas.releasePointerCapture(e.pointerId);
      }
    });

    canvas.addEventListener("pointerleave", () => {
      if (!this.draggingCursor && !this.panning) {
        this.tooltip.style.display = "none";
        this.hoveredEvents = [];
        this.emitTopicRowHover(null);
      }
    });

    // Prevent context menu on middle-click
    canvas.addEventListener("auxclick", (e) => {
      if (e.button === 1) e.preventDefault();
    });

    // Wheel: ctrl+wheel zoom, shift+wheel horizontal pan, plain wheel vertical row scroll.
    canvas.addEventListener(
      "wheel",
      (e) => {
        e.preventDefault();
        const wheelDeltaPx =
          e.deltaMode === 1 ? e.deltaY * ROW_H : e.deltaMode === 2 ? e.deltaY * this.logicalH : e.deltaY;
        if (e.ctrlKey) {
          const range = this.viewEndUs - this.viewStartUs;
          // Zoom around mouse position.
          const mouseTime = this.xToTime(e.offsetX);
          const factor = wheelDeltaPx > 0 ? 1.15 : 1 / 1.15;
          const newRange = Math.max(1_000, Math.min(range * factor, 600_000_000));
          const mouseFrac = (mouseTime - this.viewStartUs) / range;
          const newStart = mouseTime - mouseFrac * newRange;
          this.setViewRange(newStart, newStart + newRange, true, true);
        } else if (e.shiftKey) {
          const range = this.viewEndUs - this.viewStartUs;
          // Horizontal scroll
          const shift = range * 0.1 * (wheelDeltaPx > 0 ? 1 : -1);
          this.setViewRange(this.viewStartUs + shift, this.viewEndUs + shift, true, true);
        } else {
          this.scrollNodesBy(wheelDeltaPx);
        }
      },
      { passive: false },
    );

    // Touch: one-finger 2D pan, two-finger pinch-zoom
    let touchPanStartX = 0;
    let touchPanStartY = 0;
    let touchPanViewStart = 0;
    let touchPanViewEnd = 0;
    let touchPanNodeScroll = 0;
    let touchPanning = false;
    let touchStartX = 0; // for tap detection
    let touchStartY = 0;
    let tlPinching = false;
    let tlPinchStartDist = 0;
    let tlPinchMidX = 0;
    let tlPinchMidTime = 0;
    let tlPinchMidFrac = 0;
    let tlPinchViewStart = 0;
    let tlPinchViewEnd = 0;

    canvas.addEventListener(
      "touchstart",
      (e) => {
        if (e.touches.length === 2) {
          e.preventDefault();
          touchPanning = false;
          tlPinching = true;
          const t0 = e.touches[0],
            t1 = e.touches[1];
          tlPinchStartDist = Math.abs(t1.clientX - t0.clientX) || 1;
          const rect = canvas.getBoundingClientRect();
          tlPinchMidX = (t0.clientX + t1.clientX) / 2 - rect.left;
          tlPinchViewStart = this.viewStartUs;
          tlPinchViewEnd = this.viewEndUs;
          const plotW = this.logicalW - GUTTER_W;
          tlPinchMidFrac = plotW > 0 ? (tlPinchMidX - GUTTER_W) / plotW : 0.5;
          tlPinchMidTime = this.viewStartUs + tlPinchMidFrac * (this.viewEndUs - this.viewStartUs);
        } else if (e.touches.length === 1 && !tlPinching) {
          e.preventDefault();
          touchPanning = true;
          const rect = canvas.getBoundingClientRect();
          touchPanStartX = e.touches[0].clientX - rect.left;
          touchPanStartY = e.touches[0].clientY - rect.top;
          touchStartX = touchPanStartX;
          touchStartY = touchPanStartY;
          touchPanViewStart = this.viewStartUs;
          touchPanViewEnd = this.viewEndUs;
          touchPanNodeScroll = this.nodeScrollPx;
        }
      },
      { passive: false },
    );

    canvas.addEventListener(
      "touchmove",
      (e) => {
        if (tlPinching && e.touches.length >= 2) {
          e.preventDefault();
          const t0 = e.touches[0],
            t1 = e.touches[1];
          const dist = Math.abs(t1.clientX - t0.clientX) || 1;
          const scale = tlPinchStartDist / dist; // inverse: spread fingers = zoom in = smaller range

          const origRange = tlPinchViewEnd - tlPinchViewStart;
          const newRange = Math.max(1_000, Math.min(origRange * scale, 600_000_000));

          const start = tlPinchMidTime - tlPinchMidFrac * newRange;
          this.setViewRange(start, start + newRange, true, true);
        } else if (touchPanning && e.touches.length === 1) {
          e.preventDefault();
          const rect = canvas.getBoundingClientRect();
          const x = e.touches[0].clientX - rect.left;
          const y = e.touches[0].clientY - rect.top;
          const dx = x - touchPanStartX;
          const dy = y - touchPanStartY;
          const plotW = this.logicalW - GUTTER_W;
          if (plotW > 0 && dx !== 0) {
            const range = touchPanViewEnd - touchPanViewStart;
            const shift = -(dx / plotW) * range;
            this.setViewRange(touchPanViewStart + shift, touchPanViewEnd + shift, true, true);
          }
          this.nodeScrollPx = touchPanNodeScroll - dy;
          this.clampNodeScroll(this.logicalH - AXIS_H);
        }
      },
      { passive: false },
    );

    canvas.addEventListener("touchend", (e) => {
      if (e.touches.length < 2) tlPinching = false;
      if (e.touches.length === 0) {
        if (touchPanning) {
          // Tap detection: if barely moved, treat as click → navigate
          const rect = canvas.getBoundingClientRect();
          const endX = e.changedTouches[0].clientX - rect.left;
          const endY = e.changedTouches[0].clientY - rect.top;
          if (Math.abs(endX - touchStartX) < 5 && Math.abs(endY - touchStartY) < 5) {
            if (this.isPlaying?.()) {
              this.showWarning(endX);
            } else {
              this.navigateToX(endX);
            }
          }
        }
        touchPanning = false;
      }
    });
  }

  private navigateToX(x: number): void {
    const timeUs = this.xToTime(x);
    // Binary search for nearest history index
    const times = this.historyTimes;
    if (times.length === 0) return;
    let lo = 0,
      hi = times.length - 1;
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
    this.updateTopicRowHover(y, contentH);
    if (y > contentH || x < GUTTER_W) {
      this.tooltip.style.display = "none";
      this.hoveredEvents = [];
      return;
    }

    // Net row hover (node mode only): show msg/s.
    if (this.hasNetRow() && y < ROW_H) {
      const timeUs = this.xToTime(x);
      const rate = this.getNetMsgRate(timeUs);
      const avg = this.getNetMsgRateAvg(timeUs);
      this.tooltip.style.textAlign = "right";
      this.tooltip.innerHTML = `${rate.toFixed(0)} msg/s<br>avg ${avg.toFixed(0)} msg/s`;
      this.tooltip.style.display = "block";
      const rect = this.canvas.parentElement!.getBoundingClientRect();
      this.tooltip.style.left = Math.min(x + 12, rect.width - 100) + "px";
      this.tooltip.style.top = Math.max(0, y - 20) + "px";
      this.hoveredEvents = [];
      return;
    }

    // Hit-test: collect all events within radius
    const hitRadius = 8;
    const hitsByEvent = new Map<number, { ev: TimelineEvent; dist: number }>();

    for (const ev of this.eventLog.events) {
      const ex = this.eventX(ev);
      for (const rowIdx of this.eventRowIndices(ev)) {
        const ey = this.rowCenter(rowIdx);
        if (ey < this.scrollableRowOffset() || ey > contentH) continue;
        const dx = x - ex;
        const dy = y - ey;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < hitRadius) {
          const existing = hitsByEvent.get(ev.id);
          if (!existing || dist < existing.dist) {
            hitsByEvent.set(ev.id, { ev, dist });
          }
        }
      }
    }

    const hits = [...hitsByEvent.values()];
    if (hits.length > 0) {
      hits.sort((a, b) => a.dist - b.dist);
      const evs = hits.map((h) => h.ev);
      // Only update if the set of hovered events changed
      if (evs.length !== this.hoveredEvents.length || evs.some((e, i) => e !== this.hoveredEvents[i])) {
        this.hoveredEvents = evs;
        this.showTooltip(evs, x, y);
      }
    } else {
      // Check arrow hover (node mode only)
      const arrowHit = this.hitTestArrow(x, y);
      if (arrowHit) {
        if (this.hoveredEvents.length !== 1 || this.hoveredEvents[0] !== arrowHit) {
          this.hoveredEvents = [arrowHit];
          this.showArrowTooltip(arrowHit, x, y);
        }
        return;
      }
      this.tooltip.style.display = "none";
      this.hoveredEvents = [];
    }
  }

  private hitTestArrow(x: number, y: number): TimelineEvent | null {
    if (this.mode !== "node") {
      return null;
    }
    const contentH = this.logicalH - AXIS_H;
    const threshold = 6;
    for (const ev of this.eventLog.events) {
      if (ev.receiveIds.length === 0) continue;
      const sx = this.eventX(ev);
      const sRow = this.nodeRowIndex.get(ev.nodeId) ?? -1;
      if (sRow < 0) continue;
      const sy = this.rowCenter(sRow);
      if (sy < ROW_H || sy > contentH) continue;

      for (const rid of ev.receiveIds) {
        const recv = this.eventLog.getById(rid);
        if (!recv) continue;
        const rx = this.eventX(recv);
        const rRow = this.nodeRowIndex.get(recv.nodeId) ?? -1;
        if (rRow < 0) continue;
        const ry = this.rowCenter(rRow);
        if (ry < ROW_H || ry > contentH) continue;

        const dist = this.pointToSegmentDist(x, y, sx, sy, rx, ry);
        if (dist < threshold) return ev;
      }
    }
    return null;
  }

  private pointToSegmentDist(px: number, py: number, x0: number, y0: number, x1: number, y1: number): number {
    const dx = x1 - x0,
      dy = y1 - y0;
    const lenSq = dx * dx + dy * dy;
    if (lenSq === 0) return Math.sqrt((px - x0) ** 2 + (py - y0) ** 2);
    let t = ((px - x0) * dx + (py - y0) * dy) / lenSq;
    t = Math.max(0, Math.min(1, t));
    const cx = x0 + t * dx,
      cy = y0 + t * dy;
    return Math.sqrt((px - cx) ** 2 + (py - cy) ** 2);
  }

  private formatEvent(ev: TimelineEvent): string {
    const name = CODE_NAMES[ev.code];
    const d = ev.details;
    let text = `${ev.code} - ${name}  Node${ev.nodeId}  ${(ev.timeUs / 1_000_000).toFixed(3)}s`;
    if (d.name) text += `  Topic: ${d.name}`;
    if (d.shardIndex !== undefined) text += `  Shard: ${d.shardIndex}`;
    if (Array.isArray(d.listeners)) text += `  Listeners: ${d.listeners.length}`;
    if (d.remote_name) text += `  Remote topic: ${d.remote_name}`;
    if (d.evictions !== undefined) text += `  Evictions: ${d.evictions}`;
    if (d.lage !== undefined) text += `  Lage: ${d.lage}`;
    if (d.dst !== null && d.dst !== undefined) {
      if (ev.code === "GR") text += `  Src: Node${d.dst}`;
      else text += `  Dst: Node${d.dst}`;
    }
    if (d.drop_reason === "ttl") text += "  Dropped: TTL=0";
    else if (d.drop_reason === "dedup") text += "  Dropped: recent dedup";
    else if (d.drop_reason === "ttl+dedup") text += "  Dropped: TTL=0 + recent dedup";
    if (d.type) text += `  Type: ${d.type}`;
    if (d.local_won !== undefined) text += `  Local won: ${d.local_won}`;
    if (d.oldPeer !== undefined) text += `  Old: Node${d.oldPeer}`;
    if (d.newPeer !== undefined) text += `  New: Node${d.newPeer}`;
    if (d.peerIdx !== undefined) text += `  Idx: ${d.peerIdx}`;
    if (d.reason) text += `  Reason: ${d.reason}`;
    return text;
  }

  private showTooltip(evs: TimelineEvent[], x: number, y: number): void {
    const text = evs.map((ev) => this.formatEvent(ev)).join("\n");
    this.tooltip.style.textAlign = "";
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
