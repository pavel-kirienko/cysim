// ---------------------------------------------------------------------------
// Canvas 2D renderer — port of cysim/viz.py MatplotlibRenderer
// ---------------------------------------------------------------------------

import { EventRecord, NodeSnapshot } from "./types.js";
import {
  MSG_PERSIST_US, BROADCAST_PERSIST_US, CONFLICT_FLASH_US,
  PROPAGATION_SPEED,
} from "./constants.js";
import { Viewport } from "./viewport.js";

// Colors
const C_BG          = "#000000";
const C_BROADCAST   = "#f1c40f"; // yellow expanding circle
const C_UNICAST     = "#e67e22";
const C_FORWARD     = "#9b59b6";

const BOX_WIDTH     = 280;

interface ActiveArrow {
  startUs: number;
  arriveUs: number;   // when message reaches destination
  expireUs: number;   // when arrow disappears (arriveUs + MSG_PERSIST_US)
  event: EventRecord;
}

interface ActiveBroadcast {
  expireUs: number;
  startUs: number;
  src: number;
  event: EventRecord;
}

export class Renderer {
  private canvas: HTMLCanvasElement;
  private ctx: CanvasRenderingContext2D;
  private tooltip: HTMLElement | null = null;
  private viewport: Viewport;
  nodePositions: Map<number, { x: number; y: number }> = new Map();
  private externalBoxSizes: Map<number, { w: number; h: number }> = new Map();
  private lastSnaps: Map<number, NodeSnapshot> = new Map();
  private lastTimeUs = 0;

  private activeArrows: ActiveArrow[] = [];
  private activeBroadcasts: ActiveBroadcast[] = [];
  private activeConflicts: Map<number, number> = new Map(); // nodeId -> flashUntilUs

  private get logicalW(): number { return this.canvas.width / (window.devicePixelRatio || 1); }
  private get logicalH(): number { return this.canvas.height / (window.devicePixelRatio || 1); }

  constructor(canvas: HTMLCanvasElement, viewport: Viewport, tooltip?: HTMLElement) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d")!;
    this.viewport = viewport;
    if (tooltip) {
      this.tooltip = tooltip;
      this.setupInteraction();
    }
  }

  setNodeBoxSizes(sizes: Map<number, { w: number; h: number }>): void {
    this.externalBoxSizes = sizes;
  }

  private getBoxSize(nid: number): { w: number; h: number } {
    return this.externalBoxSizes.get(nid) || { w: BOX_WIDTH, h: 200 };
  }

  isNodeInConflict(nid: number): boolean {
    const until = this.activeConflicts.get(nid);
    return until !== undefined && until >= this.lastTimeUs;
  }

  clearAnimations(): void {
    this.activeArrows = [];
    this.activeBroadcasts = [];
    this.activeConflicts.clear();
  }

  layoutNodes(nodeIds: number[]): void {
    const n = nodeIds.length;
    if (n === 0) return;
    // Circumference-based radius, centered at world origin (0,0)
    const radius = Math.max(400, n * 320 / (2 * Math.PI));
    this.nodePositions.clear();
    for (let i = 0; i < n; i++) {
      const angle = (2 * Math.PI * i) / n - Math.PI / 2;
      this.nodePositions.set(nodeIds[i], {
        x: radius * Math.cos(angle),
        y: radius * Math.sin(angle),
      });
    }
  }

  render(
    timeUs: number,
    snaps: Map<number, NodeSnapshot>,
    newEvents: EventRecord[],
  ): void {
    this.lastSnaps = snaps;
    this.lastTimeUs = timeUs;
    const ctx = this.ctx;
    const dpr = window.devicePixelRatio || 1;
    const W = this.logicalW;
    const H = this.logicalH;

    // Clear with identity transform
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.fillStyle = C_BG;
    ctx.fillRect(0, 0, W, H);

    // Apply viewport transform
    this.viewport.applyToCanvas(ctx, dpr);

    // Ingest new events
    for (const ev of newEvents) {
      if (ev.event === "broadcast") {
        this.activeBroadcasts.push({
          startUs: timeUs,
          expireUs: timeUs + BROADCAST_PERSIST_US,
          src: ev.src,
          event: ev,
        });
      } else if (ev.event === "unicast" || ev.event === "forward") {
        const delayUs = (ev.details?.delayUs as number) || 500_000;
        this.activeArrows.push({
          startUs: timeUs,
          arriveUs: timeUs + delayUs,
          expireUs: timeUs + delayUs + MSG_PERSIST_US,
          event: ev,
        });
      }
      if (ev.event === "conflict") {
        this.activeConflicts.set(ev.src, timeUs + CONFLICT_FLASH_US);
      }
    }
    this.activeArrows = this.activeArrows.filter(m => m.expireUs > timeUs);
    this.activeBroadcasts = this.activeBroadcasts.filter(m => m.expireUs > timeUs);

    // Draw broadcast circles (behind everything)
    this.drawBroadcasts(ctx, timeUs);

    // Draw unicast/forward arrows (behind nodes)
    this.drawArrows(ctx, timeUs, snaps);
  }

  // -- Info box helper --

  private drawInfoBox(
    ctx: CanvasRenderingContext2D,
    x: number, y: number,
    lines: string[],
    textColor: string,
    alpha: number,
  ): void {
    ctx.save();
    ctx.globalAlpha = alpha;
    ctx.font = "9px monospace";
    const infoLH = 12;
    const pad = 4;
    let maxW = 0;
    for (const line of lines) {
      const m = ctx.measureText(line).width;
      if (m > maxW) maxW = m;
    }
    const boxW = maxW + pad * 2;
    const boxH = lines.length * infoLH + pad * 2;

    ctx.fillStyle = "rgba(30,30,30,0.85)";
    ctx.strokeStyle = textColor;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.roundRect(x, y, boxW, boxH, 3);
    ctx.fill();
    ctx.stroke();

    ctx.fillStyle = textColor;
    ctx.textAlign = "left";
    ctx.textBaseline = "top";
    for (let i = 0; i < lines.length; i++) {
      ctx.fillText(lines[i], x + pad, y + pad + i * infoLH);
    }
    ctx.restore();
  }

  // -- Broadcast circles --

  private drawBroadcasts(ctx: CanvasRenderingContext2D, timeUs: number): void {
    for (const bc of this.activeBroadcasts) {
      const srcPos = this.nodePositions.get(bc.src);
      if (!srcPos) continue;

      const elapsed = timeUs - bc.startUs;
      const frac = elapsed / BROADCAST_PERSIST_US; // 0→1
      // Radius expands at PROPAGATION_SPEED px per sim-second
      const radius = PROPAGATION_SPEED * (elapsed / 1_000_000);
      // Slow fade: stay bright for first 60%, then fade out
      const alpha = frac < 0.6 ? 0.8 : Math.max(0, 0.8 * (1 - (frac - 0.6) / 0.4));
      const lw = 3.0 - frac * 2.0;

      ctx.save();
      ctx.globalAlpha = alpha;
      ctx.strokeStyle = C_BROADCAST;
      ctx.lineWidth = Math.max(0.5, lw);
      ctx.beginPath();
      ctx.arc(srcPos.x, srcPos.y, radius, 0, 2 * Math.PI);
      ctx.stroke();
      ctx.restore();

      // Info box near source node
      const d = bc.event.details || {};
      const bName = (d.name as string) || "?";
      const bSid = d.subjectId as number ?? "?";
      const bEv = d.evictions as number ?? "?";
      const bLage = d.lage as number ?? "?";
      const infoLines = [
        `${bName}  S=${bSid}`,
        `ev=${bEv}  lage=${bLage}`,
      ];
      const srcBox = this.getBoxSize(bc.src);
      this.drawInfoBox(ctx, srcPos.x + srcBox.w / 2 + 10, srcPos.y - 20, infoLines, C_BROADCAST, alpha);
    }
  }

  // -- Unicast/forward arrows --

  private drawArrows(
    ctx: CanvasRenderingContext2D, timeUs: number,
    _snaps: Map<number, NodeSnapshot>,
  ): void {
    for (const msg of this.activeArrows) {
      const ev = msg.event;

      let color: string, lineWidth: number, dashed: boolean;
      if (ev.event === "unicast") {
        color = C_UNICAST; lineWidth = 2.0; dashed = false;
      } else {
        color = C_FORWARD; lineWidth = 1.5; dashed = true;
      }

      const srcPos = this.nodePositions.get(ev.src);
      if (!srcPos || ev.dst === null) continue;

      const dstPos = this.nodePositions.get(ev.dst);
      if (!dstPos) continue;

      const srcBox = this.getBoxSize(ev.src);
      const dstBox = this.getBoxSize(ev.dst);
      const [x0, y0] = this.edgePoint(srcPos.x, srcPos.y, srcBox.w, srcBox.h, dstPos.x, dstPos.y);
      const [x1, y1] = this.edgePoint(dstPos.x, dstPos.y, dstBox.w, dstBox.h, srcPos.x, srcPos.y);

      // Bezier control point (slight curve)
      const mx = (x0 + x1) / 2, my = (y0 + y1) / 2;
      const dx = x1 - x0, dy = y1 - y0;
      const len = Math.sqrt(dx * dx + dy * dy);
      const off = len * 0.08;
      const nx = -dy / (len || 1), ny = dx / (len || 1);
      const cpx = mx + nx * off, cpy = my + ny * off;

      let alpha: number;
      let headX: number, headY: number;

      if (timeUs < msg.arriveUs) {
        // Travel phase: arrow head moves along the curve
        const travelDuration = msg.arriveUs - msg.startUs;
        const travelFrac = Math.min(1, (timeUs - msg.startUs) / (travelDuration || 1));
        alpha = 0.9;
        [headX, headY] = this.drawPartialBezierArrow(
          ctx, x0, y0, cpx, cpy, x1, y1, travelFrac,
          color, lineWidth, alpha, dashed,
        );
      } else {
        // Linger phase: full arrow, fading
        const lingerTotal = msg.expireUs - msg.arriveUs;
        const lingerElapsed = timeUs - msg.arriveUs;
        const lingerFrac = lingerTotal > 0 ? lingerElapsed / lingerTotal : 1;
        alpha = lingerFrac < 0.7 ? 0.9 : Math.max(0.1, 0.9 * (1 - (lingerFrac - 0.7) / 0.3));
        this.drawArrow(ctx, x0, y0, x1, y1, color, lineWidth, alpha, dashed);
        headX = x1;
        headY = y1;
      }

      // Info box near arrowhead
      const ldx = headX - x0, ldy = headY - y0;
      const llen = Math.sqrt(ldx * ldx + ldy * ldy) || 1;
      const lnx = -ldy / llen, lny = ldx / llen;
      const labelX = headX + lnx * 14, labelY = headY + lny * 14;

      const ad = ev.details || {};
      const aName = (ad.name as string) || "?";
      const aSid = ad.subjectId as number ?? "?";
      const aEv = ad.evictions as number ?? "?";
      const aLage = ad.lage as number ?? "?";
      const aTtl = ad.ttl as number ?? "?";
      const arrowLines = [
        `${aName}  S=${aSid}`,
        `ev=${aEv} lage=${aLage} ttl=${aTtl}`,
      ];
      this.drawInfoBox(ctx, labelX, labelY - 12, arrowLines, color, alpha);
    }
  }

  /** Draw a partial quadratic bezier (0 to frac) with arrowhead at the leading edge.
   *  Uses De Casteljau subdivision to split the curve at parameter frac. */
  private drawPartialBezierArrow(
    ctx: CanvasRenderingContext2D,
    x0: number, y0: number,
    cpx: number, cpy: number,
    x1: number, y1: number,
    frac: number,
    color: string, lineWidth: number, alpha: number, dashed: boolean,
  ): [number, number] {
    // De Casteljau split at frac: sub-curve from 0 to frac
    const q0x = x0 + (cpx - x0) * frac;
    const q0y = y0 + (cpy - y0) * frac;
    const q1x = cpx + (x1 - cpx) * frac;
    const q1y = cpy + (y1 - cpy) * frac;
    const bx = q0x + (q1x - q0x) * frac;
    const by = q0y + (q1y - q0y) * frac;

    ctx.save();
    ctx.globalAlpha = alpha;
    ctx.strokeStyle = color;
    ctx.lineWidth = lineWidth;
    ctx.setLineDash(dashed ? [6, 4] : []);

    // Draw partial curve: P0 -> Q0 (control) -> B(frac)
    ctx.beginPath();
    ctx.moveTo(x0, y0);
    ctx.quadraticCurveTo(q0x, q0y, bx, by);
    ctx.stroke();

    // Arrowhead at (bx, by) in tangent direction (q1 - q0)
    const tdx = q1x - q0x, tdy = q1y - q0y;
    const tlen = Math.sqrt(tdx * tdx + tdy * tdy) || 1;
    const ux = tdx / tlen, uy = tdy / tlen;
    const headLen = 8;
    ctx.setLineDash([]);
    ctx.fillStyle = color;
    ctx.beginPath();
    ctx.moveTo(bx, by);
    ctx.lineTo(bx - ux * headLen - uy * headLen * 0.4,
               by - uy * headLen + ux * headLen * 0.4);
    ctx.lineTo(bx - ux * headLen + uy * headLen * 0.4,
               by - uy * headLen - ux * headLen * 0.4);
    ctx.closePath();
    ctx.fill();

    ctx.restore();
    return [bx, by];
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

  // -- Hover interaction --

  private setupInteraction(): void {
    this.canvas.addEventListener("mousemove", (e) => {
      this.handleHover(e.offsetX, e.offsetY);
    });
    this.canvas.addEventListener("mouseleave", () => {
      if (this.tooltip) this.tooltip.style.display = "none";
    });
  }

  private handleHover(mx: number, my: number): void {
    if (!this.tooltip) return;

    // Convert screen coords to world coords for hit-testing
    const world = this.viewport.screenToWorld(mx, my);
    const wx = world.x, wy = world.y;

    // Hit-test arrows (highest priority — small targets)
    const arrow = this.hitTestArrow(wx, wy);
    if (arrow) {
      this.showTooltip(mx, my, this.formatArrow(arrow));
      return;
    }

    // Hit-test broadcast circles
    const bc = this.hitTestBroadcast(wx, wy);
    if (bc) {
      this.showTooltip(mx, my, this.formatBroadcast(bc));
      return;
    }

    this.tooltip.style.display = "none";
  }

  private hitTestArrow(mx: number, my: number): ActiveArrow | null {
    const threshold = 8;
    for (const msg of this.activeArrows) {
      const ev = msg.event;
      const srcPos = this.nodePositions.get(ev.src);
      if (!srcPos || ev.dst === null) continue;
      const dstPos = this.nodePositions.get(ev.dst);
      if (!dstPos) continue;

      const srcBox = this.getBoxSize(ev.src);
      const dstBox = this.getBoxSize(ev.dst);
      const [x0, y0] = this.edgePoint(srcPos.x, srcPos.y, srcBox.w, srcBox.h, dstPos.x, dstPos.y);
      const [x1, y1] = this.edgePoint(dstPos.x, dstPos.y, dstBox.w, dstBox.h, srcPos.x, srcPos.y);

      // Sample the bezier curve and check distance
      const midX = (x0 + x1) / 2, midY = (y0 + y1) / 2;
      const dx = x1 - x0, dy = y1 - y0;
      const len = Math.sqrt(dx * dx + dy * dy);
      const off = len * 0.08;
      const nx = -dy / (len || 1), ny = dx / (len || 1);
      const cpx = midX + nx * off, cpy = midY + ny * off;

      if (this.distToBezier(mx, my, x0, y0, cpx, cpy, x1, y1) < threshold) {
        return msg;
      }
    }
    return null;
  }

  private hitTestBroadcast(mx: number, my: number): ActiveBroadcast | null {
    const threshold = 10;
    for (const bc of this.activeBroadcasts) {
      const srcPos = this.nodePositions.get(bc.src);
      if (!srcPos) continue;
      const elapsed = this.lastTimeUs - bc.startUs;
      const radius = PROPAGATION_SPEED * (elapsed / 1_000_000);
      const dist = Math.sqrt((mx - srcPos.x) ** 2 + (my - srcPos.y) ** 2);
      if (Math.abs(dist - radius) < threshold) {
        return bc;
      }
    }
    return null;
  }

  private distToBezier(
    px: number, py: number,
    x0: number, y0: number, cpx: number, cpy: number, x1: number, y1: number,
  ): number {
    // Sample 16 points along the quadratic bezier, find min distance
    let minDist = Infinity;
    const N = 16;
    for (let i = 0; i <= N; i++) {
      const t = i / N;
      const u = 1 - t;
      const bx = u * u * x0 + 2 * u * t * cpx + t * t * x1;
      const by = u * u * y0 + 2 * u * t * cpy + t * t * y1;
      const d = Math.sqrt((px - bx) ** 2 + (py - by) ** 2);
      if (d < minDist) minDist = d;
    }
    return minDist;
  }

  private formatArrow(msg: ActiveArrow): string {
    const ev = msg.event;
    const d = ev.details || {};
    const type = ev.event === "unicast" ? "Unicast" : "Forward";
    const delayMs = ((d.delayUs as number) || 0) / 1000;
    const lines = [`${type}  Node${ev.src} → Node${ev.dst}`];
    if (d.name) lines.push(`Topic: ${d.name}`);
    if (d.subjectId !== undefined) lines.push(`Subject: ${d.subjectId}`);
    if (d.evictions !== undefined) lines.push(`Evictions: ${d.evictions}`);
    if (d.lage !== undefined) lines.push(`Lage: ${d.lage}`);
    if (d.ttl !== undefined) lines.push(`TTL: ${d.ttl}`);
    if (delayMs > 0) lines.push(`Delay: ${delayMs.toFixed(1)}ms`);
    return lines.join("\n");
  }

  private formatBroadcast(bc: ActiveBroadcast): string {
    const d = bc.event.details || {};
    const lines = [`Broadcast  Node${bc.src}`];
    if (d.name) lines.push(`Topic: ${d.name}`);
    if (d.subjectId !== undefined) lines.push(`Subject: ${d.subjectId}`);
    if (d.evictions !== undefined) lines.push(`Evictions: ${d.evictions}`);
    if (d.lage !== undefined) lines.push(`Lage: ${d.lage}`);
    return lines.join("\n");
  }

  private showTooltip(mx: number, my: number, text: string): void {
    if (!this.tooltip) return;
    this.tooltip.textContent = text;
    this.tooltip.style.display = "block";
    // Position relative to canvas container
    const containerRect = this.canvas.parentElement!.getBoundingClientRect();
    const tipX = Math.min(mx + 14, containerRect.width - 250);
    const tipY = Math.max(4, my - 20);
    this.tooltip.style.left = tipX + "px";
    this.tooltip.style.top = tipY + "px";
  }
}
