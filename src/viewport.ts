// ---------------------------------------------------------------------------
// Viewport — zoom, pan, coordinate transforms
// ---------------------------------------------------------------------------

export interface ViewportState {
  panX: number;
  panY: number;
  zoom: number;
}

const ZOOM_MIN = 0.15;
const ZOOM_MAX = 2.0;
const ZOOM_SPEED = 0.002;

export class Viewport {
  panX = 0;
  panY = 0;
  zoom = 1;

  private wrapper: HTMLElement | null = null;
  private container: HTMLElement | null = null;

  get currentZoom(): number { return this.zoom; }

  attach(container: HTMLElement): void {
    this.container = container;

    // Wheel zoom (centered on cursor)
    container.addEventListener("wheel", (e) => {
      e.preventDefault();
      const rect = container.getBoundingClientRect();
      const mx = e.clientX - rect.left;
      const my = e.clientY - rect.top;

      const oldZoom = this.zoom;
      const delta = -e.deltaY * ZOOM_SPEED;
      this.zoom = Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, this.zoom * (1 + delta)));

      // Adjust pan so point under cursor stays fixed
      const scale = this.zoom / oldZoom;
      this.panX = mx - scale * (mx - this.panX);
      this.panY = my - scale * (my - this.panY);

      this.applyToWrapper();
    }, { passive: false });

    // Left-click, middle-click, or Ctrl+drag pan
    let panning = false;
    let startX = 0, startY = 0, startPanX = 0, startPanY = 0;

    container.addEventListener("mousedown", (e) => {
      if (e.button === 1 || e.button === 0) {
        e.preventDefault();
        panning = true;
        startX = e.clientX;
        startY = e.clientY;
        startPanX = this.panX;
        startPanY = this.panY;
      }
    });

    const onMove = (e: MouseEvent) => {
      if (!panning) return;
      this.panX = startPanX + (e.clientX - startX);
      this.panY = startPanY + (e.clientY - startY);
      this.applyToWrapper();
    };

    const onUp = () => { panning = false; };

    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);

    // Touch: one-finger pan, two-finger pinch-zoom
    let touchPanning = false;
    let touchStartX = 0, touchStartY = 0, touchStartPanX = 0, touchStartPanY = 0;
    let pinching = false;
    let pinchStartDist = 0;
    let pinchStartZoom = 0;
    let pinchStartPanX = 0, pinchStartPanY = 0;
    let pinchMidX = 0, pinchMidY = 0;

    container.addEventListener("touchstart", (e) => {
      if (e.touches.length === 2) {
        e.preventDefault();
        touchPanning = false;
        pinching = true;
        const rect = container.getBoundingClientRect();
        const t0 = e.touches[0], t1 = e.touches[1];
        const dx = t1.clientX - t0.clientX, dy = t1.clientY - t0.clientY;
        pinchStartDist = Math.sqrt(dx * dx + dy * dy);
        pinchStartZoom = this.zoom;
        pinchStartPanX = this.panX;
        pinchStartPanY = this.panY;
        pinchMidX = (t0.clientX + t1.clientX) / 2 - rect.left;
        pinchMidY = (t0.clientY + t1.clientY) / 2 - rect.top;
      } else if (e.touches.length === 1 && !pinching) {
        e.preventDefault();
        touchPanning = true;
        touchStartX = e.touches[0].clientX;
        touchStartY = e.touches[0].clientY;
        touchStartPanX = this.panX;
        touchStartPanY = this.panY;
      }
    }, { passive: false });

    container.addEventListener("touchmove", (e) => {
      if (pinching && e.touches.length >= 2) {
        e.preventDefault();
        const rect = container.getBoundingClientRect();
        const t0 = e.touches[0], t1 = e.touches[1];
        const dx = t1.clientX - t0.clientX, dy = t1.clientY - t0.clientY;
        const dist = Math.sqrt(dx * dx + dy * dy);
        const scale = dist / pinchStartDist;
        const newZoom = Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, pinchStartZoom * scale));
        const zoomRatio = newZoom / pinchStartZoom;

        // New midpoint for simultaneous pan
        const midX = (t0.clientX + t1.clientX) / 2 - rect.left;
        const midY = (t0.clientY + t1.clientY) / 2 - rect.top;

        // Zoom around original midpoint, then shift by midpoint movement
        this.panX = pinchStartPanX + (midX - pinchMidX) - (zoomRatio - 1) * (pinchMidX - pinchStartPanX);
        this.panY = pinchStartPanY + (midY - pinchMidY) - (zoomRatio - 1) * (pinchMidY - pinchStartPanY);
        this.zoom = newZoom;
        this.applyToWrapper();
      } else if (touchPanning && e.touches.length === 1) {
        e.preventDefault();
        this.panX = touchStartPanX + (e.touches[0].clientX - touchStartX);
        this.panY = touchStartPanY + (e.touches[0].clientY - touchStartY);
        this.applyToWrapper();
      }
    }, { passive: false });

    container.addEventListener("touchend", (e) => {
      if (e.touches.length < 2) pinching = false;
      if (e.touches.length === 0) touchPanning = false;
    });
  }

  setWrapper(wrapper: HTMLElement): void {
    this.wrapper = wrapper;
  }

  applyToCanvas(ctx: CanvasRenderingContext2D, dpr: number): void {
    ctx.setTransform(dpr * this.zoom, 0, 0, dpr * this.zoom, dpr * this.panX, dpr * this.panY);
  }

  applyToWrapper(): void {
    if (!this.wrapper) return;
    this.wrapper.style.transform = `translate(${this.panX}px, ${this.panY}px) scale(${this.zoom})`;
  }

  screenToWorld(sx: number, sy: number): { x: number; y: number } {
    return {
      x: (sx - this.panX) / this.zoom,
      y: (sy - this.panY) / this.zoom,
    };
  }

  zoomToFit(
    nodePositions: Map<number, { x: number; y: number }>,
    boxSizes: Map<number, { w: number; h: number }>,
    vpW: number, vpH: number,
    padding = 80,
  ): void {
    if (nodePositions.size === 0) return;

    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    for (const [nid, pos] of nodePositions) {
      const sz = boxSizes.get(nid) || { w: 260, h: 300 };
      const hw = sz.w / 2, hh = sz.h / 2;
      if (pos.x - hw < minX) minX = pos.x - hw;
      if (pos.y - hh < minY) minY = pos.y - hh;
      if (pos.x + hw > maxX) maxX = pos.x + hw;
      if (pos.y + hh > maxY) maxY = pos.y + hh;
    }

    const bw = maxX - minX + padding * 2;
    const bh = maxY - minY + padding * 2;
    const scaleX = vpW / bw;
    const scaleY = vpH / bh;
    this.zoom = Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, Math.min(scaleX, scaleY)));

    const cx = (minX + maxX) / 2;
    const cy = (minY + maxY) / 2;
    this.panX = vpW / 2 - cx * this.zoom;
    this.panY = vpH / 2 - cy * this.zoom;

    this.applyToWrapper();
  }
}
