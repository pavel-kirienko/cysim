// ---------------------------------------------------------------------------
// UI controls & per-node overlay buttons
// ---------------------------------------------------------------------------

import { NodeSnapshot } from "./types.js";
import { Simulation } from "./sim.js";
import { Renderer } from "./render.js";

// Colors for legend
const C_ONLINE    = "#d5e8d4";
const C_CONFLICT  = "#f8cecc";
const C_OFFLINE   = "#555555";
const C_BROADCAST = "#f1c40f";
const C_UNICAST   = "#e67e22";
const C_FORWARD   = "#9b59b6";
const C_PEER_FRESH = "#27ae60";
const C_PEER_STALE = "#95a5a6";
const C_CONVERGED = "#27ae60";
const C_DIVERGED  = "#c0392b";

export class UI {
  private sim: Simulation;
  private renderer: Renderer;
  private overlayContainer: HTMLElement;
  private overlays: Map<number, HTMLElement> = new Map();
  // Track previous topic hashes per node to avoid rebuilding buttons every frame
  private prevTopicKeys: Map<number, string> = new Map();

  // Control elements
  private playBtn!: HTMLButtonElement;
  private stepBtn!: HTMLButtonElement;
  private rewindBtn!: HTMLButtonElement;
  private speedSlider!: HTMLInputElement;
  private speedLabel!: HTMLSpanElement;
  private addNodeBtn!: HTMLButtonElement;
  private timeDisplay!: HTMLSpanElement;
  private convergenceDisplay!: HTMLSpanElement;
  private eventCountsEl!: HTMLElement;

  // State
  playing = true;
  speedMultiplier = 1.0;

  // Callbacks
  onRewind: (() => void) | null = null;
  onRelayout: (() => void) | null = null;
  onStepCallback: (() => void) | null = null;

  constructor(
    sim: Simulation,
    renderer: Renderer,
    topBar: HTMLElement,
    sidePanel: HTMLElement,
    overlayContainer: HTMLElement,
  ) {
    this.sim = sim;
    this.renderer = renderer;
    this.overlayContainer = overlayContainer;
    this.buildTopBar(topBar);
    this.buildSidePanel(sidePanel);
  }

  /** Replace the simulation reference (used on rewind). Clears all overlays. */
  setSim(sim: Simulation): void {
    this.sim = sim;
    for (const el of this.overlays.values()) el.remove();
    this.overlays.clear();
    this.prevTopicKeys.clear();
  }

  private buildTopBar(bar: HTMLElement): void {
    this.playBtn = this.btn("Play/Pause", "▶ Play");
    this.playBtn.addEventListener("click", () => {
      this.playing = !this.playing;
      this.playBtn.textContent = this.playing ? "⏸ Pause" : "▶ Play";
    });
    this.playing = true;
    this.playBtn.textContent = "⏸ Pause";

    this.stepBtn = this.btn("Step", "⏭ Step");
    this.stepBtn.addEventListener("click", () => {
      if (!this.playing) {
        this.onStepCallback?.();
      }
    });

    this.rewindBtn = this.btn("Rewind", "⏮ Rewind");
    this.rewindBtn.addEventListener("click", () => {
      this.onRewind?.();
    });

    this.speedSlider = document.createElement("input");
    this.speedSlider.type = "range";
    this.speedSlider.min = "-22";
    this.speedSlider.max = "10";
    this.speedSlider.value = "0";
    this.speedSlider.step = "1";
    this.speedSlider.style.width = "120px";
    this.speedSlider.style.verticalAlign = "middle";
    this.speedSlider.addEventListener("input", () => {
      const val = parseInt(this.speedSlider.value);
      this.speedMultiplier = Math.pow(2, val / 3.33);
      const s = this.speedMultiplier;
      this.speedLabel.textContent = (s < 0.1 ? s.toFixed(3) : s < 1 ? s.toFixed(2) : s.toFixed(1)) + "x";
    });

    this.speedLabel = document.createElement("span");
    this.speedLabel.textContent = "1.0x";
    this.speedLabel.style.marginLeft = "4px";
    this.speedLabel.style.minWidth = "60px";
    this.speedLabel.style.display = "inline-block";

    this.addNodeBtn = this.btn("Add Node", "+ Node");
    this.addNodeBtn.addEventListener("click", () => {
      this.sim.addNode();
      this.onRelayout?.();
    });

    this.timeDisplay = document.createElement("span");
    this.timeDisplay.style.marginLeft = "12px";
    this.timeDisplay.style.fontWeight = "bold";

    this.convergenceDisplay = document.createElement("span");
    this.convergenceDisplay.style.marginLeft = "12px";
    this.convergenceDisplay.style.fontWeight = "bold";

    const speedGroup = document.createElement("span");
    const speedTitle = document.createElement("span");
    speedTitle.textContent = "Speed: ";
    speedGroup.append(speedTitle, this.speedSlider, this.speedLabel);

    bar.append(
      this.playBtn, this.stepBtn, this.rewindBtn,
      this.sep(), speedGroup,
      this.sep(), this.addNodeBtn,
      this.sep(), this.timeDisplay, this.convergenceDisplay,
    );
  }

  private buildSidePanel(panel: HTMLElement): void {
    // Legend
    const legendTitle = document.createElement("div");
    legendTitle.textContent = "Legend";
    legendTitle.style.fontWeight = "bold";
    legendTitle.style.marginBottom = "6px";
    legendTitle.style.fontSize = "13px";
    panel.appendChild(legendTitle);

    const legendItems: [string, string, string][] = [
      [C_ONLINE,    "box",    "Node online"],
      [C_CONFLICT,  "box",    "Node in conflict"],
      [C_OFFLINE,   "box",    "Node offline"],
      [C_BROADCAST, "circle", "Broadcast gossip"],
      [C_UNICAST,   "line",   "Unicast epidemic"],
      [C_FORWARD,   "dash",   "Epidemic forward"],
      [C_PEER_FRESH,"dot",    "Peer (fresh)"],
      [C_PEER_STALE,"dot",    "Peer (stale)"],
    ];

    for (const [color, kind, label] of legendItems) {
      const row = document.createElement("div");
      row.style.display = "flex";
      row.style.alignItems = "center";
      row.style.marginBottom = "3px";

      const swatch = document.createElement("span");
      swatch.style.display = "inline-block";
      swatch.style.marginRight = "8px";
      swatch.style.flexShrink = "0";

      if (kind === "box") {
        swatch.style.width = "14px";
        swatch.style.height = "10px";
        swatch.style.background = color;
        swatch.style.border = "1px solid #888";
        swatch.style.borderRadius = "2px";
      } else if (kind === "circle") {
        swatch.style.width = "14px";
        swatch.style.height = "14px";
        swatch.style.border = `2px solid ${color}`;
        swatch.style.borderRadius = "50%";
      } else if (kind === "line" || kind === "dash") {
        swatch.style.width = "20px";
        swatch.style.height = "0px";
        swatch.style.borderTop = `2px ${kind === "dash" ? "dashed" : "solid"} ${color}`;
      } else {
        swatch.style.width = "8px";
        swatch.style.height = "8px";
        swatch.style.background = color;
        swatch.style.borderRadius = "50%";
      }

      const lbl = document.createElement("span");
      lbl.textContent = label;
      lbl.style.fontSize = "11px";

      row.append(swatch, lbl);
      panel.appendChild(row);
    }

    // Event counts
    const evTitle = document.createElement("div");
    evTitle.textContent = "Event counts";
    evTitle.style.fontWeight = "bold";
    evTitle.style.marginTop = "16px";
    evTitle.style.marginBottom = "6px";
    evTitle.style.fontSize = "13px";
    panel.appendChild(evTitle);

    this.eventCountsEl = document.createElement("div");
    this.eventCountsEl.style.fontSize = "11px";
    this.eventCountsEl.style.fontFamily = "monospace";
    panel.appendChild(this.eventCountsEl);
  }

  updateFrame(timeUs: number, snaps: Map<number, NodeSnapshot>): void {
    // Time display
    this.timeDisplay.textContent = `t = ${(timeUs / 1_000_000).toFixed(2)}s`;

    // Convergence
    const conv = this.sim.checkConvergenceFromSnaps(snaps);
    this.convergenceDisplay.textContent = `Converged: ${conv ? "YES" : "NO"}`;
    this.convergenceDisplay.style.color = conv ? C_CONVERGED : C_DIVERGED;

    // Event counts
    const types = ["broadcast", "unicast", "forward", "conflict", "resolved", "join"];
    const lines = types.map(t => {
      const c = this.sim.eventCounts[t] || 0;
      return `${t.padEnd(12)} ${c}`;
    });
    this.eventCountsEl.textContent = lines.join("\n");
    this.eventCountsEl.style.whiteSpace = "pre";

    // Update per-node overlays
    this.syncOverlays(snaps);
  }

  private syncOverlays(snaps: Map<number, NodeSnapshot>): void {
    // Remove overlays for destroyed nodes
    for (const [nid, el] of this.overlays) {
      if (!snaps.has(nid)) {
        el.remove();
        this.overlays.delete(nid);
        this.prevTopicKeys.delete(nid);
      }
    }

    // Create or update overlays
    for (const [nid, snap] of snaps) {
      const pos = this.renderer.nodePositions.get(nid);
      if (!pos) continue;

      let el = this.overlays.get(nid);
      if (!el) {
        el = this.createOverlay(nid);
        this.overlayContainer.appendChild(el);
        this.overlays.set(nid, el);
      }

      // Position below the node box
      const boxH = 200; // approximate
      el.style.left = (pos.x - 130) + "px";
      el.style.top = (pos.y + boxH / 2 + 4) + "px";

      // Update partition button
      const partBtn = el.querySelector(".part-btn") as HTMLButtonElement;
      if (partBtn) {
        partBtn.textContent = `partition ${snap.partitionSet}`;
        partBtn.style.background = snap.partitionSet === "A" ? "#3498db" : "#e67e22";
      }

      // Only rebuild topic buttons if topic list changed
      const key = snap.topics.map(t => t.hash.toString(36) + ":" + t.evictions).join(",");
      if (this.prevTopicKeys.get(nid) !== key) {
        this.prevTopicKeys.set(nid, key);
        this.rebuildTopicButtons(el, nid, snap);
      }
    }
  }

  private createOverlay(nodeId: number): HTMLElement {
    const el = document.createElement("div");
    el.className = "node-overlay";
    el.style.position = "absolute";
    el.style.display = "flex";
    el.style.gap = "3px";
    el.style.flexWrap = "wrap";
    el.style.maxWidth = "260px";
    el.style.justifyContent = "center";

    const partBtn = this.miniBtn("partition A", "part-btn");
    partBtn.style.background = "#3498db";
    partBtn.addEventListener("click", () => {
      const node = this.sim.nodes.get(nodeId);
      if (node) {
        const newSet = node.partitionSet === "A" ? "B" : "A";
        this.sim.setPartition(nodeId, newSet);
      }
    });

    const restartBtn = this.miniBtn("Restart");
    restartBtn.addEventListener("click", () => {
      this.sim.restartNode(nodeId);
    });

    const destroyBtn = this.miniBtn("Destroy");
    destroyBtn.style.background = "#c0392b";
    destroyBtn.addEventListener("click", () => {
      this.sim.destroyNode(nodeId);
      el.remove();
      this.overlays.delete(nodeId);
      this.prevTopicKeys.delete(nodeId);
      this.onRelayout?.();
    });

    const addTopicBtn = this.miniBtn("+Topic");
    addTopicBtn.style.background = "#2980b9";
    addTopicBtn.addEventListener("click", () => {
      const name = window.prompt("Topic name (leave empty for auto):", "");
      if (name === null) return; // cancelled
      if (name === "") {
        this.sim.addTopicToNode(nodeId);
      } else {
        this.sim.addTopicToNode(nodeId, name);
      }
      this.prevTopicKeys.delete(nodeId); // force rebuild
    });

    const collideBtn = this.miniBtn("+Collide");
    collideBtn.style.background = "#8e44ad";
    collideBtn.addEventListener("click", () => {
      const input = window.prompt("Target subject-ID (decimal) to collide with:", "");
      if (input === null || input === "") return;
      const sid = parseInt(input, 10);
      if (isNaN(sid) || sid < 0) return;
      this.sim.addTopicToNode(nodeId, undefined, sid);
      this.prevTopicKeys.delete(nodeId); // force rebuild
    });

    const topicContainer = document.createElement("div");
    topicContainer.className = "topic-btns";
    topicContainer.style.display = "flex";
    topicContainer.style.gap = "2px";
    topicContainer.style.flexWrap = "wrap";
    topicContainer.style.width = "100%";
    topicContainer.style.justifyContent = "center";

    el.append(partBtn, restartBtn, destroyBtn, addTopicBtn, collideBtn, topicContainer);
    return el;
  }

  /** Rebuild topic buttons — only called when topic list actually changes. */
  private rebuildTopicButtons(el: HTMLElement, nodeId: number, snap: NodeSnapshot): void {
    const container = el.querySelector(".topic-btns")!;
    container.innerHTML = "";
    for (const t of snap.topics) {
      const row = document.createElement("span");
      row.style.display = "inline-flex";
      row.style.gap = "1px";

      // Topic name button — click to copy name to clipboard
      const nameBtn = this.miniBtn(t.name.length > 10 ? t.name.slice(0, 10) : t.name);
      nameBtn.style.background = "#34495e";
      nameBtn.title = `Click to copy: ${t.name}`;
      nameBtn.addEventListener("click", () => {
        navigator.clipboard.writeText(t.name);
        nameBtn.textContent = "copied!";
        setTimeout(() => {
          nameBtn.textContent = t.name.length > 10 ? t.name.slice(0, 10) : t.name;
        }, 800);
      });

      // Remove button
      const rmBtn = this.miniBtn("\u00d7");
      rmBtn.style.background = "#7f8c8d";
      rmBtn.style.fontSize = "9px";
      rmBtn.title = `Remove ${t.name}`;
      rmBtn.addEventListener("click", () => {
        this.sim.destroyTopicOnNode(nodeId, t.hash);
        this.prevTopicKeys.delete(nodeId); // force rebuild next frame
      });

      row.append(nameBtn, rmBtn);
      container.appendChild(row);
    }
  }

  private btn(title: string, label: string): HTMLButtonElement {
    const b = document.createElement("button");
    b.textContent = label;
    b.title = title;
    b.style.cssText = `
      padding: 4px 10px; margin: 0 2px; cursor: pointer;
      background: #444; color: #eee; border: 1px solid #666;
      border-radius: 4px; font-size: 12px;
    `;
    b.addEventListener("mouseenter", () => { b.style.background = "#555"; });
    b.addEventListener("mouseleave", () => { b.style.background = "#444"; });
    return b;
  }

  private miniBtn(label: string, className?: string): HTMLButtonElement {
    const b = document.createElement("button");
    b.textContent = label;
    if (className) b.className = className;
    b.style.cssText = `
      padding: 1px 5px; cursor: pointer; font-size: 10px;
      background: #555; color: #eee; border: 1px solid #777;
      border-radius: 3px;
    `;
    return b;
  }

  private sep(): HTMLSpanElement {
    const s = document.createElement("span");
    s.style.margin = "0 6px";
    s.style.borderLeft = "1px solid #555";
    s.style.height = "20px";
    s.style.display = "inline-block";
    s.style.verticalAlign = "middle";
    return s;
  }
}
