// ---------------------------------------------------------------------------
// UI controls & per-node overlay buttons
// ---------------------------------------------------------------------------

import { NodeSnapshot, TopicSnap } from "./types.js";
import { Simulation, topicHash, subjectId } from "./sim.js";
import { SUBJECT_ID_MODULUS, LAGE_MIN, LAGE_MAX } from "./constants.js";
import { Renderer } from "./render.js";
import { Viewport } from "./viewport.js";
import { NodeBlock, NodeBlockCallbacks } from "./node-block.js";

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
  private viewport: Viewport;
  private overlayContainer: HTMLElement;
  private nodeBlocks: Map<number, NodeBlock> = new Map();

  // Topic panel
  private topicPanel: HTMLElement;
  private topicCacheKey = "";
  private topicTableCells: Map<string, HTMLTableCellElement> = new Map(); // "hash36:nodeId" -> td

  // Control elements
  private playBtn!: HTMLButtonElement;
  private speedSlider!: HTMLInputElement;
  private speedLabel!: HTMLSpanElement;
  private addNodeBtn!: HTMLButtonElement;
  private timeDisplay!: HTMLSpanElement;
  private historyDisplay!: HTMLSpanElement;
  private convergenceDisplay!: HTMLSpanElement;
  private seedInput!: HTMLInputElement;

  // State
  playing = true;
  speedMultiplier = 0.1;

  // Callbacks
  onRelayout: (() => void) | null = null;
  onApplySeed: ((seed: number) => void) | null = null;
  onUserInteraction: (() => void) | null = null;
  onFitView: (() => void) | null = null;

  constructor(
    sim: Simulation,
    renderer: Renderer,
    viewport: Viewport,
    topBar: HTMLElement,
    sidePanel: HTMLElement,
    overlayContainer: HTMLElement,
    topicPanel: HTMLElement,
  ) {
    this.sim = sim;
    this.renderer = renderer;
    this.viewport = viewport;
    this.overlayContainer = overlayContainer;
    this.topicPanel = topicPanel;
    this.buildTopBar(topBar);
    this.buildSidePanel(sidePanel);
    this.initTopicPanelResize();

    if (window.innerWidth < 1500) {
      const overlay = document.createElement("div");
      overlay.style.cssText =
        "position:fixed;inset:0;background:rgba(0,0,0,0.75);display:flex;align-items:center;justify-content:center;z-index:9999";
      const box = document.createElement("div");
      box.style.cssText =
        "background:#2a2a2a;border:1px solid #555;border-radius:8px;padding:24px 32px;max-width:360px;text-align:center;color:#e0e0e0;font-family:inherit";
      box.innerHTML =
        '<div style="font-size:14px;font-weight:bold;margin-bottom:12px">Desktop recommended</div>' +
        '<div style="font-size:12px;margin-bottom:18px;color:#bbb">This simulator is designed for desktop and tablet screens. The experience on smaller screens may be suboptimal.</div>';
      const btn = document.createElement("button");
      btn.textContent = "Got it";
      btn.style.cssText =
        "background:#444;color:#e0e0e0;border:1px solid #666;border-radius:4px;padding:6px 20px;cursor:pointer;font-family:inherit;font-size:12px";
      btn.onclick = () => overlay.remove();
      box.appendChild(btn);
      overlay.appendChild(box);
      document.body.appendChild(overlay);
    }
  }

  /** Replace the simulation reference (used on seed reset). Clears all node blocks. */
  setSim(sim: Simulation): void {
    this.sim = sim;
    for (const block of this.nodeBlocks.values()) block.el.remove();
    this.nodeBlocks.clear();
    this.topicCacheKey = "";
    const table = this.topicPanel.querySelector("table");
    if (table) table.remove();
  }

  setSeedDisplay(seed: number): void {
    this.seedInput.value = String(seed >>> 0);
  }

  private updatePlayBtn(): void {
    this.playBtn.innerHTML = this.playing
      ? '<span style="display:inline-flex;align-items:center;gap:4px"><span style="font-size:14px;line-height:1">⏸</span>Pause\u2004</span>'
      : '<span style="display:inline-flex;align-items:center;gap:4px"><span style="font-size:14px;line-height:1">▶</span>Resume</span>';
    this.playBtn.style.background = this.playing ? "#6b2020" : "#1a5c2a";
  }

  private buildTopBar(bar: HTMLElement): void {
    this.playBtn = this.btn("Resume/Pause", "");
    this.playBtn.style.width = "90px";
    this.updatePlayBtn();
    const togglePlay = () => {
      this.playing = !this.playing;
      this.updatePlayBtn();
    };
    this.playBtn.addEventListener("click", togglePlay);

    document.addEventListener("keydown", (e) => {
      if (e.code === "Space" && !(e.target instanceof HTMLInputElement)) {
        e.preventDefault();
        togglePlay();
      }
    });

    const speedSteps = [0.001, 0.01, 0.1, 1];
    const defaultIdx = speedSteps.indexOf(0.1);
    this.speedSlider = document.createElement("input");
    this.speedSlider.type = "range";
    this.speedSlider.min = "0";
    this.speedSlider.max = String(speedSteps.length - 1);
    this.speedSlider.value = String(defaultIdx);
    this.speedSlider.step = "1";
    this.speedSlider.style.width = "120px";
    this.speedSlider.style.verticalAlign = "middle";
    this.speedSlider.addEventListener("input", () => {
      const idx = parseInt(this.speedSlider.value);
      this.speedMultiplier = speedSteps[idx];
      this.speedLabel.textContent = "x" + this.speedMultiplier;
    });

    this.speedLabel = document.createElement("span");
    this.speedLabel.textContent = "x" + this.speedMultiplier;
    this.speedLabel.style.marginLeft = "4px";
    this.speedLabel.style.minWidth = "60px";
    this.speedLabel.style.display = "inline-block";

    this.addNodeBtn = this.btn("Add Node", "+ Node");
    this.addNodeBtn.addEventListener("click", () => {
      this.sim.addNode();
      this.onRelayout?.();
      this.onUserInteraction?.();
    });

    this.timeDisplay = document.createElement("span");
    this.timeDisplay.style.marginLeft = "12px";
    this.timeDisplay.style.fontWeight = "bold";

    this.historyDisplay = document.createElement("span");
    this.historyDisplay.style.marginLeft = "12px";
    this.historyDisplay.style.fontSize = "11px";
    this.historyDisplay.style.color = "#999";

    this.convergenceDisplay = document.createElement("span");
    this.convergenceDisplay.style.marginLeft = "12px";
    this.convergenceDisplay.style.fontWeight = "bold";

    // Seed UI
    const seedLabel = document.createElement("span");
    seedLabel.textContent = "Seed:";
    seedLabel.style.marginLeft = "6px";
    seedLabel.style.fontSize = "12px";

    this.seedInput = document.createElement("input");
    this.seedInput.type = "text";
    this.seedInput.style.width = "90px";
    this.seedInput.style.marginLeft = "4px";
    this.seedInput.style.font = '12px "Ubuntu Mono", monospace';
    this.seedInput.style.background = "#333";
    this.seedInput.style.color = "#eee";
    this.seedInput.style.border = "1px solid #666";
    this.seedInput.style.borderRadius = "3px";
    this.seedInput.style.padding = "2px 4px";

    const applyBtn = this.btn("Apply Seed", "Apply");
    applyBtn.addEventListener("click", () => {
      const val = parseInt(this.seedInput.value);
      if (!isNaN(val)) {
        this.onApplySeed?.(val);
      }
    });

    const speedGroup = document.createElement("span");
    const speedTitle = document.createElement("span");
    speedTitle.textContent = "Speed: ";
    speedGroup.append(speedTitle, this.speedSlider, this.speedLabel);

    // Loss probability slider
    const lossSlider = document.createElement("input");
    lossSlider.type = "range";
    lossSlider.min = "0";
    lossSlider.max = "100";
    lossSlider.value = "0";
    lossSlider.step = "1";
    lossSlider.style.width = "80px";
    lossSlider.style.verticalAlign = "middle";

    const lossLabel = document.createElement("span");
    lossLabel.textContent = "0%";
    lossLabel.style.marginLeft = "4px";
    lossLabel.style.minWidth = "32px";
    lossLabel.style.display = "inline-block";

    lossSlider.addEventListener("input", () => {
      const val = parseInt(lossSlider.value);
      this.sim.net.lossProbability = val / 100;
      lossLabel.textContent = val + "%";
    });

    const lossGroup = document.createElement("span");
    const lossTitle = document.createElement("span");
    lossTitle.textContent = "Message loss: ";
    lossGroup.append(lossTitle, lossSlider, lossLabel);

    const seedGroup = document.createElement("span");
    seedGroup.append(seedLabel, this.seedInput, applyBtn);

    const fitBtn = this.btn("Zoom to Fit", "Fit");
    fitBtn.addEventListener("click", () => this.onFitView?.());

    bar.append(
      this.playBtn,
      this.sep(), speedGroup,
      this.sep(), lossGroup,
      this.sep(), this.addNodeBtn, fitBtn,
      this.sep(), this.timeDisplay, this.historyDisplay, this.convergenceDisplay,
      this.sep(), seedGroup,
    );
  }

  private buildSidePanel(panel: HTMLElement): void {
    // Logo
    const logo = document.createElement("img");
    logo.src = "static/opencyphal-dark.png";
    logo.style.width = "100%";
    logo.style.marginBottom = "12px";
    panel.appendChild(logo);

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

  }

  updateFrame(timeUs: number, snaps: Map<number, NodeSnapshot>, historySize?: number, maxTimeUs?: number): void {
    // Time display
    this.timeDisplay.textContent = `t = ${(timeUs / 1_000_000).toFixed(3)}s`;

    // History display
    if (historySize !== undefined && maxTimeUs !== undefined) {
      const maxT = (maxTimeUs / 1_000_000).toFixed(3);
      this.historyDisplay.textContent = `${historySize} states | max ${maxT}s`;
    }

    // Convergence
    const conv = this.sim.checkConvergenceFromSnaps(snaps);
    this.convergenceDisplay.textContent = `Converged: ${conv ? "YES" : "NO"}`;
    this.convergenceDisplay.style.color = conv ? C_CONVERGED : C_DIVERGED;

    // Update topic view table
    this.updateTopicView(snaps);

    // Update per-node blocks
    this.syncOverlays(snaps, timeUs);
  }

  private syncOverlays(snaps: Map<number, NodeSnapshot>, timeUs: number): void {
    // Remove blocks for destroyed nodes
    for (const [nid, block] of this.nodeBlocks) {
      if (!snaps.has(nid)) {
        block.el.remove();
        this.nodeBlocks.delete(nid);
      }
    }

    const boxSizes = new Map<number, { w: number; h: number }>();
    const minimal = this.viewport.currentZoom < 0.8;

    // Create or update blocks
    for (const [nid, snap] of snaps) {
      const pos = this.renderer.nodePositions.get(nid);
      if (!pos) continue;

      let block = this.nodeBlocks.get(nid);
      if (!block) {
        block = this.createNodeBlock(nid);
        this.overlayContainer.appendChild(block.el);
        this.nodeBlocks.set(nid, block);
      }

      block.setMinimalMode(minimal);
      block.update(snap, timeUs, this.renderer.isNodeInConflict(nid));
      block.setPosition(pos.x, pos.y);
      boxSizes.set(nid, block.getSize());
    }

    this.renderer.setNodeBoxSizes(boxSizes);
  }

  private createNodeBlock(nodeId: number): NodeBlock {
    const callbacks: NodeBlockCallbacks = {
      onPartitionToggle: (nid) => {
        const node = this.sim.nodes.get(nid);
        if (node) {
          const newSet = node.partitionSet === "A" ? "B" : "A";
          this.sim.setPartition(nid, newSet);
          this.onUserInteraction?.();
        }
      },
      onRestart: (nid) => {
        this.sim.restartNode(nid);
        this.onUserInteraction?.();
      },
      onDestroy: (nid) => {
        this.sim.destroyNode(nid);
        const block = this.nodeBlocks.get(nid);
        if (block) {
          block.el.remove();
          this.nodeBlocks.delete(nid);
        }
        this.onRelayout?.();
        this.onUserInteraction?.();
      },
      onAddTopic: (nid) => {
        this.openTopicDialog(nid);
      },
      onDeleteTopic: (nid, hash) => {
        this.sim.destroyTopicOnNode(nid, hash);
        this.onUserInteraction?.();
      },
      onChangeEvictions: (nid, hash, delta) => {
        this.sim.adjustTopicEvictions(nid, hash, delta);
        this.onUserInteraction?.();
      },
      onChangeLage: (nid, hash, delta) => {
        this.sim.adjustTopicLage(nid, hash, delta);
        this.onUserInteraction?.();
      },
      onDragMove: (nid, dx, dy) => {
        const pos = this.renderer.nodePositions.get(nid);
        if (!pos) return;
        pos.x += dx / this.viewport.currentZoom;
        pos.y += dy / this.viewport.currentZoom;
      },
      onTopicHover: (nid, hash) => {
        this.handleNodeBlockTopicHover(nid, hash);
      },
    };
    return new NodeBlock(nodeId, callbacks);
  }

  private openTopicDialog(nodeId: number): void {
    // Remove any existing dialog
    document.querySelector(".topic-dialog")?.remove();

    const dlg = document.createElement("div");
    dlg.className = "topic-dialog";
    dlg.style.cssText = `
      position: fixed; z-index: 1000;
      top: 50%; left: 50%; transform: translate(-50%, -50%);
      background: #2a2a2a; border: 1px solid #666; border-radius: 6px;
      padding: 12px; min-width: 280px;
      font: 12px "Ubuntu Mono", monospace; color: #eee;
      display: flex; flex-direction: column; gap: 8px;
    `;

    const title = document.createElement("div");
    title.textContent = `Add topic to Node${nodeId}`;
    title.style.fontWeight = "bold";
    title.style.marginBottom = "2px";

    // Name input
    const nameRow = document.createElement("div");
    nameRow.style.display = "flex";
    nameRow.style.alignItems = "center";
    nameRow.style.gap = "6px";
    const nameLabel = document.createElement("span");
    nameLabel.textContent = "Name:";
    nameLabel.style.minWidth = "60px";
    const nameInput = document.createElement("input");
    nameInput.type = "text";
    nameInput.placeholder = "(auto)";
    nameInput.style.cssText = `
      flex: 1; font: 12px "Ubuntu Mono", monospace;
      background: #333; color: #eee; border: 1px solid #666;
      border-radius: 3px; padding: 3px 6px;
    `;

    nameRow.append(nameLabel, nameInput);

    // Subject-ID display (updates live)
    const sidRow = document.createElement("div");
    sidRow.style.display = "flex";
    sidRow.style.alignItems = "center";
    sidRow.style.gap = "6px";
    const sidLabel = document.createElement("span");
    sidLabel.textContent = "Subject:";
    sidLabel.style.minWidth = "60px";
    const sidValue = document.createElement("span");
    sidValue.style.color = "#aaa";
    sidValue.textContent = "—";
    sidRow.append(sidLabel, sidValue);

    // Collide button row
    const collideRow = document.createElement("div");
    collideRow.style.display = "flex";
    collideRow.style.alignItems = "center";
    collideRow.style.gap = "6px";
    const collideLabel = document.createElement("span");
    collideLabel.textContent = "Collide:";
    collideLabel.style.minWidth = "60px";
    const collideSidInput = document.createElement("input");
    collideSidInput.type = "text";
    collideSidInput.placeholder = "target subject";
    collideSidInput.style.cssText = `
      width: 80px; font: 12px "Ubuntu Mono", monospace;
      background: #333; color: #eee; border: 1px solid #666;
      border-radius: 3px; padding: 3px 6px;
    `;
    const collideBtn = this.miniBtn("Generate");
    collideBtn.style.background = "#8e44ad";
    collideRow.append(collideLabel, collideSidInput, collideBtn);

    // Evictions input
    const evRow = document.createElement("div");
    evRow.style.display = "flex";
    evRow.style.alignItems = "center";
    evRow.style.gap = "6px";
    const evLabel = document.createElement("span");
    evLabel.textContent = "Evictions:";
    evLabel.style.minWidth = "60px";
    const evInput = document.createElement("input");
    evInput.type = "number";
    evInput.value = "0";
    evInput.min = "0";
    evInput.style.cssText = `
      width: 60px; font: 12px "Ubuntu Mono", monospace;
      background: #333; color: #eee; border: 1px solid #666;
      border-radius: 3px; padding: 3px 6px;
    `;
    evRow.append(evLabel, evInput);

    // Lage input
    const lageRow = document.createElement("div");
    lageRow.style.display = "flex";
    lageRow.style.alignItems = "center";
    lageRow.style.gap = "6px";
    const lageLabel = document.createElement("span");
    lageLabel.textContent = "Lage:";
    lageLabel.style.minWidth = "60px";
    const lageInput = document.createElement("input");
    lageInput.type = "number";
    lageInput.value = String(LAGE_MIN);
    lageInput.min = String(LAGE_MIN);
    lageInput.max = String(LAGE_MAX);
    lageInput.style.cssText = `
      width: 60px; font: 12px "Ubuntu Mono", monospace;
      background: #333; color: #eee; border: 1px solid #666;
      border-radius: 3px; padding: 3px 6px;
    `;
    lageRow.append(lageLabel, lageInput);

    // Buttons
    const btnRow = document.createElement("div");
    btnRow.style.display = "flex";
    btnRow.style.justifyContent = "flex-end";
    btnRow.style.gap = "6px";
    btnRow.style.marginTop = "4px";
    const cancelBtn = this.miniBtn("Cancel");
    cancelBtn.style.padding = "3px 12px";
    const createBtn = this.miniBtn("Create");
    createBtn.style.background = "#2980b9";
    createBtn.style.padding = "3px 12px";
    btnRow.append(cancelBtn, createBtn);

    // Live subject-ID update
    const updateSid = () => {
      const name = nameInput.value.trim();
      const ev = parseInt(evInput.value) || 0;
      if (!name) {
        sidValue.textContent = "—";
        return;
      }
      const h = topicHash(name);
      const sid = subjectId(h, ev, SUBJECT_ID_MODULUS);
      sidValue.textContent = String(sid);
    };
    nameInput.addEventListener("input", updateSid);
    evInput.addEventListener("input", updateSid);

    // Collide: generate a name that maps to the target SID
    collideBtn.addEventListener("click", () => {
      const val = parseInt(collideSidInput.value);
      if (isNaN(val) || val < 0) return;
      // Use sim's findNameForSid via addTopicToNode with targetSid, but we just need the name.
      // Brute-force here to keep it self-contained.
      const target = val;
      const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
      const ev = parseInt(evInput.value) || 0;
      for (let attempt = 0; attempt < 100000; attempt++) {
        let name = "c/";
        for (let i = 0; i < 5; i++) name += alphabet[Math.floor(Math.random() * alphabet.length)];
        const h = topicHash(name);
        if (subjectId(h, ev, SUBJECT_ID_MODULUS) === target) {
          nameInput.value = name;
          updateSid();
          return;
        }
      }
    });

    // Create action
    const doCreate = () => {
      const name = nameInput.value.trim() || undefined;
      const ev = parseInt(evInput.value) || 0;
      const lage = parseInt(lageInput.value);
      const initEv = ev > 0 ? ev : undefined;
      const initLage = !isNaN(lage) && lage !== LAGE_MIN ? lage : undefined;
      this.sim.addTopicToNode(nodeId, name, undefined, initEv, initLage);
      this.onUserInteraction?.();
      dlg.remove();
    };
    createBtn.addEventListener("click", doCreate);
    nameInput.addEventListener("keydown", (e) => { if (e.key === "Enter") doCreate(); });

    cancelBtn.addEventListener("click", () => dlg.remove());
    document.addEventListener("keydown", function esc(e) {
      if (e.key === "Escape") { dlg.remove(); document.removeEventListener("keydown", esc); }
    });

    dlg.append(title, nameRow, sidRow, collideRow, evRow, lageRow, btnRow);
    document.body.appendChild(dlg);
    nameInput.focus();
  }

  private initTopicPanelResize(): void {
    const handle = this.topicPanel.querySelector("#topic-resize") as HTMLElement;
    if (!handle) return;
    let startX = 0, startW = 0;
    const onMove = (e: MouseEvent) => {
      const newW = Math.max(120, startW + (e.clientX - startX));
      this.topicPanel.style.width = newW + "px";
    };
    const onUp = () => {
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };
    handle.addEventListener("mousedown", (e: MouseEvent) => {
      e.preventDefault();
      startX = e.clientX;
      startW = this.topicPanel.offsetWidth;
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    });
  }

  private handleNodeBlockTopicHover(nid: number, hash: bigint | null): void {
    // Clear previous table cell highlights
    for (const cell of this.topicTableCells.values()) {
      cell.classList.remove("cell-highlighted");
    }
    // Clear node block highlights
    for (const block of this.nodeBlocks.values()) {
      block.setHighlighted(false);
      block.highlightTopic(null);
    }
    if (hash === null) return;
    // Highlight the source node block + topic row
    const block = this.nodeBlocks.get(nid);
    if (block) {
      block.setHighlighted(true);
      block.highlightTopic(hash);
    }
    // Highlight the specific cell in the topic table
    const key = `${hash.toString(36)}:${nid}`;
    const cell = this.topicTableCells.get(key);
    if (cell) cell.classList.add("cell-highlighted");
  }

  private updateTopicView(snaps: Map<number, NodeSnapshot>): void {
    // 1. Build matrix: Map<hash, { name, cells: Map<nodeId, TopicSnap> }>
    type TopicRow = { name: string; cells: Map<number, TopicSnap> };
    const matrix = new Map<bigint, TopicRow>();
    const nodeIds: number[] = [];
    for (const [nid, snap] of snaps) {
      if (!snap.online) continue;
      nodeIds.push(nid);
      for (const t of snap.topics) {
        let row = matrix.get(t.hash);
        if (!row) {
          row = { name: t.name, cells: new Map() };
          matrix.set(t.hash, row);
        }
        row.cells.set(nid, t);
      }
    }
    nodeIds.sort((a, b) => a - b);

    // 2. Cache key — skip DOM rebuild if unchanged
    let key = "";
    for (const [hash, row] of matrix) {
      for (const [nid, t] of row.cells) {
        key += `${hash.toString(36)}:${nid}:${t.evictions}:${t.lage}:${t.subjectId},`;
      }
    }
    if (key === this.topicCacheKey) return;
    this.topicCacheKey = key;

    // 3. Detect conflicts (partition-aware, mirrors checkConvergenceImpl)
    // Each conflict gets a group ID; cells in the same group share a color hue.
    let nextGroupId = 0;
    const conflictInfo = new Map<string, { groupId: number; reasons: string[] }>(); // "hash:nodeId"
    const assignGroup = (cells: { hash: bigint; nid: number }[], reason: string) => {
      // Check if any cell already has a group — reuse it so overlapping conflicts merge
      let gid = -1;
      for (const c of cells) {
        const existing = conflictInfo.get(`${c.hash}:${c.nid}`);
        if (existing) { gid = existing.groupId; break; }
      }
      if (gid < 0) gid = nextGroupId++;
      for (const c of cells) {
        const k = `${c.hash}:${c.nid}`;
        let info = conflictInfo.get(k);
        if (!info) { info = { groupId: gid, reasons: [] }; conflictInfo.set(k, info); }
        if (!info.reasons.includes(reason)) info.reasons.push(reason);
      }
    };
    // Group online nodes by partition
    const partitionNodes = new Map<string, number[]>();
    for (const [nid, snap] of snaps) {
      if (!snap.online) continue;
      let group = partitionNodes.get(snap.partitionSet);
      if (!group) { group = []; partitionNodes.set(snap.partitionSet, group); }
      group.push(nid);
    }

    for (const [, pNodes] of partitionNodes) {
      const hashToEvByNode = new Map<bigint, Map<number, number>>();
      const sidToHashes = new Map<number, Map<bigint, number[]>>();

      for (const nid of pNodes) {
        const snap = snaps.get(nid)!;
        for (const t of snap.topics) {
          let evMap = hashToEvByNode.get(t.hash);
          if (!evMap) { evMap = new Map(); hashToEvByNode.set(t.hash, evMap); }
          evMap.set(nid, t.evictions);
          let hMap = sidToHashes.get(t.subjectId);
          if (!hMap) { hMap = new Map(); sidToHashes.set(t.subjectId, hMap); }
          let nList = hMap.get(t.hash);
          if (!nList) { nList = []; hMap.set(t.hash, nList); }
          nList.push(nid);
        }
      }

      // Eviction divergence: same hash, different eviction counts
      for (const [hash, evMap] of hashToEvByNode) {
        const vals = new Set(evMap.values());
        if (vals.size > 1) {
          const detail = [...evMap.entries()].map(([n, e]) => `Node${n}=${e}`).join(", ");
          const cells = [...evMap.keys()].map(nid => ({ hash, nid }));
          assignGroup(cells, `eviction count diverged (${detail})`);
        }
      }
      // Subject-ID collision: same subject, different hashes
      for (const [sid, hashMap] of sidToHashes) {
        if (hashMap.size > 1) {
          const names: string[] = [];
          const cells: { hash: bigint; nid: number }[] = [];
          for (const [h, nids] of hashMap) {
            const row = matrix.get(h);
            names.push(`"${row?.name ?? "?"}" on Node${nids.join(",Node")}`);
            for (const nid of nids) cells.push({ hash: h, nid });
          }
          assignGroup(cells, `subject ${sid} collision: ${names.join(" vs ")}`);
        }
      }
    }

    // Build hue palette: avoid green (~80-160) and yellow/stale zone (~50-80)
    // Usable range: 160-410 (wrapping), i.e. cyan→blue→purple→red→orange
    const groupCount = nextGroupId;
    const groupHues: number[] = [];
    const usableStart = 160, usableRange = 250; // 160..410 (mod 360)
    for (let i = 0; i < groupCount; i++) {
      const hue = (usableStart + (i * 137.508) % usableRange) % 360;
      groupHues.push(hue);
    }

    // 4. Detect staleness: per topic, find max lage; cells with lage < maxLage get yellow
    const staleCells = new Set<string>();
    const topicMaxLage = new Map<bigint, number>();
    for (const [hash, row] of matrix) {
      let maxLage = 0;
      for (const [, t] of row.cells) {
        if (t.lage > maxLage) maxLage = t.lage;
      }
      topicMaxLage.set(hash, maxLage);
      if (maxLage > 0) {
        for (const [nid, t] of row.cells) {
          const k = `${hash}:${nid}`;
          if (t.lage < maxLage && !conflictInfo.has(k)) {
            staleCells.add(k);
          }
        }
      }
    }

    // 5. Build DOM
    this.topicTableCells.clear();
    const clearAllHighlights = () => {
      for (const block of this.nodeBlocks.values()) {
        block.setHighlighted(false);
        block.highlightTopic(null);
      }
      for (const cell of this.topicTableCells.values()) {
        cell.classList.remove("cell-highlighted");
      }
    };

    const highlightNodeAndTopic = (nid: number, hash: bigint | null) => {
      const block = this.nodeBlocks.get(nid);
      if (block) {
        block.setHighlighted(true);
        if (hash !== null) block.highlightTopic(hash);
      }
    };

    const highlightAllForTopic = (hash: bigint, topicRow: TopicRow) => {
      for (const [nid] of topicRow.cells) {
        highlightNodeAndTopic(nid, hash);
      }
    };

    const table = document.createElement("table");
    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");
    const thTopic = document.createElement("th");
    thTopic.textContent = "Topic";
    headerRow.appendChild(thTopic);
    for (const nid of nodeIds) {
      const th = document.createElement("th");
      th.textContent = `Node${nid}`;
      th.addEventListener("mouseenter", () => {
        clearAllHighlights();
        const block = this.nodeBlocks.get(nid);
        if (block) block.setHighlighted(true);
      });
      headerRow.appendChild(th);
    }
    const thConsensus = document.createElement("th");
    thConsensus.textContent = "Consensus";
    headerRow.appendChild(thConsensus);
    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    // Sort topics by earliest tsCreatedUs ascending (oldest on top, newest at bottom)
    const sortedTopics = [...matrix.entries()].sort((a, b) => {
      let minA = Infinity, minB = Infinity;
      for (const t of a[1].cells.values()) if (t.tsCreatedUs < minA) minA = t.tsCreatedUs;
      for (const t of b[1].cells.values()) if (t.tsCreatedUs < minB) minB = t.tsCreatedUs;
      return minA - minB;
    });
    for (const [hash, row] of sortedTopics) {
      const tr = document.createElement("tr");

      // Topic name cell (first col)
      const tdName = document.createElement("td");
      tdName.textContent = row.name.length > 18 ? row.name.slice(0, 18) : row.name;
      tdName.title = row.name;
      tdName.addEventListener("mouseenter", () => {
        clearAllHighlights();
        highlightAllForTopic(hash, row);
      });
      tr.appendChild(tdName);

      for (const nid of nodeIds) {
        const td = document.createElement("td");
        const t = row.cells.get(nid);
        const capturedHash = hash;
        td.addEventListener("mouseenter", () => {
          clearAllHighlights();
          highlightNodeAndTopic(nid, t ? capturedHash : null);
        });
        if (t) {
          const line1 = document.createElement("div");
          line1.className = "cell-line1";
          line1.textContent = `${t.evictions} ${t.lage}`;
          const line2 = document.createElement("div");
          line2.className = "cell-line2";
          line2.textContent = `${t.subjectId}`;
          td.append(line1, line2);
          const k = `${hash}:${nid}`;
          const info = conflictInfo.get(k);
          const base = `evictions: ${t.evictions}, lage: ${t.lage}, subject: ${t.subjectId}`;
          if (info) {
            const hue = groupHues[info.groupId] ?? 0;
            td.style.background = `hsl(${hue}, 60%, 25%)`;
            td.style.borderColor = `hsl(${hue}, 70%, 40%)`;
            td.title = `CONFLICT: ${info.reasons.join("; ")}\n${base}`;
          } else if (staleCells.has(k)) {
            td.className = "cell-stale";
            const ml = topicMaxLage.get(hash) ?? 0;
            td.title = `STALE: lage ${t.lage} behind max ${ml} across nodes\n${base}`;
          } else {
            td.title = base;
          }
        }
        tr.appendChild(td);
        this.topicTableCells.set(`${hash.toString(36)}:${nid}`, td);
      }

      // Consensus cell (last col)
      const tdCons = document.createElement("td");
      let winner: TopicSnap | null = null;
      for (const [, t] of row.cells) {
        if (!winner || t.evictions > winner.evictions) {
          winner = t;
        }
      }
      if (winner) {
        const line1 = document.createElement("div");
        line1.className = "cell-line1";
        line1.textContent = `${winner.evictions} ${winner.lage}`;
        const line2 = document.createElement("div");
        line2.className = "cell-line2";
        line2.textContent = `${winner.subjectId}`;
        tdCons.append(line1, line2);
      }
      tdCons.addEventListener("mouseenter", () => {
        clearAllHighlights();
        highlightAllForTopic(hash, row);
      });
      tr.appendChild(tdCons);

      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    table.addEventListener("mouseleave", () => { clearAllHighlights(); });

    // 6. Replace previous table
    const old = this.topicPanel.querySelector("table");
    if (old) old.remove();
    this.topicPanel.appendChild(table);
  }

  private btn(title: string, label: string): HTMLButtonElement {
    const b = document.createElement("button");
    b.textContent = label;
    b.title = title;
    const defaultBg = "#444";
    b.style.cssText = `
      padding: 0 10px; margin: 0 2px; cursor: pointer;
      background: ${defaultBg}; color: #eee; border: 1px solid #666;
      border-radius: 4px; font: 12px "Ubuntu Mono", monospace;
      height: 26px; box-sizing: border-box;
      display: inline-flex; align-items: center; justify-content: center;
    `;
    b.addEventListener("mouseenter", () => { b.style.filter = "brightness(1.2)"; });
    b.addEventListener("mouseleave", () => { b.style.filter = ""; });
    return b;
  }

  private miniBtn(label: string, className?: string): HTMLButtonElement {
    const b = document.createElement("button");
    b.textContent = label;
    if (className) b.className = className;
    b.style.cssText = `
      padding: 1px 5px; cursor: pointer; font: 10px "Ubuntu Mono", monospace;
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
