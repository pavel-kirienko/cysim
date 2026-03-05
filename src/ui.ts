// ---------------------------------------------------------------------------
// UI controls & per-node overlay buttons
// ---------------------------------------------------------------------------

import { NodeSnapshot, TopicSnap } from "./types.js";
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

  // Topic panel
  private topicPanel: HTMLElement;
  private topicCacheKey = "";

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
  playing = false;
  speedMultiplier = 0.1;

  // Callbacks
  onRelayout: (() => void) | null = null;
  onApplySeed: ((seed: number) => void) | null = null;
  onUserInteraction: (() => void) | null = null;

  constructor(
    sim: Simulation,
    renderer: Renderer,
    topBar: HTMLElement,
    sidePanel: HTMLElement,
    overlayContainer: HTMLElement,
    topicPanel: HTMLElement,
  ) {
    this.sim = sim;
    this.renderer = renderer;
    this.overlayContainer = overlayContainer;
    this.topicPanel = topicPanel;
    this.buildTopBar(topBar);
    this.buildSidePanel(sidePanel);
    this.initTopicPanelResize();
  }

  /** Replace the simulation reference (used on seed reset). Clears all overlays. */
  setSim(sim: Simulation): void {
    this.sim = sim;
    for (const el of this.overlays.values()) el.remove();
    this.overlays.clear();
    this.prevTopicKeys.clear();
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

    bar.append(
      this.playBtn,
      this.sep(), speedGroup,
      this.sep(), lossGroup,
      this.sep(), this.addNodeBtn,
      this.sep(), this.timeDisplay, this.historyDisplay, this.convergenceDisplay,
      this.sep(), seedGroup,
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
        this.onUserInteraction?.();
      }
    });

    const restartBtn = this.miniBtn("Restart");
    restartBtn.addEventListener("click", () => {
      this.sim.restartNode(nodeId);
      this.onUserInteraction?.();
    });

    const destroyBtn = this.miniBtn("Destroy");
    destroyBtn.style.background = "#c0392b";
    destroyBtn.addEventListener("click", () => {
      this.sim.destroyNode(nodeId);
      el.remove();
      this.overlays.delete(nodeId);
      this.prevTopicKeys.delete(nodeId);
      this.onRelayout?.();
      this.onUserInteraction?.();
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
      this.onUserInteraction?.();
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
      this.onUserInteraction?.();
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
        this.onUserInteraction?.();
      });

      row.append(nameBtn, rmBtn);
      container.appendChild(row);
    }
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
          const detail = [...evMap.entries()].map(([n, e]) => `N${n}=${e}`).join(", ");
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
            names.push(`"${row?.name ?? "?"}" on N${nids.join(",N")}`);
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
    const table = document.createElement("table");
    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");
    const thTopic = document.createElement("th");
    thTopic.textContent = "Topic";
    headerRow.appendChild(thTopic);
    for (const nid of nodeIds) {
      const th = document.createElement("th");
      th.textContent = `N${nid}`;
      headerRow.appendChild(th);
    }
    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    // Sort topics by max lage descending (oldest on top)
    const sortedTopics = [...matrix.entries()].sort((a, b) => {
      const la = topicMaxLage.get(a[0]) ?? 0;
      const lb = topicMaxLage.get(b[0]) ?? 0;
      if (lb !== la) return lb - la;
      return a[1].name.localeCompare(b[1].name);
    });
    for (const [hash, row] of sortedTopics) {
      const tr = document.createElement("tr");
      const tdName = document.createElement("td");
      tdName.textContent = row.name.length > 18 ? row.name.slice(0, 18) : row.name;
      tdName.title = row.name;
      tr.appendChild(tdName);
      for (const nid of nodeIds) {
        const td = document.createElement("td");
        const t = row.cells.get(nid);
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
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);

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
