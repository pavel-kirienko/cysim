// ---------------------------------------------------------------------------
// UI controls & per-node overlay buttons
// ---------------------------------------------------------------------------

import { NodeSnapshot, TopicSnap } from "./types.js";
import { Simulation, topicHash, subjectId, topicLage } from "./sim.js";
import { SUBJECT_ID_MODULUS, LAGE_MIN, LAGE_MAX } from "./constants.js";
import { Renderer } from "./render.js";
import { Viewport } from "./viewport.js";
import { NodeBlock, NodeBlockCallbacks } from "./node-block.js";
import { Timeline } from "./timeline.js";

// Colors for legend
const C_BROADCAST = "#f1c40f";
const C_UNICAST   = "#e67e22";
const C_FORWARD   = "#9b59b6";
const C_PEER_FRESH = "#27ae60";
const C_PEER_STALE = "#95a5a6";
const C_CONVERGED = "#27ae60";
const C_DIVERGED  = "#c0392b";

/** JSON.stringify with leaf containers (no nested arrays/objects) on a single line. */
function compactJSON(obj: any, indent = 2): string {
  function isPrimitive(v: any): boolean {
    return v === null || typeof v !== "object";
  }
  function isLeaf(v: any): boolean {
    if (isPrimitive(v)) return true;
    if (Array.isArray(v)) return v.every(isPrimitive);
    return Object.values(v).every(isPrimitive);
  }
  function fmt(v: any, depth: number): string {
    if (v === null || typeof v !== "object") return JSON.stringify(v);
    const pad = " ".repeat(depth * indent);
    const inner = " ".repeat((depth + 1) * indent);
    if (Array.isArray(v)) {
      if (v.length === 0) return "[]";
      if (isLeaf(v)) return "[ " + v.map(e => fmt(e, 0)).join(", ") + " ]";
      return "[\n" + v.map(e => inner + fmt(e, depth + 1)).join(",\n") + "\n" + pad + "]";
    }
    const keys = Object.keys(v);
    if (keys.length === 0) return "{}";
    if (isLeaf(v)) return "{ " + keys.map(k => JSON.stringify(k) + ": " + fmt(v[k], 0)).join(", ") + " }";
    return "{\n" + keys.map(k => inner + JSON.stringify(k) + ": " + fmt(v[k], depth + 1)).join(",\n") + "\n" + pad + "}";
  }
  return fmt(obj, 0);
}

function genTopicName(index: number): string {
  const letter = String.fromCharCode(97 + (index % 26));
  return index < 26 ? "topic/" + letter : "topic/" + letter + Math.floor(index / 26);
}

function findCollidingPair(targetSid: number): [string, string] {
  const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
  const names: string[] = [];
  for (let attempt = 0; names.length < 2 && attempt < 100000; attempt++) {
    let name = "";
    for (let i = 0; i < 4; i++) name += alphabet[Math.random() * alphabet.length | 0];
    const hash = topicHash(name);
    if (subjectId(hash, 0, SUBJECT_ID_MODULUS) === targetSid) {
      if (names.length === 0 || name !== names[0]) names.push(name);
    }
  }
  return [names[0] ?? "collide_a", names[1] ?? "collide_b"];
}

const EDITOR_TEXT_CSS = "margin:0;padding:6px;font:11px/1.3 'Ubuntu Mono',monospace;white-space:pre;tab-size:2;letter-spacing:normal;word-spacing:normal;text-rendering:auto";

function escapeHTML(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;");
}

function highlightJSON(src: string): string {
  let out = "";
  let i = 0;
  while (i < src.length) {
    const ch = src[i];
    if (ch === '"') {
      let j = i + 1;
      while (j < src.length && src[j] !== '"') {
        if (src[j] === '\\') j++;
        j++;
      }
      j++; // closing quote
      const raw = escapeHTML(src.slice(i, j));
      let k = j;
      while (k < src.length && (src[k] === ' ' || src[k] === '\t')) k++;
      if (src[k] === ':') {
        out += '<span class="jk">' + raw + '</span>';
      } else {
        out += '<span class="js">' + raw + '</span>';
      }
      i = j;
    } else if (ch === '-' || (ch >= '0' && ch <= '9')) {
      let j = i;
      if (src[j] === '-') j++;
      while (j < src.length && ((src[j] >= '0' && src[j] <= '9') || src[j] === '.' || src[j] === 'e' || src[j] === 'E' || src[j] === '+' || src[j] === '-')) j++;
      if (j > i + (src[i] === '-' ? 1 : 0)) {
        out += '<span class="jn">' + escapeHTML(src.slice(i, j)) + '</span>';
        i = j;
      } else {
        out += escapeHTML(ch);
        i++;
      }
    } else if (src.startsWith("true", i) || src.startsWith("false", i) || src.startsWith("null", i)) {
      const word = src.startsWith("true", i) ? "true" : src.startsWith("false", i) ? "false" : "null";
      out += '<span class="jb">' + word + '</span>';
      i += word.length;
    } else {
      out += escapeHTML(ch);
      i++;
    }
  }
  return out;
}

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
  private timeline: Timeline | null = null;

  // Focus state
  private stickyTopicHash: bigint | null = null;
  private hoverTopicHash: bigint | null = null;
  private focusedTopicName: string | null = null;
  private statusBar: HTMLElement;

  // Control elements
  private playBtn!: HTMLButtonElement;
  private speedSlider!: HTMLInputElement;
  private speedLabel!: HTMLSpanElement;
  private addNodeBtn!: HTMLButtonElement;
  private timeDisplay!: HTMLSpanElement;
  private historyDisplay!: HTMLSpanElement;
  private convergenceDisplay!: HTMLSpanElement;
  private configTextarea!: HTMLTextAreaElement;
  private configPre!: HTMLPreElement;

  // State
  playing = true;
  speedMultiplier = 0.1;

  // Callbacks
  onRelayout: (() => void) | null = null;
  onApplyConfig: ((config: any) => void) | null = null;
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
    this.statusBar = document.getElementById("status-bar")!;
    this.buildTopBar(topBar);
    this.buildSidePanel(sidePanel);
    this.initTopicPanelResize();
    this.initSidePanelResize();

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
    // Clear focus state so stale hashes don't persist into the new sim
    this.stickyTopicHash = null;
    this.hoverTopicHash = null;
    this.focusedTopicName = null;
    this.statusBar.textContent = "";
  }

  setTimeline(tl: Timeline): void {
    this.timeline = tl;
  }

  get focusedTopic(): bigint | null {
    return this.hoverTopicHash ?? this.stickyTopicHash;
  }

  private updatePlayBtn(): void {
    this.playBtn.innerHTML = this.playing
      ? '<span style="display:inline-flex;align-items:center;gap:4px"><span style="font-size:14px;line-height:1">⏸</span>Pause\u2004</span>'
      : '<span style="display:inline-flex;align-items:center;gap:4px"><span style="font-size:14px;line-height:1">▶</span>Resume</span>';
    this.playBtn.style.background = this.playing ? "#6b2020" : "#1a5c2a";
  }

  private buildTopBar(bar: HTMLElement): void {
    this.playBtn = this.btn("Resume/Pause\n[space bar]", "");
    this.playBtn.style.width = "90px";
    this.updatePlayBtn();
    const togglePlay = () => {
      this.playing = !this.playing;
      this.updatePlayBtn();
    };
    this.playBtn.addEventListener("click", togglePlay);

    document.addEventListener("keydown", (e) => {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      if (e.code === "Space") {
        e.preventDefault();
        togglePlay();
      } else if (e.code === "ArrowLeft" || e.code === "ArrowRight") {
        e.preventDefault();
        if (this.playing) {
          this.timeline?.pan(e.code === "ArrowRight" ? 1 : -1);
        } else {
          this.timeline?.stepToEvent(e.code === "ArrowRight" ? 1 : -1);
        }
      } else if (e.key === "+" || e.key === "=" || e.key === "-") {
        e.preventDefault();
        this.timeline?.zoom(e.key === "-" ? -1 : 1);
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

    this.addNodeBtn = this.btn("Add node", "Add node");
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
    this.historyDisplay.style.color = "#fff";

    this.convergenceDisplay = document.createElement("span");
    this.convergenceDisplay.style.marginLeft = "12px";
    this.convergenceDisplay.style.fontWeight = "bold";

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

    const fitBtn = this.btn("Zoom to fit", "Fit");
    fitBtn.addEventListener("click", () => this.onFitView?.());

    bar.append(
      this.playBtn,
      this.sep(), speedGroup,
      this.sep(), lossGroup,
      this.sep(), this.addNodeBtn, fitBtn,
      this.sep(), this.timeDisplay, this.historyDisplay, this.convergenceDisplay,
    );
  }

  private buildSidePanel(panel: HTMLElement): void {
    // Logo
    const logo = document.createElement("img");
    logo.src = "static/opencyphal-dark.png";
    logo.style.cssText = "display:block;width:100%;max-width:150px;height:auto;margin:0 auto";
    logo.style.marginBottom = "4px";
    panel.appendChild(logo);

    const ghLink = document.createElement("a");
    ghLink.href = "https://github.com/pavel-kirienko/gerasim";
    ghLink.target = "_blank";
    ghLink.rel = "noopener noreferrer";
    ghLink.addEventListener("click", (e) => {
      e.preventDefault();
      window.open(ghLink.href, "_blank", "noopener,noreferrer");
    });
    ghLink.innerHTML = "Sources &amp; issues on GitHub &#x29C9;";
    ghLink.style.fontSize = "11px";
    ghLink.style.color = "#f7941d";
    ghLink.style.textDecoration = "underline";
    ghLink.style.display = "block";
    ghLink.style.textAlign = "center";
    ghLink.style.marginBottom = "12px";
    panel.appendChild(ghLink);

    // Legend
    const legendTitle = document.createElement("div");
    legendTitle.textContent = "Legend";
    legendTitle.style.fontWeight = "bold";
    legendTitle.style.marginBottom = "6px";
    legendTitle.style.fontSize = "13px";
    panel.appendChild(legendTitle);

    const legendItems: [string, string, string][] = [
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

    // Network Config editor
    const configTitle = document.createElement("div");
    configTitle.textContent = "Network config";
    configTitle.style.fontWeight = "bold";
    configTitle.style.marginTop = "12px";
    configTitle.style.marginBottom = "6px";
    configTitle.style.fontSize = "13px";
    panel.appendChild(configTitle);

    const editorWrap = document.createElement("div");
    editorWrap.style.cssText = "position:relative;flex:1;min-height:120px;overflow:hidden;border:1px solid #444;border-radius:3px;background:#1e1e1e";

    const pre = document.createElement("pre");
    pre.style.cssText = EDITOR_TEXT_CSS + ";position:absolute;top:0;left:0;right:0;bottom:0;overflow:auto;color:#d4d4d4;pointer-events:none;z-index:0";
    pre.setAttribute("aria-hidden", "true");
    this.configPre = pre;

    const ta = document.createElement("textarea");
    ta.style.cssText = EDITOR_TEXT_CSS + ";position:absolute;top:0;left:0;width:100%;height:100%;color:transparent;caret-color:#d4d4d4;background:transparent;border:none;outline:none;resize:none;overflow:auto;z-index:1";
    ta.spellcheck = false;
    ta.addEventListener("input", () => this.syncHighlight());
    ta.addEventListener("scroll", () => {
      pre.scrollTop = ta.scrollTop;
      pre.scrollLeft = ta.scrollLeft;
    });
    this.configTextarea = ta;

    editorWrap.append(pre, ta);
    panel.appendChild(editorWrap);

    // Buttons
    const btnRow = document.createElement("div");
    btnRow.style.cssText = "display:flex;gap:6px;margin-top:6px;flex-shrink:0";

    const captureBtn = document.createElement("button");
    captureBtn.textContent = "Capture";
    captureBtn.style.cssText = "flex:1;padding:4px 0;font:12px 'Ubuntu Mono',monospace;background:#2980b9;color:#fff;border:1px solid #666;border-radius:3px;cursor:pointer";
    captureBtn.addEventListener("click", () => this.captureConfig());

    const applyConfigBtn = document.createElement("button");
    applyConfigBtn.textContent = "Apply";
    applyConfigBtn.style.cssText = "flex:1;padding:4px 0;font:12px 'Ubuntu Mono',monospace;background:#27ae60;color:#fff;border:1px solid #666;border-radius:3px;cursor:pointer";
    applyConfigBtn.addEventListener("click", () => {
      try {
        const config = JSON.parse(this.configTextarea.value);
        if (typeof config.seed !== "number") throw new Error("seed must be a number");
        if (!Array.isArray(config.nodes)) throw new Error("nodes must be an array");
        this.onApplyConfig?.(config);
      } catch (e: any) {
        alert("Invalid config: " + e.message);
      }
    });

    const generateBtn = document.createElement("button");
    generateBtn.textContent = "Generate\u2026";
    generateBtn.style.cssText = "flex:1;padding:4px 0;font:12px 'Ubuntu Mono',monospace;background:#8e44ad;color:#fff;border:1px solid #666;border-radius:3px;cursor:pointer";
    generateBtn.addEventListener("click", () => this.showGenerateDialog());

    btnRow.append(captureBtn, applyConfigBtn, generateBtn);
    panel.appendChild(btnRow);

    // Populate editor with current config on load
    this.captureConfig();
  }

  updateFrame(timeUs: number, snaps: Map<number, NodeSnapshot>, historySize?: number, maxTimeUs?: number, rewound = false): void {
    // Propagate focus state
    this.renderer.stickyTopicHash = this.stickyTopicHash;
    this.renderer.hoverTopicHash = this.hoverTopicHash;
    if (this.timeline) {
      this.timeline.stickyTopicHash = this.stickyTopicHash;
      this.timeline.hoverTopicHash = this.hoverTopicHash;
    }

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

    // Status bar: show rewind warning when paused at a rewound position with no other message
    const anyFocused = this.stickyTopicHash !== null || this.hoverTopicHash !== null;
    if (rewound && !this.playing && !anyFocused) {
      this.statusBar.textContent = "Rewound — resuming playback will erase history after this point";
    } else if (!anyFocused) {
      this.statusBar.textContent = "";
    }

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
    const minimal = this.viewport.currentZoom < 0.4;

    // Create or update blocks
    for (const [nid, snap] of snaps) {
      const pos = this.renderer.nodePositions.get(nid);
      if (!pos) continue;

      let block = this.nodeBlocks.get(nid);
      if (!block) {
        block = this.createNodeBlock(nid);
        block.onHover = (id) => { this.renderer.hoveredNodeId = id; };
        this.overlayContainer.appendChild(block.el);
        this.nodeBlocks.set(nid, block);
      }

      block.setMinimalMode(minimal);
      block.update(snap, timeUs, this.renderer.isNodeInConflict(nid), this.renderer.getPeerFlashIndices(nid));
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
      onTopicHover: (nid, hash, name) => {
        this.handleNodeBlockTopicHover(nid, hash, name);
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
    title.textContent = `Add topic to node ${nodeId}`;
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
    sidValue.style.color = "#fff";
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
    const handle = document.getElementById("topic-resize");
    if (!handle) return;
    let startX = 0, startW = 0;
    const onMove = (e: MouseEvent) => {
      const newW = Math.max(120, startW + (e.clientX - startX));
      this.topicPanel.style.width = newW + "px";
      window.dispatchEvent(new Event("resize"));
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

  private initSidePanelResize(): void {
    const handle = document.getElementById("side-resize");
    if (!handle) return;
    const panel = handle.parentElement!;
    let startX = 0, startW = 0;
    const onMove = (e: MouseEvent) => {
      const newW = Math.min(600, Math.max(200, startW - (e.clientX - startX)));
      panel.style.width = newW + "px";
      window.dispatchEvent(new Event("resize"));
    };
    const onUp = () => {
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };
    handle.addEventListener("mousedown", (e: MouseEvent) => {
      e.preventDefault();
      startX = e.clientX;
      startW = panel.offsetWidth;
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    });
  }

  captureConfig(): void {
    const nodeIds = [...this.sim.nodes.keys()].sort((a, b) => a - b);
    const nodes = nodeIds.map(nid => {
      const node = this.sim.nodes.get(nid)!;
      const topics = [...node.topics.values()].map(t => {
        const entry: any = { name: t.name };
        if (t.evictions !== 0) entry.evictions = t.evictions;
        const lage = topicLage(t.tsCreatedUs, this.sim.nowUs);
        if (lage !== LAGE_MIN) entry.lage = lage;
        return entry;
      });
      return topics.length > 0 ? { topics } : {};
    });
    const obj: any = {
      seed: this.sim.seed >>> 0,
      network: {
        delay_us: this.sim.net.delayUs,
        loss_probability: this.sim.net.lossProbability,
      },
      nodes,
    };
    this.configTextarea.value = compactJSON(obj);
    this.syncHighlight();
  }

  private syncHighlight(): void {
    this.configPre.innerHTML = highlightJSON(this.configTextarea.value);
  }

  private showGenerateDialog(): void {
    const overlay = document.createElement("div");
    overlay.style.cssText = "position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.5);display:flex;align-items:center;justify-content:center;z-index:10000";

    const box = document.createElement("div");
    box.style.cssText = "background:#2a2a2a;border:1px solid #444;border-radius:6px;padding:16px;min-width:260px";

    const title = document.createElement("div");
    title.textContent = "Generate network config";
    title.style.cssText = "font:bold 13px 'Ubuntu Mono',monospace;color:#fff;margin-bottom:12px";
    box.appendChild(title);

    const makeField = (label: string, def: string, numeric?: boolean): HTMLInputElement => {
      const row = document.createElement("div");
      row.style.cssText = "margin-bottom:8px;display:flex;align-items:center;justify-content:space-between";
      const lbl = document.createElement("label");
      lbl.textContent = label;
      lbl.style.cssText = "font:11px 'Ubuntu Mono',monospace;color:#ccc";
      const inp = document.createElement("input");
      inp.type = "text";
      inp.inputMode = "numeric";
      inp.value = def;
      inp.style.cssText = "width:100px;padding:2px 4px;font:11px 'Ubuntu Mono',monospace;background:#1a1a1a;color:#fff;border:1px solid #555;border-radius:3px;text-align:right";
      if (numeric) inp.addEventListener("input", () => { inp.value = inp.value.replace(/[^0-9]/g, ""); });
      row.append(lbl, inp);
      box.appendChild(row);
      return inp;
    };

    const curSeed = this.sim.seed >>> 0;
    const curDelay = this.sim.net.delayUs;
    const curLoss = this.sim.net.lossProbability;
    const curNodes = this.sim.nodes.size;

    const seedInput = makeField("Seed", String(curSeed), true);
    const nodesInput = makeField("Nodes", String(curNodes), true);
    const topicsInput = makeField("Topics", "6", true);
    const collidingInput = makeField("Colliding topics", "2", true);
    const delayInput = makeField("Delay (µs)", `${curDelay[0]}, ${curDelay[1]}`);
    const lossInput = makeField("Loss probability", String(curLoss));

    const btnRow = document.createElement("div");
    btnRow.style.cssText = "display:flex;gap:8px;margin-top:12px";

    const genBtn = document.createElement("button");
    genBtn.textContent = "Generate";
    genBtn.style.cssText = "flex:1;padding:4px 0;font:12px 'Ubuntu Mono',monospace;background:#8e44ad;color:#fff;border:1px solid #666;border-radius:3px;cursor:pointer";
    genBtn.addEventListener("click", () => {
      const nc = Math.max(1, parseInt(nodesInput.value) || 1);
      const tc = Math.max(0, parseInt(topicsInput.value) || 0);
      const cc = Math.max(0, parseInt(collidingInput.value) || 0);
      const seed = (parseInt(seedInput.value) || curSeed) >>> 0;
      const delayParts = delayInput.value.split(",").map(s => parseInt(s.trim()));
      const delay: [number, number] = [
        delayParts[0] > 0 ? delayParts[0] : curDelay[0],
        (delayParts[1] ?? delayParts[0]) > 0 ? (delayParts[1] ?? delayParts[0]) : curDelay[1],
      ];
      const loss = Math.max(0, Math.min(1, parseFloat(lossInput.value) || curLoss));
      overlay.remove();
      this.generateConfig(nc, tc, cc, seed, delay, loss);
    });

    const cancelBtn = document.createElement("button");
    cancelBtn.textContent = "Cancel";
    cancelBtn.style.cssText = "flex:1;padding:4px 0;font:12px 'Ubuntu Mono',monospace;background:#555;color:#fff;border:1px solid #666;border-radius:3px;cursor:pointer";
    cancelBtn.addEventListener("click", () => overlay.remove());

    btnRow.append(genBtn, cancelBtn);
    box.appendChild(btnRow);
    box.addEventListener("keydown", (e: KeyboardEvent) => {
      if (e.key === "Enter") { e.preventDefault(); genBtn.click(); }
    });
    overlay.appendChild(box);
    overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
    document.body.appendChild(overlay);
  }

  private generateConfig(
    nodeCount: number, topicCount: number, collidingCount: number,
    seed: number, delay: [number, number], loss: number,
  ): void {

    // Build per-node topic arrays
    const nodes: { topics: { name: string }[] }[] = [];
    for (let i = 0; i < nodeCount; i++) nodes.push({ topics: [] });

    const randNode = () => Math.random() * nodeCount | 0;
    const pickN = (count: number): number[] => {
      const picked = new Set<number>();
      while (picked.size < count) picked.add(randNode());
      return [...picked];
    };

    // Distribute regular topics: each assigned to 1–min(3, nodeCount) random nodes
    const maxFanout = Math.min(3, nodeCount);
    for (let i = 0; i < topicCount; i++) {
      const name = genTopicName(i);
      const fanout = 1 + (Math.random() * maxFanout | 0); // 1..maxFanout inclusive
      for (const n of pickN(fanout)) nodes[n].topics.push({ name });
    }

    // Generate and distribute colliding topic groups
    for (let g = 0; g < collidingCount; g++) {
      const targetSid = 10000 + g;
      const [nameA, nameB] = findCollidingPair(targetSid);
      // Shuffle node indices and split into two groups for the two names
      const indices = Array.from({ length: nodeCount }, (_, i) => i);
      for (let i = indices.length - 1; i > 0; i--) {
        const j = Math.random() * (i + 1) | 0;
        [indices[i], indices[j]] = [indices[j], indices[i]];
      }
      const half = Math.max(1, nodeCount >> 1);
      for (let i = 0; i < nodeCount; i++) {
        nodes[indices[i]].topics.push({ name: i < half ? nameA : nameB });
      }
    }

    const obj: any = {
      seed: seed >>> 0,
      network: {
        delay_us: [delay[0], delay[1]],
        loss_probability: loss,
      },
      nodes: nodes.map(n => n.topics.length > 0 ? { topics: n.topics } : {}),
    };
    this.configTextarea.value = compactJSON(obj);
    this.syncHighlight();
  }

  private handleNodeBlockTopicHover(nid: number, hash: bigint | null, name: string | null): void {
    // Clear previous table cell highlights
    for (const cell of this.topicTableCells.values()) {
      cell.classList.remove("cell-highlighted");
    }
    // Clear node block highlights
    for (const block of this.nodeBlocks.values()) {
      block.setHighlighted(false);
      block.highlightTopic(null);
    }
    this.hoverTopicHash = hash;
    if (hash === null) {
      if (!this.stickyTopicHash) {
        this.focusedTopicName = null;
        this.statusBar.textContent = "";
      }
      return;
    }
    this.focusedTopicName = name;
    if (name) this.statusBar.textContent = `Highlighting events related to topic "${name}"`;
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
    const hoverHighlightedCells: HTMLElement[] = [];
    const hoverHighlightedRows: HTMLTableRowElement[] = [];
    const hoverHighlightedColCells: HTMLElement[] = [];
    const stickyHighlightedCells: HTMLElement[] = [];
    const stickyHighlightedRows: HTMLTableRowElement[] = [];
    const stickyHighlightedColCells: HTMLElement[] = [];

    const clearHoverHighlight = () => {
      for (const cell of hoverHighlightedCells) cell.classList.remove("cell-highlighted");
      hoverHighlightedCells.length = 0;
      for (const r of hoverHighlightedRows) r.classList.remove("row-highlight");
      hoverHighlightedRows.length = 0;
      for (const c of hoverHighlightedColCells) c.classList.remove("col-highlight");
      hoverHighlightedColCells.length = 0;
      this.hoverTopicHash = null;
    };

    const clearStickyHighlight = () => {
      for (const cell of stickyHighlightedCells) cell.classList.remove("cell-sticky");
      stickyHighlightedCells.length = 0;
      for (const r of stickyHighlightedRows) r.classList.remove("row-highlight");
      stickyHighlightedRows.length = 0;
      for (const c of stickyHighlightedColCells) c.classList.remove("col-highlight");
      stickyHighlightedColCells.length = 0;
      this.stickyTopicHash = null;
    };

    const clearAllHighlights = () => {
      for (const block of this.nodeBlocks.values()) {
        block.setHighlighted(false);
        block.highlightTopic(null);
      }
      clearHoverHighlight();
      clearStickyHighlight();
      this.focusedTopicName = null;
      this.statusBar.textContent = "";
    };

    const highlightNodeAndTopic = (nid: number, hash: bigint | null) => {
      const block = this.nodeBlocks.get(nid);
      if (block) {
        block.setHighlighted(true);
        if (hash !== null) block.highlightTopic(hash);
      }
    };

    // Lookup helper: find topic row data by hash from sortedTopics (built below)
    const topicRowByHash = matrix;

    const applyStickyForTopic = (hash: bigint) => {
      const topicRow = topicRowByHash.get(hash);
      if (!topicRow) return;
      for (const [nid] of topicRow.cells) {
        highlightNodeAndTopic(nid, hash);
      }
      // Mark cells with sticky class
      for (const [nid] of topicRow.cells) {
        const key = `${hash.toString(36)}:${nid}`;
        const cell = this.topicTableCells.get(key);
        if (cell) {
          cell.classList.add("cell-sticky");
          stickyHighlightedCells.push(cell);
        }
      }
    };

    const applyHoverForTopic = (hash: bigint) => {
      const topicRow = topicRowByHash.get(hash);
      if (!topicRow) return;
      for (const [nid] of topicRow.cells) {
        highlightNodeAndTopic(nid, hash);
      }
      // Mark cells with highlighted class
      for (const [nid] of topicRow.cells) {
        const key = `${hash.toString(36)}:${nid}`;
        const cell = this.topicTableCells.get(key);
        if (cell) {
          cell.classList.add("cell-highlighted");
          hoverHighlightedCells.push(cell);
        }
      }
    };

    let stickyActive = false;

    const table = document.createElement("table");
    const caption = document.createElement("caption");
    caption.textContent = "Distributed topic allocation table";
    table.appendChild(caption);
    const columnCells = new Map<number, HTMLElement[]>();
    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");
    const thTopic = document.createElement("th");
    thTopic.textContent = "Topic";
    headerRow.appendChild(thTopic);
    columnCells.set(0, [thTopic]);
    for (let i = 0; i < nodeIds.length; i++) {
      const nid = nodeIds[i];
      const colIdx = i + 1;
      const th = document.createElement("th");
      th.textContent = `Node${nid}`;
      th.addEventListener("mouseenter", () => {
        if (stickyActive) return;
        clearAllHighlights();
        const block = this.nodeBlocks.get(nid);
        if (block) block.setHighlighted(true);
      });
      th.addEventListener("click", () => {
        if (stickyActive) {
          stickyActive = false;
          clearAllHighlights();
          const block = this.nodeBlocks.get(nid);
          if (block) block.setHighlighted(true);
        } else {
          stickyActive = true;
        }
      });
      headerRow.appendChild(th);
      columnCells.set(colIdx, [th]);
    }
    const consColIdx = nodeIds.length + 1;
    const thConsensus = document.createElement("th");
    thConsensus.textContent = "Consensus";
    headerRow.appendChild(thConsensus);
    columnCells.set(consColIdx, [thConsensus]);
    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    // Sort topics by earliest sortOrder ascending (oldest on top, newest at bottom)
    const sortedTopics = [...matrix.entries()].sort((a, b) => {
      let minA = Infinity, minB = Infinity;
      for (const t of a[1].cells.values()) if (t.sortOrder < minA) minA = t.sortOrder;
      for (const t of b[1].cells.values()) if (t.sortOrder < minB) minB = t.sortOrder;
      return minA - minB;
    });
    const applyRowColHighlight = (tr: HTMLTableRowElement, colIdx: number, isSticky: boolean) => {
      const rows = isSticky ? stickyHighlightedRows : hoverHighlightedRows;
      const cols = isSticky ? stickyHighlightedColCells : hoverHighlightedColCells;
      tr.classList.add("row-highlight");
      rows.push(tr);
      const col = columnCells.get(colIdx);
      if (col) for (const c of col) {
        c.classList.add("col-highlight");
        cols.push(c);
      }
    };

    const applyTopicHover = (hash: bigint, row: TopicRow, tr: HTMLTableRowElement, colIdx: number) => {
      clearHoverHighlight();
      this.hoverTopicHash = hash;
      applyHoverForTopic(hash);
      applyRowColHighlight(tr, colIdx, false);
      this.focusedTopicName = row.name;
      this.statusBar.textContent = `Highlighting events related to topic "${row.name}"`;
    };

    const handleTopicMouseenter = (hash: bigint, row: TopicRow, tr: HTMLTableRowElement, colIdx: number) => {
      if (stickyActive) {
        applyTopicHover(hash, row, tr, colIdx);
        return;
      }
      clearAllHighlights();
      this.hoverTopicHash = hash;
      applyHoverForTopic(hash);
      applyRowColHighlight(tr, colIdx, false);
      this.focusedTopicName = row.name;
      this.statusBar.textContent = `Highlighting events related to topic "${row.name}"`;
    };

    const handleTopicClick = (hash: bigint, row: TopicRow, tr: HTMLTableRowElement, colIdx: number) => {
      if (stickyActive) {
        if (this.stickyTopicHash === hash) {
          // Clicking same topic: unstick, keep as hover
          stickyActive = false;
          clearStickyHighlight();
          // Re-apply as hover-only
          clearHoverHighlight();
          for (const block of this.nodeBlocks.values()) {
            block.setHighlighted(false);
            block.highlightTopic(null);
          }
          this.hoverTopicHash = hash;
          applyHoverForTopic(hash);
          applyRowColHighlight(tr, colIdx, false);
          this.focusedTopicName = row.name;
          this.statusBar.textContent = `Highlighting events related to topic "${row.name}"`;
        } else {
          // Clicking different topic: move sticky to new topic
          clearStickyHighlight();
          clearHoverHighlight();
          for (const block of this.nodeBlocks.values()) {
            block.setHighlighted(false);
            block.highlightTopic(null);
          }
          this.stickyTopicHash = hash;
          applyStickyForTopic(hash);
          applyRowColHighlight(tr, colIdx, true);
          this.focusedTopicName = row.name;
          this.statusBar.textContent = `Sticky highlight: topic "${row.name}"`;
        }
      } else {
        // Activate sticky
        stickyActive = true;
        this.stickyTopicHash = hash;
        // Convert current hover highlight to sticky
        clearHoverHighlight();
        for (const block of this.nodeBlocks.values()) {
          block.setHighlighted(false);
          block.highlightTopic(null);
        }
        applyStickyForTopic(hash);
        applyRowColHighlight(tr, colIdx, true);
        this.focusedTopicName = row.name;
        this.statusBar.textContent = `Sticky highlight: topic "${row.name}"`;
      }
    };

    for (const [hash, row] of sortedTopics) {
      const tr = document.createElement("tr");

      // Topic name cell (first col)
      const tdName = document.createElement("td");
      tdName.textContent = row.name.length > 18 ? row.name.slice(0, 18) : row.name;
      tdName.title = row.name;
      tdName.addEventListener("mouseenter", () => handleTopicMouseenter(hash, row, tr, 0));
      tdName.addEventListener("click", () => handleTopicClick(hash, row, tr, 0));
      tr.appendChild(tdName);
      columnCells.get(0)!.push(tdName);

      for (let i = 0; i < nodeIds.length; i++) {
        const nid = nodeIds[i];
        const colIdx = i + 1;
        const td = document.createElement("td");
        const t = row.cells.get(nid);
        const capturedHash = hash;
        td.addEventListener("mouseenter", () => handleTopicMouseenter(capturedHash, row, tr, colIdx));
        td.addEventListener("click", () => handleTopicClick(capturedHash, row, tr, colIdx));
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
        columnCells.get(colIdx)!.push(td);
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
      tdCons.addEventListener("mouseenter", () => handleTopicMouseenter(hash, row, tr, consColIdx));
      tdCons.addEventListener("click", () => handleTopicClick(hash, row, tr, consColIdx));
      tr.appendChild(tdCons);
      columnCells.get(consColIdx)!.push(tdCons);

      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    table.addEventListener("mouseleave", () => {
      if (stickyActive) {
        clearHoverHighlight();
      } else {
        clearAllHighlights();
      }
    });

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
