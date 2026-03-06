// ---------------------------------------------------------------------------
// NodeBlock — HTML DOM node block with tables and interactive controls
// ---------------------------------------------------------------------------

import { NodeSnapshot, TopicSnap, PeerSnap } from "./types.js";
import { GOSSIP_PEER_ELIGIBLE } from "./constants.js";

const BOX_WIDTH = 320;

export interface NodeBlockCallbacks {
  onPartitionToggle(nid: number): void;
  onRestart(nid: number): void;
  onDestroy(nid: number): void;
  onAddTopic(nid: number): void;
  onDeleteTopic(nid: number, hash: bigint): void;
  onChangeEvictions(nid: number, hash: bigint, delta: number): void;
  onChangeLage(nid: number, hash: bigint, delta: number): void;
  onDragMove(nid: number, dx: number, dy: number): void;
  onTopicHover(nid: number, hash: bigint | null, name: string | null): void;
}

export class NodeBlock {
  readonly el: HTMLDivElement;
  readonly nodeId: number;
  private callbacks: NodeBlockCallbacks;

  private headerEl: HTMLElement;
  private statusLabel: HTMLSpanElement;
  private partBtn: HTMLButtonElement;
  private statusSection: HTMLElement;
  private topicsBody: HTMLTableSectionElement;
  private peersBody: HTMLTableSectionElement;
  private topicsContainer: HTMLElement;

  private statusCacheKey = "";
  private topicCacheKey = "";
  private peerCacheKey = "";
  private peerCells: [HTMLElement, HTMLElement][] = [];
  onHover: ((nodeId: number | null) => void) | null = null;

  constructor(nodeId: number, callbacks: NodeBlockCallbacks) {
    this.nodeId = nodeId;
    this.callbacks = callbacks;

    this.el = document.createElement("div");
    this.el.className = "node-block";

    // Header
    this.headerEl = document.createElement("div");
    this.headerEl.className = "nb-header";

    const idSpan = document.createElement("span");
    idSpan.textContent = `Node${nodeId}`;
    idSpan.style.fontWeight = "bold";

    this.statusLabel = document.createElement("span");
    this.statusLabel.style.marginLeft = "6px";

    this.partBtn = this.mkBtn("A", "nb-part-btn");
    this.partBtn.addEventListener("click", () => callbacks.onPartitionToggle(nodeId));

    const restartBtn = this.mkBtn("restart", "nb-ctrl-btn");
    restartBtn.addEventListener("click", () => callbacks.onRestart(nodeId));

    const destroyBtn = this.mkBtn("destroy", "nb-ctrl-btn nb-destroy");
    destroyBtn.addEventListener("click", () => callbacks.onDestroy(nodeId));

    const addTopicBtn = this.mkBtn("+topic", "nb-ctrl-btn nb-add-topic");
    addTopicBtn.addEventListener("click", () => callbacks.onAddTopic(nodeId));

    this.headerEl.append(idSpan, this.statusLabel, this.partBtn, restartBtn, destroyBtn, addTopicBtn);
    this.setupDrag();

    this.el.addEventListener("mouseenter", () => this.onHover?.(this.nodeId));
    this.el.addEventListener("mouseleave", () => this.onHover?.(null));

    // Status section
    this.statusSection = document.createElement("div");
    this.statusSection.className = "nb-status";

    // Topics section
    this.topicsContainer = document.createElement("div");
    this.topicsContainer.className = "nb-topics";
    const topicTable = document.createElement("table");
    const topicThead = document.createElement("thead");
    topicThead.innerHTML = "<tr><th>name</th><th>sid</th><th>ev</th><th>lage</th><th></th></tr>";
    this.topicsBody = document.createElement("tbody");
    topicTable.append(topicThead, this.topicsBody);
    this.topicsContainer.appendChild(topicTable);

    // Peers section
    const peersContainer = document.createElement("div");
    peersContainer.className = "nb-peers";
    const peerTable = document.createElement("table");
    this.peersBody = document.createElement("tbody");
    peerTable.appendChild(this.peersBody);
    peersContainer.appendChild(peerTable);

    // Resize handle
    const resizeHandle = document.createElement("div");
    resizeHandle.className = "nb-resize";
    this.setupResize(resizeHandle);

    this.el.append(this.headerEl, this.statusSection, this.topicsContainer, peersContainer, resizeHandle);
  }

  update(snap: NodeSnapshot, timeUs: number, isConflict: boolean, peerFlashIndices?: Set<number>): void {
    // Status label
    const status = snap.online ? "ONLINE" : "OFFLINE";
    this.statusLabel.textContent = status;
    this.statusLabel.style.color = snap.online ? "#8f8" : "#f88";

    // Partition button
    this.partBtn.textContent = `partition ${snap.partitionSet}`;
    this.partBtn.style.background = snap.partitionSet === "A" ? "#3498db" : "#e67e22";

    // Border color for conflict
    if (!snap.online) {
      this.el.style.borderColor = "#555";
      this.el.style.background = "#1a1a1a";
    } else if (isConflict) {
      this.el.style.borderColor = "#e74c3c";
      this.el.style.background = "#3a1a1a";
    } else {
      this.el.style.borderColor = "#555";
      this.el.style.background = "#2a2a2a";
    }

    // Status lines
    let html = "";
    if (snap.online && snap.nextBroadcastUs > 0) {
      const dt = Math.max(0, snap.nextBroadcastUs - timeUs) / 1_000_000;
      html += `<div>next broadcast: ${dt.toFixed(2)}s</div>`;
    } else {
      html += "<div>next broadcast: --</div>";
    }

    let nextTopicName = "--";
    const nxtH = snap.gossipUrgentFront ?? snap.gossipQueueFront;
    if (snap.online && nxtH !== null) {
      for (const ts of snap.topics) {
        if (ts.hash === nxtH) { nextTopicName = ts.name; break; }
      }
    }
    html += `<div>next to gossip: ${nextTopicName}</div>`;

    if (snap.online && snap.lastUrgentUs > 0) {
      const ago = (timeUs - snap.lastUrgentUs) / 1_000_000;
      html += `<div>last urgent: ${ago.toFixed(2)}s ago</div>`;
    } else {
      html += "<div>last urgent: --</div>";
    }
    if (html !== this.statusCacheKey) {
      this.statusCacheKey = html;
      this.statusSection.innerHTML = html;
    }

    // Topics table
    const topicKey = snap.topics.map(t => `${t.hash.toString(36)}:${t.evictions}:${t.lage}:${t.subjectId}`).join(",");
    if (topicKey !== this.topicCacheKey) {
      this.topicCacheKey = topicKey;
      this.rebuildTopics(snap.topics);
    }

    // Peers
    const peerKey = snap.peers.map(p => p ? `${p.nodeId}:${p.lastSeenUs}` : "-").join(",");
    if (peerKey !== this.peerCacheKey) {
      this.peerCacheKey = peerKey;
      this.rebuildPeers(snap.peers, timeUs);
    }

    // Apply/clear peer flash
    for (let i = 0; i < this.peerCells.length; i++) {
      const bg = peerFlashIndices?.has(i) ? "#ff00ff" : "";
      this.peerCells[i][0].style.background = bg;
      this.peerCells[i][1].style.background = bg;
    }
  }

  setHighlighted(on: boolean): void {
    this.el.classList.toggle("nb-highlighted", on);
  }

  highlightTopic(hash: bigint | null): void {
    // Clear previous
    const prev = this.topicsBody.querySelector("tr.nb-topic-highlighted");
    if (prev) prev.classList.remove("nb-topic-highlighted");
    if (hash !== null) {
      const key = hash.toString(36);
      const row = this.topicsBody.querySelector(`tr[data-hash="${key}"]`);
      if (row) row.classList.add("nb-topic-highlighted");
    }
  }

  setMinimalMode(minimal: boolean): void {
    this.el.classList.toggle("minimal", minimal);
  }

  setPosition(cx: number, cy: number): void {
    this.el.style.left = (cx - BOX_WIDTH / 2) + "px";
    this.el.style.top = (cy - this.el.offsetHeight / 2) + "px";
  }

  getSize(): { w: number; h: number } {
    return { w: this.el.offsetWidth || BOX_WIDTH, h: this.el.offsetHeight || 200 };
  }

  private rebuildTopics(topics: TopicSnap[]): void {
    this.topicsBody.innerHTML = "";
    if (topics.length === 0) {
      const tr = document.createElement("tr");
      tr.innerHTML = '<td colspan="5" style="color:#666;text-align:center">(no topics)</td>';
      this.topicsBody.appendChild(tr);
      return;
    }
    for (const t of topics) {
      const tr = document.createElement("tr");
      tr.dataset.hash = t.hash.toString(36);

      const tdName = document.createElement("td");
      tdName.textContent = t.name.length > 10 ? t.name.slice(0, 10) : t.name;
      tdName.title = t.name;

      const tdSid = document.createElement("td");
      tdSid.textContent = String(t.subjectId);

      const tdEv = document.createElement("td");
      tdEv.className = "nb-ev-cell";
      const evMinus = this.mkInlineBtn("-");
      const evVal = document.createElement("span");
      evVal.textContent = String(t.evictions);
      const evPlus = this.mkInlineBtn("+");
      evMinus.addEventListener("click", () => this.callbacks.onChangeEvictions(this.nodeId, t.hash, -1));
      evPlus.addEventListener("click", () => this.callbacks.onChangeEvictions(this.nodeId, t.hash, 1));
      tdEv.append(evMinus, evVal, evPlus);

      const tdLage = document.createElement("td");
      tdLage.className = "nb-lage-cell";
      const lageMinus = this.mkInlineBtn("-");
      const lageVal = document.createElement("span");
      lageVal.textContent = String(t.lage);
      const lagePlus = this.mkInlineBtn("+");
      lageMinus.addEventListener("click", () => this.callbacks.onChangeLage(this.nodeId, t.hash, -1));
      lagePlus.addEventListener("click", () => this.callbacks.onChangeLage(this.nodeId, t.hash, 1));
      tdLage.append(lageMinus, lageVal, lagePlus);

      const tdDel = document.createElement("td");
      const delBtn = this.mkInlineBtn("\u00d7");
      delBtn.style.color = "#e74c3c";
      delBtn.addEventListener("click", () => this.callbacks.onDeleteTopic(this.nodeId, t.hash));
      tdDel.appendChild(delBtn);

      tr.append(tdName, tdSid, tdEv, tdLage, tdDel);
      tr.addEventListener("mouseenter", () => this.callbacks.onTopicHover(this.nodeId, t.hash, t.name));
      tr.addEventListener("mouseleave", () => this.callbacks.onTopicHover(this.nodeId, null, null));
      this.topicsBody.appendChild(tr);
    }
  }

  private rebuildPeers(peers: (PeerSnap | null)[], timeUs: number): void {
    this.peersBody.innerHTML = "";
    this.peerCells = [];
    const row1 = document.createElement("tr");
    const row2 = document.createElement("tr");
    for (const p of peers) {
      const td1 = document.createElement("td");
      const td2 = document.createElement("td");
      this.peerCells.push([td1, td2]);
      if (p) {
        td1.textContent = `Node${p.nodeId}`;
        const age = (timeUs - p.lastSeenUs) / 1_000_000;
        td2.textContent = `${age.toFixed(1)}s`;
        const fresh = (timeUs - p.lastSeenUs) < GOSSIP_PEER_ELIGIBLE;
        td2.style.color = fresh ? "#27ae60" : "#95a5a6";
      } else {
        td1.textContent = "\u2014";
        td1.style.color = "#555";
        td2.textContent = "";
      }
      row1.appendChild(td1);
      row2.appendChild(td2);
    }
    this.peersBody.append(row1, row2);
  }

  private setupDrag(): void {
    let dragging = false;
    let lastX = 0, lastY = 0;

    this.headerEl.addEventListener("mousedown", (e) => {
      if ((e.target as HTMLElement).tagName === "BUTTON") return;
      e.preventDefault();
      e.stopPropagation();
      dragging = true;
      lastX = e.clientX;
      lastY = e.clientY;
      this.headerEl.style.cursor = "grabbing";
    });

    document.addEventListener("mousemove", (e) => {
      if (!dragging) return;
      const dx = e.clientX - lastX;
      const dy = e.clientY - lastY;
      lastX = e.clientX;
      lastY = e.clientY;
      this.callbacks.onDragMove(this.nodeId, dx, dy);
    });

    document.addEventListener("mouseup", () => {
      if (dragging) {
        dragging = false;
        this.headerEl.style.cursor = "";
      }
    });
  }

  private setupResize(handle: HTMLElement): void {
    let resizing = false;
    let startY = 0;
    let startH = 0;

    handle.addEventListener("mousedown", (e) => {
      e.preventDefault();
      e.stopPropagation();
      resizing = true;
      startY = e.clientY;
      startH = this.topicsContainer.offsetHeight;
    });

    document.addEventListener("mousemove", (e) => {
      if (!resizing) return;
      const dy = e.clientY - startY;
      const newH = Math.max(40, startH + dy);
      this.topicsContainer.style.maxHeight = newH + "px";
    });

    document.addEventListener("mouseup", () => { resizing = false; });
  }

  private mkBtn(label: string, className: string): HTMLButtonElement {
    const btn = document.createElement("button");
    btn.textContent = label;
    btn.className = className;
    return btn;
  }

  private mkInlineBtn(label: string): HTMLButtonElement {
    const btn = document.createElement("button");
    btn.textContent = label;
    btn.className = "nb-inline-btn";
    return btn;
  }
}
