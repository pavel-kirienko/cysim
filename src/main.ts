// ---------------------------------------------------------------------------
// Entry point — animation loop, bootstrap
// ---------------------------------------------------------------------------

import { NetworkConfig, EventRecord } from "./types.js";
import { Simulation, SimState } from "./sim.js";
import { Renderer } from "./render.js";
import { UI } from "./ui.js";
import { EventLog } from "./event-log.js";
import { Timeline } from "./timeline.js";
import { Viewport } from "./viewport.js";

const INITIAL_NODES = 6;
const STEP_US = 1_000; // 1ms sim time per step
const MAX_BUDGET_US = 1_000_000; // cap budget to 1s sim time per frame

let sim: Simulation;
let renderer: Renderer;
let ui: UI;
let viewport: Viewport;
let canvas: HTMLCanvasElement;
let ctx: CanvasRenderingContext2D;
let lastWallTime: number | null = null;
let simTimeBudget = 0;

// History
let history: SimState[] = [];
let historyIndex = -1; // -1 = no snapshots yet
let historyTimes: number[] = [];

// Event log & timeline
let eventLog: EventLog;
let timeline: Timeline;

function sharedTopicName(index: number): string {
  const letter = String.fromCharCode(97 + (index % 26));
  return index < 26 ? "topic/" + letter : "topic/" + letter + Math.floor(index / 26);
}

function createSim(seed?: number): Simulation {
  if (seed === undefined) {
    seed = Math.random() * 0xFFFFFFFF | 0;
  }
  const net: NetworkConfig = {
    delayUs: [1_000, 10_000],
    lossProbability: 0.0,
  };
  const s = new Simulation(net, seed);
  for (let i = 0; i < INITIAL_NODES; i++) {
    s.addNode(i);
  }
  // Default topic config: overlapping topics between adjacent node pairs
  for (let i = 0; i < INITIAL_NODES; i++) {
    const name = sharedTopicName(i);
    s.addTopicToNode(i, name);
    s.addTopicToNode((i + 1) % INITIAL_NODES, name);
  }
  // Even nodes: colliding topics on subject 10000
  for (let i = 0; i < INITIAL_NODES; i += 2) {
    s.addTopicToNode(i, undefined, 10000);
  }
  // Odd nodes: colliding topics on subject 10001
  for (let i = 1; i < INITIAL_NODES; i += 2) {
    s.addTopicToNode(i, undefined, 10001);
  }
  // Clear initialization events — they're not interesting for the timeline
  s.pendingEvents.length = 0;
  return s;
}

function relayout(): void {
  const ids = [...sim.nodes.keys()].sort((a, b) => a - b);
  renderer.layoutNodes(ids);
  sim.setNodePositions(renderer.nodePositions);
  timeline.setNodeIds(ids);
  zoomToFit();
}

function zoomToFit(): void {
  const container = canvas.parentElement!;
  viewport.zoomToFit(
    renderer.nodePositions,
    new Map(),
    container.clientWidth,
    container.clientHeight,
  );
  viewport.applyToWrapper();
}

function resizeCanvas(): void {
  const container = canvas.parentElement!;
  const dpr = window.devicePixelRatio || 1;
  const w = container.clientWidth;
  const h = container.clientHeight;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  canvas.style.width = w + "px";
  canvas.style.height = h + "px";
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  timeline.resize();
  relayout();
  viewport.applyToWrapper();
}

function setupTimelineResize(): void {
  const handle = document.getElementById("timeline-resize-handle")!;
  const container = document.getElementById("timeline-container")!;
  let dragging = false;
  let startY = 0;
  let startH = 0;

  handle.addEventListener("pointerdown", (e) => {
    e.preventDefault();
    dragging = true;
    startY = e.clientY;
    startH = container.offsetHeight;
    handle.setPointerCapture(e.pointerId);
  });

  handle.addEventListener("pointermove", (e) => {
    if (!dragging) return;
    const newH = Math.max(100, startH - (e.clientY - startY));
    container.style.height = newH + "px";
    resizeCanvas();
  });

  handle.addEventListener("pointerup", (e) => {
    if (!dragging) return;
    dragging = false;
    handle.releasePointerCapture(e.pointerId);
  });
}

function saveSnapshot(events?: EventRecord[]): void {
  history.push(sim.saveState());
  historyTimes.push(sim.nowUs);
  historyIndex = history.length - 1;
  timeline.setHistoryTimes(historyTimes);
  timeline.setCurrentIndex(historyIndex);

  if (events && events.length > 0) {
    eventLog.ingest(events, historyIndex);
  }
}

/** Single 10ms step. Returns true if events occurred (snapshot saved). */
function doStep(): boolean {
  // Truncate future history immediately when stepping from a rewound position
  if (historyIndex < history.length - 1) {
    history.length = historyIndex + 1;
    historyTimes.length = historyIndex + 1;
    eventLog.truncateAfter(historyIndex);
    timeline.truncateConvergenceAfter(historyTimes[historyTimes.length - 1] ?? 0);
    timeline.setHistoryTimes(historyTimes);
  }
  const newEvents = sim.stepUntil(sim.nowUs + STEP_US);
  if (newEvents.length > 0) {
    saveSnapshot(newEvents);
    renderCurrent(newEvents);
    return true;
  }
  renderCurrent([]);
  return false;
}

function renderCurrent(events: EventRecord[] = []): void {
  const snaps = sim.snapshot();
  renderer.render(sim.nowUs, snaps, events);
  const maxTimeUs = historyTimes.length > 0 ? historyTimes[historyTimes.length - 1] : 0;
  const rewound = historyIndex >= 0 && historyIndex < history.length - 1;
  ui.updateFrame(sim.nowUs, snaps, history.length, maxTimeUs, rewound);
  const conv = sim.checkConvergenceFromSnaps(snaps);
  timeline.recordConvergence(sim.nowUs, conv);
  timeline.render(sim.nowUs);
}

function navigateTo(index: number): void {
  if (index < 0 || index >= history.length) return;
  sim.loadState(history[index]);
  historyIndex = index;
  timeline.setCurrentIndex(historyIndex);
  renderer.rebuildAnimationsFromLog(eventLog, sim.nowUs);
  renderCurrent();
}

function resetWithSeed(seed: number): void {
  sim = createSim(seed);
  ui.setSim(sim);
  history = [];
  historyTimes = [];
  historyIndex = -1;
  lastWallTime = null;
  simTimeBudget = 0;
  eventLog.clear();
  renderer.clearAnimations();
  timeline.resetNodeIds();
  timeline.setHistoryTimes(historyTimes);
  timeline.setCurrentIndex(-1);
  relayout();
  renderCurrent();
}

function tick(wallTime: number): void {
  if (lastWallTime === null) lastWallTime = wallTime;
  const wallDtMs = Math.min(wallTime - lastWallTime, 50); // cap at 50ms to avoid jumps after modal dialogs
  lastWallTime = wallTime;

  if (ui.playing) {
    simTimeBudget += wallDtMs * 1000 * ui.speedMultiplier;
    if (simTimeBudget > MAX_BUDGET_US) simTimeBudget = MAX_BUDGET_US;
    if (simTimeBudget < 0) simTimeBudget = 0;

    while (simTimeBudget >= STEP_US) {
      doStep();
      simTimeBudget -= STEP_US;
    }
  } else {
    // Always render when paused so UI interactions are visible
    renderCurrent();
  }

  requestAnimationFrame(tick);
}

function init(): void {
  canvas = document.getElementById("sim-canvas") as HTMLCanvasElement;
  ctx = canvas.getContext("2d")!;
  const topBar = document.getElementById("top-bar")!;
  const sidePanel = document.getElementById("side-panel")!;
  const overlayContainer = document.getElementById("overlay-container")!;
  const worldWrapper = document.getElementById("world-wrapper")!;
  const timelineCanvas = document.getElementById("timeline-canvas") as HTMLCanvasElement;
  const timelineTooltip = document.getElementById("timeline-tooltip")!;
  const canvasContainer = document.getElementById("canvas-container")!;

  const simTooltip = document.getElementById("sim-tooltip")!;
  sim = createSim();
  viewport = new Viewport();
  viewport.setWrapper(worldWrapper);
  viewport.attach(canvasContainer);
  renderer = new Renderer(canvas, viewport, simTooltip);
  eventLog = new EventLog();
  timeline = new Timeline(timelineCanvas, timelineTooltip, eventLog);
  const topicPanel = document.getElementById("topic-panel")!;
  ui = new UI(sim, renderer, viewport, topBar, sidePanel, overlayContainer, topicPanel);

  ui.setTimeline(timeline);
  ui.onRelayout = relayout;
  ui.onApplySeed = resetWithSeed;
  ui.onFitView = zoomToFit;
  ui.onUserInteraction = () => {
    const pendingEvents = sim.drainPendingEvents();
    saveSnapshot(pendingEvents);
    renderCurrent();
  };
  ui.setSeedDisplay(sim.seed);

  timeline.onNavigate = navigateTo;
  timeline.isPlaying = () => ui.playing;

  resizeCanvas();
  setupTimelineResize();
  window.addEventListener("resize", resizeCanvas);

  renderCurrent();
  requestAnimationFrame(tick);
}

document.addEventListener("DOMContentLoaded", init);
