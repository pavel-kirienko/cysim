// ---------------------------------------------------------------------------
// Entry point — animation loop, bootstrap
// ---------------------------------------------------------------------------

import { NetworkConfig, EventRecord } from "./types.js";
import { Simulation, SimState } from "./sim.js";
import { Renderer } from "./render.js";
import { UI } from "./ui.js";
import { EventLog } from "./event-log.js";
import { Timeline } from "./timeline.js";

const INITIAL_NODES = 6;
const STEP_US = 1_000; // 1ms sim time per step
const MAX_BUDGET_US = 1_000_000; // cap budget to 1s sim time per frame

let sim: Simulation;
let renderer: Renderer;
let ui: UI;
let canvas: HTMLCanvasElement;
let lastWallTime: number | null = null;
let simTimeBudget = 0;

// History
let history: SimState[] = [];
let historyIndex = -1; // -1 = no snapshots yet
let historyTimes: number[] = [];

// Event log & timeline
let eventLog: EventLog;
let timeline: Timeline;

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
  return s;
}

function relayout(): void {
  const ids = [...sim.nodes.keys()].sort((a, b) => a - b);
  renderer.layoutNodes(ids);
  sim.setNodePositions(renderer.nodePositions);
  timeline.setNodeIds(ids);
}

function resizeCanvas(): void {
  const container = canvas.parentElement!;
  canvas.width = container.clientWidth;
  canvas.height = container.clientHeight;
  timeline.resize();
  relayout();
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

/** Step until next event occurs (for Step button). */
function doStepToNextEvent(): void {
  const maxUs = sim.nowUs + MAX_BUDGET_US;
  while (sim.nowUs < maxUs) {
    if (doStep()) return;
  }
}

function renderCurrent(events: EventRecord[] = []): void {
  const snaps = sim.snapshot();
  renderer.render(sim.nowUs, snaps, events);
  const maxTimeUs = historyTimes.length > 0 ? historyTimes[historyTimes.length - 1] : 0;
  ui.updateFrame(sim.nowUs, snaps, history.length, maxTimeUs);
  const conv = sim.checkConvergenceFromSnaps(snaps);
  timeline.recordConvergence(sim.nowUs, conv);
  timeline.render(sim.nowUs);
}

function navigateTo(index: number): void {
  if (index < 0 || index >= history.length) return;
  sim.loadState(history[index]);
  historyIndex = index;
  timeline.setCurrentIndex(historyIndex);
  renderer.clearAnimations();
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
  const topBar = document.getElementById("top-bar")!;
  const sidePanel = document.getElementById("side-panel")!;
  const overlayContainer = document.getElementById("overlay-container")!;
  const timelineCanvas = document.getElementById("timeline-canvas") as HTMLCanvasElement;
  const timelineTooltip = document.getElementById("timeline-tooltip")!;

  sim = createSim();
  renderer = new Renderer(canvas);
  eventLog = new EventLog();
  timeline = new Timeline(timelineCanvas, timelineTooltip, eventLog);
  ui = new UI(sim, renderer, topBar, sidePanel, overlayContainer);

  ui.onRelayout = relayout;
  ui.onStepCallback = doStepToNextEvent;
  ui.onApplySeed = resetWithSeed;
  ui.onUserInteraction = () => {
    const pendingEvents = sim.drainPendingEvents();
    saveSnapshot(pendingEvents);
    renderCurrent();
  };
  ui.setSeedDisplay(sim.seed);

  timeline.onNavigate = navigateTo;
  timeline.isPlaying = () => ui.playing;

  resizeCanvas();
  window.addEventListener("resize", resizeCanvas);

  renderCurrent();
  requestAnimationFrame(tick);
}

document.addEventListener("DOMContentLoaded", init);
