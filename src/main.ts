// ---------------------------------------------------------------------------
// Entry point — animation loop, bootstrap
// ---------------------------------------------------------------------------

import { NetworkConfig } from "./types.js";
import { Simulation } from "./sim.js";
import { Renderer } from "./render.js";
import { UI } from "./ui.js";

const INITIAL_NODES = 6;
const STEP_US = 3_000_000; // 3s sim time per manual step
const MAX_SIM_DT_US = 1_000_000; // cap per frame to prevent freeze

let sim: Simulation;
let renderer: Renderer;
let ui: UI;
let canvas: HTMLCanvasElement;
let lastWallTime: number | null = null;

function createSim(): Simulation {
  const net: NetworkConfig = {
    delayUs: [1_000, 10_000],
    lossProbability: 0.0,
  };
  const s = new Simulation(net, 42);
  for (let i = 0; i < INITIAL_NODES; i++) {
    s.addNode(i);
  }
  return s;
}

function relayout(): void {
  const ids = [...sim.nodes.keys()].sort((a, b) => a - b);
  renderer.layoutNodes(ids);
}

function resizeCanvas(): void {
  const container = canvas.parentElement!;
  canvas.width = container.clientWidth;
  canvas.height = container.clientHeight;
  relayout();
}

function rewind(): void {
  sim = createSim();
  lastWallTime = null;
  relayout();
}

function doStep(): void {
  const newEvents = sim.stepUntil(sim.nowUs + STEP_US);
  const snaps = sim.snapshot();
  renderer.render(sim.nowUs, snaps, newEvents);
  ui.updateFrame(sim.nowUs, snaps);
}

function tick(wallTime: number): void {
  if (lastWallTime === null) lastWallTime = wallTime;
  const wallDtMs = wallTime - lastWallTime;
  lastWallTime = wallTime;

  if (ui.playing) {
    let simDtUs = wallDtMs * 1000 * ui.speedMultiplier;
    if (simDtUs > MAX_SIM_DT_US) simDtUs = MAX_SIM_DT_US;
    if (simDtUs < 0) simDtUs = 0;

    const newEvents = sim.stepUntil(sim.nowUs + simDtUs);
    const snaps = sim.snapshot();
    renderer.render(sim.nowUs, snaps, newEvents);
    ui.updateFrame(sim.nowUs, snaps);
  }

  requestAnimationFrame(tick);
}

function init(): void {
  canvas = document.getElementById("sim-canvas") as HTMLCanvasElement;
  const topBar = document.getElementById("top-bar")!;
  const sidePanel = document.getElementById("side-panel")!;
  const overlayContainer = document.getElementById("overlay-container")!;

  sim = createSim();
  renderer = new Renderer(canvas);
  ui = new UI(sim, renderer, topBar, sidePanel, overlayContainer);

  ui.onRewind = rewind;
  ui.onRelayout = relayout;
  ui.onStepCallback = doStep;

  resizeCanvas();
  window.addEventListener("resize", resizeCanvas);

  requestAnimationFrame(tick);
}

document.addEventListener("DOMContentLoaded", init);
