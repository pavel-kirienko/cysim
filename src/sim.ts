// ---------------------------------------------------------------------------
// Simulation engine -- cy.c-aligned CRDT core model
// ---------------------------------------------------------------------------

import {
  NetworkConfig, Topic, GossipPeer, DedupEntry, Node,
  EventRecord, TopicSnap, PeerSnap, NodeSnapshot,
} from "./types.js";
import {
  GOSSIP_PERIOD, GOSSIP_TTL, GOSSIP_OUTDEGREE,
  GOSSIP_PERIODIC_UNICAST_PERIOD, GOSSIP_PERIODIC_UNICAST_TTL,
  GOSSIP_PEER_COUNT, GOSSIP_DEDUP_CAP, GOSSIP_DEDUP_TIMEOUT,
  GOSSIP_PEER_STALE, GOSSIP_PEER_ELIGIBLE,
  GOSSIP_PEER_REPLACEMENT_PROBABILITY_RECIPROCAL,
  SUBJECT_ID_PINNED_MAX, SUBJECT_ID_MODULUS, LAGE_MIN, LAGE_MAX,
  PROPAGATION_SPEED,
} from "./constants.js";

const U64_MASK = 0xFFFF_FFFF_FFFF_FFFFn;
const BIG_BANG = Number.MIN_SAFE_INTEGER;
const HEAT_DEATH = Number.MAX_SAFE_INTEGER;

enum CrdtMergeOutcome {
  Consensus = "consensus",
  LocalWin = "local_win",
  LocalLoss = "local_loss",
}

function asU64(x: bigint): bigint {
  return x & U64_MASK;
}

function isPinned(hash: bigint): boolean {
  return hash <= BigInt(SUBJECT_ID_PINNED_MAX);
}

// ---------------------------------------------------------------------------
// Seeded RNG -- Mulberry32
// ---------------------------------------------------------------------------

export class Rng {
  private state: number;

  constructor(seed: number) {
    this.state = seed | 0;
  }

  getState(): number { return this.state; }
  setState(s: number): void { this.state = s; }

  random(): number {
    let t = (this.state += 0x6D2B79F5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  randint(lo: number, hi: number): number {
    return lo + Math.floor(this.random() * (hi - lo + 1));
  }

  randrange(n: number): number {
    return Math.floor(this.random() * n);
  }

  choice<T>(arr: T[]): T {
    return arr[Math.floor(this.random() * arr.length)];
  }

  // cy.c random_int(min,max): [min, max) if min < max else min
  randomInt(min: number, maxExclusive: number): number {
    if (min < maxExclusive) {
      return Math.floor(this.random() * (maxExclusive - min)) + min;
    }
    return min;
  }

  chance(probabilityReciprocal: number): boolean {
    return probabilityReciprocal > 0 && this.randrange(probabilityReciprocal) === 0;
  }
}

// ---------------------------------------------------------------------------
// Priority queue (min-heap) for events
// ---------------------------------------------------------------------------

export interface SimEvent {
  timeUs: number;
  seq: number;
  type: string;
  payload: Record<string, unknown>;
}

export class MinHeap {
  private data: SimEvent[] = [];

  get length(): number { return this.data.length; }

  push(ev: SimEvent): void {
    this.data.push(ev);
    this.siftUp(this.data.length - 1);
  }

  pop(): SimEvent | undefined {
    const d = this.data;
    if (d.length === 0) return undefined;
    const top = d[0];
    const last = d.pop()!;
    if (d.length > 0) {
      d[0] = last;
      this.siftDown(0);
    }
    return top;
  }

  peek(): SimEvent | undefined {
    return this.data[0];
  }

  toArray(): SimEvent[] {
    return this.data.slice();
  }

  static fromArray(data: SimEvent[]): MinHeap {
    const h = new MinHeap();
    h.data = data.slice();
    return h;
  }

  private less(a: SimEvent, b: SimEvent): boolean {
    return a.timeUs < b.timeUs || (a.timeUs === b.timeUs && a.seq < b.seq);
  }

  private siftUp(i: number): void {
    const d = this.data;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (this.less(d[i], d[p])) {
        [d[i], d[p]] = [d[p], d[i]];
        i = p;
      } else {
        break;
      }
    }
  }

  private siftDown(i: number): void {
    const d = this.data;
    const n = d.length;
    while (true) {
      let smallest = i;
      const l = 2 * i + 1;
      const r = 2 * i + 2;
      if (l < n && this.less(d[l], d[smallest])) smallest = l;
      if (r < n && this.less(d[r], d[smallest])) smallest = r;
      if (smallest === i) break;
      [d[i], d[smallest]] = [d[smallest], d[i]];
      i = smallest;
    }
  }
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

export function subjectId(topicHash: bigint, evictions: number, modulus: number): number {
  if (isPinned(topicHash)) {
    return Number(topicHash);
  }
  const e = BigInt(Math.max(0, Math.floor(evictions)));
  const sq = asU64(e * e);
  const sid = BigInt(SUBJECT_ID_PINNED_MAX + 1) + (asU64(topicHash + sq) % BigInt(modulus));
  return Number(sid);
}

export function leftWins(lLage: number, lHash: bigint, rLage: number, rHash: bigint): boolean {
  if (lLage !== rLage) {
    return lLage > rLage;
  }
  return lHash < rHash;
}

function log2Floor(x: number): number {
  if (x <= 0) return LAGE_MIN;
  return Math.floor(Math.log2(x));
}

function pow2us(exp: number): number {
  if (exp < 0) return 0;
  if (exp > 62) return HEAT_DEATH;
  return 2 ** exp;
}

export function gossipDedupHash(topicHash: bigint, evictions: number, lage: number): bigint {
  const lageClamped = Math.max(LAGE_MIN, Math.min(lage, LAGE_MAX));
  const e = BigInt(Math.max(0, Math.floor(evictions)));
  const other = asU64((e << 16n) | (BigInt(lageClamped - LAGE_MIN) << 56n));
  return asU64(topicHash ^ other);
}

export function topicLage(tsCreatedUs: number, nowUs: number): number {
  const ageSeconds = Math.max(0, Math.floor((nowUs - tsCreatedUs) / 1_000_000));
  const out = log2Floor(ageSeconds);
  return Math.max(LAGE_MIN, Math.min(out, LAGE_MAX));
}

function topicMergeLage(topic: Topic, nowUs: number, remoteLage: number): void {
  const rLage = Math.max(LAGE_MIN, Math.min(remoteLage, LAGE_MAX));
  topic.tsCreatedUs = Math.min(topic.tsCreatedUs, nowUs - (pow2us(rLage) * 1_000_000));
}

function topicSubjectId(t: Topic): number {
  return subjectId(t.hash, t.evictions, SUBJECT_ID_MODULUS);
}

function parseHashOverride(name: string): bigint | null {
  let out = 0n;
  const maxNibbles = Math.min(name.length, 17);
  for (let i = 0; i < maxNibbles; i++) {
    const ch = name.charCodeAt(name.length - i - 1);
    if (ch === 35) { // '#'
      return i > 0 ? out : null;
    }
    let digit = -1;
    if (ch >= 48 && ch <= 57) {
      digit = ch - 48;
    } else if (ch >= 97 && ch <= 102) {
      digit = ch - 97 + 10;
    } else {
      break;
    }
    out |= BigInt(digit) << BigInt(i * 4);
  }
  return null;
}

function rapidRead32LE(bytes: Uint8Array, offset: number): bigint {
  return BigInt(
    (bytes[offset] ?? 0) |
    ((bytes[offset + 1] ?? 0) << 8) |
    ((bytes[offset + 2] ?? 0) << 16) |
    ((bytes[offset + 3] ?? 0) << 24),
  );
}

function rapidRead64LE(bytes: Uint8Array, offset: number): bigint {
  let out = 0n;
  for (let i = 0; i < 8; i++) {
    out |= BigInt(bytes[offset + i] ?? 0) << BigInt(i * 8);
  }
  return out;
}

function rapidMum(a: bigint, b: bigint): [bigint, bigint] {
  const p = a * b;
  return [asU64(p), asU64(p >> 64n)];
}

function rapidMix(a: bigint, b: bigint): bigint {
  const [lo, hi] = rapidMum(a, b);
  return asU64(lo ^ hi);
}

const RAPID_SECRET = [
  0x2d358dccaa6c78a5n,
  0x8bb84b93962eacc9n,
  0x4b33a62ed433d4a3n,
  0x4d5a2da51de1aa47n,
  0xa0761d6478bd642fn,
  0xe7037ed1a0b428dbn,
  0x90ed1765281c388cn,
  0xaaaaaaaaaaaaaaaan,
].map(asU64);

function rapidhashInternal(bytes: Uint8Array): bigint {
  let seed = 0n;
  let p = 0;
  let i = bytes.length;
  seed = asU64(seed ^ rapidMix(asU64(seed ^ RAPID_SECRET[2]), RAPID_SECRET[1]));

  let a = 0n;
  let b = 0n;

  if (bytes.length <= 16) {
    if (bytes.length >= 4) {
      seed = asU64(seed ^ BigInt(bytes.length));
      if (bytes.length >= 8) {
        const plast = p + bytes.length - 8;
        a = rapidRead64LE(bytes, p);
        b = rapidRead64LE(bytes, plast);
      } else {
        const plast = p + bytes.length - 4;
        a = rapidRead32LE(bytes, p);
        b = rapidRead32LE(bytes, plast);
      }
    } else if (bytes.length > 0) {
      a = (BigInt(bytes[p]) << 45n) | BigInt(bytes[p + bytes.length - 1]);
      b = BigInt(bytes[p + (bytes.length >> 1)]);
    }
  } else {
    if (bytes.length > 112) {
      let see1 = seed;
      let see2 = seed;
      let see3 = seed;
      let see4 = seed;
      let see5 = seed;
      let see6 = seed;

      do {
        seed = rapidMix(asU64(rapidRead64LE(bytes, p) ^ RAPID_SECRET[0]), asU64(rapidRead64LE(bytes, p + 8) ^ seed));
        see1 = rapidMix(asU64(rapidRead64LE(bytes, p + 16) ^ RAPID_SECRET[1]), asU64(rapidRead64LE(bytes, p + 24) ^ see1));
        see2 = rapidMix(asU64(rapidRead64LE(bytes, p + 32) ^ RAPID_SECRET[2]), asU64(rapidRead64LE(bytes, p + 40) ^ see2));
        see3 = rapidMix(asU64(rapidRead64LE(bytes, p + 48) ^ RAPID_SECRET[3]), asU64(rapidRead64LE(bytes, p + 56) ^ see3));
        see4 = rapidMix(asU64(rapidRead64LE(bytes, p + 64) ^ RAPID_SECRET[4]), asU64(rapidRead64LE(bytes, p + 72) ^ see4));
        see5 = rapidMix(asU64(rapidRead64LE(bytes, p + 80) ^ RAPID_SECRET[5]), asU64(rapidRead64LE(bytes, p + 88) ^ see5));
        see6 = rapidMix(asU64(rapidRead64LE(bytes, p + 96) ^ RAPID_SECRET[6]), asU64(rapidRead64LE(bytes, p + 104) ^ see6));
        p += 112;
        i -= 112;
      } while (i > 112);

      seed = asU64(seed ^ see1);
      see2 = asU64(see2 ^ see3);
      see4 = asU64(see4 ^ see5);
      seed = asU64(seed ^ see6);
      see2 = asU64(see2 ^ see4);
      seed = asU64(seed ^ see2);
    }

    if (i > 16) {
      seed = rapidMix(asU64(rapidRead64LE(bytes, p) ^ RAPID_SECRET[2]), asU64(rapidRead64LE(bytes, p + 8) ^ seed));
      if (i > 32) {
        seed = rapidMix(asU64(rapidRead64LE(bytes, p + 16) ^ RAPID_SECRET[2]), asU64(rapidRead64LE(bytes, p + 24) ^ seed));
        if (i > 48) {
          seed = rapidMix(asU64(rapidRead64LE(bytes, p + 32) ^ RAPID_SECRET[1]), asU64(rapidRead64LE(bytes, p + 40) ^ seed));
          if (i > 64) {
            seed = rapidMix(asU64(rapidRead64LE(bytes, p + 48) ^ RAPID_SECRET[1]), asU64(rapidRead64LE(bytes, p + 56) ^ seed));
            if (i > 80) {
              seed = rapidMix(asU64(rapidRead64LE(bytes, p + 64) ^ RAPID_SECRET[2]), asU64(rapidRead64LE(bytes, p + 72) ^ seed));
              if (i > 96) {
                seed = rapidMix(asU64(rapidRead64LE(bytes, p + 80) ^ RAPID_SECRET[1]), asU64(rapidRead64LE(bytes, p + 88) ^ seed));
              }
            }
          }
        }
      }
    }

    a = asU64(rapidRead64LE(bytes, p + i - 16) ^ BigInt(i));
    b = rapidRead64LE(bytes, p + i - 8);
  }

  a = asU64(a ^ RAPID_SECRET[1]);
  b = asU64(b ^ seed);
  [a, b] = rapidMum(a, b);
  return rapidMix(asU64(a ^ RAPID_SECRET[7]), asU64((b ^ RAPID_SECRET[1]) ^ BigInt(i)));
}

const textEncoder = new TextEncoder();

export function topicHash(name: string): bigint {
  const override = parseHashOverride(name);
  if (override !== null) {
    return asU64(override);
  }
  return rapidhashInternal(textEncoder.encode(name));
}

// ---------------------------------------------------------------------------
// Node helpers
// ---------------------------------------------------------------------------

export function makeNode(nodeId: number): Node {
  const dedup: DedupEntry[] = [];
  for (let i = 0; i < GOSSIP_DEDUP_CAP; i++) {
    dedup.push({ hash: 0n, lastSeenUs: BIG_BANG });
  }
  const peers: (GossipPeer | null)[] = [];
  for (let i = 0; i < GOSSIP_PEER_COUNT; i++) {
    peers.push(null);
  }
  return {
    nodeId,
    online: false,
    topics: new Map(),
    gossipQueue: [],
    gossipUrgent: [],
    peers,
    dedup,
    gossipNextUs: HEAT_DEATH,
    gossipPeriodicNextUs: HEAT_DEATH,
    gossipPollScheduledUs: HEAT_DEATH,
    gossipPeriodUs: GOSSIP_PERIOD,
    partitionSet: "A",
    peerReplacementMoratoriumUntil: BIG_BANG,
    lastUrgentUs: 0,
  };
}

export function nodeAddTopic(node: Node, topic: Topic): void {
  node.topics.set(topic.hash, topic);
  if (!node.gossipQueue.includes(topic.hash)) {
    node.gossipQueue.push(topic.hash);
  }
}

export function nodeFindBySubjectId(node: Node, sid: number): Topic | null {
  for (const t of node.topics.values()) {
    if (isPinned(t.hash)) continue;
    if (topicSubjectId(t) === sid) {
      return t;
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// Simulation class
// ---------------------------------------------------------------------------

export interface SimState {
  nowUs: number;
  seq: number;
  nextAutoId: number;
  nextAutoTopicChar: number;
  rngState: number;
  nodes: Map<number, Node>;
  queueData: SimEvent[];
}

export class Simulation {
  net: NetworkConfig;
  rng: Rng;
  nodes: Map<number, Node> = new Map();
  nodePositions: Map<number, { x: number; y: number }> = new Map();
  nowUs = 0;

  private queue = new MinHeap();
  private seq = 0;
  private nextAutoId = 0;
  private nextAutoTopicChar = 97; // 'a'
  readonly seed: number;

  pendingEvents: EventRecord[] = [];

  constructor(net: NetworkConfig, rngSeed = 42) {
    this.net = {
      ...net,
      periodicUnicastEnabled: net.periodicUnicastEnabled ?? true,
    };
    this.seed = rngSeed;
    this.rng = new Rng(rngSeed);
  }

  saveState(): SimState {
    const nodes = new Map<number, Node>();
    for (const [id, n] of this.nodes) {
      const topics = new Map<bigint, Topic>();
      for (const [h, t] of n.topics) {
        topics.set(h, { ...t });
      }
      nodes.set(id, {
        nodeId: n.nodeId,
        online: n.online,
        topics,
        gossipQueue: n.gossipQueue.slice(),
        gossipUrgent: n.gossipUrgent.slice(),
        peers: n.peers.map(p => (p ? { ...p } : null)),
        dedup: n.dedup.map(d => ({ ...d })),
        gossipNextUs: n.gossipNextUs,
        gossipPeriodicNextUs: n.gossipPeriodicNextUs,
        gossipPollScheduledUs: n.gossipPollScheduledUs,
        gossipPeriodUs: n.gossipPeriodUs,
        partitionSet: n.partitionSet,
        peerReplacementMoratoriumUntil: n.peerReplacementMoratoriumUntil,
        lastUrgentUs: n.lastUrgentUs,
      });
    }
    const queueData = this.queue.toArray().map(ev => ({ ...ev, payload: { ...ev.payload } }));
    return {
      nowUs: this.nowUs,
      seq: this.seq,
      nextAutoId: this.nextAutoId,
      nextAutoTopicChar: this.nextAutoTopicChar,
      rngState: this.rng.getState(),
      nodes,
      queueData,
    };
  }

  loadState(state: SimState): void {
    this.nowUs = state.nowUs;
    this.seq = state.seq;
    this.nextAutoId = state.nextAutoId;
    this.nextAutoTopicChar = state.nextAutoTopicChar;
    this.rng.setState(state.rngState);
    this.nodes.clear();
    for (const [id, n] of state.nodes) {
      const topics = new Map<bigint, Topic>();
      for (const [h, t] of n.topics) {
        topics.set(h, { ...t, sortOrder: t.sortOrder ?? t.tsCreatedUs });
      }
      this.nodes.set(id, {
        nodeId: n.nodeId,
        online: n.online,
        topics,
        gossipQueue: n.gossipQueue.slice(),
        gossipUrgent: n.gossipUrgent.slice(),
        peers: n.peers.map(p => (p ? { ...p } : null)),
        dedup: n.dedup.map(d => ({ ...d })),
        gossipNextUs: n.gossipNextUs,
        gossipPeriodicNextUs: n.gossipPeriodicNextUs,
        gossipPollScheduledUs: n.gossipPollScheduledUs,
        gossipPeriodUs: n.gossipPeriodUs,
        partitionSet: n.partitionSet,
        peerReplacementMoratoriumUntil: n.peerReplacementMoratoriumUntil,
        lastUrgentUs: n.lastUrgentUs,
      });
    }

    const queueData = state.queueData.map(ev => ({ ...ev, payload: { ...ev.payload } }));
    this.queue = MinHeap.fromArray(queueData);
  }

  setNodePositions(positions: Map<number, { x: number; y: number }>): void {
    this.nodePositions = positions;
  }

  // -- public interactive API --

  addNode(nodeId?: number): Node {
    if (nodeId === undefined) {
      while (this.nodes.has(this.nextAutoId)) this.nextAutoId++;
      nodeId = this.nextAutoId++;
    }
    if (nodeId >= this.nextAutoId) this.nextAutoId = nodeId + 1;
    const node = makeNode(nodeId);
    this.nodes.set(nodeId, node);
    this.pushEvent(this.nowUs, "NODE_JOIN", { node_id: nodeId });
    return node;
  }

  destroyNode(nodeId: number): void {
    const node = this.nodes.get(nodeId);
    if (node) {
      node.online = false;
      this.nodes.delete(nodeId);
      this.pendingEvents.push({
        timeUs: this.nowUs, event: "node_expunged", src: nodeId, dst: null,
        topicHash: 0n, details: {},
      });
    }
  }

  restartNode(nodeId: number): void {
    const node = this.nodes.get(nodeId);
    if (!node) return;
    node.online = false;
    // Keep topics but reset lage and evictions
    for (const t of node.topics.values()) {
      t.evictions = 0;
      t.tsCreatedUs = this.nowUs; // lage will compute to -1
    }
    node.gossipQueue.length = 0;
    node.gossipUrgent.length = 0;
    // Re-enqueue all topics for gossip and re-allocate subject IDs
    for (const t of node.topics.values()) {
      node.gossipQueue.push(t.hash);
      this.topicAllocate(node, t, 0, this.nowUs);
    }
    for (let i = 0; i < node.peers.length; i++) node.peers[i] = null;
    for (const d of node.dedup) {
      d.hash = 0n;
      d.lastSeenUs = BIG_BANG;
    }
    node.gossipNextUs = HEAT_DEATH;
    node.gossipPeriodicNextUs = HEAT_DEATH;
    node.gossipPollScheduledUs = HEAT_DEATH;
    node.peerReplacementMoratoriumUntil = BIG_BANG;
    node.lastUrgentUs = 0;
    this.pushEvent(this.nowUs, "NODE_JOIN", { node_id: nodeId });
  }

  setPartition(nodeId: number, set: "A" | "B"): void {
    const node = this.nodes.get(nodeId);
    if (node) node.partitionSet = set;
  }

  addTopicToNode(nodeId: number, name?: string, targetSid?: number, initEvictions?: number, initLage?: number): Topic | null {
    const node = this.nodes.get(nodeId);
    if (!node) return null;

    if (name === undefined) {
      if (targetSid !== undefined) {
        name = this.findNameForSid(targetSid);
      } else {
        name = "topic/" + String.fromCharCode(this.nextAutoTopicChar);
        this.nextAutoTopicChar++;
        if (this.nextAutoTopicChar > 122) this.nextAutoTopicChar = 97;
      }
    }

    const hash = topicHash(name);
    const existing = node.topics.get(hash);
    if (existing) return existing;

    const ev = initEvictions !== undefined ? Math.max(0, Math.floor(initEvictions)) : 0;
    let tsCreated = this.nowUs;
    if (initLage !== undefined) {
      const clamped = Math.max(LAGE_MIN, Math.min(Math.floor(initLage), LAGE_MAX));
      tsCreated = this.nowUs - pow2us(clamped) * 1_000_000;
    }

    const topic: Topic = { name, hash, evictions: ev, tsCreatedUs: tsCreated, sortOrder: tsCreated };
    nodeAddTopic(node, topic);
    this.topicAllocate(node, topic, topic.evictions, this.nowUs);
    this.gossipBegin(node);

    this.pendingEvents.push({
      timeUs: this.nowUs, event: "topic_new", src: nodeId, dst: null,
      topicHash: hash, details: { name },
    });
    return topic;
  }

  private findNameForSid(targetSid: number): string {
    if (targetSid <= SUBJECT_ID_PINNED_MAX) {
      return `topic/pinned#${targetSid.toString(16)}`;
    }
    const maxSid = SUBJECT_ID_PINNED_MAX + SUBJECT_ID_MODULUS;
    const sid = Math.min(Math.max(targetSid, SUBJECT_ID_PINNED_MAX + 1), maxSid);
    const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
    for (;;) {
      let name = "";
      for (let i = 0; i < 4; i++) {
        name += alphabet[this.rng.randrange(alphabet.length)];
      }
      const hash = topicHash(name);
      if (subjectId(hash, 0, SUBJECT_ID_MODULUS) === sid) {
        return name;
      }
    }
  }

  adjustTopicEvictions(nodeId: number, hash: bigint, delta: number): void {
    const node = this.nodes.get(nodeId);
    if (!node) return;
    const topic = node.topics.get(hash);
    if (!topic) return;
    const newEv = Math.max(0, topic.evictions + delta);
    if (newEv === topic.evictions) return;
    this.topicAllocate(node, topic, newEv, this.nowUs);
    this.pendingEvents.push({
      timeUs: this.nowUs, event: "topic_adjust", src: nodeId, dst: null,
      topicHash: hash, details: { name: topic.name, evictions: topic.evictions },
    });
  }

  adjustTopicLage(nodeId: number, hash: bigint, delta: number): void {
    const node = this.nodes.get(nodeId);
    if (!node) return;
    const topic = node.topics.get(hash);
    if (!topic) return;
    const currentLage = topicLage(topic.tsCreatedUs, this.nowUs);
    const newLage = Math.max(LAGE_MIN, Math.min(LAGE_MAX, currentLage + delta));
    if (newLage === currentLage) return;
    topic.tsCreatedUs = this.nowUs - pow2us(newLage) * 1_000_000;
    this.pendingEvents.push({
      timeUs: this.nowUs, event: "topic_adjust", src: nodeId, dst: null,
      topicHash: hash, details: { name: topic.name, lage: newLage },
    });
  }

  destroyTopicOnNode(nodeId: number, hash: bigint): void {
    const node = this.nodes.get(nodeId);
    if (!node) return;
    const topic = node.topics.get(hash);
    node.topics.delete(hash);
    this.delist(node.gossipQueue, hash);
    this.delist(node.gossipUrgent, hash);
    this.pendingEvents.push({
      timeUs: this.nowUs, event: "topic_expunged", src: nodeId, dst: null,
      topicHash: hash, details: { name: topic?.name ?? "?" },
    });
  }

  drainPendingEvents(): EventRecord[] {
    const events = this.pendingEvents.slice();
    this.pendingEvents.length = 0;
    return events;
  }

  stepUntil(targetUs: number): EventRecord[] {
    const newEvents: EventRecord[] = [];
    const pushLog = (rec: EventRecord) => {
      newEvents.push(rec);
    };

    while (this.queue.length > 0) {
      const top = this.queue.peek()!;
      if (top.timeUs > targetUs) break;
      const ev = this.queue.pop()!;
      this.nowUs = ev.timeUs;

      if (ev.type === "NODE_JOIN") {
        this.handleNodeJoin(ev.payload["node_id"] as number, pushLog);
      } else if (ev.type === "MSG_ARRIVE") {
        this.handleMsgArrive(ev.payload, pushLog);
      } else if (ev.type === "GOSSIP_POLL") {
        this.handleGossipPollEvent(ev.payload, pushLog);
      }
    }

    if (targetUs > this.nowUs) this.nowUs = targetUs;
    return newEvents;
  }

  snapshot(): Map<number, NodeSnapshot> {
    const result = new Map<number, NodeSnapshot>();
    for (const [nid, node] of this.nodes) {
      result.set(nid, this.snapNode(node));
    }
    return result;
  }

  checkConvergence(): boolean {
    return this.checkConvergenceImpl(
      this.onlineNodes().map(n => ({
        partition: n.partitionSet,
        topics: [...n.topics.values()].map(t => ({
          hash: t.hash, subjectId: topicSubjectId(t), evictions: t.evictions,
        })),
      })),
    );
  }

  checkConvergenceFromSnaps(snaps: Map<number, NodeSnapshot>): boolean {
    const nodes: { partition: string; topics: { hash: bigint; subjectId: number; evictions: number }[] }[] = [];
    for (const s of snaps.values()) {
      if (!s.online) continue;
      nodes.push({
        partition: s.partitionSet,
        topics: s.topics.map(t => ({ hash: t.hash, subjectId: t.subjectId, evictions: t.evictions })),
      });
    }
    return this.checkConvergenceImpl(nodes);
  }

  private checkConvergenceImpl(
    nodes: { partition: string; topics: { hash: bigint; subjectId: number; evictions: number }[] }[],
  ): boolean {
    const byPartition = new Map<string, typeof nodes>();
    for (const n of nodes) {
      let group = byPartition.get(n.partition);
      if (!group) {
        group = [];
        byPartition.set(n.partition, group);
      }
      group.push(n);
    }
    for (const group of byPartition.values()) {
      const sidToHash = new Map<number, bigint>();
      const hashToEvictions = new Map<bigint, number>();
      for (const n of group) {
        for (const t of n.topics) {
          const existing = sidToHash.get(t.subjectId);
          if (existing !== undefined && existing !== t.hash) return false;
          sidToHash.set(t.subjectId, t.hash);
          const existingEv = hashToEvictions.get(t.hash);
          if (existingEv !== undefined && existingEv !== t.evictions) return false;
          hashToEvictions.set(t.hash, t.evictions);
        }
      }
    }
    return true;
  }

  // -- private --

  private pushEvent(timeUs: number, type: string, payload: Record<string, unknown>): void {
    this.queue.push({ timeUs, seq: this.seq++, type, payload });
  }

  private ditherInt(mean: number, deviation: number): number {
    return mean + this.rng.randomInt(-deviation, deviation);
  }

  private randDelay(): number {
    const [lo, hi] = this.net.delayUs;
    return this.rng.randint(lo, hi);
  }

  private distanceDelay(srcId: number, dstId: number): number {
    const srcPos = this.nodePositions.get(srcId);
    const dstPos = this.nodePositions.get(dstId);
    if (!srcPos || !dstPos) return this.randDelay();
    const dx = dstPos.x - srcPos.x;
    const dy = dstPos.y - srcPos.y;
    const distance = Math.sqrt(dx * dx + dy * dy);
    const baseDelay = (distance / PROPAGATION_SPEED) * 1_000_000;
    const jitter = 1.0 + (this.rng.random() - 0.5) * 0.2;
    return Math.max(1_000, Math.round(baseDelay * jitter));
  }

  private onlineNodes(): Node[] {
    const result: Node[] = [];
    for (const n of this.nodes.values()) {
      if (n.online) result.push(n);
    }
    return result;
  }

  private delist(list: bigint[], hash: bigint): void {
    const idx = list.indexOf(hash);
    if (idx >= 0) {
      list.splice(idx, 1);
    }
  }

  private enlistHead(list: bigint[], hash: bigint): void {
    this.delist(list, hash);
    list.unshift(hash);
  }

  private enlistTail(list: bigint[], hash: bigint): void {
    this.delist(list, hash);
    list.push(hash);
  }

  private listTail(list: bigint[]): bigint | null {
    return list.length > 0 ? list[list.length - 1] : null;
  }

  private scheduleGossip(node: Node, hash: bigint): void {
    const topic = node.topics.get(hash);
    const eligible = topic !== undefined && !isPinned(topic.hash);
    if (eligible) {
      this.enlistHead(node.gossipQueue, hash);
    } else {
      this.delist(node.gossipQueue, hash);
    }
  }

  private scheduleGossipUrgent(node: Node, hash: bigint): void {
    const topic = node.topics.get(hash);
    if (!topic || isPinned(topic.hash)) {
      return;
    }
    if (node.gossipUrgent.includes(hash)) {
      return;
    }
    this.enlistTail(node.gossipQueue, hash);
    this.enlistHead(node.gossipUrgent, hash);
    this.ensurePollScheduled(node);
  }

  private hasEligiblePeer(node: Node): boolean {
    const threshold = this.nowUs - GOSSIP_PEER_ELIGIBLE;
    for (const p of node.peers) {
      if (!p) continue;
      if (p.lastSeenUs < threshold) continue;
      return true;
    }
    return false;
  }

  private maybeActivatePeriodicUnicast(node: Node): void {
    if (!this.net.periodicUnicastEnabled) {
      node.gossipPeriodicNextUs = HEAT_DEATH;
      return;
    }
    if (node.gossipPeriodicNextUs < HEAT_DEATH) {
      return;
    }
    if (!node.online) {
      return;
    }
    if (!this.hasEligiblePeer(node)) {
      return;
    }
    const deviation = Math.floor(GOSSIP_PERIODIC_UNICAST_PERIOD / 8);
    node.gossipPeriodicNextUs = this.nowUs + this.ditherInt(GOSSIP_PERIODIC_UNICAST_PERIOD, deviation);
  }

  private desiredPollTime(node: Node): number {
    if (!node.online) {
      return HEAT_DEATH;
    }

    let when = HEAT_DEATH;

    if (this.listTail(node.gossipUrgent) !== null) {
      // Urgent repair gossips go out immediately, even before the first broadcast slot.
      when = this.nowUs;
    }

    if (node.gossipNextUs <= this.nowUs) {
      when = this.nowUs;
    } else if (node.gossipNextUs < HEAT_DEATH) {
      when = Math.min(when, node.gossipNextUs);
    }

    if (this.net.periodicUnicastEnabled) {
      if (node.gossipPeriodicNextUs <= this.nowUs) {
        when = this.nowUs;
      } else if (node.gossipPeriodicNextUs < HEAT_DEATH) {
        when = Math.min(when, node.gossipPeriodicNextUs);
      }
    }

    return when;
  }

  private ensurePollScheduled(node: Node): void {
    this.maybeActivatePeriodicUnicast(node);
    const when = this.desiredPollTime(node);
    if (when >= HEAT_DEATH) return;
    if (node.gossipPollScheduledUs === HEAT_DEATH || when < node.gossipPollScheduledUs) {
      node.gossipPollScheduledUs = when;
      this.pushEvent(when, "GOSSIP_POLL", { node_id: node.nodeId, scheduled_us: when });
    }
  }

  private gossipBegin(node: Node): void {
    if (node.gossipNextUs === HEAT_DEATH) {
      const deviation = Math.floor(node.gossipPeriodUs / 8);
      node.gossipNextUs = this.nowUs + this.rng.randomInt(0, deviation + 1);
      this.ensurePollScheduled(node);
    }
  }

  private sendBroadcast(
    sender: Node,
    hash: bigint,
    evictions: number,
    lage: number,
    name: string,
    pushLog: (r: EventRecord) => void,
  ): boolean {
    for (const dest of this.nodes.values()) {
      if (dest.nodeId === sender.nodeId) continue;
      if (!dest.online) continue;
      if (dest.partitionSet !== sender.partitionSet) continue;
      if (this.rng.random() < this.net.lossProbability) continue;
      const delay = this.distanceDelay(sender.nodeId, dest.nodeId);
      this.pushEvent(this.nowUs + delay, "MSG_ARRIVE", {
        src: sender.nodeId,
        dst: dest.nodeId,
        topic_hash: hash,
        evictions,
        lage,
        name,
        ttl: 0,
        msg_type: "broadcast",
        send_time_us: this.nowUs,
      });
    }
    pushLog({
      timeUs: this.nowUs,
      event: "broadcast",
      src: sender.nodeId,
      dst: null,
      topicHash: hash,
      details: {
        evictions,
        lage,
        name,
        subjectId: subjectId(hash, evictions, SUBJECT_ID_MODULUS),
      },
    });
    // Transport send is modeled as always successful on the sender side.
    return true;
  }

  private sendUnicast(
    sender: Node,
    destId: number,
    hash: bigint,
    evictions: number,
    lage: number,
    name: string,
    ttl: number,
    msgType: string,
    pushLog: (r: EventRecord) => void,
  ): boolean {
    const dest = this.nodes.get(destId);
    if (!dest || !dest.online) return false;
    if (dest.partitionSet !== sender.partitionSet) return false;
    if (this.rng.random() < this.net.lossProbability) return false;

    const delay = this.distanceDelay(sender.nodeId, destId);
    this.pushEvent(this.nowUs + delay, "MSG_ARRIVE", {
      src: sender.nodeId,
      dst: destId,
      topic_hash: hash,
      evictions,
      lage,
      name,
      ttl,
      msg_type: msgType,
      send_time_us: this.nowUs,
    });
    pushLog({
      timeUs: this.nowUs,
      event: msgType,
      src: sender.nodeId,
      dst: destId,
      topicHash: hash,
      details: {
        evictions,
        lage,
        ttl,
        delayUs: delay,
        name,
        subjectId: subjectId(hash, evictions, SUBJECT_ID_MODULUS),
      },
    });
    return true;
  }

  private randomPeerExcept(node: Node, blacklist: Set<number>): GossipPeer | null {
    const threshold = this.nowUs - GOSSIP_PEER_ELIGIBLE;
    const eligible: GossipPeer[] = [];
    for (const p of node.peers) {
      if (!p) continue;
      if (p.lastSeenUs < threshold) continue;
      if (blacklist.has(p.nodeId)) continue;
      eligible.push(p);
    }
    if (eligible.length === 0) return null;
    return this.rng.choice(eligible);
  }

  private randomPeriodicTopic(node: Node): Topic | null {
    const eligible: Topic[] = [];
    for (const topic of node.topics.values()) {
      if (!isPinned(topic.hash)) {
        eligible.push(topic);
      }
    }
    if (eligible.length === 0) {
      return null;
    }
    return this.rng.choice(eligible);
  }

  private dedupMatchOrLru(node: Node, dhash: bigint): DedupEntry {
    let oldest = node.dedup[0];
    for (const entry of node.dedup) {
      if (entry.hash === dhash) {
        return entry;
      }
      if (oldest.lastSeenUs > entry.lastSeenUs) {
        oldest = entry;
      }
    }
    return oldest;
  }

  private dedupIsFresh(entry: DedupEntry, dhash: bigint): boolean {
    return (entry.hash !== dhash) || (entry.lastSeenUs < (this.nowUs - GOSSIP_DEDUP_TIMEOUT));
  }

  private dedupUpdate(entry: DedupEntry, dhash: bigint): void {
    entry.hash = dhash;
    entry.lastSeenUs = this.nowUs;
  }

  private gossipEpidemicForward(
    node: Node,
    senderId: number,
    originalTtl: number,
    hash: bigint,
    evictions: number,
    lage: number,
    name: string,
    pushLog: (r: EventRecord) => void,
  ): void {
    if (originalTtl <= 0) return;
    this.gossipBegin(node);
    const ttl = originalTtl - 1;
    const blacklist = new Set<number>([senderId]);
    for (let i = 0; i < GOSSIP_OUTDEGREE; i++) {
      const peer = this.randomPeerExcept(node, blacklist);
      if (!peer) break;
      blacklist.add(peer.nodeId);
      this.sendUnicast(node, peer.nodeId, hash, evictions, lage, name, ttl, "forward", pushLog);
    }
  }

  private gossipPeerUpdate(node: Node, senderId: number, pushLog: (r: EventRecord) => void): void {
    const postUpdate = (): void => {
      this.maybeActivatePeriodicUnicast(node);
      this.ensurePollScheduled(node);
    };

    for (const p of node.peers) {
      if (p && p.nodeId === senderId) {
        p.lastSeenUs = this.nowUs;
        postUpdate();
        return;
      }
    }

    const threshold = this.nowUs - GOSSIP_PEER_STALE;
    let oldestIdx = 0;
    let oldestSeen = node.peers[0]?.lastSeenUs ?? BIG_BANG;
    for (let i = 1; i < node.peers.length; i++) {
      const seen = node.peers[i]?.lastSeenUs ?? BIG_BANG;
      if (seen < oldestSeen) {
        oldestSeen = seen;
        oldestIdx = i;
      }
    }

    if (oldestSeen < threshold) {
      const oldNodeId = node.peers[oldestIdx]?.nodeId ?? null;
      node.peers[oldestIdx] = { nodeId: senderId, lastSeenUs: this.nowUs };
      pushLog({ timeUs: this.nowUs, event: "peer_refresh", src: node.nodeId, dst: null,
        topicHash: 0n, details: { oldPeer: oldNodeId, newPeer: senderId, peerIdx: oldestIdx, reason: "stale" } });
      postUpdate();
      return;
    }

    if (
      this.nowUs >= node.peerReplacementMoratoriumUntil &&
      this.rng.chance(GOSSIP_PEER_REPLACEMENT_PROBABILITY_RECIPROCAL)
    ) {
      const idx = this.rng.randrange(GOSSIP_PEER_COUNT);
      const oldNodeId = node.peers[idx]?.nodeId ?? null;
      node.peers[idx] = { nodeId: senderId, lastSeenUs: this.nowUs };
      pushLog({ timeUs: this.nowUs, event: "peer_refresh", src: node.nodeId, dst: null,
        topicHash: 0n, details: { oldPeer: oldNodeId, newPeer: senderId, peerIdx: idx, reason: "random" } });
      const moratorium = GOSSIP_PERIOD >> 1;
      node.peerReplacementMoratoriumUntil = this.nowUs + this.ditherInt(moratorium, moratorium);
    }
    postUpdate();
  }

  private handleGossipPollEvent(payload: Record<string, unknown>, pushLog: (r: EventRecord) => void): void {
    const nodeId = payload["node_id"] as number;
    const scheduled = payload["scheduled_us"] as number;
    const node = this.nodes.get(nodeId);
    if (!node || !node.online) return;
    if (node.gossipPollScheduledUs !== scheduled) return;
    node.gossipPollScheduledUs = HEAT_DEATH;
    this.gossipPoll(node, pushLog);
  }

  private gossipPoll(node: Node, pushLog: (r: EventRecord) => void): void {
    this.maybeActivatePeriodicUnicast(node);

    if (this.nowUs >= node.gossipNextUs) {
      const hash = this.listTail(node.gossipQueue);
      if (hash !== null) {
        const topic = node.topics.get(hash);
        if (topic) {
          this.scheduleGossip(node, hash);
          const lage = topicLage(topic.tsCreatedUs, this.nowUs);
          const sent = this.sendBroadcast(node, topic.hash, topic.evictions, lage, topic.name, pushLog);
          if (sent) {
            const dhash = gossipDedupHash(topic.hash, topic.evictions, lage);
            this.dedupUpdate(this.dedupMatchOrLru(node, dhash), dhash);
          }
        } else {
          this.delist(node.gossipQueue, hash);
        }
      }
      const deviation = Math.floor(node.gossipPeriodUs / 8);
      node.gossipNextUs = this.nowUs + this.ditherInt(node.gossipPeriodUs, deviation);
    }

    if (this.net.periodicUnicastEnabled && this.nowUs >= node.gossipPeriodicNextUs) {
      const topic = this.randomPeriodicTopic(node);
      const peer = this.randomPeerExcept(node, new Set<number>());
      if (topic && peer) {
        const lage = topicLage(topic.tsCreatedUs, this.nowUs);
        const sent = this.sendUnicast(
          node,
          peer.nodeId,
          topic.hash,
          topic.evictions,
          lage,
          topic.name,
          GOSSIP_PERIODIC_UNICAST_TTL,
          "periodic_unicast",
          pushLog,
        );
        if (sent) {
          const dhash = gossipDedupHash(topic.hash, topic.evictions, lage);
          this.dedupUpdate(this.dedupMatchOrLru(node, dhash), dhash);
        }
      }
      const deviation = Math.floor(GOSSIP_PERIODIC_UNICAST_PERIOD / 8);
      node.gossipPeriodicNextUs = this.nowUs + this.ditherInt(GOSSIP_PERIODIC_UNICAST_PERIOD, deviation);
    }

    const urgentHash = this.listTail(node.gossipUrgent);
    if (urgentHash !== null) {
      this.gossipBegin(node);
      this.delist(node.gossipUrgent, urgentHash);
      const topic = node.topics.get(urgentHash);
      if (topic) {
        const lage = topicLage(topic.tsCreatedUs, this.nowUs);
        const blacklist = new Set<number>();
        let succeeded = false;
        for (let i = 0; i < GOSSIP_OUTDEGREE; i++) {
          const peer = this.randomPeerExcept(node, blacklist);
          if (!peer) break;
          blacklist.add(peer.nodeId);
          const ok = this.sendUnicast(
            node,
            peer.nodeId,
            topic.hash,
            topic.evictions,
            lage,
            topic.name,
            GOSSIP_TTL,
            "unicast",
            pushLog,
          );
          succeeded = succeeded || ok;
        }
        if (succeeded) {
          const dhash = gossipDedupHash(topic.hash, topic.evictions, lage);
          this.dedupUpdate(this.dedupMatchOrLru(node, dhash), dhash);
          node.lastUrgentUs = this.nowUs;
        }
      }
    }

    this.ensurePollScheduled(node);
  }

  private onGossipKnownTopic(
    node: Node,
    mine: Topic,
    remoteEvictions: number,
    remoteLage: number,
    pushLog: (r: EventRecord) => void,
  ): CrdtMergeOutcome {
    const mineLage = topicLage(mine.tsCreatedUs, this.nowUs);
    let out = CrdtMergeOutcome.Consensus;

    if (mine.evictions !== remoteEvictions) {
      const won = (mineLage > remoteLage) || ((mineLage === remoteLage) && (mine.evictions > remoteEvictions));
      pushLog({
        timeUs: this.nowUs,
        event: "conflict",
        src: node.nodeId,
        dst: null,
        topicHash: mine.hash,
        details: {
          type: "divergence",
          name: mine.name,
          local_won: won,
          local_evictions: mine.evictions,
          remote_evictions: remoteEvictions,
          local_lage: mineLage,
          remote_lage: remoteLage,
        },
      });

      if (won) {
        this.gossipBegin(node);
        this.scheduleGossipUrgent(node, mine.hash);
        out = CrdtMergeOutcome.LocalWin;
      } else {
        topicMergeLage(mine, this.nowUs, remoteLage);
        this.topicAllocate(node, mine, remoteEvictions, this.nowUs);
        pushLog({
          timeUs: this.nowUs,
          event: "resolved",
          src: node.nodeId,
          dst: null,
          topicHash: mine.hash,
          details: { name: mine.name, accepted_evictions: mine.evictions, new_sid: topicSubjectId(mine) },
        });
        out = CrdtMergeOutcome.LocalLoss;
      }
    } else {
      this.scheduleGossip(node, mine.hash);
    }

    topicMergeLage(mine, this.nowUs, remoteLage);
    return out;
  }

  private onGossipUnknownTopic(
    node: Node,
    remoteHash: bigint,
    remoteEvictions: number,
    remoteLage: number,
    remoteName: string,
    pushLog: (r: EventRecord) => void,
  ): CrdtMergeOutcome {
    const sid = subjectId(remoteHash, remoteEvictions, SUBJECT_ID_MODULUS);
    const mine = this.findBySubjectId(node, sid, null);
    if (!mine) return CrdtMergeOutcome.Consensus;

    const mineLage = topicLage(mine.tsCreatedUs, this.nowUs);
    const won = leftWins(mineLage, mine.hash, remoteLage, remoteHash);
    pushLog({
      timeUs: this.nowUs,
      event: "conflict",
      src: node.nodeId,
      dst: null,
      topicHash: mine.hash,
      details: {
        type: "collision",
        name: mine.name,
        remote_name: remoteName,
        local_won: won,
        local_sid: topicSubjectId(mine),
        remote_hash: remoteHash,
        remote_evictions: remoteEvictions,
      },
    });

    if (won) {
      this.gossipBegin(node);
      this.scheduleGossipUrgent(node, mine.hash);
      return CrdtMergeOutcome.LocalWin;
    } else {
      this.topicAllocate(node, mine, mine.evictions + 1, this.nowUs);
      pushLog({
        timeUs: this.nowUs,
        event: "resolved",
        src: node.nodeId,
        dst: null,
        topicHash: mine.hash,
        details: { name: mine.name, new_evictions: mine.evictions, new_sid: topicSubjectId(mine) },
      });
      return CrdtMergeOutcome.LocalLoss;
    }
  }

  private handleMsgArrive(payload: Record<string, unknown>, pushLog: (r: EventRecord) => void): void {
    const dstId = payload["dst"] as number;
    const node = this.nodes.get(dstId);
    if (!node || !node.online) return;

    const srcId = payload["src"] as number;
    const hash = payload["topic_hash"] as bigint;
    const evictions = payload["evictions"] as number;
    const lage = payload["lage"] as number;
    const name = payload["name"] as string;
    const ttl = payload["ttl"] as number;
    const msgType = payload["msg_type"] as string;

    if ((lage < LAGE_MIN) || (lage > LAGE_MAX)) return;
    if (isPinned(hash) && evictions !== 0) return;

    this.gossipPeerUpdate(node, srcId, pushLog);

    const dedupHash = gossipDedupHash(hash, evictions, lage);
    const dedup = this.dedupMatchOrLru(node, dedupHash);
    const dedupFresh = this.dedupIsFresh(dedup, dedupHash);
    const ttlPositive = ttl > 0;
    const shouldForward = dedupFresh && ttlPositive;
    this.dedupUpdate(dedup, dedupHash);

    pushLog({
      timeUs: this.nowUs,
      event: "received",
      src: dstId,
      dst: srcId,
      topicHash: hash,
      details: {
        originSrc: srcId,
        sendTimeUs: payload["send_time_us"],
        name,
        msgType,
      },
    });

    const maybeLogGossipXterminated = (forwardAllowedByOutcome: boolean): void => {
      const epidemicUnicast = msgType === "unicast" || msgType === "forward" || msgType === "periodic_unicast";
      if (!epidemicUnicast || !forwardAllowedByOutcome || shouldForward) {
        return;
      }
      const ttlDropped = !ttlPositive;
      const dedupDropped = !dedupFresh;
      const dropReason = ttlDropped && dedupDropped ? "ttl+dedup" : (ttlDropped ? "ttl" : "dedup");
      pushLog({
        timeUs: this.nowUs,
        event: "gossip_xterminated",
        src: node.nodeId,
        dst: null,
        topicHash: hash,
        details: {
          name,
          msgType,
          ttl,
          drop_reason: dropReason,
        },
      });
    };

    const mine = node.topics.get(hash);
    if (mine) {
      const outcome = this.onGossipKnownTopic(node, mine, evictions, lage, pushLog);
      const allowForward = outcome === CrdtMergeOutcome.Consensus;
      if (shouldForward && allowForward) {
        this.gossipEpidemicForward(
          node,
          srcId,
          ttl,
          hash,
          mine.evictions,
          topicLage(mine.tsCreatedUs, this.nowUs),
          name,
          pushLog,
        );
      }
      maybeLogGossipXterminated(allowForward);
    } else {
      const outcome = this.onGossipUnknownTopic(node, hash, evictions, lage, name, pushLog);
      const allowForward = (outcome === CrdtMergeOutcome.Consensus) || (outcome === CrdtMergeOutcome.LocalLoss);
      if (shouldForward && allowForward) {
        this.gossipEpidemicForward(node, srcId, ttl, hash, evictions, lage, name, pushLog);
      }
      maybeLogGossipXterminated(allowForward);
    }
  }

  private topicAllocate(node: Node, topic: Topic, newEvictions: number, nowUs: number): void {
    if (isPinned(topic.hash)) {
      topic.evictions = 0;
      return;
    }

    let ev = Math.max(0, Math.floor(newEvictions));
    for (let iter = 0; iter < 20000; iter++) {
      const sid = subjectId(topic.hash, ev, SUBJECT_ID_MODULUS);
      const that = this.findBySubjectId(node, sid, topic);
      const victory = !that || leftWins(topicLage(topic.tsCreatedUs, nowUs), topic.hash, topicLage(that.tsCreatedUs, nowUs), that.hash);

      if (victory) {
        topic.evictions = ev;
        this.scheduleGossipUrgent(node, topic.hash);
        if (that) {
          this.topicAllocate(node, that, that.evictions + 1, nowUs);
        }
        return;
      }
      ev += 1;
    }
  }

  private findBySubjectId(node: Node, sid: number, exclude: Topic | null): Topic | null {
    for (const t of node.topics.values()) {
      if (exclude && t === exclude) continue;
      if (isPinned(t.hash)) continue;
      if (topicSubjectId(t) === sid) return t;
    }
    return null;
  }

  private handleNodeJoin(nodeId: number, pushLog: (r: EventRecord) => void): void {
    const node = this.nodes.get(nodeId);
    if (!node) return;
    node.online = true;
    node.peerReplacementMoratoriumUntil = BIG_BANG;
    this.maybeActivatePeriodicUnicast(node);
    // If gossip was already commenced while offline (e.g., topic created before join),
    // schedule polling now that the node is online.
    if (node.gossipNextUs < HEAT_DEATH || node.gossipPeriodicNextUs < HEAT_DEATH) {
      this.ensurePollScheduled(node);
    }
    pushLog({
      timeUs: this.nowUs,
      event: "join",
      src: nodeId,
      dst: null,
      topicHash: 0n,
      details: {},
    });
  }

  private snapNode(node: Node): NodeSnapshot {
    const topics: TopicSnap[] = [];
    const sorted = [...node.topics.values()].sort((a, b) => a.sortOrder - b.sortOrder);
    for (const t of sorted) {
      topics.push({
        name: t.name,
        hash: t.hash,
        evictions: t.evictions,
        subjectId: topicSubjectId(t),
        lage: topicLage(t.tsCreatedUs, this.nowUs),
        tsCreatedUs: t.tsCreatedUs,
        sortOrder: t.sortOrder,
      });
    }

    const peers: (PeerSnap | null)[] = node.peers.map(
      p => (p ? { nodeId: p.nodeId, lastSeenUs: p.lastSeenUs } : null),
    );

    return {
      nodeId: node.nodeId,
      online: node.online,
      topics,
      peers,
      gossipQueueFront: this.listTail(node.gossipQueue),
      gossipUrgentFront: this.listTail(node.gossipUrgent),
      nextBroadcastUs: node.gossipNextUs >= HEAT_DEATH ? 0 : node.gossipNextUs,
      lastUrgentUs: node.lastUrgentUs,
      partitionSet: node.partitionSet,
    };
  }

  private topicSidMap(node: Node): Map<bigint, number> {
    const m = new Map<bigint, number>();
    for (const [h, t] of node.topics) {
      m.set(h, topicSubjectId(t));
    }
    return m;
  }
}
