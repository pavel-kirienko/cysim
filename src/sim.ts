// ---------------------------------------------------------------------------
// Simulation engine -- model-aligned CRDT core
// ---------------------------------------------------------------------------

import {
  NetworkConfig,
  Topic,
  Node,
  TopicScheduleState,
  PendingUrgentGossip,
  EventRecord,
  TopicSnap,
  NodeSnapshot,
} from "./types.js";
import {
  DEFAULT_GOSSIP_BROADCAST_RATIO,
  DEFAULT_GOSSIP_PERIOD,
  GOSSIP_PERIOD_DITHER_RATIO,
  DEFAULT_GOSSIP_URGENT_DELAY,
  DEFAULT_SHARD_COUNT,
  SUBJECT_ID_MODULUS,
  LAGE_MIN,
  LAGE_MAX,
  PROPAGATION_SPEED,
  SPIN_BLOCK_MAX,
} from "./constants.js";

const U64_MASK = 0xffff_ffff_ffff_ffffn;
const HEAT_DEATH = Number.MAX_SAFE_INTEGER;

enum CrdtMergeOutcome {
  Consensus = "consensus",
  LocalWin = "local_win",
  LocalLoss = "local_loss",
}

function asU64(x: bigint): bigint {
  return x & U64_MASK;
}

function isPrime(n: number): boolean {
  const x = Math.floor(n);
  if (x <= 1) return false;
  if (x <= 3) return true;
  if (x % 2 === 0 || x % 3 === 0) return false;
  for (let i = 5; i * i <= x; i += 6) {
    if (x % i === 0 || x % (i + 2) === 0) {
      return false;
    }
  }
  return true;
}

// ---------------------------------------------------------------------------
// Seeded RNG -- Mulberry32
// ---------------------------------------------------------------------------

export class Rng {
  private state: number;

  constructor(seed: number) {
    this.state = seed | 0;
  }

  getState(): number {
    return this.state;
  }
  setState(s: number): void {
    this.state = s;
  }

  random(): number {
    let t = (this.state += 0x6d2b79f5);
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

  get length(): number {
    return this.data.length;
  }

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
  const e = BigInt(Math.max(0, Math.floor(evictions)));
  const sq = asU64(e * e);
  const sid = asU64(topicHash + sq) % BigInt(Math.max(2, Math.floor(modulus)));
  return Number(sid);
}

export function leftWins(lLage: number, lHash: bigint, rLage: number, rHash: bigint): boolean {
  if (lLage !== rLage) {
    return lLage > rLage;
  }
  return lHash < rHash;
}

function leftWinsDivergence(lLage: number, lEvictions: number, rLage: number, rEvictions: number): boolean {
  return lLage > rLage || (lLage === rLage && lEvictions > rEvictions);
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
  const rLage = Math.floor(remoteLage);
  topic.tsCreatedUs = Math.min(topic.tsCreatedUs, nowUs - pow2us(rLage) * 1_000_000);
}

function topicSubjectId(t: Topic, modulus: number): number {
  return subjectId(t.hash, t.evictions, modulus);
}

function parseHashOverride(name: string): bigint | null {
  let out = 0n;
  const maxNibbles = Math.min(name.length, 17);
  for (let i = 0; i < maxNibbles; i++) {
    const ch = name.charCodeAt(name.length - i - 1);
    if (ch === 35) {
      // '#'
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
        see1 = rapidMix(
          asU64(rapidRead64LE(bytes, p + 16) ^ RAPID_SECRET[1]),
          asU64(rapidRead64LE(bytes, p + 24) ^ see1),
        );
        see2 = rapidMix(
          asU64(rapidRead64LE(bytes, p + 32) ^ RAPID_SECRET[2]),
          asU64(rapidRead64LE(bytes, p + 40) ^ see2),
        );
        see3 = rapidMix(
          asU64(rapidRead64LE(bytes, p + 48) ^ RAPID_SECRET[3]),
          asU64(rapidRead64LE(bytes, p + 56) ^ see3),
        );
        see4 = rapidMix(
          asU64(rapidRead64LE(bytes, p + 64) ^ RAPID_SECRET[4]),
          asU64(rapidRead64LE(bytes, p + 72) ^ see4),
        );
        see5 = rapidMix(
          asU64(rapidRead64LE(bytes, p + 80) ^ RAPID_SECRET[5]),
          asU64(rapidRead64LE(bytes, p + 88) ^ see5),
        );
        see6 = rapidMix(
          asU64(rapidRead64LE(bytes, p + 96) ^ RAPID_SECRET[6]),
          asU64(rapidRead64LE(bytes, p + 104) ^ see6),
        );
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
        seed = rapidMix(
          asU64(rapidRead64LE(bytes, p + 16) ^ RAPID_SECRET[2]),
          asU64(rapidRead64LE(bytes, p + 24) ^ seed),
        );
        if (i > 48) {
          seed = rapidMix(
            asU64(rapidRead64LE(bytes, p + 32) ^ RAPID_SECRET[1]),
            asU64(rapidRead64LE(bytes, p + 40) ^ seed),
          );
          if (i > 64) {
            seed = rapidMix(
              asU64(rapidRead64LE(bytes, p + 48) ^ RAPID_SECRET[1]),
              asU64(rapidRead64LE(bytes, p + 56) ^ seed),
            );
            if (i > 80) {
              seed = rapidMix(
                asU64(rapidRead64LE(bytes, p + 64) ^ RAPID_SECRET[2]),
                asU64(rapidRead64LE(bytes, p + 72) ^ seed),
              );
              if (i > 96) {
                seed = rapidMix(
                  asU64(rapidRead64LE(bytes, p + 80) ^ RAPID_SECRET[1]),
                  asU64(rapidRead64LE(bytes, p + 88) ^ seed),
                );
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
  return rapidMix(asU64(a ^ RAPID_SECRET[7]), asU64(b ^ RAPID_SECRET[1] ^ BigInt(i)));
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
  return {
    nodeId,
    online: false,
    topics: new Map(),
    topicScheduleByHash: new Map(),
    pendingUrgentByHash: new Map(),
    gossipPollScheduledUs: HEAT_DEATH,
    partitionSet: "A",
    lastUrgentUs: 0,
  };
}

export function nodeAddTopic(node: Node, topic: Topic): void {
  node.topics.set(topic.hash, topic);
}

export function nodeFindBySubjectId(node: Node, sid: number, modulus = SUBJECT_ID_MODULUS): Topic | null {
  for (const t of node.topics.values()) {
    if (topicSubjectId(t, modulus) === sid) {
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
    const protocol = net.protocol ?? ({} as NetworkConfig["protocol"]);
    const subjectIdModulus = Math.floor(protocol.subjectIdModulus ?? SUBJECT_ID_MODULUS);
    if (!isPrime(subjectIdModulus)) {
      throw new Error(`subjectIdModulus must be prime, got ${subjectIdModulus}`);
    }
    const shardCount = Math.floor(protocol.shardCount ?? DEFAULT_SHARD_COUNT);
    if (shardCount <= 0) {
      throw new Error("shardCount must be positive");
    }
    this.net = {
      delay: net.delay,
      lossProbability: net.lossProbability,
      protocol: {
        subjectIdModulus,
        shardCount,
        gossipPeriod: Math.max(0, protocol.gossipPeriod ?? DEFAULT_GOSSIP_PERIOD),
        gossipBroadcastRatio: Math.max(1, Math.floor(protocol.gossipBroadcastRatio ?? DEFAULT_GOSSIP_BROADCAST_RATIO)),
        gossipUrgentDelay: Math.max(0, protocol.gossipUrgentDelay ?? DEFAULT_GOSSIP_URGENT_DELAY),
      },
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
      const topicScheduleByHash = new Map<bigint, TopicScheduleState>();
      for (const [h, s] of n.topicScheduleByHash) {
        topicScheduleByHash.set(h, { ...s });
      }
      const pendingUrgentByHash = new Map<bigint, PendingUrgentGossip>();
      for (const [h, u] of n.pendingUrgentByHash) {
        pendingUrgentByHash.set(h, { ...u });
      }
      nodes.set(id, {
        nodeId: n.nodeId,
        online: n.online,
        topics,
        topicScheduleByHash,
        pendingUrgentByHash,
        gossipPollScheduledUs: n.gossipPollScheduledUs,
        partitionSet: n.partitionSet,
        lastUrgentUs: n.lastUrgentUs,
      });
    }
    const queueData = this.queue.toArray().map((ev) => ({ ...ev, payload: { ...ev.payload } }));
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
      const topicScheduleByHash = new Map<bigint, TopicScheduleState>();
      for (const [h, s] of n.topicScheduleByHash) {
        topicScheduleByHash.set(h, { ...s });
      }
      const pendingUrgentByHash = new Map<bigint, PendingUrgentGossip>();
      for (const [h, u] of n.pendingUrgentByHash) {
        pendingUrgentByHash.set(h, { ...u });
      }

      this.nodes.set(id, {
        nodeId: n.nodeId,
        online: n.online,
        topics,
        topicScheduleByHash,
        pendingUrgentByHash,
        gossipPollScheduledUs: n.gossipPollScheduledUs,
        partitionSet: n.partitionSet,
        lastUrgentUs: n.lastUrgentUs,
      });
    }

    const queueData = state.queueData.map((ev) => ({ ...ev, payload: { ...ev.payload } }));
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
        timeUs: this.nowUs,
        event: "node_expunged",
        src: nodeId,
        dst: null,
        topicHash: 0n,
        details: {},
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
    node.topicScheduleByHash.clear();
    node.pendingUrgentByHash.clear();

    // Re-allocate and re-schedule all topics.
    for (const t of node.topics.values()) {
      this.topicAllocate(node, t, 0, this.nowUs);
      this.ensureTopicScheduleEntry(node, t.hash);
      this.scheduleUrgent(node, t.hash);
    }

    node.gossipPollScheduledUs = HEAT_DEATH;
    node.lastUrgentUs = 0;
    this.pushEvent(this.nowUs, "NODE_JOIN", { node_id: nodeId });
  }

  setPartition(nodeId: number, set: "A" | "B"): void {
    const node = this.nodes.get(nodeId);
    if (node) {
      node.partitionSet = set;
      this.ensurePollScheduled(node);
    }
  }

  addTopicToNode(
    nodeId: number,
    name?: string,
    targetSid?: number,
    initEvictions?: number,
    initLage?: number,
  ): Topic | null {
    const node = this.nodes.get(nodeId);
    if (!node) return null;
    if (node.topics.size >= Math.floor(this.net.protocol.subjectIdModulus / 2)) {
      throw new Error("too many topics for this subjectIdModulus");
    }

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
    this.ensureTopicScheduleEntry(node, topic.hash);
    this.scheduleUrgent(node, topic.hash);
    this.ensurePollScheduled(node);

    this.pendingEvents.push({
      timeUs: this.nowUs,
      event: "topic_new",
      src: nodeId,
      dst: null,
      topicHash: hash,
      details: { name },
    });
    return topic;
  }

  private findNameForSid(targetSid: number): string {
    const modulus = this.net.protocol.subjectIdModulus;
    const sid = Math.min(Math.max(targetSid, 0), modulus - 1);
    const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
    for (;;) {
      let name = "";
      for (let i = 0; i < 4; i++) {
        name += alphabet[this.rng.randrange(alphabet.length)];
      }
      const hash = topicHash(name);
      if (subjectId(hash, 0, modulus) === sid) {
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
    this.ensurePollScheduled(node);
    this.pendingEvents.push({
      timeUs: this.nowUs,
      event: "topic_adjust",
      src: nodeId,
      dst: null,
      topicHash: hash,
      details: { name: topic.name, evictions: topic.evictions },
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
      timeUs: this.nowUs,
      event: "topic_adjust",
      src: nodeId,
      dst: null,
      topicHash: hash,
      details: { name: topic.name, lage: newLage },
    });
  }

  destroyTopicOnNode(nodeId: number, hash: bigint): void {
    const node = this.nodes.get(nodeId);
    if (!node) return;
    const topic = node.topics.get(hash);
    node.topics.delete(hash);
    node.topicScheduleByHash.delete(hash);
    node.pendingUrgentByHash.delete(hash);
    this.ensurePollScheduled(node);
    this.pendingEvents.push({
      timeUs: this.nowUs,
      event: "topic_expunged",
      src: nodeId,
      dst: null,
      topicHash: hash,
      details: { name: topic?.name ?? "?" },
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
      this.onlineNodes().map((n) => ({
        partition: n.partitionSet,
        topics: [...n.topics.values()].map((t) => ({
          hash: t.hash,
          subjectId: topicSubjectId(t, this.net.protocol.subjectIdModulus),
          evictions: t.evictions,
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
        topics: s.topics.map((t) => ({ hash: t.hash, subjectId: t.subjectId, evictions: t.evictions })),
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

  private durationRangeToUs(range: [number, number]): number {
    const lo = Math.min(range[0], range[1]);
    const hi = Math.max(range[0], range[1]);
    const sampleSec = lo === hi ? lo : lo + this.rng.random() * (hi - lo);
    return Math.max(0, Math.round(sampleSec * 1_000_000));
  }

  private secToUs(seconds: number): number {
    return Math.max(0, Math.round(seconds * 1_000_000));
  }

  private randomIntUs(minUs: number, maxUsExclusive: number): number {
    const lo = Math.max(0, Math.floor(minUs));
    const hi = Math.max(0, Math.floor(maxUsExclusive));
    return this.rng.randomInt(lo, hi);
  }

  private periodicIntervalBoundsUs(gossipCounter: number, suppressed: boolean): [number, number] {
    const periodUs = this.secToUs(this.net.protocol.gossipPeriod);
    const ditherUs = Math.floor(periodUs / GOSSIP_PERIOD_DITHER_RATIO);

    if (suppressed) {
      return [periodUs + ditherUs, periodUs * 3];
    }

    let delayMinUs = Math.max(0, periodUs - ditherUs);
    const delayMaxUs = periodUs + ditherUs;
    if (gossipCounter < this.net.protocol.gossipBroadcastRatio) {
      delayMinUs = Math.floor(delayMinUs / 16);
    }
    return [delayMinUs, delayMaxUs];
  }

  private randDelay(): number {
    return this.durationRangeToUs(this.net.delay);
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

  private shardForTopic(hash: bigint): number {
    const shardCount = BigInt(this.net.protocol.shardCount);
    const offset = Number(asU64(hash) % shardCount);
    return this.net.protocol.subjectIdModulus + offset;
  }

  private nodeListensToShard(node: Node, shardIndex: number): boolean {
    for (const topic of node.topics.values()) {
      if (this.shardForTopic(topic.hash) === shardIndex) {
        return true;
      }
    }
    return false;
  }

  private nextPeriodicTopic(node: Node): { hash: bigint; nextUs: number } | null {
    let bestHash: bigint | null = null;
    let bestNext = HEAT_DEATH;
    const stale: bigint[] = [];

    for (const [hash, state] of node.topicScheduleByHash) {
      if (!node.topics.has(hash)) {
        stale.push(hash);
        continue;
      }
      if (
        state.nextGossipUs < bestNext ||
        (state.nextGossipUs === bestNext && (bestHash === null || hash < bestHash))
      ) {
        bestHash = hash;
        bestNext = state.nextGossipUs;
      }
    }

    for (const hash of stale) {
      node.topicScheduleByHash.delete(hash);
      node.pendingUrgentByHash.delete(hash);
    }

    if (bestHash === null) {
      return null;
    }
    return { hash: bestHash, nextUs: bestNext };
  }

  private earliestUrgentDeadline(node: Node): number {
    let out = HEAT_DEATH;
    const stale: bigint[] = [];

    for (const [hash, pending] of node.pendingUrgentByHash) {
      if (!node.topics.has(hash)) {
        stale.push(hash);
        continue;
      }
      if (pending.deadlineUs < out) {
        out = pending.deadlineUs;
      }
    }

    for (const hash of stale) {
      node.pendingUrgentByHash.delete(hash);
    }

    return out;
  }

  private ensureTopicScheduleEntry(node: Node, hash: bigint): void {
    if (node.topicScheduleByHash.has(hash)) {
      return;
    }
    node.topicScheduleByHash.set(hash, {
      nextGossipUs: HEAT_DEATH,
      gossipCounter: 0,
    });
  }

  private schedulePeriodic(node: Node, hash: bigint, suppressed: boolean): void {
    const state = node.topicScheduleByHash.get(hash);
    if (!state) return;
    const [delayMinUs, delayMaxUs] = this.periodicIntervalBoundsUs(state.gossipCounter, suppressed);
    state.nextGossipUs = this.nowUs + this.randomIntUs(delayMinUs, delayMaxUs);
  }

  private scheduleAfterHeard(node: Node, hash: bigint): void {
    this.schedulePeriodic(node, hash, true);
  }

  private scheduleUrgent(node: Node, hash: bigint): void {
    if (!node.topics.has(hash)) {
      return;
    }
    this.ensureTopicScheduleEntry(node, hash);
    const period = node.topicScheduleByHash.get(hash);
    const periodDeadlineUs = period?.nextGossipUs ?? HEAT_DEATH;
    const sampledUs = this.nowUs + this.randomIntUs(0, this.secToUs(this.net.protocol.gossipUrgentDelay));
    const deadlineUs = Math.min(sampledUs, periodDeadlineUs);

    const existing = node.pendingUrgentByHash.get(hash);
    if (!existing) {
      node.pendingUrgentByHash.set(hash, { deadlineUs });
      this.ensurePollScheduled(node);
      return;
    }

    if (deadlineUs < existing.deadlineUs) {
      existing.deadlineUs = deadlineUs;
    }
    this.ensurePollScheduled(node);
  }

  private cancelPendingUrgentIfUpToDate(
    node: Node,
    hash: bigint,
    receivedEvictions: number,
    receivedLage: number,
  ): void {
    const topic = node.topics.get(hash);
    if (!topic) return;
    const localEvictions = topic.evictions;
    const localLage = topicLage(topic.tsCreatedUs, this.nowUs);
    if (leftWinsDivergence(localLage, localEvictions, receivedLage, receivedEvictions)) {
      return;
    }
    const pending = node.pendingUrgentByHash.get(hash);
    if (!pending) return;
    if (pending.deadlineUs > this.nowUs) {
      node.pendingUrgentByHash.delete(hash);
    }
  }

  private choosePeriodicScope(node: Node, hash: bigint): "broadcast" | "shard" {
    const state = node.topicScheduleByHash.get(hash);
    const counter = state?.gossipCounter ?? 0;
    const ratio = Math.max(1, this.net.protocol.gossipBroadcastRatio);
    const broadcast = counter < ratio || counter % ratio === 0;
    if (state) {
      state.gossipCounter += 1;
    }
    return broadcast ? "broadcast" : "shard";
  }

  private desiredPollTime(node: Node): number {
    if (!node.online) {
      return HEAT_DEATH;
    }

    let when = HEAT_DEATH;

    const urgent = this.earliestUrgentDeadline(node);
    if (urgent <= this.nowUs) {
      when = this.nowUs;
    } else if (urgent < HEAT_DEATH) {
      when = Math.min(when, urgent);
    }

    const periodic = this.nextPeriodicTopic(node);
    if (periodic !== null) {
      if (periodic.nextUs <= this.nowUs) {
        when = this.nowUs;
      } else {
        when = Math.min(when, periodic.nextUs);
      }
    }

    return when;
  }

  private ensurePollScheduled(node: Node): void {
    const when = this.desiredPollTime(node);
    if (when >= HEAT_DEATH) return;
    if (node.gossipPollScheduledUs === HEAT_DEATH || when < node.gossipPollScheduledUs) {
      node.gossipPollScheduledUs = when;
      this.pushEvent(when, "GOSSIP_POLL", { node_id: node.nodeId, scheduled_us: when });
    }
  }

  private sendBroadcast(
    sender: Node,
    hash: bigint,
    evictions: number,
    lage: number,
    name: string,
    pushLog: (r: EventRecord) => void,
  ): void {
    const recipients: number[] = [];
    const recipientDelays: Record<string, number> = {};

    for (const dest of this.nodes.values()) {
      if (dest.nodeId === sender.nodeId) continue;
      if (!dest.online) continue;
      if (dest.partitionSet !== sender.partitionSet) continue;
      const delay = this.distanceDelay(sender.nodeId, dest.nodeId);
      recipients.push(dest.nodeId);
      recipientDelays[String(dest.nodeId)] = delay;
      if (this.rng.random() < this.net.lossProbability) continue;
      this.pushEvent(this.nowUs + delay, "MSG_ARRIVE", {
        src: sender.nodeId,
        dst: dest.nodeId,
        topic_hash: hash,
        evictions,
        lage,
        name,
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
        subjectId: subjectId(hash, evictions, this.net.protocol.subjectIdModulus),
        recipients,
        recipientDelays,
      },
    });
  }

  private sendShard(
    sender: Node,
    hash: bigint,
    evictions: number,
    lage: number,
    name: string,
    shardIndex: number,
    pushLog: (r: EventRecord) => void,
  ): void {
    const listeners: number[] = [];
    const listenerDelays: Record<string, number> = {};

    for (const dest of this.nodes.values()) {
      if (dest.nodeId === sender.nodeId) continue;
      if (!dest.online) continue;
      if (dest.partitionSet !== sender.partitionSet) continue;
      if (!this.nodeListensToShard(dest, shardIndex)) continue;
      const delay = this.distanceDelay(sender.nodeId, dest.nodeId);
      listeners.push(dest.nodeId);
      listenerDelays[String(dest.nodeId)] = delay;
      if (this.rng.random() < this.net.lossProbability) continue;
      this.pushEvent(this.nowUs + delay, "MSG_ARRIVE", {
        src: sender.nodeId,
        dst: dest.nodeId,
        topic_hash: hash,
        evictions,
        lage,
        name,
        msg_type: "shard",
        shard_index: shardIndex,
        send_time_us: this.nowUs,
      });
    }

    pushLog({
      timeUs: this.nowUs,
      event: "shard",
      src: sender.nodeId,
      dst: null,
      topicHash: hash,
      details: {
        evictions,
        lage,
        name,
        subjectId: subjectId(hash, evictions, this.net.protocol.subjectIdModulus),
        shardIndex,
        listeners,
        listenerDelays,
      },
    });
  }

  private transmitTopicGossip(
    node: Node,
    hash: bigint,
    scope: "broadcast" | "shard",
    pushLog: (r: EventRecord) => void,
  ): boolean {
    const topic = node.topics.get(hash);
    if (!topic) {
      return false;
    }

    const lage = topicLage(topic.tsCreatedUs, this.nowUs);
    if (scope === "broadcast") {
      this.sendBroadcast(node, topic.hash, topic.evictions, lage, topic.name, pushLog);
    } else {
      this.sendShard(node, topic.hash, topic.evictions, lage, topic.name, this.shardForTopic(topic.hash), pushLog);
    }
    return true;
  }

  private emitDueUrgent(node: Node, pushLog: (r: EventRecord) => void): void {
    const due: { hash: bigint; deadlineUs: number }[] = [];

    for (const [hash, pending] of node.pendingUrgentByHash) {
      if (!node.topics.has(hash)) {
        continue;
      }
      if (pending.deadlineUs <= this.nowUs) {
        due.push({ hash, deadlineUs: pending.deadlineUs });
      }
    }

    due.sort((a, b) => {
      if (a.deadlineUs !== b.deadlineUs) return a.deadlineUs - b.deadlineUs;
      return a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0;
    });

    let sentAny = false;
    for (const item of due) {
      node.pendingUrgentByHash.delete(item.hash);
      this.schedulePeriodic(node, item.hash, false);
      const state = node.topicScheduleByHash.get(item.hash);
      if (state) {
        state.gossipCounter = 0;
      }
      const sent = this.transmitTopicGossip(node, item.hash, "broadcast", pushLog);
      sentAny = sentAny || sent;
    }

    if (sentAny) {
      node.lastUrgentUs = this.nowUs;
    }
  }

  private emitPeriodicGossip(node: Node, pushLog: (r: EventRecord) => void): void {
    const next = this.nextPeriodicTopic(node);
    if (!next || next.nextUs > this.nowUs) {
      return;
    }

    this.schedulePeriodic(node, next.hash, false);
    const scope = this.choosePeriodicScope(node, next.hash);
    this.transmitTopicGossip(node, next.hash, scope, pushLog);
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
    this.emitDueUrgent(node, pushLog);
    this.emitPeriodicGossip(node, pushLog);
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
      const won = leftWinsDivergence(mineLage, mine.evictions, remoteLage, remoteEvictions);
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
        this.scheduleUrgent(node, mine.hash);
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
          details: {
            name: mine.name,
            accepted_evictions: mine.evictions,
            new_sid: topicSubjectId(mine, this.net.protocol.subjectIdModulus),
          },
        });
        out = CrdtMergeOutcome.LocalLoss;
      }
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
    const sid = subjectId(remoteHash, remoteEvictions, this.net.protocol.subjectIdModulus);
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
        local_sid: topicSubjectId(mine, this.net.protocol.subjectIdModulus),
        remote_hash: remoteHash,
        remote_evictions: remoteEvictions,
      },
    });

    if (won) {
      this.scheduleUrgent(node, mine.hash);
      return CrdtMergeOutcome.LocalWin;
    }

    const bumped = mine.evictions + 1;
    this.topicAllocate(node, mine, bumped, this.nowUs);
    pushLog({
      timeUs: this.nowUs,
      event: "resolved",
      src: node.nodeId,
      dst: null,
      topicHash: mine.hash,
      details: {
        name: mine.name,
        new_evictions: mine.evictions,
        new_sid: topicSubjectId(mine, this.net.protocol.subjectIdModulus),
      },
    });
    return CrdtMergeOutcome.LocalLoss;
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
    const msgType = payload["msg_type"] as string;
    const shardIndex = payload["shard_index"] as number | undefined;

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
        shardIndex,
      },
    });

    const mine = node.topics.get(hash);
    if (mine) {
      this.onGossipKnownTopic(node, mine, evictions, lage, pushLog);
      this.scheduleAfterHeard(node, hash);
      this.cancelPendingUrgentIfUpToDate(node, hash, evictions, lage);
    } else {
      this.onGossipUnknownTopic(node, hash, evictions, lage, name, pushLog);
    }

    this.ensurePollScheduled(node);
  }

  private topicAllocate(node: Node, topic: Topic, newEvictions: number, nowUs: number): number {
    const originalEvictionsByHash = new Map<bigint, number>();
    const markOriginalEvictions = (t: Topic): void => {
      if (!originalEvictionsByHash.has(t.hash)) {
        originalEvictionsByHash.set(t.hash, t.evictions);
      }
    };

    let moving = topic;
    let targetEvictions = Math.max(0, Math.floor(newEvictions));

    for (let iter = 0; iter < SPIN_BLOCK_MAX; iter++) {
      if (moving.evictions !== targetEvictions) {
        markOriginalEvictions(moving);
      }
      moving.evictions = targetEvictions;

      const sid = topicSubjectId(moving, this.net.protocol.subjectIdModulus);
      const collided = this.findBySubjectId(node, sid, moving);
      if (!collided) {
        break;
      }

      const movingWins = leftWins(
        topicLage(moving.tsCreatedUs, nowUs),
        moving.hash,
        topicLage(collided.tsCreatedUs, nowUs),
        collided.hash,
      );

      if (movingWins) {
        markOriginalEvictions(collided);
        moving = collided;
        targetEvictions = collided.evictions + 1;
      } else {
        markOriginalEvictions(moving);
        targetEvictions = moving.evictions + 1;
      }
    }

    for (const [hash, originalEvictions] of originalEvictionsByHash) {
      const updated = node.topics.get(hash);
      if (!updated) continue;
      if (updated.evictions !== originalEvictions) {
        this.scheduleUrgent(node, hash);
      }
    }

    return topic.evictions;
  }

  private findBySubjectId(node: Node, sid: number, exclude: Topic | null): Topic | null {
    for (const t of node.topics.values()) {
      if (exclude && t === exclude) continue;
      if (topicSubjectId(t, this.net.protocol.subjectIdModulus) === sid) return t;
    }
    return null;
  }

  private handleNodeJoin(nodeId: number, pushLog: (r: EventRecord) => void): void {
    const node = this.nodes.get(nodeId);
    if (!node) return;
    node.online = true;
    this.ensurePollScheduled(node);
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
        subjectId: topicSubjectId(t, this.net.protocol.subjectIdModulus),
        lage: topicLage(t.tsCreatedUs, this.nowUs),
        tsCreatedUs: t.tsCreatedUs,
        sortOrder: t.sortOrder,
      });
    }

    const shardSet = new Set<number>();
    for (const t of node.topics.values()) {
      shardSet.add(this.shardForTopic(t.hash));
    }
    const shardIds = [...shardSet].sort((a, b) => a - b);

    const nextPeriodic = this.nextPeriodicTopic(node);

    return {
      nodeId: node.nodeId,
      online: node.online,
      topics,
      shardIds,
      nextTopicHash: nextPeriodic?.hash ?? null,
      nextGossipUs: nextPeriodic ? nextPeriodic.nextUs : 0,
      pendingUrgentCount: node.pendingUrgentByHash.size,
      lastUrgentUs: node.lastUrgentUs,
      partitionSet: node.partitionSet,
    };
  }

  private topicSidMap(node: Node): Map<bigint, number> {
    const m = new Map<bigint, number>();
    for (const [h, t] of node.topics) {
      m.set(h, topicSubjectId(t, this.net.protocol.subjectIdModulus));
    }
    return m;
  }
}
