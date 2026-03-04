// ---------------------------------------------------------------------------
// Type definitions — port of Python dataclasses to TypeScript interfaces
// ---------------------------------------------------------------------------

export interface NetworkConfig {
  delayUs: [number, number]; // [min, max]
  lossProbability: number;
}

export interface Topic {
  name: string;
  hash: bigint;
  evictions: number;
  tsCreatedUs: number;
}

export interface GossipPeer {
  nodeId: number;
  lastSeenUs: number;
}

export interface DedupEntry {
  hash: bigint;
  lastSeenUs: number;
}

export interface Node {
  nodeId: number;
  online: boolean;
  topics: Map<bigint, Topic>;
  gossipQueue: bigint[];
  gossipUrgent: bigint[];
  peers: (GossipPeer | null)[];
  dedup: DedupEntry[];
  nextBroadcastUs: number;
  partitionSet: "A" | "B";
  peerReplacementMoratoriumUntil: number;
}

export interface EventRecord {
  timeUs: number;
  event: string;
  src: number;
  dst: number | null;
  topicHash: bigint;
  details: Record<string, unknown>;
}

// Lightweight snapshots for rendering

export interface TopicSnap {
  name: string;
  hash: bigint;
  evictions: number;
  subjectId: number;
}

export interface PeerSnap {
  nodeId: number;
  lastSeenUs: number;
}

export interface NodeSnapshot {
  nodeId: number;
  online: boolean;
  topics: TopicSnap[];
  peers: (PeerSnap | null)[];
  gossipQueueFront: bigint | null;
  gossipUrgentFront: bigint | null;
  nextBroadcastUs: number;
  partitionSet: "A" | "B";
}
