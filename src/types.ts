// ---------------------------------------------------------------------------
// Type definitions — port of Python dataclasses to TypeScript interfaces
// ---------------------------------------------------------------------------

export interface NetworkConfig {
  delayUs: [number, number]; // [min, max]
  lossProbability: number;
  periodicUnicastEnabled: boolean;
}

export interface Topic {
  name: string;
  hash: bigint;
  evictions: number;
  tsCreatedUs: number;
  sortOrder: number;
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
  // list head is index 0, list tail is the last element
  gossipQueue: bigint[];
  gossipUrgent: bigint[];
  peers: (GossipPeer | null)[];
  dedup: DedupEntry[];
  gossipNextUs: number;
  gossipPeriodicNextUs: number;
  gossipPollScheduledUs: number;
  gossipPeriodUs: number;
  partitionSet: "A" | "B";
  peerReplacementMoratoriumUntil: number;
  lastUrgentUs: number;
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
  lage: number;
  tsCreatedUs: number;
  sortOrder: number;
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
  lastUrgentUs: number;
  partitionSet: "A" | "B";
}

// ---------------------------------------------------------------------------
// Timeline types
// ---------------------------------------------------------------------------

export type TimelineCode = "GB"|"GU"|"GP"|"GF"|"GR"|"GX"|"TN"|"TC"|"TD"|"TX"|"NN"|"NX"|"CR"|"PR";

export interface TimelineEvent {
  id: number;
  timeUs: number;
  code: TimelineCode;
  nodeId: number;
  topicHash: bigint;
  details: Record<string, unknown>;
  secondaryTopicHash: bigint | null; // for TC events: the remote topic hash
  receiveIds: number[];      // for send events: linked receive event IDs
  sendId: number | null;     // for GR events: the originating send event ID
  historyIndex: number;
}
