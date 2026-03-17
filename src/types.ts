// ---------------------------------------------------------------------------
// Type definitions — port of Python dataclasses to TypeScript interfaces
// ---------------------------------------------------------------------------

export interface ProtocolConfig {
  subjectIdModulus: number;
  shardCount: number;
  gossipPeriod: number;
  gossipBroadcastRatio: number;
  gossipUrgentDelay: number;
}

export interface NetworkConfig {
  delay: [number, number]; // [min, max], seconds
  lossProbability: number;
  protocol: ProtocolConfig;
}

export interface Topic {
  name: string;
  hash: bigint;
  evictions: number;
  tsCreatedUs: number;
  sortOrder: number;
}

export interface TopicScheduleState {
  nextGossipUs: number;
  gossipCounter: number;
}

export interface PendingUrgentGossip {
  deadlineUs: number;
}

export interface Node {
  nodeId: number;
  online: boolean;
  topics: Map<bigint, Topic>;
  topicScheduleByHash: Map<bigint, TopicScheduleState>;
  pendingUrgentByHash: Map<bigint, PendingUrgentGossip>;
  gossipPollScheduledUs: number;
  partitionSet: "A" | "B";
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

export interface NodeSnapshot {
  nodeId: number;
  online: boolean;
  topics: TopicSnap[];
  shardIds: number[];
  nextTopicHash: bigint | null;
  nextGossipUs: number;
  pendingUrgentCount: number;
  lastUrgentUs: number;
  partitionSet: "A" | "B";
}

// ---------------------------------------------------------------------------
// Timeline types
// ---------------------------------------------------------------------------

export type TimelineCode =
  | "GB"
  | "GS"
  | "GU"
  | "GP"
  | "GF"
  | "GR"
  | "GX"
  | "TN"
  | "TC"
  | "TD"
  | "TX"
  | "NN"
  | "NX"
  | "CR"
  | "PR";

export interface TimelineEvent {
  id: number;
  timeUs: number;
  code: TimelineCode;
  nodeId: number;
  topicHash: bigint;
  details: Record<string, unknown>;
  secondaryTopicHash: bigint | null; // for TC events: the remote topic hash
  receiveIds: number[]; // for send events: linked receive event IDs
  sendId: number | null; // for GR events: the originating send event ID
  historyIndex: number;
}
