// ---------------------------------------------------------------------------
// Protocol constants
// ---------------------------------------------------------------------------

export const SUBJECT_ID_MODULUS = 1999;
export const LAGE_MIN = -1;
export const LAGE_MAX = 35;
export const DEFAULT_SHARD_COUNT = 1984;
export const DEFAULT_GOSSIP_PERIOD = 5.0;
export const GOSSIP_PERIOD_DITHER_RATIO = 8;
export const DEFAULT_GOSSIP_BROADCAST_RATIO = 10;
export const DEFAULT_GOSSIP_URGENT_DELAY = 0.01;
export const SPIN_BLOCK_MAX = 5_000;

// ---------------------------------------------------------------------------
// Rendering constants
// ---------------------------------------------------------------------------

export const PROPAGATION_SPEED = 8000; // pixels per second of sim time
export const MSG_PERSIST_US = 1_000_000; // unicast/forward arrow linger after arrival
export const BROADCAST_PERSIST_US = 200_000; // expanding circle lifetime
export const CONFLICT_FLASH_US = 200_000;
