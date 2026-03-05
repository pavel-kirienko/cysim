// ---------------------------------------------------------------------------
// Protocol constants
// ---------------------------------------------------------------------------

export const GOSSIP_PERIOD          = 3_000_000;
export const GOSSIP_TTL             = 16;
export const GOSSIP_OUTDEGREE       = 2;
export const GOSSIP_PEER_COUNT      = 8;
export const GOSSIP_DEDUP_CAP       = 16;
export const GOSSIP_DEDUP_TIMEOUT   = 1_000_000;
export const GOSSIP_PEER_STALE      = 2 * GOSSIP_PERIOD;
export const GOSSIP_PEER_ELIGIBLE   = 3 * GOSSIP_PERIOD;
export const GOSSIP_PEER_REPLACEMENT_PROBABILITY_RECIPROCAL = 8;
export const SUBJECT_ID_PINNED_MAX  = 0x1FFF;
export const SUBJECT_ID_MODULUS     = 8191;  // Keep small to keep bruteforce collision generation manageable
export const LAGE_MIN               = -1;
export const LAGE_MAX               = 35;
export const SPIN_BLOCK_MAX         = 5_000;

// ---------------------------------------------------------------------------
// Rendering constants
// ---------------------------------------------------------------------------

export const PROPAGATION_SPEED      = 8000;         // pixels per second of sim time
export const MSG_PERSIST_US         = 1_000_000;    // unicast/forward arrow linger after arrival
export const BROADCAST_PERSIST_US   = 1_000_000;    // expanding circle lifetime
export const CONFLICT_FLASH_US      = 200_000;
