// ---------------------------------------------------------------------------
// Protocol constants (matching cy.c / sim.py)
// ---------------------------------------------------------------------------

export const GOSSIP_PERIOD       = 3_000_000;       // 3 s in microseconds
export const GOSSIP_DITHER       = 375_000;          // ±375 ms
export const GOSSIP_TTL          = 16;
export const GOSSIP_OUTDEGREE    = 2;
export const GOSSIP_PEER_COUNT   = 8;
export const GOSSIP_DEDUP_CAP    = 16;
export const GOSSIP_DEDUP_TIMEOUT = 1_000_000;       // 1 s
export const GOSSIP_PEER_STALE   = 2 * GOSSIP_PERIOD; // 6 s
export const GOSSIP_PEER_ELIGIBLE = 3 * GOSSIP_PERIOD; // 9 s
export const PEER_REPLACE_PROB   = 1.0 / 8;
export const SUBJECT_ID_PINNED_MAX = 0x1FFF;         // 8191
export const SUBJECT_ID_MODULUS  = 8192;
export const LAGE_MIN            = -1;
export const LAGE_MAX            = 35;

// ---------------------------------------------------------------------------
// Rendering constants
// ---------------------------------------------------------------------------

export const MSG_PERSIST_US      = 600_000;          // 600 ms
export const CONFLICT_FLASH_US   = 400_000;          // 400 ms
