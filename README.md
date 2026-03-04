# Cyphal v1.1 epidemic gossip simulation and visualization tool

This is a simple script that simulates the behavior of Cyphal v1.1 networks, specifically the epidemic gossips around nodes joining/leaving the network and CRDT consensus repairs. The protocol implementation and specification are available in <https://github.com/OpenCyphal-Garage/cy>. Each node broadcasts a gossip message every N seconds. Every other online node receives that gossip, adjusted for network losses. When CRDT consensus issues are found, urgent repairs are initiated, which amount to two things:

- **Constant-cadence broadcast:** The affected entry (topic) is scheduled to broadcast-gossip at the next slot, but it may take up to 3 seconds due to the fixed broadcast cadence, which is slow. Broadcast gossips provide a deterministric upper bound on the worst case convergence times and node discovery times.

- **Immediate epidemic unicast:** The affected entry is also immediately unicast to two randomly chosen peers that the local node knows about. This is similar to Cyclone/HyParView/etc. Such epidemic gossips are forwarded to other nodes subject to deduplication (each node keeps 16 last seen gossips to break cycles early) and TTL (decremented at each forward, used as a last-resort deterministic cycle breaker). If an epidemic message carries information concerning a known topic, it is first validated to ensure it is not obsolete, and amended as necessary (e.g., a CRDT message that is older than the current state is corrected before forwarding).

## Usage

Requires Node.js (for `npx`). From the repo root:

```bash
cd cysim
npm install
./build.sh
```

Open `cysim/index.html` in a browser. No server needed — it's a static page.

```bash
xdg-open cysim/index.html   # GNU/Linux
open cysim/index.html       # macOS
start cysim/index.html      # Windows
npx serve cysim             # Or just serve locally if you prefer
```
