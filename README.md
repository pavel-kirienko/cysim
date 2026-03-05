<div align="center">

<img src="https://opencyphal.org/favicon-192.png" width="60px">

# Cyphal v1.1 distributed consensus visual simulator

🌐 **[gerasim.opencyphal.org](https://gerasim.opencyphal.org/)**

[![Forum](https://img.shields.io/discourse/https/forum.opencyphal.org/users.svg?logo=discourse&color=1700b3)](https://forum.opencyphal.org)

</div>

-----

This is a simple script that simulates the behavior of Cyphal v1.1 networks, specifically the epidemic gossips around nodes joining/leaving the network and CRDT consensus repairs. The protocol reference implementation and specification are available in `submodules/cy`.

Each node broadcasts a gossip message every N seconds. Every other online node receives that gossip, adjusted for network losses. When CRDT consensus issues are found, urgent repairs are initiated, which amount to two things:

- **Constant-cadence broadcast:** The affected entry (topic) is scheduled to broadcast-gossip at the next slot, but it may take up to 3 seconds due to the fixed broadcast cadence, which is slow. Broadcast gossips provide a deterministric upper bound on the worst case convergence times and node discovery times.

- **Immediate epidemic unicast:** The affected entry is also immediately unicast to two randomly chosen peers that the local node knows about. This is similar to Cyclone/HyParView/etc. Such epidemic gossips are forwarded to other nodes subject to deduplication (each node keeps 16 last seen gossips to break cycles early) and TTL (decremented at each forward, used as a last-resort deterministic cycle breaker). If an epidemic message carries information concerning a known topic, it is first validated to ensure it is not obsolete, and amended as necessary (e.g., a CRDT message that is older than the current state is corrected before forwarding).

## Usage

Requires Node.js:

```bash
npm install
./build.sh
python3 -m http.server 8080
```

## Testing

```bash
npm test            # run tests once
npm run test:watch  # run tests in watch mode
```
