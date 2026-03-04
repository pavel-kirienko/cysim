"""CLI entry point: python -m cysim"""

from __future__ import annotations

import argparse
import sys

from .scenarios import SCENARIOS
from .sim import NetworkConfig
from .viz import MatplotlibRenderer


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="cysim",
        description="Cyphal v1.1 epidemic gossip simulation & visualization",
    )
    parser.add_argument("--nodes", "-n", type=int, default=6, help="Number of nodes (default: 6)")
    parser.add_argument("--scenario", "-s", choices=list(SCENARIOS.keys()), default="collision",
                        help="Scenario preset (default: collision)")
    parser.add_argument("--delay-min", type=int, default=1_000, help="Min network delay in us (default: 1000)")
    parser.add_argument("--delay-max", type=int, default=10_000, help="Max network delay in us (default: 10000)")
    parser.add_argument("--loss", type=float, default=0.0, help="Packet loss probability 0.0-1.0 (default: 0.0)")
    parser.add_argument("--duration", type=float, default=30.0, help="Simulation duration in seconds (default: 30)")
    parser.add_argument("--output", "-o", type=str, default=None,
                        help="Output file (.mp4 or .gif). If omitted, show interactive window.")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed (default: 42)")
    args = parser.parse_args()

    net = NetworkConfig(
        delay_us=(args.delay_min, args.delay_max),
        loss_probability=args.loss,
    )

    builder = SCENARIOS[args.scenario]
    sim = builder(num_nodes=args.nodes, net=net, seed=args.seed)

    duration_us = int(args.duration * 1_000_000)
    print(f"Running {args.scenario} scenario: {args.nodes} nodes, {args.duration}s, "
          f"delay={args.delay_min}-{args.delay_max}us, loss={args.loss:.0%}")

    from .viz import FRAME_WINDOW_US
    sim.run(until_us=duration_us, snapshot_interval_us=FRAME_WINDOW_US)

    # Check convergence
    converged = sim.check_convergence()
    print(f"Convergence: {'YES' if converged else 'NO'}")
    if not converged:
        details = sim.convergence_details()
        for nid, mapping in sorted(details.items()):
            print(f"  N{nid}: {mapping}")

    # Summary stats
    by_type: dict[str, int] = {}
    for ev in sim.log:
        by_type[ev.event] = by_type.get(ev.event, 0) + 1
    print(f"Events: {len(sim.log)} total — {by_type}")

    # Convergence assertion
    online_nodes = [n for n in sim.nodes.values() if n.online]
    if len(online_nodes) >= 2:
        ref = {th: t.subject_id() for th, t in online_nodes[0].topics.items()}
        for node in online_nodes[1:]:
            node_map = {th: t.subject_id() for th, t in node.topics.items()}
            for th in ref:
                if th in node_map and ref[th] != node_map[th]:
                    print(f"WARNING: N{node.node_id} disagrees on topic {th:#x}: "
                          f"S{node_map[th]} vs S{ref[th]}", file=sys.stderr)

    # Render
    renderer = MatplotlibRenderer(sim, output=args.output)
    renderer.run(duration_us)


if __name__ == "__main__":
    main()
