"""Matplotlib animated visualization for the gossip simulation."""

from __future__ import annotations

import math
from typing import Protocol

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
from matplotlib.animation import FuncAnimation
from matplotlib.lines import Line2D

from .sim import (
    EventRecord, NetworkConfig, Node, NodeSnapshot, Simulation,
    GOSSIP_PERIOD, GOSSIP_PEER_ELIGIBLE,
)

# ---------------------------------------------------------------------------
# Renderer protocol (swappable interface)
# ---------------------------------------------------------------------------

class Renderer(Protocol):
    def setup(self, nodes: list[Node], config: NetworkConfig) -> None: ...
    def render_frame(self, time_us: int, events: list[EventRecord],
                     node_states: dict[int, Node]) -> None: ...
    def finalize(self) -> None: ...


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FRAME_WINDOW_US = 50_000   # 50 ms per animation frame

# How long message lines persist on screen (sim-time microseconds)
MSG_PERSIST_US = 600_000   # 600 ms — long enough to be visible

CONFLICT_FLASH_US = 400_000

# Colors
C_ONLINE      = "#d5e8d4"   # light green fill
C_OFFLINE     = "#e0e0e0"
C_CONFLICT    = "#f8cecc"   # light red fill
C_BORDER      = "#333333"
C_BROADCAST   = "#888888"
C_UNICAST     = "#e67e22"
C_FORWARD     = "#9b59b6"
C_PEER_FRESH  = "#27ae60"
C_PEER_STALE  = "#95a5a6"
C_PEER_EMPTY  = "#cccccc"
C_CONVERGED   = "#27ae60"
C_DIVERGED    = "#c0392b"


# ---------------------------------------------------------------------------
# Matplotlib renderer
# ---------------------------------------------------------------------------

class MatplotlibRenderer:
    def __init__(self, sim: Simulation, output: str | None = None, interval_ms: int = 30):
        self.sim = sim
        self.output = output
        self.interval_ms = interval_ms

    # -- layout --

    @staticmethod
    def _node_positions(node_ids: list[int]) -> dict[int, tuple[float, float]]:
        n = len(node_ids)
        pos = {}
        radius = 1.2 + 0.18 * n  # scale ring with node count
        for i, nid in enumerate(node_ids):
            angle = 2 * math.pi * i / n - math.pi / 2
            pos[nid] = (radius * math.cos(angle), radius * math.sin(angle))
        return pos

    # -- main entry --

    def run(self, duration_us: int) -> None:
        log = self.sim.log
        node_ids = sorted(self.sim.nodes.keys())
        pos = self._node_positions(node_ids)

        fig = plt.figure(figsize=(18, 10))
        gs = fig.add_gridspec(1, 2, width_ratios=[3, 1], wspace=0.02)
        ax = fig.add_subplot(gs[0])
        ap = fig.add_subplot(gs[1])

        num_frames = duration_us // FRAME_WINDOW_US + 1

        # Pre-bucket events by frame index
        frame_events: list[list[EventRecord]] = [[] for _ in range(num_frames)]
        for ev in log:
            fi = ev.time_us // FRAME_WINDOW_US
            if 0 <= fi < num_frames:
                frame_events[fi].append(ev)

        # Persistent state across frames
        active_conflicts: dict[int, int] = {}        # node_id -> flash_until_us
        active_msgs: list[tuple[int, EventRecord]] = []  # (expire_us, record)

        # Node box dimensions (data coords) — sized for content
        bw = 0.58   # box half-width
        bh = 0.55   # box half-height

        def _edge_point(cx: float, cy: float, tx: float, ty: float) -> tuple[float, float]:
            """Point on the box edge closest to target (tx, ty)."""
            dx, dy = tx - cx, ty - cy
            if dx == 0 and dy == 0:
                return cx, cy
            # Scale so the line hits the box edge
            sx = bw / abs(dx) if dx != 0 else 1e9
            sy = bh / abs(dy) if dy != 0 else 1e9
            s = min(sx, sy)
            return cx + dx * s, cy + dy * s

        def update(frame_idx: int) -> list:
            t = frame_idx * FRAME_WINDOW_US
            ax.clear()
            ap.clear()

            # --- network axes setup ---
            margin = 1.0
            xs = [p[0] for p in pos.values()]
            ys = [p[1] for p in pos.values()]
            ax.set_xlim(min(xs) - bw - margin, max(xs) + bw + margin)
            ax.set_ylim(min(ys) - bh - margin, max(ys) + bh + margin)
            ax.set_aspect("equal")
            ax.axis("off")

            evs = frame_events[frame_idx] if frame_idx < len(frame_events) else []

            # Update conflict flash tracker
            for ev in evs:
                if ev.event == "conflict":
                    active_conflicts[ev.src] = t + CONFLICT_FLASH_US

            # Add new messages and expire old ones
            for ev in evs:
                if ev.event in ("broadcast", "unicast", "forward"):
                    active_msgs.append((t + MSG_PERSIST_US, ev))
            while active_msgs and active_msgs[0][0] < t:
                active_msgs.pop(0)

            # --- draw message lines (behind nodes) ---
            for expire_us, ev in active_msgs:
                age_frac = 1.0 - (expire_us - t) / MSG_PERSIST_US  # 0=new, 1=about to expire
                alpha = max(0.15, 0.9 - 0.75 * age_frac)

                if ev.event == "broadcast":
                    color, ls, lw = C_BROADCAST, "-", 0.7
                elif ev.event == "unicast":
                    color, ls, lw = C_UNICAST, "-", 1.8
                else:  # forward
                    color, ls, lw = C_FORWARD, "--", 1.4

                src_pos = pos.get(ev.src)
                if src_pos is None:
                    continue

                dsts: list[int] = []
                if ev.dst is not None:
                    dsts = [ev.dst]
                else:
                    dsts = [nid for nid in node_ids if nid != ev.src]

                for did in dsts:
                    dp = pos.get(did)
                    if dp is None:
                        continue
                    x0, y0 = _edge_point(src_pos[0], src_pos[1], dp[0], dp[1])
                    x1, y1 = _edge_point(dp[0], dp[1], src_pos[0], src_pos[1])
                    ax.annotate(
                        "", xy=(x1, y1), xytext=(x0, y0),
                        arrowprops=dict(
                            arrowstyle="-|>", color=color,
                            linewidth=lw, linestyle=ls, alpha=alpha,
                            connectionstyle="arc3,rad=0.12",
                            shrinkA=0, shrinkB=0,
                        ), zorder=1,
                    )

            # --- get snapshots for this frame ---
            snaps = self.sim.snapshots.get(frame_idx, {})

            # --- draw rectangular node boxes ---
            for nid in node_ids:
                cx, cy_ = pos[nid]
                snap = snaps.get(nid)

                is_online = snap.online if snap else False
                in_conflict = nid in active_conflicts and active_conflicts[nid] >= t
                if not is_online:
                    bg = C_OFFLINE
                elif in_conflict:
                    bg = C_CONFLICT
                else:
                    bg = C_ONLINE

                box = FancyBboxPatch(
                    (cx - bw, cy_ - bh), 2 * bw, 2 * bh,
                    boxstyle="round,pad=0.02", facecolor=bg,
                    edgecolor=C_BORDER, linewidth=1.2, zorder=3,
                )
                ax.add_patch(box)

                # --- text content inside the box ---
                fs = 5.5  # font size
                x_left = cx - bw + 0.04
                y_top = cy_ + bh - 0.05
                line_h = 0.065

                def put(row: int, text: str, **kw) -> None:
                    defaults = dict(fontsize=fs, fontfamily="monospace",
                                    va="top", ha="left", zorder=4, clip_on=True)
                    defaults.update(kw)
                    ax.text(x_left, y_top - row * line_h, text, **defaults)

                if snap is None:
                    put(0, f"N{nid}  OFFLINE", fontweight="bold", fontsize=fs + 1)
                    continue

                # Row 0: header
                status = "ONLINE" if snap.online else "OFFLINE"
                put(0, f"N{nid}  {status}", fontweight="bold", fontsize=fs + 1)

                # Row 1: next heartbeat countdown
                if snap.online and snap.next_broadcast_us > 0:
                    dt = max(0, snap.next_broadcast_us - t) / 1_000_000
                    put(1, f"next HB: {dt:.2f}s")
                else:
                    put(1, "next HB: --")

                # Row 2: next topic scheduled for broadcast
                # Resolve hash to name from snapshot topics
                next_topic_name = "--"
                nxt_h = snap.gossip_urgent_front or snap.gossip_queue_front
                if snap.online and nxt_h is not None:
                    for ts in snap.topics:
                        if ts.hash == nxt_h:
                            next_topic_name = ts.name
                            break
                put(2, f"next tx: {next_topic_name}")

                # Row 3: separator
                put(3, "-" * 26, color="#999999")

                # Row 4+: topics  (name -> SID, evictions)
                row = 4
                if snap.topics:
                    for ts in snap.topics:  # already sorted by name in snapshot
                        put(row, f"{ts.name[:12]:12s} S{ts.subject_id:<5d} e{ts.evictions}")
                        row += 1
                else:
                    put(row, "(no topics)")
                    row += 1

                # Separator before peers
                put(row, "-" * 26, color="#999999")
                row += 1

                # Peers: show populated ones, then count empties
                put(row, "peers:", fontweight="bold")
                row += 1
                n_empty = 0
                for p in snap.peers:
                    if p is None:
                        n_empty += 1
                    else:
                        age = (t - p.last_seen_us) / 1_000_000
                        fresh = (t - p.last_seen_us) < GOSSIP_PEER_ELIGIBLE
                        c = C_PEER_FRESH if fresh else C_PEER_STALE
                        put(row, f"  N{p.node_id} {age:5.1f}s ago", color=c)
                        row += 1
                if n_empty > 0:
                    put(row, f"  ({n_empty} empty)", color=C_PEER_EMPTY)
                    row += 1

            # --- right panel: title + convergence + legend ---
            ap.axis("off")
            ap.set_xlim(0, 1)
            ap.set_ylim(0, 1)

            y = 0.96
            dy = 0.035

            ap.text(0.05, y, f"t = {t / 1_000_000:.3f} s",
                    fontsize=13, fontweight="bold", fontfamily="monospace",
                    transform=ap.transAxes, va="top")
            y -= 2 * dy

            # Check convergence from snapshot state
            if snaps:
                sid_maps = []
                for s in snaps.values():
                    if s.online:
                        sid_maps.append({ts.hash: ts.subject_id for ts in s.topics})
                converged = len(sid_maps) < 2 or all(m == sid_maps[0] for m in sid_maps[1:])
            else:
                converged = True
            conv_color = C_CONVERGED if converged else C_DIVERGED
            ap.text(0.05, y, f"Converged: {'YES' if converged else 'NO'}",
                    fontsize=11, fontweight="bold", color=conv_color,
                    fontfamily="monospace", transform=ap.transAxes, va="top")
            y -= 2.5 * dy

            # --- color legend ---
            ap.text(0.05, y, "Legend", fontsize=10, fontweight="bold",
                    transform=ap.transAxes, va="top")
            y -= 1.2 * dy

            legend_items = [
                (C_ONLINE,      "s", "Node online"),
                (C_CONFLICT,    "s", "Node in conflict"),
                (C_OFFLINE,     "s", "Node offline"),
                (C_BROADCAST,   "-", "Broadcast gossip"),
                (C_UNICAST,     "-", "Unicast epidemic"),
                (C_FORWARD,     "--", "Epidemic forward"),
                (C_PEER_FRESH,  "o", "Peer (fresh)"),
                (C_PEER_STALE,  "o", "Peer (stale)"),
            ]

            for color, marker, label in legend_items:
                if marker == "s":
                    ap.add_patch(FancyBboxPatch(
                        (0.05, y - 0.008), 0.04, 0.02,
                        boxstyle="round,pad=0.002", facecolor=color,
                        edgecolor=C_BORDER, linewidth=0.6,
                        transform=ap.transAxes, clip_on=False,
                    ))
                elif marker == "o":
                    ap.plot(0.07, y + 0.002, "o", color=color, markersize=5,
                            transform=ap.transAxes, clip_on=False)
                else:
                    ls = marker
                    ax_x = [0.05, 0.09]
                    ax_y = [y + 0.002, y + 0.002]
                    line = Line2D(ax_x, ax_y, color=color, linewidth=2 if ls == "-" else 1.5,
                                  linestyle=ls, transform=ap.transAxes, clip_on=False)
                    ap.add_line(line)
                ap.text(0.12, y, label, fontsize=8, fontfamily="monospace",
                        transform=ap.transAxes, va="top")
                y -= dy

            # --- event counts ---
            y -= dy
            ap.text(0.05, y, "Event counts", fontsize=10, fontweight="bold",
                    transform=ap.transAxes, va="top")
            y -= 1.2 * dy

            by_type: dict[str, int] = {}
            for ev in self.sim.log:
                if ev.time_us <= t:
                    by_type[ev.event] = by_type.get(ev.event, 0) + 1
            for etype in ("broadcast", "unicast", "forward", "conflict",
                          "resolved", "learned", "join"):
                cnt = by_type.get(etype, 0)
                ap.text(0.05, y, f"  {etype:12s} {cnt}",
                        fontsize=8, fontfamily="monospace",
                        transform=ap.transAxes, va="top")
                y -= dy

            return []

        anim = FuncAnimation(
            fig, update, frames=num_frames,
            interval=self.interval_ms, blit=False, repeat=False,
        )

        if self.output:
            if self.output.endswith(".gif"):
                anim.save(self.output, writer="pillow", fps=1000 // self.interval_ms)
            else:
                anim.save(self.output, writer="ffmpeg", fps=1000 // self.interval_ms)
            print(f"Saved animation to {self.output}")
        else:
            plt.tight_layout()
            plt.show()
