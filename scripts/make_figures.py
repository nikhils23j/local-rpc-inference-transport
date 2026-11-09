#!/usr/bin/env python3
"""Draw the README figures from results.json. Reads nothing else.

usage: python3 scripts/make_figures.py      (writes docs/img/*.png)

Every plotted value comes from results.json (written by evaluate.py).
Each point is the median of the repetitions; whiskers are min-max.
"""
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter, NullFormatter  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "img"

# Fixed categorical order (validated for CVD separation), text inks, hairline grid.
C1, C2, C3 = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#8a8984", "#e4e3df"

plt.rcParams.update({
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "font.size": 11, "axes.titlesize": 11.5, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "text.color": INK, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "legend.frameon": False,
})


def stat(case, key):
    a = case["aggregate"][key]
    return a["median"], a["median"] - a["min"], a["max"] - a["median"]


def suptitle(fig, text):
    fig.suptitle(text, x=0.01, ha="left", fontweight="bold", fontsize=12.5)


def batching(results):
    cases = [c for c in results["cases"] if c["experiment"] == "E1_batching"]
    modes = [("none", "none (1 sequence per step)", C1),
             ("static", "static batching", C2),
             ("continuous", "continuous batching", C3)]
    fig, (ax_g, ax_t) = plt.subplots(1, 2, figsize=(12, 5))
    rates = sorted({c["client"]["rate"] for c in cases})
    ax_g.plot(rates, rates, color=MUTED, lw=1, ls=(0, (4, 3)), zorder=1)
    ax_g.text(rates[-2], rates[-2] * 1.04, "goodput = offered", rotation=0, ha="right",
              va="bottom", fontsize=9, color=INK2)
    for mode, label, color in modes:
        rows = sorted((c for c in cases if c["server"]["mode"] == mode), key=lambda c: c["client"]["rate"])
        x = [c["client"]["rate"] for c in rows]
        for ax, key in ((ax_g, "goodput_rps"), (ax_t, "ttft_p99_ms")):
            m, lo, hi = zip(*(stat(c, key) for c in rows))
            ax.errorbar(x, m, yerr=[lo, hi], color=color, lw=2, marker="o", ms=7, mec="white",
                        mew=1.5, elinewidth=1.3, capsize=3, label=label, zorder=3)
    deadline = results["base_client"]["b-deadline-ms"]
    ax_t.axhline(deadline, color=MUTED, lw=1, zorder=1)
    ax_t.text(rates[1] + 10, deadline * 1.06, f"{deadline} ms deadline", va="bottom", fontsize=9, color=INK2)
    ax_t.set_yscale("log")
    ax_t.set_ylim(top=deadline * 1.6)
    ax_t.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax_t.yaxis.set_minor_formatter(NullFormatter())
    for ax in (ax_g, ax_t):
        ax.set_xlabel("offered load (requests / s, open-loop Poisson)")
        ax.set_xticks(rates)
    ax_g.set_ylabel("goodput (requests / s finished in deadline)")
    ax_t.set_ylabel("TTFT p99 (ms, log scale)")
    ax_g.set_title("Goodput")
    ax_t.set_title("Time to first token, p99")
    ax_g.legend(loc="upper left")
    w = results["base_server"]["workers"]
    suptitle(fig, f"Batching mode vs offered load ({w} workers, no admission control; "
                  f"median of {results['repetitions']} runs, whisker = min–max)")
    fig.tight_layout()
    fig.savefig(OUT / "batching_modes.png", dpi=200)
    plt.close(fig)


POLICY_LABEL = {
    "unbounded": "unbounded",
    "taildrop64": "tail-drop, 64 waiting",
    "taildrop256": "tail-drop, 256 waiting",
    "deadline": "deadline-aware (slack 1.0)",
    "deadline_slack0.8": "deadline-aware (slack 0.8)",
    "adaptive": "adaptive AIMD",
}


def admission(results):
    rate = max(c["client"]["rate"] for c in results["cases"] if c["experiment"] == "E2_admission")
    rows = [c for c in results["cases"] if c["experiment"] == "E2_admission" and c["client"]["rate"] == rate]
    names = [POLICY_LABEL[c["label"].split("@")[0]] for c in rows]
    y = list(range(len(rows)))
    fig, axes = plt.subplots(1, 3, figsize=(13, 5.2), sharey=True,
                             gridspec_kw={"width_ratios": [1.1, 1.2, 1.0]})
    ax_g, ax_f, ax_p = axes

    def hbar(ax, ys, key, color, h=0.6, label=None):
        m, lo, hi = zip(*(stat(c, key) for c in rows))
        ax.barh(ys, m, h, color=color, label=label, zorder=2)
        ax.errorbar(m, ys, xerr=[lo, hi], fmt="none", ecolor=INK, elinewidth=1.1, capsize=3, zorder=3)
        return m, hi

    m, hi = hbar(ax_g, y, "goodput_rps", C1)
    for yy, v, h in zip(y, m, hi):
        ax_g.text(v + h + 6, yy, f"{v:g}/s", va="center", fontsize=9.5, color=INK2)
    ax_g.axvline(rate, color=MUTED, lw=1, ls=(0, (4, 3)))
    ax_g.set_xlim(0, rate * 1.15)
    ax_g.set_title(f"Goodput, req/s\n(dashed = {rate}/s offered)")

    hbar(ax_f, [v - 0.2 for v in y], "rejected", C2, h=0.38, label="rejected up front")
    hbar(ax_f, [v + 0.2 for v in y], "timed_out", C3, h=0.38, label="timed out after admission")
    ax_f.set_title("Requests turned away at the door\nvs. dropped late (count per run)")
    ax_f.legend(loc="upper center", bbox_to_anchor=(0.5, -0.07), ncols=2, fontsize=9.5)

    m, hi = hbar(ax_p, y, "p99_ms", INK2)
    deadline = results["base_client"]["b-deadline-ms"]
    ax_p.axvline(deadline, color=MUTED, lw=1, ls=(0, (4, 3)))
    ax_p.set_title(f"p99 latency, ms\n(dashed = {deadline} ms deadline)")

    ax_g.set_yticks(y, names)
    ax_g.invert_yaxis()
    for ax in axes:
        ax.grid(axis="y", visible=False)
        ax.tick_params(axis="y", length=0)
    sent = rows[0]["aggregate"]["sent"]["median"]
    suptitle(fig, f"Admission policy under overload: {rate}/s offered (~{sent:,} requests per run), "
                  f"continuous batching, median of {results['repetitions']}, whisker = min–max")
    fig.tight_layout()
    fig.savefig(OUT / "admission_policies.png", dpi=200)
    plt.close(fig)


def workers(results):
    rows = sorted((c for c in results["cases"] if c["experiment"] == "E4_workers"),
                  key=lambda c: c["server"]["workers"])
    w = [c["server"]["workers"] for c in rows]
    rate = rows[0]["client"]["rate"]
    x = list(range(len(rows)))
    fig, axes = plt.subplots(1, 3, figsize=(12, 4.4))
    panels = [("goodput_rps", "Goodput (req/s in deadline)", C1, "{:g}/s"),
              ("rejected", "Rejected at the door (requests)", C2, "{:,.0f}"),
              ("p99_ms", "p99 latency (ms)", INK2, "{:.0f} ms")]
    for ax, (key, title, color, fmt) in zip(axes, panels):
        m, lo, hi = zip(*(stat(c, key) for c in rows))
        ax.bar(x, m, 0.6, color=color, zorder=2)
        ax.errorbar(x, m, yerr=[lo, hi], fmt="none", ecolor=INK, elinewidth=1.1, capsize=3, zorder=3)
        top = max(v + h for v, h in zip(m, hi))
        for xx, v, h in zip(x, m, hi):
            ax.text(xx, v + h + top * 0.02, fmt.format(v), ha="center", va="bottom", fontsize=9.5, color=INK2)
        ax.set_xticks(x, [f"{n} worker{'s' if n > 1 else ''}" for n in w])
        ax.tick_params(axis="x", length=0)
        ax.grid(axis="x", visible=False)
        ax.set_title(title)
        ax.set_ylim(0, top * 1.18)
    axes[0].axhline(rate, color=MUTED, lw=1, ls=(0, (4, 3)))
    axes[0].set_ylim(0, rate * 1.15)
    axes[0].text(-0.4, rate * 1.02, f"offered {rate}/s", fontsize=9, color=INK2)
    cap = rows[0]["server"]["capacity"]
    suptitle(fig, f"Scaling workers with the queue policy held fixed (tail-drop {cap}, {rate}/s offered; "
                  f"median of {results['repetitions']}, whisker = min–max)")
    fig.tight_layout()
    fig.savefig(OUT / "workers_scaling.png", dpi=200)
    plt.close(fig)


def main():
    results = json.loads((ROOT / "results.json").read_text())
    OUT.mkdir(parents=True, exist_ok=True)
    batching(results)
    admission(results)
    workers(results)
    for p in sorted(OUT.glob("*.png")):
        print(f"{p.relative_to(ROOT)}  {p.stat().st_size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
