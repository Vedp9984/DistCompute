"""Shared matplotlib style for every HW3 figure.

One palette, assigned in fixed order (never cycled), thin marks, recessive
grid, one y-axis per chart, direct labels where they fit.
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
SURFACE = "#fcfcfb"
TEXT = "#0b0b0b"
TEXT2 = "#52514e"
GRID = "#e6e5e1"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": TEXT2, "xtick.color": TEXT2, "ytick.color": TEXT2,
    "text.color": TEXT, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "axes.spines.left": False,
    "axes.axisbelow": True, "lines.linewidth": 2, "lines.markersize": 7,
    "font.size": 10, "axes.titlesize": 11, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "legend.frameon": False, "legend.fontsize": 9, "figure.dpi": 130,
})


def color(i):
    return SERIES[i % len(SERIES)]


import textwrap
from matplotlib.ticker import FuncFormatter

thousands = FuncFormatter(lambda v, _: f"{v:,.0f}")


def finish(fig, path, note=None):
    for ax in fig.axes:
        if ax.get_yscale() == "linear" and ax.get_ylim()[1] >= 10000:
            ax.yaxis.set_major_formatter(thousands)
    if note:
        width = int(fig.get_figwidth() * 15)
        fig.text(0.01, 0.005, "\n".join(textwrap.wrap(note, width)), fontsize=8, color=TEXT2, ha="left", va="bottom")
    lines = len(textwrap.wrap(note, int(fig.get_figwidth() * 15))) if note else 0
    fig.tight_layout(rect=(0, 0.035 * lines, 1, 1))
    fig.savefig(path)
    plt.close(fig)
    print("wrote", path)
