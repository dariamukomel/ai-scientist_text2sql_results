import numpy as np
import matplotlib.pyplot as plt

# Ensure grid is behind plot elements
plt.rcParams['axes.axisbelow'] = True

# Selected run indices and labels
RUN_ORDER = [0, 1, 3, 5, 7, 8, 15, 17, 24, 25]
LABELS = [
    "Baseline", "Run 1", "Run 3", "Run 5", "Run 7",
    "Run 8", "Run 15", "Run 17", "Run 24", "Final Combined"
]

# (easy+medium, total) accuracy per run
ACCURACY_DATA = {
    0:  (85.5, 86.7),
    1:  (83.6, 85.0),
    3:  (81.8, 83.3),
    5:  (89.1, 90.0),
    7:  (87.3, 88.3),
    8:  (89.1, 90.0),
    15: (83.6, 85.0),
    17: (89.1, 90.0),
    24: (89.1, 90.0),
    25: (90.9, 91.7),
}

# Full bucket counts per run
DISTRIBUTION_COUNTS = {
    0:  {"not parsed":1, "0%":0, "(0-25%]":11, "(25-50%]":10, "(50-75%]":7,  "(75-100%]":31},
    1:  {"not parsed":0, "0%":0, "(0-25%]":5,  "(25-50%]":13, "(50-75%]":13, "(75-100%]":29},
    3:  {"not parsed":2, "0%":0, "(0-25%]":6,  "(25-50%]":12, "(50-75%]":7,  "(75-100%]":33},
    5:  {"not parsed":1, "0%":0, "(0-25%]":5,  "(25-50%]":13, "(50-75%]":9,  "(75-100%]":32},
    7:  {"not parsed":1, "0%":0, "(0-25%]":4,  "(25-50%]":13, "(50-75%]":10, "(75-100%]":32},
    8:  {"not parsed":1, "0%":0, "(0-25%]":5,  "(25-50%]":13, "(50-75%]":10, "(75-100%]":31},
    15: {"not parsed":1, "0%":0, "(0-25%]":6,  "(25-50%]":11, "(50-75%]":10, "(75-100%]":32},
    17: {"not parsed":1, "0%":0, "(0-25%]":6,  "(25-50%]":12, "(50-75%]":12, "(75-100%]":29},
    24: {"not parsed":1, "0%":0, "(0-25%]":5,  "(25-50%]":10, "(50-75%]":11, "(75-100%]":33},
    25: {"not parsed":1, "0%":0, "(0-25%]":5,  "(25-50%]":13, "(50-75%]":10, "(75-100%]":31},
}

# High‐scoring query counts = the "(75-100%]" bucket
HIGH_SCORES = { r: DISTRIBUTION_COUNTS[r]["(75-100%]"] for r in RUN_ORDER }

# Buckets order & colors for distribution
BUCKETS = ["not parsed", "0%", "(0-25%]", "(25-50%]", "(50-75%]", "(75-100%]"]
COLORS_DIST = {
    "not parsed": "#d62728",
    "0%":           "#ff7f0e",
    "(0-25%]":      "#1f77b4",
    "(25-50%]":     "#2ca02c",
    "(50-75%]":     "#9467bd",
    "(75-100%]":    "#8c564b",
}

def save_png(fig, name):
    fig.savefig(f"{name}.png", bbox_inches="tight")

def plot_accuracy_comparison():
    x = np.arange(len(RUN_ORDER))
    easy  = [ACCURACY_DATA[r][0] for r in RUN_ORDER]
    total = [ACCURACY_DATA[r][1] for r in RUN_ORDER]

    fig, ax = plt.subplots(figsize=(10,5))
    ax.bar(x-0.17, easy,  0.34, label="Easy+Medium Accuracy", color="#1f77b4", zorder=2)
    ax.bar(x+0.17, total, 0.34, label="Total Accuracy",       color="#ff7f0e", zorder=2)

    ax.set_xticks(x)
    ax.set_xticklabels(LABELS, rotation=45, ha="right")
    ax.set_ylabel("Accuracy (%)")
    ax.set_title("Text-to-SQL Performance by Selected Runs")
    ax.set_ylim(60, 100)
    ax.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.7, zorder=1)
    ax.legend()
    plt.tight_layout()
    save_png(fig, "accuracy_comparison")
    plt.show()

def plot_score_distribution():
    data_pct = {b: [] for b in BUCKETS}
    for r in RUN_ORDER:
        counts = DISTRIBUTION_COUNTS[r]
        total = sum(counts.values())
        for b in BUCKETS:
            data_pct[b].append(counts[b] / total * 100)

    fig, ax = plt.subplots(figsize=(10,5))
    bottom = np.zeros(len(RUN_ORDER))
    for b in BUCKETS:
        ax.bar(
            LABELS,
            data_pct[b],
            bottom=bottom,
            color=COLORS_DIST[b],
            label=b,
            zorder=2
        )
        bottom += data_pct[b]

    ax.set_ylabel("Percentage of Queries")
    ax.set_title("Score Distribution by Run")
    ax.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.7, zorder=1)
    ax.legend(title="Score Buckets", bbox_to_anchor=(1.05,1), loc="upper left")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    save_png(fig, "score_distribution")
    plt.show()

def plot_improvement_timeline():
    x = np.arange(len(RUN_ORDER))
    easy  = [ACCURACY_DATA[r][0] for r in RUN_ORDER]
    total = [ACCURACY_DATA[r][1] for r in RUN_ORDER]
    high  = [HIGH_SCORES[r]      for r in RUN_ORDER]

    fig, ax = plt.subplots(figsize=(10,5))
    ax.plot(x, easy,  marker="o", color="#1f77b4", label="Easy+Medium Accuracy", zorder=2)
    ax.plot(x, total, marker="s", color="#ff7f0e", label="Total Accuracy",       zorder=2)
    ax.plot(x, high,  marker="^", color="#2ca02c", label="High-Scoring Queries (75-100%)", zorder=2)

    ax.set_xticks(x)
    ax.set_xticklabels(LABELS, rotation=45, ha="right")
    ax.set_ylabel("Score / Count")
    ax.set_title("Performance Improvement Timeline for Selected Runs")
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7, zorder=1)
    ax.legend()
    plt.tight_layout()
    save_png(fig, "improvement_timeline")
    plt.show()

if __name__ == "__main__":
    plot_accuracy_comparison()
    plot_score_distribution()
    plot_improvement_timeline()
