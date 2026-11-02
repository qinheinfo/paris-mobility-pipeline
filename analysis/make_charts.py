from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyhive import hive

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "docs" / "images"

BLUE = "#2a78d6"
ORANGE = "#eb6834"
TEXT = "#0b0b0b"
MUTED = "#52514e"
GRID = "#e4e3df"
SURFACE = "#fcfcfb"

TOP_EMPTY_SQL = """
SELECT station_name, empty_pct
FROM demo.velib.mart_station_availability
WHERE coverage_pct >= 80
ORDER BY empty_pct DESC
LIMIT 10
"""

HOURLY_SQL = """
SELECT
    hour(from_utc_timestamp(CAST(ingestion_time AS TIMESTAMP), 'Europe/Paris')) AS hour_of_day,
    ROUND(AVG(CASE WHEN num_bikes_available = 0 THEN 1.0 ELSE 0.0 END) * 100, 2) AS empty_pct,
    ROUND(AVG(CASE WHEN num_docks_available = 0 THEN 1.0 ELSE 0.0 END) * 100, 2) AS full_pct
FROM demo.velib.station_status_bronze
WHERE is_renting = 1
GROUP BY 1
ORDER BY 1
"""


def run_query(sql):
    conn = hive.connect(host="localhost", port=10000)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        conn.close()


def style_axes(ax):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=MUTED, length=0)
    ax.set_axisbelow(True)


def plot_top_empty(rows):
    names = [row[0] for row in rows][::-1]
    values = [float(row[1]) for row in rows][::-1]

    fig, ax = plt.subplots(figsize=(9, 5), facecolor=SURFACE)
    style_axes(ax)
    ax.barh(names, values, color=BLUE, height=0.6)
    for y, value in enumerate(values):
        ax.text(value + 1, y, f"{value:.0f}%", va="center", color=TEXT, fontsize=9)

    ax.set_xlim(0, 110)
    ax.xaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_xlabel("Share of in-service time with no bike available (%)", color=MUTED)
    ax.set_title(
        "Stations most often empty (in service at least 80% of the time)",
        loc="left", color=TEXT, fontsize=12,
    )
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "top_empty_stations.png", dpi=150)
    plt.close(fig)


def plot_hourly(rows):
    hours = [int(row[0]) for row in rows]
    empty = [float(row[1]) for row in rows]
    full = [float(row[2]) for row in rows]

    fig, ax = plt.subplots(figsize=(9, 4.5), facecolor=SURFACE)
    style_axes(ax)
    ax.plot(hours, empty, color=BLUE, linewidth=2, label="No bike available")
    ax.plot(hours, full, color=ORANGE, linewidth=2, label="No free dock")

    ax.set_xticks(range(0, 24, 3))
    ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 24, 3)])
    ax.set_xlim(0, 23)
    ax.set_ylim(bottom=0)
    ax.yaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_ylabel("Share of in-service snapshots (%)", color=MUTED)
    ax.set_title(
        "When are stations empty or full? (hour of day, Paris time)",
        loc="left", color=TEXT, fontsize=12,
    )
    ax.legend(frameon=False, loc="upper left", labelcolor=TEXT)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "hourly_pattern.png", dpi=150)
    plt.close(fig)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    top_rows = run_query(TOP_EMPTY_SQL)
    plot_top_empty(top_rows)
    print(f"Saved top_empty_stations.png ({len(top_rows)} stations)")

    hourly_rows = run_query(HOURLY_SQL)
    plot_hourly(hourly_rows)
    print(f"Saved hourly_pattern.png ({len(hourly_rows)} hours)")


if __name__ == "__main__":
    main()