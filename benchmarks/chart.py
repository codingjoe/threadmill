"""Render the queue throughput chart that the README embeds.

Generate the numbers first, then the chart:

    uv run pytest benchmarks -m benchmark --benchmark-json=benchmark.json
    uv run python benchmarks/chart.py benchmark.json
"""

import dataclasses
import json
import pathlib
import sys

IMAGE_DIRECTORY = pathlib.Path("docs/images")
LIGHT_THEME_PATH = IMAGE_DIRECTORY / "backend-comparison-light.svg"
DARK_THEME_PATH = IMAGE_DIRECTORY / "backend-comparison-dark.svg"

WIDTH = 900
HEIGHT = 332

LABEL_X = 180
PLOT_X0 = 200
PLOT_WIDTH = 560

FONT = (
    'system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif'
)


@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class Theme:
    """Colors the chart is drawn in."""

    canvas: str
    """Background of the chart card."""

    border: str
    """Outline of the chart card."""

    ink: str
    """Primary text."""

    muted: str
    """Secondary text, and the bars that are not highlighted."""

    faint: str
    """Footnotes."""

    accent: str
    """The highlighted queue and its value."""

    accent_bar: str
    """Fill of the highlighted bar."""


LIGHT_THEME = Theme(
    canvas="#ffffff",
    border="#e5e7eb",
    ink="#111827",
    muted="#6b7280",
    faint="#9ca3af",
    accent="#4f46e5",
    accent_bar="#4f46e5",
)

DARK_THEME = Theme(
    canvas="#0d1117",
    border="#30363d",
    ink="#e6edf3",
    muted="#8b949e",
    faint="#6e7681",
    accent="#818cf8",
    accent_bar="#6366f1",
)


@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class QueueResult:
    """Measured timings of one queue."""

    name: str
    enqueue_seconds: float
    start_seconds: float
    process_seconds: float
    task_count: int

    @property
    def throughput(self) -> float:
        """Tasks the worker processes per second while the queue is busy."""
        return self.task_count / (self.process_seconds - self.start_seconds)


def read_results(json_path: pathlib.Path) -> list[QueueResult]:
    """Return the per-queue results of a pytest-benchmark run, fastest first."""
    benchmarks = json.loads(json_path.read_text())["benchmarks"]
    means = {
        (benchmark["name"].split("[")[0], benchmark["param"]): benchmark
        for benchmark in benchmarks
    }
    queue_names = [
        queue_name
        for (benchmark_name, queue_name) in means
        if benchmark_name == "test_process_queue__benchmark"
    ]
    results = []
    for queue_name in queue_names:
        process = means[("test_process_queue__benchmark", queue_name)]
        start = means[("test_start_worker__benchmark", queue_name)]
        enqueue = means[("test_enqueue__benchmark", queue_name)]
        results.append(
            QueueResult(
                name=queue_name,
                enqueue_seconds=enqueue["stats"]["mean"],
                start_seconds=start["stats"]["mean"],
                process_seconds=process["stats"]["mean"],
                task_count=process["extra_info"]["tasks"],
            )
        )
    return sorted(results, key=lambda result: result.throughput, reverse=True)


def text(x, y, content, *, theme, size=13, fill=None, weight=400, anchor="start"):
    """Render one SVG text element."""
    return (
        f'<text x="{x:g}" y="{y:g}" font-family=\'{FONT}\' font-size="{size:g}" '
        f'font-weight="{weight}" fill="{fill or theme.ink}" '
        f'text-anchor="{anchor}">{content}</text>'
    )


def describe(results: list[QueueResult]) -> str:
    """Return a sentence describing the throughput of every queue."""
    return "Tasks per second with one worker: " + ", ".join(
        f"{result.name} {result.throughput:,.0f}" for result in results
    )


def build_chart(results: list[QueueResult], theme: Theme) -> str:
    """Return the chart as an SVG document drawn in the given theme."""
    fastest = results[0]
    scale = PLOT_WIDTH / fastest.throughput
    row_centers = [110 + index * 40 for index in range(len(results))]

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {HEIGHT}" '
        f'width="{WIDTH}" height="{HEIGHT}" role="img" '
        f'aria-label="{describe(results)}.">',
        "<style>svg{max-width:100%;height:auto}</style>",
        f'<rect x="0.5" y="0.5" width="{WIDTH - 1}" height="{HEIGHT - 1}" rx="14" '
        f'fill="{theme.canvas}" stroke="{theme.border}"/>',
        text(28, 46, "Queue throughput", theme=theme, size=19, weight=700),
        text(
            28,
            68,
            f"{fastest.task_count:,} trivial tasks per queue · one worker process, "
            "one thread · higher is better",
            theme=theme,
            size=12.5,
            fill=theme.muted,
        ),
    ]

    for result, center in zip(results, row_centers, strict=True):
        is_fastest = result is fastest
        width = result.throughput * scale
        parts.append(
            text(
                LABEL_X,
                center + 5,
                result.name,
                theme=theme,
                size=14,
                weight=700 if is_fastest else 400,
                fill=theme.ink if is_fastest else theme.muted,
                anchor="end",
            )
        )
        parts.append(
            f'<rect x="{PLOT_X0}" y="{center - 10}" width="{width:.2f}" height="20" rx="5" '
            f'fill="{theme.accent_bar if is_fastest else theme.muted}" '
            f'opacity="{1 if is_fastest else 0.25}"/>'
        )
        parts.append(
            text(
                PLOT_X0 + width + 10,
                center + 5,
                f"{result.throughput:,.0f}/s",
                theme=theme,
                size=13,
                weight=700 if is_fastest else 400,
                fill=theme.accent if is_fastest else theme.muted,
            )
        )

    parts.append(
        text(
            28,
            row_centers[-1] + 38,
            "One message in flight per worker — no queue reads ahead.",
            theme=theme,
            size=11.5,
            fill=theme.faint,
        )
    )
    parts.append("</svg>")
    return "\n".join(parts)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} <benchmark.json>")
    queues = read_results(pathlib.Path(sys.argv[1]))
    for theme, path in ((LIGHT_THEME, LIGHT_THEME_PATH), (DARK_THEME, DARK_THEME_PATH)):
        path.write_text(build_chart(queues, theme) + "\n")
        print(f"wrote {path} ({path.stat().st_size} bytes)")
    for queue in queues:
        print(
            f"{queue.name:20s} {queue.throughput:8,.0f}/s "
            f"(enqueue {1 / queue.enqueue_seconds:8,.0f}/s, start {queue.start_seconds:.4f}s)"
        )
