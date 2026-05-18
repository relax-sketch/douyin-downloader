from __future__ import annotations

import time
from typing import Dict, Iterable, Optional

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

console = Console()


class PipelineProgressDisplay:
    _STAGE_LABELS = {
        "frames": "抽帧 / 拼图",
        "classify": "模型判分",
        "export": "导出 CSV",
        "organize": "归类复制",
    }

    def __init__(self):
        self.console = console
        self._progress_ctx: Optional[Progress] = None
        self._progress: Optional[Progress] = None
        self._overall_task_id: Optional[int] = None
        self._stage_task_ids: Dict[str, int] = {}
        self._stage_totals: Dict[str, int] = {}
        self._stage_completed: Dict[str, int] = {}
        self._stage_stats: Dict[str, Dict[str, int]] = {}
        self._planned_stages: list[str] = []
        self._completed_stages: set[str] = set()
        self._stage_start_times: Dict[str, float] = {}
        self._stage_batch_times: Dict[str, list] = {}
        self._stage_batch_start: Dict[str, float] = {}

    def create_progress(self) -> Progress:
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("[dim]elapsed"),
            TimeElapsedColumn(),
            TextColumn("[dim]ETA"),
            TimeRemainingColumn(),
            TextColumn("[dim]{task.fields[detail]}"),
            console=self.console,
            transient=False,
            refresh_per_second=6,
        )

    def start_pipeline(self, run_id: str, stages: Iterable[str]) -> None:
        if self._progress is not None:
            return

        self._planned_stages = [stage for stage in stages if stage in self._STAGE_LABELS]
        self._progress_ctx = self.create_progress()
        self._progress = self._progress_ctx.__enter__()
        self._overall_task_id = self._progress.add_task(
            "分析总进度",
            total=max(len(self._planned_stages), 1),
            completed=0,
            detail=f"run {run_id}",
        )

    def start_batch_timer(self, stage: str) -> None:
        self._stage_batch_start[stage] = time.time()

    def record_batch_time(self, stage: str) -> float:
        t = time.time() - self._stage_batch_start.get(stage, time.time())
        times = self._stage_batch_times.setdefault(stage, [])
        times.append(t)
        return t

    def avg_batch_time(self, stage: str) -> float:
        times = self._stage_batch_times.get(stage, [])
        if not times:
            return 0
        return sum(times) / len(times)

    def stop_pipeline(self) -> None:
        if self._progress_ctx is not None:
            self._progress_ctx.__exit__(None, None, None)

        self._progress_ctx = None
        self._progress = None
        self._overall_task_id = None
        self._stage_task_ids = {}
        self._stage_totals = {}
        self._stage_completed = {}
        self._stage_stats = {}
        self._planned_stages = []
        self._completed_stages = set()
        self._stage_start_times = {}
        self._stage_batch_times = {}
        self._stage_batch_start = {}

    def start_stage(self, stage: str, total: int, detail: str = "") -> None:
        if not self._progress or stage not in self._STAGE_LABELS:
            return

        self._stage_start_times[stage] = time.time()
        normalized_total = max(int(total or 0), 1)
        self._stage_totals[stage] = normalized_total
        self._stage_completed[stage] = 1 if int(total or 0) == 0 else 0
        self._stage_stats[stage] = {"success": 0, "failed": 0, "skipped": 0}
        completed = self._stage_completed[stage]
        stage_detail = detail or ("无需处理" if int(total or 0) == 0 else "")

        if stage in self._stage_task_ids:
            self._progress.update(
                self._stage_task_ids[stage],
                total=normalized_total,
                completed=completed,
                description=self._format_stage_description(stage),
                detail=stage_detail,
            )
        else:
            self._stage_task_ids[stage] = self._progress.add_task(
                self._format_stage_description(stage),
                total=normalized_total,
                completed=completed,
                detail=stage_detail,
            )

        if int(total or 0) == 0:
            self.finish_stage(stage, stage_detail)

    def update_stage_detail(self, stage: str, detail: str) -> None:
        if not self._progress or stage not in self._stage_task_ids:
            return
        self._progress.update(self._stage_task_ids[stage], detail=detail)

    def update_stage_attempt(
        self,
        stage: str,
        *,
        batch_index: int,
        total_batches: int,
        attempt: int,
        total_attempts: int,
        status: str,
        detail: str = "",
    ) -> None:
        parts = [
            f"批次 {batch_index}/{total_batches}",
            f"尝试 {attempt}/{total_attempts}",
            status,
        ]
        if detail:
            parts.append(detail)
        self.update_stage_detail(stage, " · ".join(parts))

    def advance_stage_item(self, stage: str, status: str, detail: str = "") -> None:
        if not self._progress or stage not in self._stage_task_ids:
            return

        stats = self._stage_stats.setdefault(stage, {"success": 0, "failed": 0, "skipped": 0})
        if status in stats:
            stats[status] += 1

        task_id = self._stage_task_ids[stage]
        total = self._stage_totals.get(stage, 1)
        completed = min(self._stage_completed.get(stage, 0) + 1, total)
        self._stage_completed[stage] = completed
        self._progress.update(
            task_id,
            completed=completed,
            description=self._format_stage_description(stage),
            detail=detail,
        )

    def finish_stage(self, stage: str, detail: str = "完成") -> None:
        if not self._progress or stage not in self._STAGE_LABELS:
            return

        task_id = self._stage_task_ids.get(stage)
        if task_id is not None:
            self._progress.update(
                task_id,
                completed=self._stage_totals.get(stage, 1),
                description=self._format_stage_description(stage),
                detail=detail,
            )
            self._stage_completed[stage] = self._stage_totals.get(stage, 1)

        if stage not in self._completed_stages:
            self._completed_stages.add(stage)
            if self._overall_task_id is not None:
                self._progress.advance(self._overall_task_id, 1)

    def _format_stage_description(self, stage: str) -> str:
        label = self._STAGE_LABELS.get(stage, stage)
        stats = self._stage_stats.get(stage)
        if not stats:
            return label
        return f"{label}  S:{stats['success']} F:{stats['failed']} K:{stats['skipped']}"
