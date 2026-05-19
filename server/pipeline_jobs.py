"""Background analysis pipeline jobs for the built-in web dashboard."""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional

from analysis import AnalysisDebugStop, AnalysisPipeline
from config import ConfigLoader
from storage import Database, FileManager


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class PipelineJobConflict(RuntimeError):
    """Raised when a pipeline job is already running."""


class WebPipelineProgressReporter:
    _STAGE_LABELS = {
        "frames": "抽帧 / 拼图",
        "classify": "模型判分",
        "export": "导出 CSV",
        "organize": "归类复制",
    }

    def __init__(self, job: "PipelineJob"):
        self.job = job
        self._stage_batch_times: Dict[str, List[float]] = {}
        self._stage_batch_start: Dict[str, float] = {}

    def start_pipeline(self, run_id: str, stages: Iterable[str]) -> None:
        self.job.run_id = run_id
        self.job.planned_stages = [stage for stage in stages if stage in self._STAGE_LABELS]

    def start_batch_timer(self, stage: str) -> None:
        self._stage_batch_start[stage] = time.time()

    def record_batch_time(self, stage: str) -> float:
        elapsed = time.time() - self._stage_batch_start.get(stage, time.time())
        self._stage_batch_times.setdefault(stage, []).append(elapsed)
        return elapsed

    def avg_batch_time(self, stage: str) -> float:
        times = self._stage_batch_times.get(stage, [])
        return sum(times) / len(times) if times else 0.0

    def start_stage(self, stage: str, total: int, detail: str = "") -> None:
        total_int = max(int(total or 0), 0)
        self.job.current_stage = stage
        self.job.stages[stage] = {
            "label": self._STAGE_LABELS.get(stage, stage),
            "total": total_int,
            "completed": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "detail": detail or ("无需处理" if total_int == 0 else ""),
            "attempt": None,
            "started_at": _now_iso(),
            "finished_at": None,
        }
        if total_int == 0:
            self.finish_stage(stage, self.job.stages[stage]["detail"])

    def update_stage_detail(self, stage: str, detail: str) -> None:
        self.job.stages.setdefault(stage, {})["detail"] = detail

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
        stage_state = self.job.stages.setdefault(stage, {})
        stage_state["attempt"] = {
            "batch_index": batch_index,
            "total_batches": total_batches,
            "attempt": attempt,
            "total_attempts": total_attempts,
            "status": status,
            "detail": detail,
        }
        parts = [
            f"批次 {batch_index}/{total_batches}",
            f"尝试 {attempt}/{total_attempts}",
            status,
        ]
        if detail:
            parts.append(detail)
        stage_state["detail"] = " · ".join(parts)

    def advance_stage_item(self, stage: str, status: str, detail: str = "") -> None:
        stage_state = self.job.stages.setdefault(stage, {})
        if status in {"success", "failed", "skipped"}:
            stage_state[status] = int(stage_state.get(status) or 0) + 1
        total = int(stage_state.get("total") or 0)
        completed = int(stage_state.get("completed") or 0) + 1
        stage_state["completed"] = min(completed, total) if total else completed
        stage_state["detail"] = detail

    def finish_stage(self, stage: str, detail: str = "完成") -> None:
        stage_state = self.job.stages.setdefault(stage, {})
        total = int(stage_state.get("total") or 0)
        stage_state["completed"] = total
        stage_state["detail"] = detail
        stage_state["finished_at"] = _now_iso()


class PipelineJob:
    def __init__(self, *, action: str, run_id: Optional[str] = None, limit: int = 0):
        self.job_id = uuid.uuid4().hex[:12]
        self.action = action
        self.run_id = run_id
        self.limit = int(limit or 0)
        self.status = "pending"
        self.created_at = _now_iso()
        self.started_at: Optional[str] = None
        self.finished_at: Optional[str] = None
        self.error: Optional[str] = None
        self.debug_report: Optional[str] = None
        self.result: Dict[str, Any] = {}
        self.current_stage: Optional[str] = None
        self.planned_stages: List[str] = []
        self.stages: Dict[str, Dict[str, Any]] = {}
        self._task: Optional[asyncio.Task] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "action": self.action,
            "run_id": self.run_id,
            "limit": self.limit,
            "status": self.status,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
            "debug_report": self.debug_report,
            "result": self.result,
            "current_stage": self.current_stage,
            "planned_stages": self.planned_stages,
            "stages": self.stages,
        }


PipelineFactory = Callable[..., Any]


class PipelineJobManager:
    TERMINAL = {"success", "failed"}
    ACTIONS = {"continue", "retry", "run_all", "organize", "organize_rebuild"}

    def __init__(
        self,
        *,
        config: ConfigLoader,
        db_path: str,
        pipeline_factory: Optional[PipelineFactory] = None,
    ):
        self.config = config
        self.db_path = db_path
        self.pipeline_factory = pipeline_factory or AnalysisPipeline
        self._jobs: Dict[str, PipelineJob] = {}
        self._lock = asyncio.Lock()

    async def submit(
        self,
        *,
        action: str,
        run_id: Optional[str] = None,
        limit: int = 0,
    ) -> PipelineJob:
        if action not in self.ACTIONS:
            raise ValueError(f"unsupported pipeline action: {action}")
        async with self._lock:
            active = next((j for j in self._jobs.values() if j.status not in self.TERMINAL), None)
            if active:
                raise PipelineJobConflict(f"pipeline job already running: {active.job_id}")
            job = PipelineJob(action=action, run_id=run_id, limit=limit)
            self._jobs[job.job_id] = job
            job._task = asyncio.create_task(self._run(job))
            return job

    async def get(self, job_id: str) -> Optional[PipelineJob]:
        async with self._lock:
            return self._jobs.get(job_id)

    async def list_jobs(self) -> List[PipelineJob]:
        async with self._lock:
            return list(self._jobs.values())

    async def shutdown(self) -> None:
        tasks = [job._task for job in self._jobs.values() if job._task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run(self, job: PipelineJob) -> None:
        job.status = "running"
        job.started_at = _now_iso()
        database = Database(self.db_path)
        await database.initialize()
        try:
            reporter = WebPipelineProgressReporter(job)
            file_manager = FileManager(self.config.get("path"))
            pipeline = self.pipeline_factory(
                raw_config=self.config.config,
                database=database,
                file_manager=file_manager,
                progress_reporter=reporter,
            )
            if job.action == "run_all":
                await self._run_all(job, database, pipeline, reporter)
            elif job.action == "continue":
                run_id = await self._resolve_active_or_best(database, explicit_run_id=job.run_id)
                reporter.start_pipeline(run_id, ("frames", "classify", "export", "organize"))
                await pipeline.resume(run_id)
                job.run_id = run_id
            elif job.action == "retry":
                run_id = await self._resolve_active_or_best(database, explicit_run_id=job.run_id)
                reset_count = await database.reset_failed_analysis_items(run_id)
                job.result["reset_count"] = reset_count
                reporter.start_pipeline(run_id, ("frames", "classify", "export", "organize"))
                await pipeline.resume(run_id)
                job.run_id = run_id
            else:
                run_id = await self._resolve_organize_run(database, explicit_run_id=job.run_id)
                reporter.start_pipeline(run_id, ("organize",))
                await pipeline.run_organize(run_id, rebuild=(job.action == "organize_rebuild"))
                job.run_id = run_id

            if job.run_id:
                status = await database.refresh_analysis_run_status(job.run_id)
                job.result["run_status"] = status
            job.status = "success"
        except AnalysisDebugStop as exc:
            job.status = "failed"
            job.error = str(exc)
            job.debug_report = exc.report
        except Exception as exc:
            job.status = "failed"
            job.error = f"{type(exc).__name__}: {exc}"
        finally:
            job.finished_at = _now_iso()
            await database.close()

    async def _run_all(
        self,
        job: PipelineJob,
        database: Database,
        pipeline: Any,
        reporter: WebPipelineProgressReporter,
    ) -> None:
        run_id = await pipeline.create_run(
            source_type="all",
            source_payload={"scope": "all", "limit": int(job.limit or 0)},
        )
        count = await pipeline.prepare_from_all_videos(run_id=run_id, limit=int(job.limit or 0))
        job.run_id = run_id
        job.result["prepared_count"] = count
        self._remember_analysis_value("active_run_id", run_id)
        reporter.start_pipeline(run_id, ("frames", "classify", "export", "organize"))
        await pipeline.resume(run_id)

    async def _resolve_active_or_best(
        self,
        database: Database,
        *,
        explicit_run_id: Optional[str] = None,
    ) -> str:
        if explicit_run_id:
            run = await database.get_analysis_run(explicit_run_id)
            if not run:
                raise ValueError(f"analysis run not found: {explicit_run_id}")
            self._remember_analysis_value("active_run_id", explicit_run_id)
            return explicit_run_id

        analysis_cfg = self.config.get("analysis", {}) or {}
        active_run_id = str(analysis_cfg.get("active_run_id") or "").strip()
        if active_run_id:
            run = await database.get_analysis_run(active_run_id)
            if run and run.get("status") in {"prepared", "running", "partial"}:
                return active_run_id

        run = await database.get_best_unfinished_run()
        if not run:
            raise ValueError("no unfinished analysis run found")
        run_id = str(run["run_id"])
        self._remember_analysis_value("active_run_id", run_id)
        return run_id

    async def _resolve_organize_run(
        self,
        database: Database,
        *,
        explicit_run_id: Optional[str] = None,
    ) -> str:
        if explicit_run_id:
            run = await database.get_analysis_run(explicit_run_id)
            if not run:
                raise ValueError(f"analysis run not found: {explicit_run_id}")
            self._remember_analysis_value("organize_run_id", explicit_run_id)
            return explicit_run_id

        analysis_cfg = self.config.get("analysis", {}) or {}
        remembered = str(analysis_cfg.get("organize_run_id") or "").strip()
        if remembered:
            run = await database.get_analysis_run(remembered)
            if run and int(run.get("classified") or 0) > 0:
                return remembered

        run = await database.get_latest_classified_run()
        if not run:
            raise ValueError("no classified analysis run found")
        run_id = str(run["run_id"])
        self._remember_analysis_value("organize_run_id", run_id)
        return run_id

    def _remember_analysis_value(self, key: str, value: str) -> None:
        analysis_cfg = dict(self.config.get("analysis", {}) or {})
        if str(analysis_cfg.get(key) or "") == str(value):
            return
        analysis_cfg[key] = str(value)
        self.config.update(analysis=analysis_cfg)
        self.config.save()
