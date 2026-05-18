import uuid
from pathlib import Path
from typing import Dict, List, Optional, Protocol

from analysis.config import (
    analysis_config,
    build_prompt,
    load_attributes,
    load_buckets,
    primary_attribute_key,
    render_batch_prompt,
)
from analysis.discovery import CandidateDiscovery
from analysis.exporter import CsvExporter
from analysis.frames import FFmpegFrameExtractor, GridBuilder
from analysis.organizer import ClassifiedOrganizer
from analysis.provider import VisionProvider, build_provider
from storage import Database, FileManager
from utils.logger import setup_logger

logger = setup_logger("AnalysisPipeline")


class AnalysisProgressReporter(Protocol):
    def start_stage(self, stage: str, total: int, detail: str = "") -> None: ...

    def update_stage_detail(self, stage: str, detail: str) -> None: ...

    def advance_stage_item(self, stage: str, status: str, detail: str = "") -> None: ...

    def finish_stage(self, stage: str, detail: str = "完成") -> None: ...


class AnalysisPipeline:
    def __init__(
        self,
        *,
        raw_config: Dict[str, object],
        database: Database,
        file_manager: FileManager,
        provider: Optional[VisionProvider] = None,
        frame_extractor: Optional[FFmpegFrameExtractor] = None,
        grid_builder: Optional[GridBuilder] = None,
        exporter: Optional[CsvExporter] = None,
        organizer: Optional[ClassifiedOrganizer] = None,
        progress_reporter: Optional[AnalysisProgressReporter] = None,
    ):
        self.raw_config = raw_config
        self.cfg = analysis_config(raw_config)
        self.database = database
        self.file_manager = file_manager
        self.attributes = load_attributes(raw_config)
        self.primary_attribute = primary_attribute_key(raw_config, self.attributes)
        self.buckets = load_buckets(raw_config)
        self.prompt = build_prompt(raw_config, self.attributes)
        self.batch_size = max(1, int(self.cfg.get("batch_size") or 1))
        self.allow_partial_batch = bool(self.cfg.get("allow_partial_batch", False))
        self.frame_count = max(1, int(self.cfg.get("frame_count") or 9))
        self.grid_rows = max(1, int(self.cfg.get("grid_rows") or 3))
        self.grid_cols = max(1, int(self.cfg.get("grid_cols") or 3))
        self.output_dir = Path(str(self.cfg.get("output_dir") or "./Analysis/"))
        self.classified_dir = Path(str(self.cfg.get("classified_dir") or "./Classified/"))
        self.discovery = CandidateDiscovery(database, file_manager.base_path)
        self.provider = provider or build_provider(self.cfg.get("provider") or {})
        self.frame_extractor = frame_extractor or FFmpegFrameExtractor()
        self.grid_builder = grid_builder or GridBuilder()
        self.exporter = exporter or CsvExporter()
        self.organizer = organizer or ClassifiedOrganizer()
        self.progress_reporter = progress_reporter

    async def create_run(self, *, source_type: str, source_payload: Dict[str, object]) -> str:
        run_id = uuid.uuid4().hex
        await self.database.create_analysis_run(
            run_id=run_id,
            source_type=source_type,
            source_payload=source_payload,
        )
        return run_id

    async def prepare_from_manifest_delta(self, *, run_id: str, start_line: int) -> int:
        candidates = self.discovery.from_manifest_delta(start_line)
        await self.database.add_analysis_items(run_id, candidates)
        return len(candidates)

    async def prepare_from_all_videos(self, *, run_id: str, limit: int = 0) -> int:
        candidates = await self.discovery.from_all_videos(limit=limit)
        await self.database.add_analysis_items(run_id, candidates)
        return len(candidates)

    async def run_frames(self, run_id: str) -> None:
        items = await self.database.list_analysis_items_for_stage(run_id, "frames")
        if self.progress_reporter:
            self.progress_reporter.start_stage("frames", len(items))
        for item in items:
            aweme_id = item["aweme_id"]
            try:
                if self.progress_reporter:
                    self.progress_reporter.update_stage_detail("frames", str(aweme_id))
                frame_dir = self.output_dir / "frames" / run_id / aweme_id
                grid_path = self.output_dir / "grids" / run_id / f"{aweme_id}.jpg"
                frames = await self.frame_extractor.extract(
                    Path(item["video_path"]),
                    frame_dir,
                    count=self.frame_count,
                )
                self.grid_builder.build(
                    frames,
                    grid_path,
                    rows=self.grid_rows,
                    cols=self.grid_cols,
                )
                await self.database.update_analysis_item_stage(
                    run_id,
                    aweme_id,
                    stage="frames",
                    status="success",
                    extra_updates={"grid_path": str(grid_path)},
                )
                if self.progress_reporter:
                    self.progress_reporter.advance_stage_item("frames", "success", str(aweme_id))
            except Exception as exc:
                logger.warning("Frame extraction failed for %s: %s", aweme_id, exc)
                await self.database.update_analysis_item_stage(
                    run_id,
                    aweme_id,
                    stage="frames",
                    status="failed",
                    error_message=str(exc),
                )
                if self.progress_reporter:
                    self.progress_reporter.advance_stage_item("frames", "failed", str(aweme_id))
        if self.progress_reporter:
            self.progress_reporter.finish_stage("frames")

    async def run_classify(self, run_id: str) -> None:
        items = await self.database.list_analysis_items_for_stage(run_id, "classify")
        if self.batch_size <= 1:
            if self.progress_reporter:
                self.progress_reporter.start_stage("classify", len(items))
            for item in items:
                aweme_id = item["aweme_id"]
                try:
                    if self.progress_reporter:
                        self.progress_reporter.update_stage_detail("classify", str(aweme_id))
                    scores = await self.provider.score(
                        Path(item["grid_path"]),
                        self.attributes,
                        self.prompt,
                    )
                    await self.database.upsert_analysis_scores(run_id, aweme_id, scores)
                    await self.database.update_analysis_item_stage(
                        run_id,
                        aweme_id,
                        stage="classify",
                        status="success",
                    )
                    if self.progress_reporter:
                        self.progress_reporter.advance_stage_item("classify", "success", str(aweme_id))
                except Exception as exc:
                    logger.warning("Classification failed for %s: %s", aweme_id, exc)
                    await self.database.update_analysis_item_stage(
                        run_id,
                        aweme_id,
                        stage="classify",
                        status="failed",
                        error_message=str(exc),
                    )
                    if self.progress_reporter:
                        self.progress_reporter.advance_stage_item("classify", "failed", str(aweme_id))
            if self.progress_reporter:
                self.progress_reporter.finish_stage("classify")
            return

        processable_count = (
            len(items)
            if self.allow_partial_batch
            else len(items) - (len(items) % self.batch_size)
        )
        pending_remainder = len(items) - processable_count
        if self.progress_reporter:
            detail = (
                f"剩余 {pending_remainder} 条等待凑满批次"
                if pending_remainder
                else ""
            )
            self.progress_reporter.start_stage("classify", processable_count, detail)

        if pending_remainder:
            logger.info(
                "Keep %d item(s) pending until a full batch of %d is available",
                pending_remainder,
                self.batch_size,
            )

        processable_items = items[:processable_count]
        total_batches = (
            (len(processable_items) + self.batch_size - 1) // self.batch_size
            if processable_items
            else 0
        )
        for start in range(0, len(processable_items), self.batch_size):
            batch = processable_items[start : start + self.batch_size]
            if self.progress_reporter:
                batch_index = (start // self.batch_size) + 1
                self.progress_reporter.start_batch_timer("classify")
            await self._classify_batch(run_id, batch)
            if self.progress_reporter:
                batch_time = self.progress_reporter.record_batch_time("classify")
                avg = self.progress_reporter.avg_batch_time("classify")
                self.progress_reporter.update_stage_detail(
                    "classify",
                    f"批次 {batch_index}/{total_batches}  avg {avg:.0f}s/batch",
                )
        if self.progress_reporter:
            detail = (
                f"完成，剩余 {pending_remainder} 条等待凑满批次"
                if pending_remainder
                else "完成"
            )
            self.progress_reporter.finish_stage("classify", detail)

    async def _classify_batch(self, run_id: str, batch: List[Dict[str, object]]) -> None:
        import asyncio

        real_video_ids = [str(item["aweme_id"]) for item in batch]
        image_paths = [Path(str(item["grid_path"])) for item in batch]
        max_retries = self.provider.retry_handler.max_retries
        delays = self.provider.retry_handler.retry_delays
        last_error = None

        for attempt in range(max_retries + 1):
            try:
                await self.provider.rate_limiter.acquire()
                prompt = render_batch_prompt(self.raw_config, real_video_ids)
                results = await self.provider._batch_score_once(
                    image_paths, real_video_ids, prompt, compress_level=attempt
                )
                by_video_id = {str(r["video_id"]): r for r in results}
                for item in batch:
                    aweme_id = str(item["aweme_id"])
                    if aweme_id not in by_video_id:
                        continue
                    result = by_video_id[aweme_id]
                    scores = {
                        "suggestiveness_score": int(result["suggestiveness_score"]),
                        "coverage_score": int(result["coverage_score"]),
                    }
                    await self.database.upsert_analysis_scores(run_id, aweme_id, scores)
                    await self.database.update_analysis_item_stage(
                        run_id, aweme_id, stage="classify", status="success",
                    )
                    if self.progress_reporter:
                        self.progress_reporter.advance_stage_item("classify", "success", aweme_id)
                return
            except Exception as exc:
                last_error = exc
                if attempt == max_retries:
                    break
                # Before last retry: drop largest image, mark it failed
                if attempt == max_retries - 1 and len(image_paths) > 1:
                    sizes = [(p.stat().st_size, i) for i, p in enumerate(image_paths)]
                    drop_idx = max(sizes)[1]
                    drop_aweme_id = real_video_ids[drop_idx]
                    logger.warning(
                        "Attempt %d: dropping largest item %s (%d KB) before final retry",
                        attempt + 1, drop_aweme_id, sizes[drop_idx][0] // 1024,
                    )
                    await self.database.update_analysis_item_stage(
                        run_id, drop_aweme_id, stage="classify",
                        status="failed",
                        error_message=f"dropped after {attempt + 1} attempts: {exc}",
                    )
                    if self.progress_reporter:
                        self.progress_reporter.advance_stage_item("classify", "failed", drop_aweme_id)
                    del image_paths[drop_idx], real_video_ids[drop_idx]
                    # Also remove from batch so it doesn't get re-processed
                    batch = [it for it in batch if str(it["aweme_id"]) != drop_aweme_id]
                delay = delays[min(attempt, len(delays) - 1)]
                logger.warning(
                    "Batch classify attempt %d/%d failed (%s), compress_level=%d, retrying in %ds...",
                    attempt + 1, max_retries + 1, exc, attempt, delay,
                )
                await asyncio.sleep(delay)

        logger.warning("Batch classification exhausted for %s: %s", real_video_ids, last_error)
        for item in batch:
            await self.database.update_analysis_item_stage(
                run_id, str(item["aweme_id"]), stage="classify",
                status="failed", error_message=str(last_error),
            )
            if self.progress_reporter:
                self.progress_reporter.advance_stage_item("classify", "failed", str(item["aweme_id"]))

    async def run_export(self, run_id: str) -> Path:
        if self.progress_reporter:
            self.progress_reporter.start_stage("export", 1)
        csv_path = await self.exporter.export(
            database=self.database,
            run_id=run_id,
            output_dir=self.output_dir / "csv",
            attributes=self.attributes,
            primary_attribute=self.primary_attribute,
            buckets=self.buckets,
        )
        await self.database.mark_analysis_exported(run_id, str(csv_path))
        if self.progress_reporter:
            self.progress_reporter.advance_stage_item("export", "success", str(csv_path))
            self.progress_reporter.finish_stage("export", str(csv_path))
        return csv_path

    async def run_organize(self, run_id: str) -> None:
        pending_rows = [
            row
            for row in await self.database.get_analysis_export_rows(run_id)
            if row.get("organize_status") not in {"success", "skipped"}
        ]
        if self.progress_reporter:
            self.progress_reporter.start_stage("organize", len(pending_rows))
        await self.organizer.organize(
            database=self.database,
            run_id=run_id,
            classified_dir=self.classified_dir,
            attributes=self.attributes,
            primary_attribute=self.primary_attribute,
            buckets=self.buckets,
            progress_callback=(
                lambda status, detail: self.progress_reporter.advance_stage_item(
                    "organize",
                    status,
                    detail,
                )
                if self.progress_reporter
                else None
            ),
        )
        if self.progress_reporter:
            self.progress_reporter.finish_stage("organize")

    async def resume(self, run_id: str) -> None:
        await self.run_frames(run_id)
        await self.run_classify(run_id)
        await self.run_export(run_id)
        await self.run_organize(run_id)
        await self.database.refresh_analysis_run_status(run_id)
