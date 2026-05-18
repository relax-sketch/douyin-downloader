from pathlib import Path

import pytest
from PIL import Image

from analysis.pipeline import AnalysisDebugStop, AnalysisPipeline
from analysis.provider import OpenAICompatibleVisionProvider, ProviderResponseError
from config import ConfigLoader
from storage import Database, FileManager


class _FakeFrameExtractor:
    async def extract(self, video_path: Path, output_dir: Path, count: int = 9):
        output_dir.mkdir(parents=True, exist_ok=True)
        paths = []
        for index in range(1, 10):
            frame = output_dir / f"frame_{index:02d}.jpg"
            frame.write_bytes(b"frame")
            paths.append(frame)
        return paths


class _FakeGridBuilder:
    def build(self, frame_paths, output_path: Path, **_kwargs):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"grid")
        return output_path


class _ImageGridBuilder:
    def build(self, frame_paths, output_path: Path, **_kwargs):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (64, 32), color=(10, 20, 30)).save(output_path)
        return output_path


class _FakeProvider:
    async def score(self, image_path, attributes, prompt):
        return {attribute.key: index for index, attribute in enumerate(attributes)}


class _FakeBatchProvider:
    def __init__(self):
        from control import RateLimiter, RetryHandler

        self.rate_limiter = RateLimiter(max_per_second=1000)
        self.retry_handler = RetryHandler(max_retries=0)

    async def _batch_score_once(self, image_paths, video_ids, prompt, compress_level=0):
        return [
            {
                "video_id": video_id,
                "suggestiveness_score": index + 1,
                "coverage_score": index + 2,
            }
            for index, video_id in enumerate(video_ids)
        ]


class _RetryingBatchProvider:
    def __init__(self):
        from control import RateLimiter, RetryHandler

        self.rate_limiter = RateLimiter(max_per_second=1000)
        self.retry_handler = RetryHandler(max_retries=1)
        self.retry_handler.retry_delays = [0]
        self.calls = 0

    async def _batch_score_once(self, image_paths, video_ids, prompt, compress_level=0):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("temporary provider failure")
        return [
            {
                "video_id": video_id,
                "suggestiveness_score": index + 1,
                "coverage_score": index + 2,
            }
            for index, video_id in enumerate(video_ids)
        ]


class _EmptyResponseRetryingBatchProvider(_RetryingBatchProvider):
    async def _batch_score_once(self, image_paths, video_ids, prompt, compress_level=0):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("temporary local failure")
        return await super()._batch_score_once(image_paths, video_ids, prompt, compress_level)


class _ResponseRetryingBatchProvider(_RetryingBatchProvider):
    async def _batch_score_once(self, image_paths, video_ids, prompt, compress_level=0):
        from analysis.provider import ProviderResponseError

        self.calls += 1
        if self.calls == 1:
            raise ProviderResponseError(
                "provider failed",
                status=503,
                response_text='{"error":"busy"}',
            )
        return await super()._batch_score_once(image_paths, video_ids, prompt, compress_level)


class _DebugFailingBatchProvider(OpenAICompatibleVisionProvider):
    def __init__(self):
        super().__init__(
            base_url="https://example.test/v1",
            model="demo",
            retry_times=3,
            debug_stop_on_api_error=True,
        )
        self.calls = 0

    async def _batch_score_once(self, image_paths, video_ids, prompt, compress_level=0):
        self.calls += 1
        raise ProviderResponseError(
            "vision provider failed: status=413",
            status=413,
            response_text='{"error":"too large"}',
            request_bytes=4096,
        )


class _FakeProgressReporter:
    def __init__(self):
        self.events = []
        self._batch_times = []

    def start_stage(self, stage, total, detail=""):
        self.events.append(("start", stage, total, detail))

    def update_stage_detail(self, stage, detail):
        self.events.append(("detail", stage, detail))

    def update_stage_attempt(
        self,
        stage,
        *,
        batch_index,
        total_batches,
        attempt,
        total_attempts,
        status,
        detail="",
    ):
        self.events.append(
            (
                "attempt",
                stage,
                batch_index,
                total_batches,
                attempt,
                total_attempts,
                status,
                detail,
            )
        )

    def advance_stage_item(self, stage, status, detail=""):
        self.events.append(("advance", stage, status, detail))

    def finish_stage(self, stage, detail="完成"):
        self.events.append(("finish", stage, detail))

    def start_batch_timer(self, stage):
        self.events.append(("batch_timer_start", stage))

    def record_batch_time(self, stage):
        self.events.append(("batch_timer_record", stage))
        self._batch_times.append(1.0)
        return 1.0

    def avg_batch_time(self, stage):
        return sum(self._batch_times) / len(self._batch_times) if self._batch_times else 0


@pytest.mark.asyncio
async def test_pipeline_resume_runs_all_stages(tmp_path):
    downloads_root = tmp_path / "Downloaded"
    video_dir = downloads_root / "author" / "post" / "demo"
    video_dir.mkdir(parents=True)
    video_path = video_dir / "demo.mp4"
    video_path.write_bytes(b"video")

    config = ConfigLoader()
    config.update(
        path=str(downloads_root),
        analysis={
            **config.get("analysis"),
            "output_dir": str(tmp_path / "Analysis"),
            "classified_dir": str(tmp_path / "Classified"),
        },
    )

    database = Database(str(tmp_path / "test.db"))
    await database.initialize()
    await database.add_aweme(
        {
            "aweme_id": "123",
            "aweme_type": "video",
            "title": "demo",
            "author_id": "author",
            "author_name": "Author",
            "create_time": 1700000000,
            "file_path": str(video_dir),
            "metadata": "{}",
        }
    )

    pipeline = AnalysisPipeline(
        raw_config=config.config,
        database=database,
        file_manager=FileManager(str(downloads_root)),
        provider=_FakeProvider(),
        frame_extractor=_FakeFrameExtractor(),
        grid_builder=_FakeGridBuilder(),
    )
    run_id = await pipeline.create_run(source_type="all", source_payload={"scope": "all"})
    count = await pipeline.prepare_from_all_videos(run_id=run_id)

    assert count == 1
    await pipeline.resume(run_id)

    run = await database.get_analysis_run(run_id)
    assert run["status"] == "completed"
    assert Path(run["csv_path"]).exists()
    classified = list((tmp_path / "Classified").rglob("*.mp4"))
    assert classified == [tmp_path / "Classified" / "Author" / "文艺范_低" / "demo.mp4"]

    await database.close()


@pytest.mark.asyncio
async def test_pipeline_batch_mode_uses_five_video_prompt_contract(tmp_path):
    downloads_root = tmp_path / "Downloaded"
    config = ConfigLoader()
    prompt_file = tmp_path / "prompt.md"
    prompt_file.write_text(
        "ids={video_id_1},{video_id_2},{video_id_3},{video_id_4},{video_id_5}",
        encoding="utf-8",
    )
    config.update(
        path=str(downloads_root),
        analysis={
            **config.get("analysis"),
            "output_dir": str(tmp_path / "Analysis"),
            "classified_dir": str(tmp_path / "Classified"),
            "prompt_file": str(prompt_file),
            "batch_size": 5,
            "attributes": [
                {
                    "key": "suggestiveness_score",
                    "label": "性暗示程度",
                    "description": "desc",
                    "min_score": 1,
                    "max_score": 10,
                },
                {
                    "key": "coverage_score",
                    "label": "覆盖程度",
                    "description": "desc",
                    "min_score": 1,
                    "max_score": 10,
                },
            ],
            "primary_attribute": "suggestiveness_score",
        },
    )

    database = Database(str(tmp_path / "test.db"))
    await database.initialize()
    for index in range(5):
        video_dir = downloads_root / "author" / "post" / f"demo-{index}"
        video_dir.mkdir(parents=True)
        (video_dir / f"demo-{index}.mp4").write_bytes(b"video")
        await database.add_aweme(
            {
                "aweme_id": str(index),
                "aweme_type": "video",
                "title": f"demo-{index}",
                "author_id": "author",
                "author_name": "Author",
                "create_time": 1700000000 + index,
                "file_path": str(video_dir),
                "metadata": "{}",
            }
        )

    pipeline = AnalysisPipeline(
        raw_config=config.config,
        database=database,
        file_manager=FileManager(str(downloads_root)),
        provider=_FakeBatchProvider(),
        frame_extractor=_FakeFrameExtractor(),
        grid_builder=_FakeGridBuilder(),
    )
    run_id = await pipeline.create_run(source_type="all", source_payload={"scope": "all"})
    assert await pipeline.prepare_from_all_videos(run_id=run_id) == 5
    await pipeline.resume(run_id)

    rows = await database.get_analysis_export_rows(run_id)
    assert len(rows) == 5
    assert sorted(row["scores"]["suggestiveness_score"] for row in rows) == [1, 2, 3, 4, 5]
    await database.close()


@pytest.mark.asyncio
async def test_prepare_from_all_videos_can_limit_candidates(tmp_path):
    downloads_root = tmp_path / "Downloaded"
    database = Database(str(tmp_path / "test.db"))
    await database.initialize()
    for index in range(3):
        video_dir = downloads_root / "author" / "post" / f"demo-{index}"
        video_dir.mkdir(parents=True)
        (video_dir / f"demo-{index}.mp4").write_bytes(b"video")
        await database.add_aweme(
            {
                "aweme_id": str(index),
                "aweme_type": "video",
                "title": f"demo-{index}",
                "author_id": "author",
                "author_name": "Author",
                "create_time": 1700000000 + index,
                "file_path": str(video_dir),
                "metadata": "{}",
            }
        )

    config = ConfigLoader()
    config.update(path=str(downloads_root))
    pipeline = AnalysisPipeline(
        raw_config=config.config,
        database=database,
        file_manager=FileManager(str(downloads_root)),
        provider=_FakeProvider(),
        frame_extractor=_FakeFrameExtractor(),
        grid_builder=_FakeGridBuilder(),
    )
    run_id = await pipeline.create_run(source_type="all", source_payload={"scope": "all"})

    assert await pipeline.prepare_from_all_videos(run_id=run_id, limit=2) == 2
    await database.close()


@pytest.mark.asyncio
async def test_pipeline_resume_reports_visible_stage_progress(tmp_path):
    downloads_root = tmp_path / "Downloaded"
    video_dir = downloads_root / "author" / "post" / "demo"
    video_dir.mkdir(parents=True)
    (video_dir / "demo.mp4").write_bytes(b"video")

    config = ConfigLoader()
    config.update(
        path=str(downloads_root),
        analysis={
            **config.get("analysis"),
            "output_dir": str(tmp_path / "Analysis"),
            "classified_dir": str(tmp_path / "Classified"),
        },
    )

    database = Database(str(tmp_path / "test.db"))
    await database.initialize()
    await database.add_aweme(
        {
            "aweme_id": "123",
            "aweme_type": "video",
            "title": "demo",
            "author_id": "author",
            "author_name": "Author",
            "create_time": 1700000000,
            "file_path": str(video_dir),
            "metadata": "{}",
        }
    )

    reporter = _FakeProgressReporter()
    pipeline = AnalysisPipeline(
        raw_config=config.config,
        database=database,
        file_manager=FileManager(str(downloads_root)),
        provider=_FakeProvider(),
        frame_extractor=_FakeFrameExtractor(),
        grid_builder=_FakeGridBuilder(),
        progress_reporter=reporter,
    )
    run_id = await pipeline.create_run(source_type="all", source_payload={"scope": "all"})
    await pipeline.prepare_from_all_videos(run_id=run_id)

    await pipeline.resume(run_id)

    assert ("start", "frames", 1, "") in reporter.events
    assert ("advance", "frames", "success", "123") in reporter.events
    assert ("start", "classify", 1, "") in reporter.events
    assert ("advance", "classify", "success", "123") in reporter.events
    assert any(event[:2] == ("start", "export") for event in reporter.events)
    assert any(event[:2] == ("start", "organize") for event in reporter.events)

    await database.close()


@pytest.mark.asyncio
async def test_batch_mode_progress_excludes_remainder_waiting_for_full_batch(tmp_path):
    downloads_root = tmp_path / "Downloaded"
    config = ConfigLoader()
    config.update(
        path=str(downloads_root),
        analysis={
            **config.get("analysis"),
            "output_dir": str(tmp_path / "Analysis"),
            "classified_dir": str(tmp_path / "Classified"),
            "batch_size": 5,
            "allow_partial_batch": False,
        },
    )

    database = Database(str(tmp_path / "test.db"))
    await database.initialize()
    for index in range(6):
        video_dir = downloads_root / "author" / "post" / f"demo-{index}"
        video_dir.mkdir(parents=True)
        (video_dir / f"demo-{index}.mp4").write_bytes(b"video")
        await database.add_aweme(
            {
                "aweme_id": str(index),
                "aweme_type": "video",
                "title": f"demo-{index}",
                "author_id": "author",
                "author_name": "Author",
                "create_time": 1700000000 + index,
                "file_path": str(video_dir),
                "metadata": "{}",
            }
        )

    reporter = _FakeProgressReporter()
    pipeline = AnalysisPipeline(
        raw_config=config.config,
        database=database,
        file_manager=FileManager(str(downloads_root)),
        provider=_FakeBatchProvider(),
        frame_extractor=_FakeFrameExtractor(),
        grid_builder=_FakeGridBuilder(),
        progress_reporter=reporter,
    )
    run_id = await pipeline.create_run(source_type="all", source_payload={"scope": "all"})
    await pipeline.prepare_from_all_videos(run_id=run_id)
    await pipeline.run_frames(run_id)
    await pipeline.run_classify(run_id)

    assert ("start", "classify", 5, "剩余 1 条等待凑满批次") in reporter.events
    assert ("finish", "classify", "完成，剩余 1 条等待凑满批次") in reporter.events

    await database.close()


@pytest.mark.asyncio
async def test_batch_mode_reports_attempt_number_and_retry_status(tmp_path):
    downloads_root = tmp_path / "Downloaded"
    config = ConfigLoader()
    config.update(
        path=str(downloads_root),
        analysis={
            **config.get("analysis"),
            "output_dir": str(tmp_path / "Analysis"),
            "classified_dir": str(tmp_path / "Classified"),
            "batch_size": 5,
            "allow_partial_batch": False,
        },
    )

    database = Database(str(tmp_path / "test.db"))
    await database.initialize()
    for index in range(5):
        video_dir = downloads_root / "author" / "post" / f"demo-{index}"
        video_dir.mkdir(parents=True)
        (video_dir / f"demo-{index}.mp4").write_bytes(b"video")
        await database.add_aweme(
            {
                "aweme_id": str(index),
                "aweme_type": "video",
                "title": f"demo-{index}",
                "author_id": "author",
                "author_name": "Author",
                "create_time": 1700000000 + index,
                "file_path": str(video_dir),
                "metadata": "{}",
            }
        )

    reporter = _FakeProgressReporter()
    pipeline = AnalysisPipeline(
        raw_config=config.config,
        database=database,
        file_manager=FileManager(str(downloads_root)),
        provider=_RetryingBatchProvider(),
        frame_extractor=_FakeFrameExtractor(),
        grid_builder=_FakeGridBuilder(),
        progress_reporter=reporter,
    )
    run_id = await pipeline.create_run(source_type="all", source_payload={"scope": "all"})
    await pipeline.prepare_from_all_videos(run_id=run_id)
    await pipeline.run_frames(run_id)
    await pipeline.run_classify(run_id)

    attempt_events = [event for event in reporter.events if event[0] == "attempt"]
    assert attempt_events == [
        ("attempt", "classify", 1, 1, 1, 2, "请求中", ""),
        ("attempt", "classify", 1, 1, 1, 2, "失败，等待重试", "回应：空 · 0s 后第 2/2 次"),
        ("attempt", "classify", 1, 1, 2, 2, "请求中", ""),
        ("attempt", "classify", 1, 1, 2, 2, "成功", ""),
    ]

    await database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider_factory", "expected_detail"),
    [
        (_ResponseRetryingBatchProvider, '回应：{"error":"busy"} · 0s 后第 2/2 次'),
        (_EmptyResponseRetryingBatchProvider, "回应：空 · 0s 后第 2/2 次"),
    ],
)
async def test_batch_mode_reports_failed_response_or_empty(
    tmp_path,
    provider_factory,
    expected_detail,
):
    downloads_root = tmp_path / "Downloaded"
    config = ConfigLoader()
    config.update(
        path=str(downloads_root),
        analysis={
            **config.get("analysis"),
            "output_dir": str(tmp_path / "Analysis"),
            "classified_dir": str(tmp_path / "Classified"),
            "batch_size": 5,
            "allow_partial_batch": False,
        },
    )

    database = Database(str(tmp_path / "test.db"))
    await database.initialize()
    for index in range(5):
        video_dir = downloads_root / "author" / "post" / f"demo-{index}"
        video_dir.mkdir(parents=True)
        (video_dir / f"demo-{index}.mp4").write_bytes(b"video")
        await database.add_aweme(
            {
                "aweme_id": str(index),
                "aweme_type": "video",
                "title": f"demo-{index}",
                "author_id": "author",
                "author_name": "Author",
                "create_time": 1700000000 + index,
                "file_path": str(video_dir),
                "metadata": "{}",
            }
        )

    reporter = _FakeProgressReporter()
    pipeline = AnalysisPipeline(
        raw_config=config.config,
        database=database,
        file_manager=FileManager(str(downloads_root)),
        provider=provider_factory(),
        frame_extractor=_FakeFrameExtractor(),
        grid_builder=_FakeGridBuilder(),
        progress_reporter=reporter,
    )
    run_id = await pipeline.create_run(source_type="all", source_payload={"scope": "all"})
    await pipeline.prepare_from_all_videos(run_id=run_id)
    await pipeline.run_frames(run_id)
    await pipeline.run_classify(run_id)

    retry_event = next(
        event
        for event in reporter.events
        if event[:7] == ("attempt", "classify", 1, 1, 1, 2, "失败，等待重试")
    )
    assert retry_event[7] == expected_detail

    await database.close()


@pytest.mark.asyncio
async def test_debug_mode_stops_on_api_error_and_prints_batch_diagnostics(tmp_path):
    downloads_root = tmp_path / "Downloaded"
    config = ConfigLoader()
    config.update(
        path=str(downloads_root),
        analysis={
            **config.get("analysis"),
            "output_dir": str(tmp_path / "Analysis"),
            "classified_dir": str(tmp_path / "Classified"),
            "batch_size": 5,
            "allow_partial_batch": False,
            "provider": {
                **config.get("analysis")["provider"],
                "debug_stop_on_api_error": True,
            },
        },
    )

    database = Database(str(tmp_path / "test.db"))
    await database.initialize()
    for index in range(5):
        video_dir = downloads_root / "author" / "post" / f"demo-{index}"
        video_dir.mkdir(parents=True)
        (video_dir / f"demo-{index}.mp4").write_bytes(b"video")
        await database.add_aweme(
            {
                "aweme_id": str(index),
                "aweme_type": "video",
                "title": f"demo-{index}",
                "author_id": "author",
                "author_name": "Author",
                "create_time": 1700000000 + index,
                "file_path": str(video_dir),
                "metadata": "{}",
            }
        )

    provider = _DebugFailingBatchProvider()
    pipeline = AnalysisPipeline(
        raw_config=config.config,
        database=database,
        file_manager=FileManager(str(downloads_root)),
        provider=provider,
        frame_extractor=_FakeFrameExtractor(),
        grid_builder=_ImageGridBuilder(),
    )
    run_id = await pipeline.create_run(source_type="all", source_payload={"scope": "all"})
    await pipeline.prepare_from_all_videos(run_id=run_id)
    await pipeline.run_frames(run_id)

    with pytest.raises(AnalysisDebugStop) as exc_info:
        await pipeline.run_classify(run_id)

    report = exc_info.value.report
    assert provider.calls == 1
    assert "HTTP 状态：413" in report
    assert '失败回应：{"error":"too large"}' in report
    assert "请求 JSON 大小：4.0 KB" in report
    assert "图片文件总大小：" in report
    assert "预处理后图片总大小：" in report
    assert "分辨率=64x32" in report

    await database.close()
