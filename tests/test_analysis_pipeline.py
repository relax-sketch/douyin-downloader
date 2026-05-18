from pathlib import Path

import pytest

from analysis.pipeline import AnalysisPipeline
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


class _FakeProvider:
    async def score(self, image_path, attributes, prompt):
        return {attribute.key: index for index, attribute in enumerate(attributes)}


class _FakeBatchProvider:
    async def batch_score(self, image_paths, video_ids, prompt):
        return [
            {
                "video_id": video_id,
                "suggestiveness_score": index + 1,
                "coverage_score": index + 2,
            }
            for index, video_id in enumerate(video_ids)
        ]


class _FakeProgressReporter:
    def __init__(self):
        self.events = []

    def start_stage(self, stage, total, detail=""):
        self.events.append(("start", stage, total, detail))

    def update_stage_detail(self, stage, detail):
        self.events.append(("detail", stage, detail))

    def advance_stage_item(self, stage, status, detail=""):
        self.events.append(("advance", stage, status, detail))

    def finish_stage(self, stage, detail="完成"):
        self.events.append(("finish", stage, detail))


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
