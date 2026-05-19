"""FastAPI 服务测试：验证 job 生命周期与 HTTP 接口。

仅测试 HTTP 层 + JobManager 抽象；不触达真实 Douyin API。
"""

import asyncio
import time
from typing import Dict

import pytest

try:
    from fastapi.testclient import TestClient  # type: ignore
except ImportError:  # pragma: no cover
    pytest.skip("fastapi not installed", allow_module_level=True)


from config import ConfigLoader
from server.app import build_app
from server.jobs import JobManager
from server.pipeline_jobs import PipelineJobManager
from storage import Database


@pytest.mark.asyncio
async def test_job_manager_runs_executor(tmp_path):
    async def fake_executor(url: str) -> Dict[str, int]:
        return {"total": 1, "success": 1, "failed": 0, "skipped": 0}

    manager = JobManager(executor=fake_executor, max_concurrency=2)
    job = await manager.submit("https://example/one")
    assert job.status == "pending"

    # 等待后台任务跑完
    await asyncio.wait_for(job._task, timeout=2.0)
    fetched = await manager.get(job.job_id)
    assert fetched is not None
    assert fetched.status == "success"
    assert fetched.success == 1


@pytest.mark.asyncio
async def test_job_manager_marks_failure_on_executor_error(tmp_path):
    async def boom(url: str) -> Dict[str, int]:
        raise RuntimeError("bad url")

    manager = JobManager(executor=boom)
    job = await manager.submit("x")
    await asyncio.wait_for(job._task, timeout=2.0)
    fetched = await manager.get(job.job_id)
    assert fetched is not None
    assert fetched.status == "failed"
    assert fetched.error is not None
    assert "bad url" in fetched.error


def test_health_endpoint(tmp_path):
    config = ConfigLoader(None)
    config.update(path=str(tmp_path))
    app = build_app(config)

    with TestClient(app) as client:
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


def test_download_endpoint_creates_job(tmp_path, monkeypatch):
    config = ConfigLoader(None)
    config.update(path=str(tmp_path))
    app = build_app(config)

    # 替换 job executor 为 fake（不去触达 Douyin）
    async def fake_executor(url: str) -> Dict[str, int]:
        return {"total": 0, "success": 0, "failed": 0, "skipped": 0}

    app.state.job_manager.executor = fake_executor

    with TestClient(app) as client:
        resp = client.post("/api/v1/download", json={"url": "https://www.douyin.com/video/123"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ("pending", "running", "success")
        assert data["url"] == "https://www.douyin.com/video/123"
        assert len(data["job_id"]) > 0

        job_id = data["job_id"]
        # job 列表应包含该 id
        list_resp = client.get("/api/v1/jobs")
        assert list_resp.status_code == 200
        ids = [j["job_id"] for j in list_resp.json()["jobs"]]
        assert job_id in ids

        # 详情接口
        detail = client.get(f"/api/v1/jobs/{job_id}")
        assert detail.status_code == 200
        assert detail.json()["job_id"] == job_id


def test_download_endpoint_rejects_empty_url(tmp_path):
    config = ConfigLoader(None)
    config.update(path=str(tmp_path))
    app = build_app(config)
    with TestClient(app) as client:
        resp = client.post("/api/v1/download", json={"url": ""})
        assert resp.status_code == 400


def test_get_unknown_job_returns_404(tmp_path):
    config = ConfigLoader(None)
    config.update(path=str(tmp_path))
    app = build_app(config)
    with TestClient(app) as client:
        resp = client.get("/api/v1/jobs/unknown-id")
        assert resp.status_code == 404


def test_build_app_shares_deps_across_requests(tmp_path):
    """重请求应复用同一个 FileManager / RateLimiter 等（避免每次重建）。"""
    config = ConfigLoader(None)
    config.update(path=str(tmp_path))
    app = build_app(config)

    deps = app.state.deps
    assert deps.file_manager is not None
    assert deps.rate_limiter is not None
    assert deps.retry_handler is not None
    assert deps.queue_manager is not None
    assert deps.cookie_manager is not None

    # 构建第二次 app 时应该是完全独立的 deps 实例，但同一 app 内是共享的
    app2 = build_app(config)
    assert app2.state.deps is not app.state.deps
    assert app.state.deps.file_manager is app.state.deps.file_manager  # identity


def test_dashboard_home_returns_html(tmp_path):
    config = ConfigLoader(None)
    config.update(path=str(tmp_path), database_path=str(tmp_path / "test.db"))
    app = build_app(config)

    with TestClient(app) as client:
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Pipeline Dashboard" in resp.text


def test_settings_endpoint_round_trips_safe_fields_without_api_key(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        """
path: ./Downloaded
thread: 2
analysis:
  batch_size: 1
  provider:
    model: old-model
    api_key: secret-key
""",
        encoding="utf-8",
    )
    config = ConfigLoader(str(config_path))
    app = build_app(config)

    with TestClient(app) as client:
        settings = client.get("/api/v1/settings")
        assert settings.status_code == 200
        data = settings.json()
        assert data["secrets"]["analysis.provider.api_key"] is True
        assert "api_key" not in data["settings"]["analysis"]["provider"]

        resp = client.patch(
            "/api/v1/settings",
            json={
                "settings": {
                    "thread": 7,
                    "analysis": {
                        "batch_size": 9,
                        "provider": {"model": "new-model", "api_key": "leak-attempt"},
                    },
                }
            },
        )
        assert resp.status_code == 200
        assert config.get("thread") == 7
        assert config.get("analysis")["batch_size"] == 9
        assert config.get("analysis")["provider"]["model"] == "new-model"
        assert config.get("analysis")["provider"]["api_key"] == "secret-key"

        persisted = config_path.read_text(encoding="utf-8")
        assert "new-model" in persisted
        assert "leak-attempt" not in persisted


@pytest.mark.asyncio
async def test_pipeline_summary_runs_and_scores_endpoints(tmp_path):
    db_path = tmp_path / "analysis.db"
    database = Database(str(db_path))
    await database.initialize()
    await database.create_analysis_run(run_id="run-scored", source_type="all", source_payload={"scope": "all"})
    await database.add_analysis_items(
        "run-scored",
        [
            {"aweme_id": "1", "author_name": "A", "video_path": str(tmp_path / "1.mp4")},
            {"aweme_id": "2", "author_name": "A", "video_path": str(tmp_path / "2.mp4")},
        ],
    )
    for aweme_id, score in {"1": 6, "2": 4}.items():
        await database.update_analysis_item_stage(
            "run-scored", aweme_id, stage="frames", status="success"
        )
        await database.update_analysis_item_stage(
            "run-scored", aweme_id, stage="classify", status="success"
        )
        await database.upsert_analysis_scores(
            "run-scored",
            aweme_id,
            {"suggestiveness_score": score, "coverage_score": 2},
        )
    await database.close()

    config = ConfigLoader(None)
    analysis = dict(config.get("analysis"))
    analysis.update(
        {
            "active_run_id": "run-scored",
            "primary_attribute": "suggestiveness_score",
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
            "buckets": [
                {"label": "1-3", "min_score": 1, "max_score": 3},
                {"label": "4", "min_score": 4, "max_score": 4},
                {"label": "5", "min_score": 5, "max_score": 5},
                {"label": "6+", "min_score": 6, "max_score": 10},
            ],
        }
    )
    config.update(path=str(tmp_path), database_path=str(db_path), analysis=analysis)
    app = build_app(config)

    with TestClient(app) as client:
        summary = client.get("/api/v1/pipeline/summary")
        assert summary.status_code == 200
        assert summary.json()["active_run"]["run_id"] == "run-scored"
        assert summary.json()["active_run"]["classified"] == 2

        runs = client.get("/api/v1/pipeline/runs")
        assert runs.status_code == 200
        assert runs.json()["runs"][0]["framed"] == 2

        scores = client.get("/api/v1/pipeline/runs/run-scored/scores")
        assert scores.status_code == 200
        body = scores.json()
        assert body["primary_attribute"] == "suggestiveness_score"
        assert body["distributions"]["suggestiveness_score"]["6"] == 1
        assert {b["label"]: b["count"] for b in body["buckets"]}["6+"] == 1


class _SlowFakePipeline:
    def __init__(self, *, progress_reporter, **_kwargs):
        self.progress_reporter = progress_reporter

    async def resume(self, run_id):
        self.progress_reporter.start_stage("classify", 2)
        self.progress_reporter.update_stage_attempt(
            "classify",
            batch_index=1,
            total_batches=1,
            attempt=1,
            total_attempts=2,
            status="请求中",
        )
        await asyncio.sleep(0.15)
        self.progress_reporter.advance_stage_item("classify", "success", "1")
        self.progress_reporter.advance_stage_item("classify", "success", "2")
        self.progress_reporter.finish_stage("classify")

    async def run_organize(self, run_id, *, rebuild=False):
        self.progress_reporter.start_stage("organize", 1)
        await asyncio.sleep(0.01)
        self.progress_reporter.advance_stage_item("organize", "success", run_id)
        self.progress_reporter.finish_stage("organize")


def test_pipeline_job_endpoint_runs_and_rejects_concurrent_jobs(tmp_path):
    db_path = tmp_path / "jobs.db"

    async def seed():
        database = Database(str(db_path))
        await database.initialize()
        await database.create_analysis_run(run_id="run-active", source_type="all", source_payload={"scope": "all"})
        await database.add_analysis_items(
            "run-active",
            [
                {"aweme_id": "1", "author_name": "A", "video_path": str(tmp_path / "1.mp4")},
                {"aweme_id": "2", "author_name": "A", "video_path": str(tmp_path / "2.mp4")},
            ],
        )
        await database.close()

    asyncio.run(seed())
    config = ConfigLoader(None)
    analysis = dict(config.get("analysis"))
    analysis["active_run_id"] = "run-active"
    config.update(path=str(tmp_path), database_path=str(db_path), analysis=analysis)
    app = build_app(config)
    app.state.pipeline_job_manager = PipelineJobManager(
        config=config,
        db_path=str(db_path),
        pipeline_factory=_SlowFakePipeline,
    )

    with TestClient(app) as client:
        first = client.post("/api/v1/pipeline/jobs", json={"action": "continue"})
        assert first.status_code == 200
        job_id = first.json()["job_id"]

        second = client.post("/api/v1/pipeline/jobs", json={"action": "retry"})
        assert second.status_code == 409

        deadline = time.time() + 3
        detail = {}
        while time.time() < deadline:
            resp = client.get(f"/api/v1/pipeline/jobs/{job_id}")
            assert resp.status_code == 200
            detail = resp.json()
            if detail["status"] in {"success", "failed"}:
                break
            time.sleep(0.05)

        assert detail["status"] == "success"
        assert detail["stages"]["classify"]["completed"] == 2
        assert detail["stages"]["classify"]["attempt"]["status"] == "请求中"


@pytest.mark.asyncio
async def test_job_manager_prunes_by_max_jobs():
    """max_jobs 超限时应优先淘汰最老的终态 job，保留 in-flight。"""

    async def fast_executor(url: str) -> Dict[str, int]:
        return {"total": 0, "success": 0, "failed": 0, "skipped": 0}

    manager = JobManager(executor=fast_executor, max_jobs=3, job_ttl_seconds=0.0)
    jobs = []
    for i in range(5):
        j = await manager.submit(f"u{i}")
        jobs.append(j)
        await asyncio.wait_for(j._task, timeout=1.0)

    remaining = await manager.list_jobs()
    # max_jobs=3：新任务 submit 时先剪裁，最终存量 ≤ max_jobs
    assert len(remaining) <= 3
    # 最新的那一批一定在，最早的那几个被淘汰
    ids_remaining = {j.job_id for j in remaining}
    assert jobs[-1].job_id in ids_remaining


@pytest.mark.asyncio
async def test_job_manager_prunes_by_ttl():
    """TTL 过期的终态 job 应在下次 submit 时被清理。"""

    async def fast_executor(url: str) -> Dict[str, int]:
        return {"total": 0, "success": 0, "failed": 0, "skipped": 0}

    manager = JobManager(executor=fast_executor, max_jobs=100, job_ttl_seconds=0.01)
    old_job = await manager.submit("old")
    await asyncio.wait_for(old_job._task, timeout=1.0)

    # 等 TTL 过期
    await asyncio.sleep(0.05)

    new_job = await manager.submit("new")
    await asyncio.wait_for(new_job._task, timeout=1.0)

    remaining_ids = {j.job_id for j in await manager.list_jobs()}
    assert old_job.job_id not in remaining_ids
    assert new_job.job_id in remaining_ids
