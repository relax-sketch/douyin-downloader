"""FastAPI REST 服务入口。

HTTP 层薄封装：
- 接收 URL，创建 job，返回 job_id
- 实际下载委托给 cli.main.download_url 的简化复用

fastapi/uvicorn 是**可选**依赖。若未安装，导入本模块会 ImportError。
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from auth import CookieManager
from analysis.config import load_attributes, load_buckets, primary_attribute_key
from config import ConfigLoader
from control import QueueManager, RateLimiter, RetryHandler
from core import DouyinAPIClient, DownloaderFactory, URLParser
from server.jobs import JobManager
from server.pipeline_jobs import PipelineJobConflict, PipelineJobManager
from storage import Database
from storage import FileManager
from utils.logger import setup_logger
from utils.validators import is_short_url, normalize_short_url

logger = setup_logger("REST")


class DownloadRequest(BaseModel):
    url: str


class JobResponse(BaseModel):
    job_id: str
    status: str
    url: str


class SettingsPatchRequest(BaseModel):
    settings: Dict[str, Any]


class PipelineJobRequest(BaseModel):
    action: str
    run_id: Optional[str] = None
    limit: int = 0


def _resolve_config_relative_path(config: ConfigLoader, value: Any, default: str) -> str:
    raw = str(value or default)
    path = Path(raw)
    if path.is_absolute():
        return str(path)
    if config.config_path:
        return str((Path(config.config_path).resolve().parent / path).resolve())
    return str(path)


class _ServerDeps:
    """跨请求复用的重量级依赖。

    REST 服务在进程生命周期内只需要一份 FileManager / RateLimiter / RetryHandler /
    QueueManager / CookieManager；每个请求重新构造既浪费又会触发文件系统 mkdir。
    DouyinAPIClient 由于持有 aiohttp.ClientSession，依旧按请求创建，避免跨请求泄漏
    连接状态或触发 "Session is closed" 错误。
    """

    def __init__(self, config: ConfigLoader):
        self.config = config
        # Resolve the cookie file path relative to the config file's directory
        # so the sidecar can find it regardless of its working directory (which
        # on macOS is often '/' when launched by Electron).
        if config.config_path:
            from pathlib import Path

            cookie_file = str(Path(config.config_path).resolve().parent / ".cookies.json")
        else:
            cookie_file = ".cookies.json"
        self.cookie_manager = CookieManager(cookie_file=cookie_file)
        # Load cookies from the config (env var / YAML cookie key) first, then
        # fall back to whatever is already on disk in the cookie file. This
        # ensures that cookies saved by a previous session are picked up on
        # restart even when the config doesn't embed them inline.
        initial_cookies = config.get_cookies()
        if initial_cookies:
            self.cookie_manager.set_cookies(initial_cookies)
        else:
            # Trigger a load from disk so get_cookies() returns the persisted
            # session without requiring a fresh login on every app restart.
            self.cookie_manager.get_cookies()
        self.file_manager = FileManager(config.get("path"))
        self.rate_limiter = RateLimiter(max_per_second=float(config.get("rate_limit", 2) or 2))
        self.retry_handler = RetryHandler(max_retries=int(config.get("retry_times", 3) or 3))
        self.queue_manager = QueueManager(max_workers=int(config.get("thread", 5) or 5))
        self.db_path = _resolve_config_relative_path(
            config,
            config.get("database_path", "dy_downloader.db"),
            "dy_downloader.db",
        )
        self.pipeline_job_manager = PipelineJobManager(config=config, db_path=self.db_path)


async def _execute_download(url: str, deps: "_ServerDeps") -> Dict[str, int]:
    """简化版 download_url：只负责执行并返回成功/失败计数。

    有意不复用 cli.main.download_url —— 后者绑定了 progress_display 的 rich 状态。
    API client 仍按请求创建（aiohttp session 不跨请求复用）；其余重量级依赖从
    _ServerDeps 共享。
    """
    async with DouyinAPIClient(deps.cookie_manager.get_cookies()) as api_client:
        if is_short_url(url):
            resolved = await api_client.resolve_short_url(normalize_short_url(url))
            if not resolved:
                raise RuntimeError(f"Failed to resolve short URL: {url}")
            url = resolved

        parsed = URLParser.parse(url)
        if not parsed:
            raise RuntimeError(f"Unsupported URL: {url}")

        downloader = DownloaderFactory.create(
            parsed["type"],
            deps.config,
            api_client,
            deps.file_manager,
            deps.cookie_manager,
            None,  # database 不在 server 场景里启用，避免单例冲突
            deps.rate_limiter,
            deps.retry_handler,
            deps.queue_manager,
            progress_reporter=None,
        )
        if downloader is None:
            raise RuntimeError(f"No downloader for url_type={parsed['type']}")

        result = await downloader.download(parsed)
        return {
            "total": result.total,
            "success": result.success,
            "failed": result.failed,
            "skipped": result.skipped,
        }


def build_app(config: ConfigLoader) -> FastAPI:
    deps = _ServerDeps(config)

    async def executor(url: str) -> Dict[str, int]:
        return await _execute_download(url, deps)

    server_cfg = config.get("server") or {}
    if not isinstance(server_cfg, dict):
        server_cfg = {}
    manager = JobManager(
        executor=executor,
        max_concurrency=int(config.get("thread", 2) or 2),
        max_jobs=int(server_cfg.get("max_jobs") or JobManager.DEFAULT_MAX_JOBS),
        job_ttl_seconds=float(
            server_cfg.get("job_ttl_seconds") or JobManager.DEFAULT_JOB_TTL_SECONDS
        ),
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await manager.shutdown()
        await deps.pipeline_job_manager.shutdown()

    app = FastAPI(
        title="Douyin Downloader API",
        version="1.0",
        description="REST API for dispatching Douyin download jobs.",
        lifespan=lifespan,
    )
    app.state.job_manager = manager
    app.state.deps = deps
    app.state.pipeline_job_manager = deps.pipeline_job_manager

    static_dir = Path(__file__).resolve().parent / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def dashboard_home():
        index_path = static_dir / "index.html"
        if index_path.exists():
            return FileResponse(index_path)
        return HTMLResponse("<h1>Douyin Pipeline Dashboard</h1>")

    @app.get("/api/v1/health")
    async def health() -> Dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/v1/settings")
    async def get_settings() -> Dict[str, Any]:
        cfg = config.config
        analysis_cfg = dict(cfg.get("analysis") or {})
        provider_cfg = dict(analysis_cfg.get("provider") or {})
        safe_provider = {
            key: value
            for key, value in provider_cfg.items()
            if key not in {"api_key"}
        }
        analysis_cfg["provider"] = safe_provider
        return {
            "settings": {
                "path": cfg.get("path"),
                "thread": cfg.get("thread"),
                "rate_limit": cfg.get("rate_limit"),
                "proxy": cfg.get("proxy"),
                "retry_times": cfg.get("retry_times"),
                "analysis": analysis_cfg,
            },
            "fields": _settings_fields(),
            "secrets": {
                "cookie": bool(config.get_cookies()),
                "analysis.provider.api_key": bool(provider_cfg.get("api_key")),
                "analysis.provider.api_key_env": provider_cfg.get("api_key_env"),
            },
        }

    @app.patch("/api/v1/settings")
    async def patch_settings(req: SettingsPatchRequest) -> Dict[str, Any]:
        _apply_settings_patch(config, req.settings)
        saved = config.save()
        return {"saved": saved, "settings": (await get_settings())["settings"]}

    @app.post("/api/v1/pipeline/jobs")
    async def create_pipeline_job(req: PipelineJobRequest) -> Dict[str, Any]:
        try:
            job = await app.state.pipeline_job_manager.submit(
                action=req.action,
                run_id=req.run_id,
                limit=req.limit,
            )
        except PipelineJobConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return job.to_dict()

    @app.get("/api/v1/pipeline/jobs/{job_id}")
    async def get_pipeline_job(job_id: str) -> Dict[str, Any]:
        job = await app.state.pipeline_job_manager.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="pipeline job not found")
        return job.to_dict()

    @app.get("/api/v1/pipeline/summary")
    async def pipeline_summary() -> Dict[str, Any]:
        database = Database(deps.db_path)
        await database.initialize()
        try:
            analysis_cfg = config.get("analysis", {}) or {}
            active_run_id = str(analysis_cfg.get("active_run_id") or "").strip()
            active_run = (
                await database.get_analysis_run_dashboard(active_run_id)
                if active_run_id
                else None
            )
            best_unfinished = await database.get_best_unfinished_run()
            latest_classified = await database.get_latest_classified_run()
            jobs = await app.state.pipeline_job_manager.list_jobs()
            return {
                "active_run_id": active_run_id,
                "active_run": active_run,
                "best_unfinished_run": best_unfinished,
                "latest_classified_run": latest_classified,
                "jobs": [job.to_dict() for job in jobs[-5:]],
            }
        finally:
            await database.close()

    @app.get("/api/v1/pipeline/runs")
    async def pipeline_runs(limit: int = 20) -> Dict[str, Any]:
        database = Database(deps.db_path)
        await database.initialize()
        try:
            return {"runs": await database.list_analysis_runs_detailed(limit=max(1, min(limit, 100)))}
        finally:
            await database.close()

    @app.get("/api/v1/pipeline/runs/{run_id}")
    async def pipeline_run_detail(run_id: str) -> Dict[str, Any]:
        database = Database(deps.db_path)
        await database.initialize()
        try:
            run = await database.get_analysis_run_dashboard(run_id)
            if run is None:
                raise HTTPException(status_code=404, detail="analysis run not found")
            return run
        finally:
            await database.close()

    @app.get("/api/v1/pipeline/runs/{run_id}/scores")
    async def pipeline_run_scores(run_id: str) -> Dict[str, Any]:
        database = Database(deps.db_path)
        await database.initialize()
        try:
            run = await database.get_analysis_run(run_id)
            if run is None:
                raise HTTPException(status_code=404, detail="analysis run not found")
            attributes = load_attributes(config.config)
            primary = primary_attribute_key(config.config, attributes)
            buckets = load_buckets(config.config)
            return await database.get_analysis_score_dashboard(
                run_id,
                primary_attribute=primary,
                buckets=[bucket.__dict__ for bucket in buckets],
            )
        finally:
            await database.close()

    @app.post("/api/v1/download", response_model=JobResponse)
    async def create_job(req: DownloadRequest) -> JobResponse:
        if not req.url:
            raise HTTPException(status_code=400, detail="url is required")
        job = await manager.submit(req.url)
        return JobResponse(job_id=job.job_id, status=job.status, url=job.url)

    @app.get("/api/v1/jobs/{job_id}")
    async def get_job(job_id: str) -> Dict[str, Any]:
        job = await manager.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job.to_dict()

    @app.get("/api/v1/jobs")
    async def list_jobs() -> Dict[str, List[Dict[str, Any]]]:
        jobs = await manager.list_jobs()
        return {"jobs": [j.to_dict() for j in jobs]}

    return app


def _settings_fields() -> Dict[str, str]:
    return {
        "path": "下载保存目录",
        "thread": "下载并发线程数",
        "rate_limit": "接口请求限速",
        "proxy": "网络代理地址",
        "retry_times": "下载重试次数",
        "analysis.active_run_id": "主流水线默认续跑的 run",
        "analysis.organize_run_id": "归类复制默认使用的 run",
        "analysis.batch_size": "每批送给模型的九宫格数量",
        "analysis.allow_partial_batch": "尾批不足时是否继续判分",
        "analysis.frame_count": "每个视频抽帧数量",
        "analysis.grid_rows": "九宫格行数",
        "analysis.grid_cols": "九宫格列数",
        "analysis.primary_attribute": "用于分桶/归类的主评分字段",
        "analysis.provider.model": "多模态模型名称",
        "analysis.provider.base_url": "OpenAI-compatible API Base URL",
        "analysis.provider.timeout": "模型请求超时秒数",
        "analysis.provider.rate_limit": "模型请求限速",
        "analysis.provider.retry_times": "模型判分重试次数",
        "analysis.provider.debug_stop_on_api_error": "API 报错时立即停止并输出诊断",
        "analysis.buckets": "CSV/评分展示分桶",
        "analysis.organize_buckets": "归类复制分桶",
    }


def _apply_settings_patch(config: ConfigLoader, incoming: Dict[str, Any]) -> None:
    top_level_allowed = {"path", "thread", "rate_limit", "proxy", "retry_times"}
    scalar_updates = {k: incoming[k] for k in top_level_allowed if k in incoming}
    if scalar_updates:
        config.update(**scalar_updates)

    if "analysis" not in incoming or not isinstance(incoming["analysis"], dict):
        return
    current_analysis = dict(config.get("analysis", {}) or {})
    incoming_analysis = incoming["analysis"]
    analysis_allowed = {
        "active_run_id",
        "organize_run_id",
        "batch_size",
        "allow_partial_batch",
        "frame_count",
        "grid_rows",
        "grid_cols",
        "primary_attribute",
        "buckets",
        "organize_buckets",
    }
    for key in analysis_allowed:
        if key in incoming_analysis:
            current_analysis[key] = incoming_analysis[key]

    if isinstance(incoming_analysis.get("provider"), dict):
        provider = dict(current_analysis.get("provider") or {})
        provider_allowed = {
            "type",
            "base_url",
            "model",
            "api_key_env",
            "timeout",
            "rate_limit",
            "retry_times",
            "debug_stop_on_api_error",
        }
        for key in provider_allowed:
            if key in incoming_analysis["provider"]:
                provider[key] = incoming_analysis["provider"][key]
        if isinstance(incoming_analysis["provider"].get("image_preprocess"), dict):
            image_preprocess = dict(provider.get("image_preprocess") or {})
            for key in {"enabled", "jpeg_quality", "optimize", "retry_scale_factor", "retry_jpeg_quality_factor"}:
                if key in incoming_analysis["provider"]["image_preprocess"]:
                    image_preprocess[key] = incoming_analysis["provider"]["image_preprocess"][key]
            provider["image_preprocess"] = image_preprocess
        current_analysis["provider"] = provider

    config.update(analysis=current_analysis)


async def run_server(config: ConfigLoader, *, host: str, port: int) -> None:
    import uvicorn

    app = build_app(config)
    uv_config = uvicorn.Config(app, host=host, port=port, log_level="info")
    server = uvicorn.Server(uv_config)
    await server.serve()
