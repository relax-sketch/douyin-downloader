import importlib
from types import SimpleNamespace

import pytest
import yaml

main_module = importlib.import_module("cli.main")


class _FakeCookieManager:
    def get_cookies(self):
        return {"msToken": "token-1"}


class _FakeAPIClient:
    def __init__(self, _cookies, proxy=None):
        self.proxy = proxy
        self.resolved_urls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def resolve_short_url(self, short_url: str):
        self.resolved_urls.append(short_url)
        return "https://www.douyin.com/video/7604129988555574538"


class _FakeDownloader:
    async def download(self, parsed):
        return SimpleNamespace(total=1, success=1, failed=0, skipped=0, parsed=parsed)


@pytest.mark.asyncio
async def test_download_url_resolves_short_link_before_parsing(monkeypatch, tmp_path):
    config = main_module.ConfigLoader()
    config.update(path=str(tmp_path))

    parsed_inputs = []

    def _fake_parse(url: str):
        parsed_inputs.append(url)
        return {"type": "video", "aweme_id": "7604129988555574538"}

    fake_downloader = _FakeDownloader()

    monkeypatch.setattr(main_module, "DouyinAPIClient", _FakeAPIClient)
    monkeypatch.setattr(main_module.URLParser, "parse", _fake_parse)
    monkeypatch.setattr(
        main_module.DownloaderFactory,
        "create",
        lambda *_args, **_kwargs: fake_downloader,
    )

    result = await main_module.download_url(
        "https://v.douyin.com/short-link/",
        config,
        _FakeCookieManager(),
        database=None,
        progress_reporter=None,
    )

    assert result is not None
    assert result.success == 1
    assert parsed_inputs == ["https://www.douyin.com/video/7604129988555574538"]


@pytest.mark.asyncio
async def test_download_url_passes_proxy_to_api_client(monkeypatch, tmp_path):
    config = main_module.ConfigLoader()
    config.update(path=str(tmp_path), proxy="http://127.0.0.1:8899")

    captured = {}

    class _ProxyAPIClient(_FakeAPIClient):
        def __init__(self, cookies, proxy=None):
            captured["cookies"] = cookies
            captured["proxy"] = proxy
            super().__init__(cookies, proxy=proxy)

    monkeypatch.setattr(main_module, "DouyinAPIClient", _ProxyAPIClient)
    monkeypatch.setattr(
        main_module.URLParser,
        "parse",
        lambda _url: {"type": "video", "aweme_id": "7604129988555574538"},
    )
    monkeypatch.setattr(
        main_module.DownloaderFactory,
        "create",
        lambda *_args, **_kwargs: _FakeDownloader(),
    )

    result = await main_module.download_url(
        "https://www.douyin.com/video/7604129988555574538",
        config,
        _FakeCookieManager(),
        database=None,
        progress_reporter=None,
    )

    assert result is not None
    assert result.success == 1
    assert captured["proxy"] == "http://127.0.0.1:8899"


def test_remember_organize_run_persists_to_yaml(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text("analysis:\n  organize_run_id: ''\n", encoding="utf-8")
    config = main_module.ConfigLoader(str(config_path))

    main_module._remember_organize_run(config, "run-123")

    written = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert written["analysis"]["organize_run_id"] == "run-123"


def test_remember_active_run_persists_to_yaml(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text("analysis:\n  active_run_id: ''\n", encoding="utf-8")
    config = main_module.ConfigLoader(str(config_path))

    main_module._remember_active_run(config, "run-456")

    written = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert written["analysis"]["active_run_id"] == "run-456"


@pytest.mark.asyncio
async def test_resolve_active_run_prefers_yaml_anchor_over_more_advanced_run(tmp_path):
    database = main_module.Database(str(tmp_path / "test.db"))
    await database.initialize()
    await database.create_analysis_run(
        run_id="active-low-progress",
        source_type="all",
        source_payload={"scope": "all"},
    )
    await database.add_analysis_items(
        "active-low-progress",
        [{"aweme_id": "1", "author_name": "Author", "video_path": "/tmp/1.mp4"}],
    )
    await database.create_analysis_run(
        run_id="fallback-high-progress",
        source_type="all",
        source_payload={"scope": "all"},
    )
    await database.add_analysis_items(
        "fallback-high-progress",
        [{"aweme_id": "2", "author_name": "Author", "video_path": "/tmp/2.mp4"}],
    )
    await database.update_analysis_item_stage(
        "fallback-high-progress",
        "2",
        stage="classify",
        status="success",
    )
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        "analysis:\n  active_run_id: active-low-progress\n",
        encoding="utf-8",
    )
    config = main_module.ConfigLoader(str(config_path))

    run = await main_module._resolve_active_or_best_unfinished_run(database, config)

    assert run["run_id"] == "active-low-progress"
    await database.close()


@pytest.mark.asyncio
async def test_resolve_active_run_falls_back_to_best_unfinished_and_remembers_it(tmp_path):
    database = main_module.Database(str(tmp_path / "test.db"))
    await database.initialize()
    await database.create_analysis_run(
        run_id="new-empty",
        source_type="all",
        source_payload={"scope": "all"},
    )
    await database.add_analysis_items(
        "new-empty",
        [{"aweme_id": "1", "author_name": "Author", "video_path": "/tmp/1.mp4"}],
    )
    await database.create_analysis_run(
        run_id="best-progress",
        source_type="all",
        source_payload={"scope": "all"},
    )
    await database.add_analysis_items(
        "best-progress",
        [{"aweme_id": "2", "author_name": "Author", "video_path": "/tmp/2.mp4"}],
    )
    await database.update_analysis_item_stage(
        "best-progress",
        "2",
        stage="classify",
        status="success",
    )
    config_path = tmp_path / "config.yml"
    config_path.write_text("analysis:\n  active_run_id: ''\n", encoding="utf-8")
    config = main_module.ConfigLoader(str(config_path))

    run = await main_module._resolve_active_or_best_unfinished_run(database, config)

    assert run["run_id"] == "best-progress"
    written = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert written["analysis"]["active_run_id"] == "best-progress"
    await database.close()
