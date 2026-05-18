from typing import Any, Dict

DEFAULT_CONFIG: Dict[str, Any] = {
    "path": "./Downloaded/",
    "music": True,
    "cover": True,
    "avatar": True,
    "json": True,
    "start_time": "",
    "end_time": "",
    "folderstyle": True,
    # 命名模板：渲染时可用变量见 utils/naming.py:ALLOWED_VARIABLES。默认保持
    # 与历史行为一致（`{date}_{title}_{id}`），用户可在设置中改写。
    "filename_template": "{date}_{title}_{id}",
    "folder_template": "{date}_{title}_{id}",
    # 作者目录层命名方式：
    #   "nickname"    - 作者昵称（默认，最直观，但重名会合并、改名会分裂）
    #   "sec_uid"     - 作者 sec_uid（稳定唯一，但不直观）
    #   "nickname_uid" - 昵称_sec_uid（直观 + 唯一）
    # 切换只影响后续下载，不会迁移已存在的目录。
    "author_dir": "nickname",
    "download_pinned": False,
    "mode": ["post"],
    "number": {
        "post": 0,
        "like": 0,
        "allmix": 0,
        "mix": 0,
        "music": 0,
        "collect": 0,
        "collectmix": 0,
    },
    "increase": {
        "post": False,
        "like": False,
        "allmix": False,
        "mix": False,
        "music": False,
    },
    "thread": 5,
    "retry_times": 3,
    "rate_limit": 2,
    "proxy": "",
    "database": True,
    "database_path": "dy_downloader.db",
    "progress": {
        "quiet_logs": True,
    },
    "transcript": {
        "enabled": False,
        "model": "gpt-4o-mini-transcribe",
        "output_dir": "",
        "response_formats": ["txt", "json"],
        "api_url": "https://api.openai.com/v1/audio/transcriptions",
        "api_key_env": "OPENAI_API_KEY",
        "api_key": "",
    },
    "analysis": {
        "enabled": False,
        "output_dir": "./Analysis/",
        "classified_dir": "./Classified/",
        "prompt_file": "",
        "batch_size": 1,
        "allow_partial_batch": False,
        "frame_count": 9,
        "grid_rows": 3,
        "grid_cols": 3,
        "primary_attribute": "artsy",
        "attributes": [
            {
                "key": "artsy",
                "label": "文艺范",
                "description": "画面、构图、色彩与整体气质是否偏文艺审美",
                "min_score": 0,
                "max_score": 10,
            },
            {
                "key": "male_oriented",
                "label": "男性向",
                "description": "内容是否更偏向吸引男性受众",
                "min_score": 0,
                "max_score": 10,
            },
            {
                "key": "female_oriented",
                "label": "女性向",
                "description": "内容是否更偏向吸引女性受众",
                "min_score": 0,
                "max_score": 10,
            },
            {
                "key": "sexual_suggestiveness",
                "label": "性暗示程度",
                "description": "画面是否包含明显的性暗示表达",
                "min_score": 0,
                "max_score": 10,
            },
        ],
        "prompt_template": (
            "请根据这张由同一条视频抽取的 3x3 九宫格图片，为下列属性分别打分。"
            "每个分数都必须是整数，范围为对应属性定义的 min_score 到 max_score。"
            "仅返回 JSON 对象，不要返回解释、Markdown 或额外文本。"
            "属性定义：{attribute_descriptions}"
        ),
        "provider": {
            "type": "openai_compatible",
            "base_url": "https://api.openai.com/v1",
            "model": "gpt-4.1-mini",
            "api_key_env": "OPENAI_API_KEY",
            "api_key": "",
            "timeout": 120,
            "rate_limit": 1,
            "retry_times": 3,
            "debug_stop_on_api_error": False,
            "image_preprocess": {
                "enabled": True,
                "jpeg_quality": 90,
                "optimize": True,
                "retry_scale_factor": 0.6,
                "retry_jpeg_quality_factor": 0.9,
            },
        },
        "buckets": [
            {"label": "低", "min_score": 0, "max_score": 3},
            {"label": "中", "min_score": 4, "max_score": 7},
            {"label": "高", "min_score": 8, "max_score": 10},
        ],
    },
    "auto_cookie": False,
    "browser_fallback": {
        "enabled": True,
        "headless": False,
        "max_scrolls": 240,
        "idle_rounds": 8,
        "wait_timeout_seconds": 600,
    },
    # 下载完成通知（可选）。providers 支持 bark / telegram / webhook。
    "notifications": {
        "enabled": False,
        "on_success": True,
        "on_failure": True,
        "providers": [],
    },
    # 评论采集（可选）。启用后每个作品会额外生成 *_comments.json。
    "comments": {
        "enabled": False,
        "include_replies": False,
        "max_comments": 0,  # 0 = 不限
        "page_size": 20,
    },
    # 直播录制（可选）。由 live.douyin.com / /follow/live/ 链接触发。
    "live": {
        "max_duration_seconds": 0,  # 0 = 直到流结束
        "chunk_size": 65536,
        "idle_timeout_seconds": 30,
    },
    # REST API 服务模式（可选，需 fastapi + uvicorn）。
    "server": {
        "max_jobs": 500,  # 内存中保留的 job 条数上限（不含 in-flight）
        "job_ttl_seconds": 86400,  # 完成态 job 保留时间（秒）
    },
}
