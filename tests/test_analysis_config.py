import pytest

from analysis.config import (
    bucket_for_score,
    build_prompt,
    load_attributes,
    load_buckets,
    load_organize_buckets,
    primary_attribute_key,
    render_batch_prompt,
)
from config import ConfigLoader


def test_analysis_config_defaults_are_valid():
    config = ConfigLoader()
    attributes = load_attributes(config.config)
    buckets = load_buckets(config.config)

    assert [item.key for item in attributes] == [
        "artsy",
        "male_oriented",
        "female_oriented",
        "sexual_suggestiveness",
    ]
    assert primary_attribute_key(config.config, attributes) == "artsy"
    assert bucket_for_score(2, buckets).label == "低"
    assert bucket_for_score(5, buckets).label == "中"
    assert bucket_for_score(9, buckets).label == "高"
    assert "artsy" in build_prompt(config.config, attributes)


def test_analysis_config_rejects_duplicate_attribute_keys():
    config = ConfigLoader()
    config.update(
        analysis={
            **config.get("analysis"),
            "attributes": [
                {
                    "key": "dup",
                    "label": "A",
                    "description": "first",
                    "min_score": 0,
                    "max_score": 10,
                },
                {
                    "key": "dup",
                    "label": "B",
                    "description": "second",
                    "min_score": 0,
                    "max_score": 10,
                },
            ],
            "primary_attribute": "dup",
        }
    )

    with pytest.raises(ValueError, match="duplicate analysis attribute key"):
        load_attributes(config.config)


def test_organize_buckets_can_filter_scores_independently_from_export_buckets():
    config = ConfigLoader()
    config.update(
        analysis={
            **config.get("analysis"),
            "organize_buckets": [
                {"label": "4", "min_score": 4, "max_score": 4},
                {"label": "7+", "min_score": 7, "max_score": 10},
            ],
        }
    )

    organize_buckets = load_organize_buckets(config.config)

    assert [item.label for item in organize_buckets] == ["4", "7+"]


def test_render_batch_prompt_adapts_prompt_file_to_requested_batch_size(tmp_path):
    prompt_file = tmp_path / "prompt.md"
    prompt_file.write_text(
        """你将看到 5 张图片。

图片与视频 ID 对应关系：
第 1 张图片：video_id = "{video_id_1}"
第 2 张图片：video_id = "{video_id_2}"
第 3 张图片：video_id = "{video_id_3}"
第 4 张图片：video_id = "{video_id_4}"
第 5 张图片：video_id = "{video_id_5}"

输出要求：
2. 数组长度必须为 5。

请严格按照以下格式返回：
[]
""",
        encoding="utf-8",
    )
    config = ConfigLoader()
    config.update(analysis={**config.get("analysis"), "prompt_file": str(prompt_file)})

    rendered = render_batch_prompt(config.config, ["a", "b", "c"])

    assert "你将看到 3 张图片。" in rendered
    assert "数组长度必须为 3。" in rendered
    assert '"video_id": "a"' in rendered
    assert '"video_id": "c"' in rendered
