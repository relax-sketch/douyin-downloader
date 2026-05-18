from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class AttributeDefinition:
    key: str
    label: str
    description: str
    min_score: int = 0
    max_score: int = 10


@dataclass(frozen=True)
class ScoreBucket:
    label: str
    min_score: int
    max_score: int


def analysis_config(raw_config: Dict[str, Any]) -> Dict[str, Any]:
    cfg = raw_config.get("analysis", {}) or {}
    if not isinstance(cfg, dict):
        raise ValueError("analysis config must be a mapping")
    return cfg


def load_attributes(raw_config: Dict[str, Any]) -> List[AttributeDefinition]:
    cfg = analysis_config(raw_config)
    raw_attributes = cfg.get("attributes") or []
    if not isinstance(raw_attributes, list) or not raw_attributes:
        raise ValueError("analysis.attributes must be a non-empty list")

    attributes: List[AttributeDefinition] = []
    seen = set()
    for index, raw in enumerate(raw_attributes):
        if not isinstance(raw, dict):
            raise ValueError(f"analysis.attributes[{index}] must be a mapping")
        key = str(raw.get("key") or "").strip()
        label = str(raw.get("label") or "").strip()
        description = str(raw.get("description") or "").strip()
        if not key or not label:
            raise ValueError(f"analysis.attributes[{index}] requires key and label")
        if key in seen:
            raise ValueError(f"duplicate analysis attribute key: {key}")
        seen.add(key)
        try:
            min_score = int(raw.get("min_score", 0))
            max_score = int(raw.get("max_score", 10))
        except (TypeError, ValueError):
            raise ValueError(f"analysis attribute {key} has invalid score bounds")
        if min_score > max_score:
            raise ValueError(f"analysis attribute {key} has min_score > max_score")
        attributes.append(
            AttributeDefinition(
                key=key,
                label=label,
                description=description,
                min_score=min_score,
                max_score=max_score,
            )
        )
    return attributes


def load_buckets(raw_config: Dict[str, Any]) -> List[ScoreBucket]:
    cfg = analysis_config(raw_config)
    raw_buckets = cfg.get("buckets") or []
    if not isinstance(raw_buckets, list) or not raw_buckets:
        raise ValueError("analysis.buckets must be a non-empty list")

    buckets: List[ScoreBucket] = []
    for index, raw in enumerate(raw_buckets):
        if not isinstance(raw, dict):
            raise ValueError(f"analysis.buckets[{index}] must be a mapping")
        label = str(raw.get("label") or "").strip()
        if not label:
            raise ValueError(f"analysis.buckets[{index}] requires label")
        try:
            min_score = int(raw["min_score"])
            max_score = int(raw["max_score"])
        except (KeyError, TypeError, ValueError):
            raise ValueError(f"analysis.buckets[{index}] has invalid score bounds")
        if min_score > max_score:
            raise ValueError(f"analysis.buckets[{index}] has min_score > max_score")
        buckets.append(ScoreBucket(label=label, min_score=min_score, max_score=max_score))
    return buckets


def load_organize_buckets(raw_config: Dict[str, Any]) -> List[ScoreBucket]:
    cfg = analysis_config(raw_config)
    if "organize_buckets" not in cfg:
        return load_buckets(raw_config)
    raw_buckets = cfg.get("organize_buckets") or []
    if not isinstance(raw_buckets, list) or not raw_buckets:
        raise ValueError("analysis.organize_buckets must be a non-empty list")

    buckets: List[ScoreBucket] = []
    for index, raw in enumerate(raw_buckets):
        if not isinstance(raw, dict):
            raise ValueError(f"analysis.organize_buckets[{index}] must be a mapping")
        label = str(raw.get("label") or "").strip()
        if not label:
            raise ValueError(f"analysis.organize_buckets[{index}] requires label")
        try:
            min_score = int(raw["min_score"])
            max_score = int(raw["max_score"])
        except (KeyError, TypeError, ValueError):
            raise ValueError(f"analysis.organize_buckets[{index}] has invalid score bounds")
        if min_score > max_score:
            raise ValueError(f"analysis.organize_buckets[{index}] has min_score > max_score")
        buckets.append(ScoreBucket(label=label, min_score=min_score, max_score=max_score))
    return buckets


def primary_attribute_key(raw_config: Dict[str, Any], attributes: List[AttributeDefinition]) -> str:
    cfg = analysis_config(raw_config)
    primary = str(cfg.get("primary_attribute") or "").strip()
    keys = {attribute.key for attribute in attributes}
    if primary not in keys:
        raise ValueError("analysis.primary_attribute must reference an existing attribute key")
    return primary


def bucket_for_score(score: int, buckets: List[ScoreBucket]) -> ScoreBucket:
    for bucket in buckets:
        if bucket.min_score <= int(score) <= bucket.max_score:
            return bucket
    raise ValueError(f"score {score} does not fit any configured bucket")


def bucket_for_score_or_none(score: int, buckets: List[ScoreBucket]) -> Optional[ScoreBucket]:
    for bucket in buckets:
        if bucket.min_score <= int(score) <= bucket.max_score:
            return bucket
    return None


def build_prompt(
    raw_config: Dict[str, Any],
    attributes: List[AttributeDefinition],
) -> str:
    cfg = analysis_config(raw_config)
    template = str(cfg.get("prompt_template") or "").strip()
    if not template:
        raise ValueError("analysis.prompt_template must not be empty")
    descriptions = "; ".join(
        f"{item.key}（{item.label}，{item.min_score}-{item.max_score}）：{item.description}"
        for item in attributes
    )
    return template.format(
        attribute_keys=", ".join(item.key for item in attributes),
        attribute_descriptions=descriptions,
    )


def render_batch_prompt(raw_config: Dict[str, Any], video_ids: List[str]) -> str:
    cfg = analysis_config(raw_config)
    prompt_file = str(cfg.get("prompt_file") or "").strip()
    if prompt_file:
        template = Path(prompt_file).read_text(encoding="utf-8")
    else:
        template = str(cfg.get("prompt_template") or "").strip()
    if not template:
        raise ValueError("analysis prompt template must not be empty")
    rendered = _adapt_prompt_batch_size(template, video_ids)
    for index, value in enumerate(video_ids, start=1):
        rendered = rendered.replace(f"{{video_id_{index}}}", value)
    return rendered


def _adapt_prompt_batch_size(template: str, video_ids: List[str]) -> str:
    """Rewrite the repository prompt's count/mapping/sample for arbitrary batch sizes.

    The user-facing prompt is intentionally kept editable as prose in a markdown
    file. This helper only normalizes the batch-shaped parts so a 10-item main
    batch and a shorter tail batch can reuse the same rubric safely.
    """
    count = len(video_ids)
    if count <= 0:
        raise ValueError("batch prompt requires at least one video id")

    mapping_heading = "图片与视频 ID 对应关系："
    output_heading = "输出要求："
    format_heading = "请严格按照以下格式返回："
    if not all(marker in template for marker in (mapping_heading, output_heading, format_heading)):
        return template

    prefix, rest = template.split(mapping_heading, 1)
    _old_mapping, rest = rest.split(output_heading, 1)
    rules, _old_sample = rest.split(format_heading, 1)

    for possible_count in range(1, 101):
        prefix = prefix.replace(f"你将看到 {possible_count} 张图片。", f"你将看到 {count} 张图片。")
        rules = rules.replace(f"数组长度必须为 {possible_count}。", f"数组长度必须为 {count}。")

    mapping = "\n".join(
        f'第 {index} 张图片：video_id = "{{video_id_{index}}}"'
        for index in range(1, count + 1)
    )
    sample = json.dumps(
        [
            {
                "video_id": f"{{video_id_{index}}}",
                "suggestiveness_score": 1,
                "coverage_score": 1,
            }
            for index in range(1, count + 1)
        ],
        ensure_ascii=False,
        indent=2,
    )
    return (
        f"{prefix}{mapping_heading}\n{mapping}\n\n"
        f"{output_heading}{rules}{format_heading}\n\n{sample}\n"
    )
