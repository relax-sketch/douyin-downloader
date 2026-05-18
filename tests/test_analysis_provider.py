import pytest

from analysis.config import AttributeDefinition
from analysis.provider import OpenAICompatibleVisionProvider


def test_validate_scores_normalizes_valid_payload():
    attributes = [
        AttributeDefinition("artsy", "文艺范", "desc"),
        AttributeDefinition("sexual", "性暗示", "desc"),
    ]

    scores = OpenAICompatibleVisionProvider._validate_scores(
        {"artsy": "8", "sexual": 3},
        attributes,
    )

    assert scores == {"artsy": 8, "sexual": 3}


def test_validate_scores_rejects_missing_attribute():
    attributes = [AttributeDefinition("artsy", "文艺范", "desc")]

    with pytest.raises(ValueError, match="missing score"):
        OpenAICompatibleVisionProvider._validate_scores({}, attributes)


def test_validate_scores_rejects_out_of_range_value():
    attributes = [AttributeDefinition("artsy", "文艺范", "desc")]

    with pytest.raises(ValueError, match="out of range"):
        OpenAICompatibleVisionProvider._validate_scores({"artsy": 99}, attributes)


def test_validate_batch_results_accepts_prompt_contract():
    results = OpenAICompatibleVisionProvider._validate_batch_results(
        [
            {
                "video_id": "1",
                "suggestiveness_score": 7,
                "coverage_score": 6,
            },
            {
                "video_id": "2",
                "suggestiveness_score": 3,
                "coverage_score": 4,
            },
        ],
        ["1", "2"],
    )

    assert results[0]["suggestiveness_score"] == 7
    assert results[1]["coverage_score"] == 4
