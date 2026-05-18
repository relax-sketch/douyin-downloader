import pytest

from analysis.config import AttributeDefinition
from analysis.provider import (
    OpenAICompatibleVisionProvider,
    ProviderResponseError,
    response_text_from_error,
)


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


def test_response_text_from_error_returns_body_or_empty_string():
    assert response_text_from_error(
        ProviderResponseError("boom", status=502, response_text='{"error":"bad gateway"}')
    ) == '{"error":"bad gateway"}'
    assert response_text_from_error(RuntimeError("local failure")) == ""


def test_provider_describe_images_reports_sizes_and_resolution(tmp_path):
    from PIL import Image

    image_path = tmp_path / "grid.jpg"
    Image.new("RGB", (64, 32), color=(10, 20, 30)).save(image_path)
    provider = OpenAICompatibleVisionProvider(
        base_url="https://example.test/v1",
        model="demo",
    )

    diagnostics = provider.describe_images([image_path])

    assert diagnostics[0]["file_bytes"] > 0
    assert diagnostics[0]["payload_bytes"] > 0
    assert diagnostics[0]["width"] == 64
    assert diagnostics[0]["height"] == 32


def test_provider_preprocess_fits_legacy_oversized_grid_before_upload(tmp_path):
    from PIL import Image

    image_path = tmp_path / "legacy_large_grid.jpg"
    Image.new("RGB", (8000, 400), color=(10, 20, 30)).save(image_path)
    provider = OpenAICompatibleVisionProvider(
        base_url="https://example.test/v1",
        model="demo",
    )

    payload = provider._image_payload_bytes(image_path)

    from io import BytesIO

    with Image.open(BytesIO(payload)) as image:
        assert image.size == (4000, 200)


def test_provider_first_retry_scales_to_sixty_percent_and_reduces_quality(tmp_path):
    from io import BytesIO
    from PIL import Image

    image_path = tmp_path / "grid.jpg"
    Image.new("RGB", (1000, 500), color=(10, 20, 30)).save(image_path)
    provider = OpenAICompatibleVisionProvider(
        base_url="https://example.test/v1",
        model="demo",
        preprocess_jpeg_quality=90,
        retry_scale_factor=0.6,
        retry_jpeg_quality_factor=0.9,
    )

    scale_factor, quality = provider._retry_transform(1)
    payload = provider._image_payload_bytes(image_path, compress_level=1)

    assert scale_factor == 0.6
    assert quality == 81
    with Image.open(BytesIO(payload)) as image:
        assert image.size == (600, 300)
