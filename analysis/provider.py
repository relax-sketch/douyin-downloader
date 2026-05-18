import base64
import io
import json
import os
from pathlib import Path
from typing import Dict, List

import aiohttp
from PIL import Image

from analysis.config import AttributeDefinition
from analysis.frames import _fit_if_oversized
from control import RateLimiter, RetryHandler
from utils.logger import setup_logger

logger = setup_logger("AnalysisProvider")


class ProviderResponseError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        response_text: str = "",
        request_bytes: int = 0,
    ):
        super().__init__(message)
        self.status = status
        self.response_text = response_text or ""
        self.request_bytes = int(request_bytes or 0)


def response_text_from_error(exc: Exception) -> str:
    return str(getattr(exc, "response_text", "") or "")


class VisionProvider:
    async def score(
        self,
        image_path: Path,
        attributes: List[AttributeDefinition],
        prompt: str,
    ) -> Dict[str, int]:
        raise NotImplementedError

    async def batch_score(
        self,
        image_paths: List[Path],
        video_ids: List[str],
        prompt: str,
    ) -> List[Dict[str, object]]:
        raise NotImplementedError


class OpenAICompatibleVisionProvider(VisionProvider):
    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        api_key_env: str = "OPENAI_API_KEY",
        api_key: str = "",
        timeout: int = 120,
        rate_limit: float = 1,
        retry_times: int = 3,
        retry_max_total_seconds: float = 600,
        debug_stop_on_api_error: bool = False,
        preprocess_enabled: bool = True,
        preprocess_jpeg_quality: int = 90,
        preprocess_optimize: bool = True,
        retry_scale_factor: float = 0.6,
        retry_jpeg_quality_factor: float = 0.9,
    ):
        self.base_url = str(base_url or "").rstrip("/")
        self.model = str(model or "").strip()
        self.api_key_env = str(api_key_env or "").strip()
        self.api_key = str(api_key or "").strip()
        self.timeout = int(timeout or 120)
        self.rate_limiter = RateLimiter(max_per_second=float(rate_limit or 1))
        self.retry_handler = RetryHandler(
            max_retries=int(retry_times or 0),
            max_total_seconds=float(retry_max_total_seconds) if retry_max_total_seconds else None,
        )
        self.debug_stop_on_api_error = bool(debug_stop_on_api_error)
        self.preprocess_enabled = bool(preprocess_enabled)
        self.preprocess_jpeg_quality = int(preprocess_jpeg_quality)
        self.preprocess_optimize = bool(preprocess_optimize)
        self.retry_scale_factor = float(retry_scale_factor or 0.6)
        self.retry_jpeg_quality_factor = float(retry_jpeg_quality_factor or 0.9)

    async def score(
        self,
        image_path: Path,
        attributes: List[AttributeDefinition],
        prompt: str,
    ) -> Dict[str, int]:
        async def _task():
            await self.rate_limiter.acquire()
            return await self._score_once(image_path, attributes, prompt)

        if self.debug_stop_on_api_error:
            return await _task()
        return await self.retry_handler.execute_with_retry(_task)

    async def batch_score(
        self,
        image_paths: List[Path],
        video_ids: List[str],
        prompt: str,
    ) -> List[Dict[str, object]]:
        async def _task():
            await self.rate_limiter.acquire()
            return await self._batch_score_once(image_paths, video_ids, prompt)

        if self.debug_stop_on_api_error:
            return await _task()
        return await self.retry_handler.execute_with_retry(_task)

    async def _score_once(
        self,
        image_path: Path,
        attributes: List[AttributeDefinition],
        prompt: str,
    ) -> Dict[str, int]:
        if not self.base_url:
            raise ValueError("analysis.provider.base_url must not be empty")
        if not self.model:
            raise ValueError("analysis.provider.model must not be empty")
        endpoint = (
            self.base_url
            if self.base_url.endswith("/chat/completions")
            else f"{self.base_url}/chat/completions"
        )
        data_url = self._to_data_url(image_path)
        request_payload = {
            "model": self.model,
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ],
        }
        request_bytes = self._request_json_bytes(request_payload)

        headers = {"Content-Type": "application/json"}
        token = self._resolve_api_key()
        if token:
            headers["Authorization"] = f"Bearer {token}"

        timeout = aiohttp.ClientTimeout(total=self.timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(endpoint, json=request_payload, headers=headers) as response:
                body = await response.text()
                if response.status != 200:
                    raise ProviderResponseError(
                        f"vision provider failed: status={response.status}",
                        status=response.status,
                        response_text=body,
                        request_bytes=request_bytes,
                    )
                try:
                    payload = json.loads(body)
                    content = payload["choices"][0]["message"]["content"]
                    raw_scores = json.loads(content) if isinstance(content, str) else content
                    return self._validate_scores(raw_scores, attributes)
                except Exception as exc:
                    raise ProviderResponseError(
                        f"vision provider returned invalid response: {exc}",
                        status=response.status,
                        response_text=body,
                        request_bytes=request_bytes,
                    ) from exc

    async def _batch_score_once(
        self,
        image_paths: List[Path],
        video_ids: List[str],
        prompt: str,
        compress_level: int = 0,
    ) -> List[Dict[str, object]]:
        if not image_paths:
            raise ValueError("batch_score requires at least one image")
        if len(image_paths) != len(video_ids):
            raise ValueError("batch_score requires matching image_paths and video_ids lengths")
        if not self.base_url:
            raise ValueError("analysis.provider.base_url must not be empty")
        if not self.model:
            raise ValueError("analysis.provider.model must not be empty")

        endpoint = (
            self.base_url
            if self.base_url.endswith("/chat/completions")
            else f"{self.base_url}/chat/completions"
        )
        content = [{"type": "text", "text": prompt}]
        content.extend(
            {"type": "image_url", "image_url": {"url": self._to_data_url(path, compress_level)}}
            for path in image_paths
        )
        request_payload = {
            "model": self.model,
            "temperature": 0,
            "messages": [{"role": "user", "content": content}],
        }
        request_bytes = self._request_json_bytes(request_payload)

        headers = {"Content-Type": "application/json"}
        token = self._resolve_api_key()
        if token:
            headers["Authorization"] = f"Bearer {token}"

        timeout = aiohttp.ClientTimeout(total=self.timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(endpoint, json=request_payload, headers=headers) as response:
                body = await response.text()
                if response.status != 200:
                    raise ProviderResponseError(
                        f"vision provider failed: status={response.status}",
                        status=response.status,
                        response_text=body,
                        request_bytes=request_bytes,
                    )
                try:
                    payload = json.loads(body)
                    raw_content = payload["choices"][0]["message"]["content"]
                    raw_results = json.loads(raw_content) if isinstance(raw_content, str) else raw_content
                    return self._validate_batch_results(raw_results, video_ids)
                except Exception as exc:
                    raise ProviderResponseError(
                        f"vision provider returned invalid response: {exc}",
                        status=response.status,
                        response_text=body,
                        request_bytes=request_bytes,
                    ) from exc

    def _resolve_api_key(self) -> str:
        if self.api_key_env:
            env_value = os.getenv(self.api_key_env, "").strip()
            if env_value:
                return env_value
        return self.api_key

    def describe_images(self, image_paths: List[Path], compress_level: int = 0) -> List[Dict[str, object]]:
        diagnostics: List[Dict[str, object]] = []
        for image_path in image_paths:
            path = Path(image_path)
            file_bytes = path.stat().st_size if path.exists() else 0
            width = None
            height = None
            try:
                with Image.open(path) as image:
                    width, height = image.size
            except Exception:
                pass
            payload_bytes = len(self._image_payload_bytes(path, compress_level)) if path.exists() else 0
            diagnostics.append(
                {
                    "path": str(path),
                    "file_bytes": file_bytes,
                    "payload_bytes": payload_bytes,
                    "width": width,
                    "height": height,
                }
            )
        return diagnostics

    def _image_payload_bytes(self, image_path: Path, compress_level: int = 0) -> bytes:
        if not Path(image_path).exists():
            raise FileNotFoundError(f"grid image not found: {image_path}")
        if self.preprocess_enabled:
            with Image.open(image_path) as source:
                img = source.copy()
            img = _fit_if_oversized(img)
            scale_factor, quality = self._retry_transform(compress_level)
            if scale_factor != 1:
                img = img.resize(
                    (
                        max(1, round(img.width * scale_factor)),
                        max(1, round(img.height * scale_factor)),
                    ),
                    Image.Resampling.LANCZOS,
                )
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=quality, optimize=self.preprocess_optimize)
            return buf.getvalue()
        else:
            return Path(image_path).read_bytes()

    def _retry_transform(self, compress_level: int) -> tuple[float, int]:
        if compress_level <= 0:
            return 1.0, self.preprocess_jpeg_quality
        scale_factor = self.retry_scale_factor
        quality = max(1, min(95, round(self.preprocess_jpeg_quality * self.retry_jpeg_quality_factor)))
        return scale_factor, quality

    def _to_data_url(self, image_path: Path, compress_level: int = 0) -> str:
        encoded = base64.b64encode(self._image_payload_bytes(image_path, compress_level)).decode("ascii")
        return f"data:image/jpeg;base64,{encoded}"

    @staticmethod
    def _request_json_bytes(payload: Dict[str, object]) -> int:
        return len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))

    @staticmethod
    def _validate_scores(raw_scores, attributes: List[AttributeDefinition]) -> Dict[str, int]:
        if not isinstance(raw_scores, dict):
            raise ValueError("vision provider must return a JSON object")
        normalized: Dict[str, int] = {}
        for attribute in attributes:
            if attribute.key not in raw_scores:
                raise ValueError(f"missing score for attribute: {attribute.key}")
            try:
                score = int(raw_scores[attribute.key])
            except (TypeError, ValueError):
                raise ValueError(f"invalid score for attribute: {attribute.key}")
            if score < attribute.min_score or score > attribute.max_score:
                raise ValueError(f"score out of range for attribute: {attribute.key}")
            normalized[attribute.key] = score
        return normalized

    @staticmethod
    def _validate_batch_results(
        raw_results,
        expected_video_ids: List[str],
    ) -> List[Dict[str, object]]:
        if not isinstance(raw_results, list):
            raise ValueError("vision provider must return a JSON array in batch mode")
        if len(raw_results) != len(expected_video_ids):
            raise ValueError("vision provider returned unexpected batch length")

        normalized: List[Dict[str, object]] = []
        for index, (item, expected_video_id) in enumerate(zip(raw_results, expected_video_ids)):
            if not isinstance(item, dict):
                raise ValueError(f"batch result {index} must be an object")
            if str(item.get("video_id")) != str(expected_video_id):
                raise ValueError(f"batch result {index} video_id mismatch")
            suggestiveness = item.get("suggestiveness_score")
            coverage = item.get("coverage_score")
            try:
                suggestiveness = int(suggestiveness)
                coverage = int(coverage)
            except (TypeError, ValueError):
                raise ValueError(f"batch result {index} has invalid scores")
            if not (1 <= suggestiveness <= 10 and 1 <= coverage <= 10):
                raise ValueError(f"batch result {index} score out of range")
            normalized.append(
                {
                    "video_id": str(expected_video_id),
                    "suggestiveness_score": suggestiveness,
                    "coverage_score": coverage,
                }
            )
        return normalized


def build_provider(provider_config: Dict[str, object]) -> VisionProvider:
    provider_type = str(provider_config.get("type") or "openai_compatible").strip()
    if provider_type != "openai_compatible":
        raise ValueError(f"unsupported analysis provider type: {provider_type}")
    preprocess = provider_config.get("image_preprocess") or {}
    return OpenAICompatibleVisionProvider(
        base_url=str(provider_config.get("base_url") or ""),
        model=str(provider_config.get("model") or ""),
        api_key_env=str(provider_config.get("api_key_env") or ""),
        api_key=str(provider_config.get("api_key") or ""),
        timeout=int(provider_config.get("timeout") or 120),
        rate_limit=float(provider_config.get("rate_limit") or 1),
        retry_times=int(provider_config.get("retry_times") or 0),
        retry_max_total_seconds=float(provider_config.get("retry_max_total_seconds") or 0) or None,
        debug_stop_on_api_error=bool(provider_config.get("debug_stop_on_api_error", False)),
        preprocess_enabled=bool(preprocess.get("enabled") if isinstance(preprocess, dict) else True),
        preprocess_jpeg_quality=int(preprocess.get("jpeg_quality", 90) if isinstance(preprocess, dict) else 90),
        preprocess_optimize=bool(preprocess.get("optimize", True) if isinstance(preprocess, dict) else True),
        retry_scale_factor=float(preprocess.get("retry_scale_factor", 0.6) if isinstance(preprocess, dict) else 0.6),
        retry_jpeg_quality_factor=float(
            preprocess.get("retry_jpeg_quality_factor", 0.9) if isinstance(preprocess, dict) else 0.9
        ),
    )
