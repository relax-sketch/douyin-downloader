import asyncio
import json
import shutil
from pathlib import Path
from typing import List, Optional

from utils.logger import setup_logger

logger = setup_logger("AnalysisFrames")

OVERSIZED_GRID_THRESHOLD = 4_000
OVERSIZED_GRID_TARGET_MAX_SIDE = 4_000


class FFmpegFrameExtractor:
    def __init__(self, ffmpeg_path: Optional[str] = None, ffprobe_path: Optional[str] = None):
        self.ffmpeg_path = ffmpeg_path or shutil.which("ffmpeg")
        self.ffprobe_path = ffprobe_path or shutil.which("ffprobe")

    async def extract(self, video_path: Path, output_dir: Path, count: int = 9) -> List[Path]:
        if not self.ffmpeg_path:
            raise RuntimeError("ffmpeg executable not found")
        if not self.ffprobe_path:
            raise RuntimeError("ffprobe executable not found")
        video_path = Path(video_path)
        if not video_path.exists():
            raise FileNotFoundError(f"video file not found: {video_path}")

        duration = await self._probe_duration(video_path)
        if duration <= 0:
            raise RuntimeError(f"invalid video duration for {video_path}")

        output_dir.mkdir(parents=True, exist_ok=True)
        timestamps = [duration * (index + 1) / (count + 1) for index in range(count)]
        frames: List[Path] = []
        for index, timestamp in enumerate(timestamps, start=1):
            frame_path = output_dir / f"frame_{index:02d}.jpg"
            await self._extract_single_frame(video_path, timestamp, frame_path)
            frames.append(frame_path)
        return frames

    async def _probe_duration(self, video_path: Path) -> float:
        process = await asyncio.create_subprocess_exec(
            self.ffprobe_path,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "json",
            str(video_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise RuntimeError(stderr.decode("utf-8", errors="ignore").strip() or "ffprobe failed")
        payload = json.loads(stdout.decode("utf-8"))
        return float(payload["format"]["duration"])

    async def _extract_single_frame(
        self,
        video_path: Path,
        timestamp: float,
        frame_path: Path,
    ) -> None:
        process = await asyncio.create_subprocess_exec(
            self.ffmpeg_path,
            "-y",
            "-ss",
            f"{max(timestamp, 0.001):.3f}",
            "-i",
            str(video_path),
            "-frames:v",
            "1",
            "-q:v",
            "2",
            str(frame_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await process.communicate()
        if process.returncode != 0 or not frame_path.exists():
            detail = stderr.decode("utf-8", errors="ignore").strip()
            raise RuntimeError(detail or f"ffmpeg failed to extract frame at {timestamp:.3f}s")


class GridBuilder:
    def build(
        self,
        frame_paths: List[Path],
        output_path: Path,
        *,
        rows: int = 3,
        cols: int = 3,
    ) -> Path:
        if not frame_paths:
            raise ValueError("grid builder requires at least one frame path")
        if len(frame_paths) > rows * cols:
            raise ValueError("grid builder received more frames than available cells")
        from PIL import Image

        images = []
        try:
            for frame_path in frame_paths:
                images.append(Image.open(frame_path).convert("RGB"))
            width, height = images[0].size
            normalized = [
                image if image.size == (width, height) else image.resize((width, height))
                for image in images
            ]
            canvas = Image.new("RGB", (width * cols, height * rows), color=(255, 255, 255))
            for index, image in enumerate(normalized):
                x = (index % cols) * width
                y = (index // cols) * height
                canvas.paste(image, (x, y))
            canvas = _fit_if_oversized(canvas)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            canvas.save(output_path, format="JPEG", quality=95)
            return output_path
        finally:
            for image in images:
                image.close()


def _fit_if_oversized(image):
    from PIL import Image

    width, height = image.size
    max_side = max(width, height)
    if max_side <= OVERSIZED_GRID_THRESHOLD:
        return image
    scale = OVERSIZED_GRID_TARGET_MAX_SIDE / max_side
    resized = image.resize(
        (
            max(1, round(width * scale)),
            max(1, round(height * scale)),
        ),
        Image.Resampling.LANCZOS,
    )
    if resized is not image:
        image.close()
    return resized
