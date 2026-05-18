import json
from pathlib import Path
from typing import Any, Dict, List

from storage import Database
from utils.logger import setup_logger

logger = setup_logger("AnalysisDiscovery")


class CandidateDiscovery:
    def __init__(self, database: Database, downloads_root: Path):
        self.database = database
        self.downloads_root = Path(downloads_root)

    def manifest_line_count(self) -> int:
        manifest_path = self.downloads_root / "download_manifest.jsonl"
        if not manifest_path.exists():
            return 0
        with manifest_path.open("r", encoding="utf-8") as handle:
            return sum(1 for _ in handle)

    def from_manifest_delta(self, start_line: int) -> List[Dict[str, str]]:
        manifest_path = self.downloads_root / "download_manifest.jsonl"
        if not manifest_path.exists():
            return []

        items: List[Dict[str, str]] = []
        with manifest_path.open("r", encoding="utf-8") as handle:
            for index, line in enumerate(handle):
                if index < start_line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Skip invalid manifest line %s", index + 1)
                    continue
                candidate = self._manifest_payload_to_candidate(payload)
                if candidate is not None:
                    items.append(candidate)
        return items

    async def from_all_videos(self, *, limit: int = 0, exclude_analyzed: bool = True) -> List[Dict[str, str]]:
        rows = await self.database.list_video_awemes(limit=limit or None)
        already = await self.database.get_analyzed_aweme_ids() if exclude_analyzed else set()
        candidates: List[Dict[str, str]] = []
        for row in rows:
            aweme_id = str(row["aweme_id"])
            if aweme_id in already:
                continue
            save_dir = Path(str(row.get("file_path") or ""))
            video_path = self._find_video_path(save_dir)
            if video_path is None:
                logger.warning("No MP4 found for aweme %s in %s", aweme_id, save_dir)
                continue
            candidates.append(
                {
                    "aweme_id": aweme_id,
                    "author_name": str(row.get("author_name") or "unknown"),
                    "video_path": str(video_path),
                }
            )
        return candidates

    def _manifest_payload_to_candidate(self, payload: Dict[str, Any]) -> Dict[str, str]:
        if not isinstance(payload, dict) or payload.get("media_type") != "video":
            return None
        video_rel_path = None
        for raw_path in payload.get("file_paths") or []:
            if str(raw_path).lower().endswith(".mp4"):
                video_rel_path = str(raw_path)
                break
        if not video_rel_path:
            return None
        video_path = Path(video_rel_path)
        if not video_path.is_absolute():
            video_path = self.downloads_root / video_path
        return {
            "aweme_id": str(payload.get("aweme_id") or "").strip(),
            "author_name": str(payload.get("author_name") or "unknown"),
            "video_path": str(video_path),
        }

    @staticmethod
    def _find_video_path(save_dir: Path) -> Path:
        if not save_dir.exists():
            return None
        matches = sorted(save_dir.glob("*.mp4"))
        return matches[0] if matches else None
