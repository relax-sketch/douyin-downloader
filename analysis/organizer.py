import asyncio
import shutil
from pathlib import Path
from typing import Callable, Dict, List, Optional

from analysis.config import AttributeDefinition, ScoreBucket, bucket_for_score
from storage import Database
from utils.logger import setup_logger
from utils.validators import sanitize_filename

logger = setup_logger("AnalysisOrganizer")


class ClassifiedOrganizer:
    async def organize(
        self,
        *,
        database: Database,
        run_id: str,
        classified_dir: Path,
        attributes: List[AttributeDefinition],
        primary_attribute: str,
        buckets: List[ScoreBucket],
        progress_callback: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        label_by_key: Dict[str, str] = {attribute.key: attribute.label for attribute in attributes}
        rows = await database.get_analysis_export_rows(run_id)
        for row in rows:
            try:
                if row.get("organize_status") in {"success", "skipped"}:
                    continue
                scores = row.get("scores", {})
                if primary_attribute not in scores:
                    await database.update_analysis_item_stage(
                        run_id,
                        row["aweme_id"],
                        stage="organize",
                        status="skipped",
                    )
                    if progress_callback:
                        progress_callback("skipped", str(row["aweme_id"]))
                    continue
                bucket = bucket_for_score(scores[primary_attribute], buckets)
                source_path = Path(row["video_path"])
                author_dir = sanitize_filename(row.get("author_name") or "unknown")
                primary_label = sanitize_filename(label_by_key[primary_attribute])
                target_dir = Path(classified_dir) / author_dir / f"{primary_label}_{bucket.label}"
                target_dir.mkdir(parents=True, exist_ok=True)
                target_path = target_dir / source_path.name
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, shutil.copy2, source_path, target_path)
                await database.update_analysis_item_stage(
                    run_id,
                    row["aweme_id"],
                    stage="organize",
                    status="success",
                    extra_updates={"organized_path": str(target_path)},
                )
                if progress_callback:
                    progress_callback("success", str(row["aweme_id"]))
            except Exception as exc:
                logger.warning("Organize failed for %s: %s", row["aweme_id"], exc)
                await database.update_analysis_item_stage(
                    run_id,
                    row["aweme_id"],
                    stage="organize",
                    status="failed",
                    error_message=str(exc),
                )
                if progress_callback:
                    progress_callback("failed", str(row["aweme_id"]))
