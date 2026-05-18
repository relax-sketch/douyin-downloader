import csv
from pathlib import Path
from typing import List

from analysis.config import AttributeDefinition, ScoreBucket, bucket_for_score
from storage import Database


class CsvExporter:
    async def export(
        self,
        *,
        database: Database,
        run_id: str,
        output_dir: Path,
        attributes: List[AttributeDefinition],
        primary_attribute: str,
        buckets: List[ScoreBucket],
    ) -> Path:
        rows = await database.get_analysis_export_rows(run_id)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / f"{run_id}.csv"
        fieldnames = [
            "run_id",
            "aweme_id",
            "author_name",
            "video_path",
            "grid_path",
            *[attribute.key for attribute in attributes],
            "primary_bucket",
        ]
        with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                scores = row.get("scores", {})
                primary_score = scores.get(primary_attribute)
                bucket = bucket_for_score(primary_score, buckets) if primary_score is not None else None
                writer.writerow(
                    {
                        "run_id": run_id,
                        "aweme_id": row["aweme_id"],
                        "author_name": row["author_name"],
                        "video_path": row["video_path"],
                        "grid_path": row["grid_path"],
                        **{attribute.key: scores.get(attribute.key, "") for attribute in attributes},
                        "primary_bucket": bucket.label if bucket else "",
                    }
                )
        return csv_path
