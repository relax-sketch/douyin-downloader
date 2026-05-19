import asyncio
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiosqlite


class Database:
    def __init__(self, db_path: str = "dy_downloader.db"):
        self.db_path = db_path
        self._initialized = False
        self._conn: Optional[aiosqlite.Connection] = None
        # 延迟到首次 _get_conn 调用时在当前 event loop 上创建 Lock，
        # 避免在 __init__ 阶段抢到错误的 loop。
        self._conn_lock: Optional[asyncio.Lock] = None

    async def _get_conn(self) -> aiosqlite.Connection:
        if self._conn is not None:
            return self._conn
        if self._conn_lock is None:
            self._conn_lock = asyncio.Lock()
        async with self._conn_lock:
            if self._conn is None:
                self._conn = await aiosqlite.connect(self.db_path)
        return self._conn

    async def initialize(self):
        if self._initialized:
            return

        db = await self._get_conn()

        # WAL gives concurrent reader/writer; NORMAL avoids fsync on every commit
        # (loses at most last few txns on power loss — acceptable for download history).
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS aweme (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                aweme_id TEXT UNIQUE NOT NULL,
                aweme_type TEXT NOT NULL,
                title TEXT,
                author_id TEXT,
                author_name TEXT,
                create_time INTEGER,
                download_time INTEGER,
                file_path TEXT,
                metadata TEXT
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS download_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                url_type TEXT NOT NULL,
                download_time INTEGER,
                total_count INTEGER,
                success_count INTEGER,
                config TEXT
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS transcript_job (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                aweme_id TEXT NOT NULL,
                video_path TEXT NOT NULL,
                transcript_dir TEXT,
                text_path TEXT,
                json_path TEXT,
                model TEXT NOT NULL,
                status TEXT NOT NULL,
                skip_reason TEXT,
                error_message TEXT,
                created_at INTEGER,
                updated_at INTEGER,
                UNIQUE(aweme_id, video_path, model)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS analysis_run (
                run_id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                source_payload TEXT,
                status TEXT NOT NULL,
                csv_path TEXT,
                created_at INTEGER,
                updated_at INTEGER
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS analysis_item (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                aweme_id TEXT NOT NULL,
                author_name TEXT,
                video_path TEXT NOT NULL,
                grid_path TEXT,
                organized_path TEXT,
                frames_status TEXT NOT NULL DEFAULT 'pending',
                classify_status TEXT NOT NULL DEFAULT 'pending',
                export_status TEXT NOT NULL DEFAULT 'pending',
                organize_status TEXT NOT NULL DEFAULT 'pending',
                error_stage TEXT,
                error_message TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER,
                updated_at INTEGER,
                UNIQUE(run_id, aweme_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS analysis_score (
                run_id TEXT NOT NULL,
                aweme_id TEXT NOT NULL,
                attribute_key TEXT NOT NULL,
                score INTEGER NOT NULL,
                PRIMARY KEY (run_id, aweme_id, attribute_key)
            )
        """)

        # `job` persists the task-center JobManager records so they survive
        # a sidecar restart. Only terminal jobs (success / failed / cancelled)
        # are ever written here — see server/jobs.py. `last_retry_summary`
        # and `overrides` are stored as JSON text.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS job (
                job_id              TEXT PRIMARY KEY,
                url                 TEXT NOT NULL,
                status              TEXT NOT NULL,
                created_at          TEXT NOT NULL,
                started_at          TEXT,
                finished_at         TEXT,
                total               INTEGER NOT NULL DEFAULT 0,
                success             INTEGER NOT NULL DEFAULT 0,
                failed              INTEGER NOT NULL DEFAULT 0,
                skipped             INTEGER NOT NULL DEFAULT 0,
                error               TEXT,
                author_nickname     TEXT,
                author_sec_uid      TEXT,
                retry_count         INTEGER NOT NULL DEFAULT 0,
                last_retry_at       TEXT,
                last_retry_summary  TEXT,
                retry_history       TEXT,
                overrides           TEXT
            )
        """)

        await db.execute("CREATE INDEX IF NOT EXISTS idx_aweme_id ON aweme(aweme_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_author_id ON aweme(author_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_download_time ON aweme(download_time)")
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_transcript_aweme_id ON transcript_job(aweme_id)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_transcript_status ON transcript_job(status)"
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_analysis_item_run ON analysis_item(run_id)")
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_analysis_item_frames ON analysis_item(frames_status)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_analysis_item_classify ON analysis_item(classify_status)"
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_job_created_at ON job(created_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_job_status ON job(status)")

        # Incremental migration: add author_sec_uid column to legacy aweme tables.
        # Running initialize() twice must be a no-op.
        cursor = await db.execute("PRAGMA table_info(aweme)")
        existing_columns = {row[1] for row in await cursor.fetchall()}
        if "author_sec_uid" not in existing_columns:
            await db.execute("ALTER TABLE aweme ADD COLUMN author_sec_uid TEXT")

        # Incremental migration: add retry_history column to legacy job
        # tables so pre-existing DB files (created before retry-history
        # persistence landed) continue to work. NULL for old rows; the
        # restore path maps NULL -> [] so the renderer gracefully shows
        # no history for those jobs.
        cursor = await db.execute("PRAGMA table_info(job)")
        existing_job_columns = {row[1] for row in await cursor.fetchall()}
        if "retry_history" not in existing_job_columns:
            await db.execute("ALTER TABLE job ADD COLUMN retry_history TEXT")

        cursor = await db.execute("PRAGMA table_info(analysis_item)")
        existing_analysis_columns = {row[1] for row in await cursor.fetchall()}
        await db.commit()
        self._initialized = True

    async def is_downloaded(self, aweme_id: str) -> bool:
        db = await self._get_conn()
        cursor = await db.execute("SELECT id FROM aweme WHERE aweme_id = ?", (aweme_id,))
        result = await cursor.fetchone()
        return result is not None

    async def add_aweme(
        self,
        aweme_data: Dict[str, Any],
        *,
        author_sec_uid: Optional[str] = None,
    ):
        db = await self._get_conn()
        # Prefer the explicit kwarg; fall back to a key on the payload so existing
        # callers (tests, legacy downloaders) keep working.
        sec_uid = author_sec_uid if author_sec_uid is not None else aweme_data.get("author_sec_uid")
        await db.execute(
            """
            INSERT OR REPLACE INTO aweme
            (aweme_id, aweme_type, title, author_id, author_name, author_sec_uid,
             create_time, download_time, file_path, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                aweme_data.get("aweme_id"),
                aweme_data.get("aweme_type"),
                aweme_data.get("title"),
                aweme_data.get("author_id"),
                aweme_data.get("author_name"),
                sec_uid,
                aweme_data.get("create_time"),
                int(datetime.now().timestamp()),
                aweme_data.get("file_path"),
                aweme_data.get("metadata"),
            ),
        )
        await db.commit()

    async def add_aweme_batch(self, items: List[Dict[str, Any]]) -> None:
        """Insert N awemes in a single transaction. Replaces existing rows by aweme_id."""
        if not items:
            return
        db = await self._get_conn()
        now_ts = int(datetime.now().timestamp())
        rows = [
            (
                item.get("aweme_id"),
                item.get("aweme_type"),
                item.get("title"),
                item.get("author_id"),
                item.get("author_name"),
                item.get("author_sec_uid"),
                item.get("create_time"),
                now_ts,
                item.get("file_path"),
                item.get("metadata"),
            )
            for item in items
        ]
        await db.executemany(
            """
            INSERT OR REPLACE INTO aweme
            (aweme_id, aweme_type, title, author_id, author_name, author_sec_uid,
             create_time, download_time, file_path, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            rows,
        )
        await db.commit()

    async def get_latest_aweme_time(self, author_id: str) -> Optional[int]:
        db = await self._get_conn()
        cursor = await db.execute(
            "SELECT MAX(create_time) FROM aweme WHERE author_id = ?", (author_id,)
        )
        result = await cursor.fetchone()
        return result[0] if result and result[0] else None

    async def add_history(self, history_data: Dict[str, Any]):
        db = await self._get_conn()
        await db.execute(
            """
            INSERT INTO download_history
            (url, url_type, download_time, total_count, success_count, config)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                history_data.get("url"),
                history_data.get("url_type"),
                int(datetime.now().timestamp()),
                history_data.get("total_count"),
                history_data.get("success_count"),
                history_data.get("config"),
            ),
        )
        await db.commit()

    async def get_aweme_history(
        self,
        *,
        page: int = 1,
        size: int = 50,
        author: Optional[str] = None,
        date_from: Optional[int] = None,
        date_to: Optional[int] = None,
        aweme_type: Optional[str] = None,
        title: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Paginated aweme history, newest download first.

        `date_from` / `date_to` are unix-seconds (filter against `create_time`).
        `aweme_type` matches the `aweme_type` column (e.g. 'video', 'gallery').
        `title` is a case-insensitive substring match on the title column.
        """
        db = await self._get_conn()
        where: list = []
        params: list = []
        if author:
            where.append("author_name = ?")
            params.append(author)
        if date_from is not None:
            where.append("create_time >= ?")
            params.append(int(date_from))
        if date_to is not None:
            where.append("create_time <= ?")
            params.append(int(date_to))
        if aweme_type:
            where.append("aweme_type = ?")
            params.append(aweme_type)
        if title:
            where.append("LOWER(COALESCE(title, '')) LIKE ?")
            params.append(f"%{title.lower()}%")
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        cursor = await db.execute(f"SELECT COUNT(*) FROM aweme {where_sql}", params)
        row = await cursor.fetchone()
        total = int(row[0]) if row else 0

        offset = max(0, (page - 1) * size)
        cursor = await db.execute(
            f"SELECT aweme_id, aweme_type, title, author_id, author_name, "
            f"author_sec_uid, create_time, download_time, file_path FROM aweme "
            f"{where_sql} ORDER BY download_time DESC, id DESC LIMIT ? OFFSET ?",
            params + [int(size), int(offset)],
        )
        rows = await cursor.fetchall()
        items = [
            {
                "aweme_id": r[0],
                "aweme_type": r[1],
                "title": r[2],
                "author_id": r[3],
                "author_name": r[4],
                "author_sec_uid": r[5],
                "create_time": r[6],
                "download_time": r[7],
                "file_path": r[8],
            }
            for r in rows
        ]
        return {"total": total, "page": int(page), "size": int(size), "items": items}

    async def get_aweme_count_by_author(self, author_id: str) -> int:
        db = await self._get_conn()
        cursor = await db.execute("SELECT COUNT(*) FROM aweme WHERE author_id = ?", (author_id,))
        result = await cursor.fetchone()
        return result[0] if result else 0

    async def get_top_authors(self, *, days: int, limit: int) -> List[Dict[str, Any]]:
        """Return the most-downloaded authors in the last ``days`` days.

        Aggregates rows in `aweme` with ``create_time >= now - days*86400`` and
        non-empty / non-null ``author_sec_uid``. Groups by ``author_sec_uid``
        and orders by ``COUNT(*) DESC, author_sec_uid ASC`` (stable tie-break
        so property tests are deterministic). Truncates to ``limit`` rows.

        ``author_name`` for each result row is the latest non-empty
        ``author_name`` for that ``sec_uid`` (ordered by ``download_time``
        descending). If all rows for that sec_uid have empty/null names,
        falls back to the Chinese placeholder ``"未知作者"``.

        Each returned dict contains ``sec_uid`` / ``author_name`` /
        ``download_count``.
        """
        cutoff = int(datetime.now().timestamp()) - int(days) * 86400
        db = await self._get_conn()
        cursor = await db.execute(
            """
            SELECT a.author_sec_uid,
                   (SELECT a2.author_name FROM aweme a2
                     WHERE a2.author_sec_uid = a.author_sec_uid
                       AND a2.author_name IS NOT NULL
                       AND a2.author_name != ''
                     ORDER BY a2.download_time DESC
                     LIMIT 1) AS author_name,
                   COUNT(*) AS download_count
              FROM aweme a
             WHERE a.create_time >= ?
               AND a.author_sec_uid IS NOT NULL
               AND a.author_sec_uid != ''
             GROUP BY a.author_sec_uid
             ORDER BY download_count DESC, a.author_sec_uid ASC
             LIMIT ?
            """,
            (cutoff, int(limit)),
        )
        rows = await cursor.fetchall()
        return [
            {
                "sec_uid": row[0],
                "author_name": row[1] if row[1] else "未知作者",
                "download_count": int(row[2]),
            }
            for row in rows
        ]

    async def upsert_transcript_job(self, job_data: Dict[str, Any]):
        now_ts = int(datetime.now().timestamp())
        db = await self._get_conn()
        await db.execute(
            """
            INSERT INTO transcript_job (
                aweme_id,
                video_path,
                transcript_dir,
                text_path,
                json_path,
                model,
                status,
                skip_reason,
                error_message,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(aweme_id, video_path, model) DO UPDATE SET
                transcript_dir = excluded.transcript_dir,
                text_path = excluded.text_path,
                json_path = excluded.json_path,
                status = excluded.status,
                skip_reason = excluded.skip_reason,
                error_message = excluded.error_message,
                updated_at = excluded.updated_at
        """,
            (
                job_data.get("aweme_id"),
                job_data.get("video_path"),
                job_data.get("transcript_dir"),
                job_data.get("text_path"),
                job_data.get("json_path"),
                job_data.get("model") or "gpt-4o-mini-transcribe",
                job_data.get("status"),
                job_data.get("skip_reason"),
                job_data.get("error_message"),
                now_ts,
                now_ts,
            ),
        )
        await db.commit()

    async def get_analyzed_aweme_ids(self) -> set:
        db = await self._get_conn()
        cursor = await db.execute(
            "SELECT DISTINCT aweme_id FROM analysis_item WHERE classify_status = 'success'"
        )
        return {row[0] for row in await cursor.fetchall()}

    async def list_video_awemes(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        db = await self._get_conn()
        sql = """
            SELECT aweme_id, author_name, file_path
            FROM aweme
            WHERE aweme_type = 'video'
              AND file_path IS NOT NULL
              AND file_path != ''
            ORDER BY download_time DESC, id DESC
        """
        params: List[Any] = []
        if limit is not None and int(limit) > 0:
            sql += " LIMIT ?"
            params.append(int(limit))
        cursor = await db.execute(sql, params)
        rows = await cursor.fetchall()
        return [
            {
                "aweme_id": row[0],
                "author_name": row[1],
                "file_path": row[2],
            }
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Analysis pipeline persistence
    # ------------------------------------------------------------------

    async def create_analysis_run(
        self,
        *,
        run_id: str,
        source_type: str,
        source_payload: Dict[str, Any],
    ) -> None:
        now_ts = int(datetime.now().timestamp())
        db = await self._get_conn()
        await db.execute(
            """
            INSERT INTO analysis_run (
                run_id, source_type, source_payload, status, created_at, updated_at
            ) VALUES (?, ?, ?, 'prepared', ?, ?)
            """,
            (
                run_id,
                source_type,
                json.dumps(source_payload, ensure_ascii=False),
                now_ts,
                now_ts,
            ),
        )
        await db.commit()

    async def get_analysis_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        db = await self._get_conn()
        cursor = await db.execute(
            """
            SELECT r.run_id, r.source_type, r.source_payload, r.status, r.csv_path, r.created_at, r.updated_at,
                   COUNT(i.id) AS item_count,
                   SUM(CASE WHEN i.frames_status = 'success' THEN 1 ELSE 0 END) AS framed,
                   SUM(CASE WHEN i.classify_status = 'success' THEN 1 ELSE 0 END) AS classified
            FROM analysis_run r
            LEFT JOIN analysis_item i ON i.run_id = r.run_id
            WHERE r.run_id = ?
            GROUP BY r.run_id
            """,
            (run_id,),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        try:
            payload = json.loads(row[2]) if row[2] else {}
        except (TypeError, ValueError):
            payload = {}
        return {
            "run_id": row[0],
            "source_type": row[1],
            "source_payload": payload,
            "status": row[3],
            "csv_path": row[4],
            "created_at": row[5],
            "updated_at": row[6],
            "item_count": int(row[7] or 0),
            "framed": int(row[8] or 0),
            "classified": int(row[9] or 0),
        }

    async def list_analysis_runs(self, limit: int = 20) -> List[Dict[str, Any]]:
        db = await self._get_conn()
        cursor = await db.execute(
            """
            SELECT r.run_id, r.source_type, r.status, r.csv_path, r.created_at, r.updated_at,
                   COUNT(i.id) AS item_count,
                   SUM(CASE WHEN i.classify_status = 'success' THEN 1 ELSE 0 END) AS classified
            FROM analysis_run r
            LEFT JOIN analysis_item i ON i.run_id = r.run_id
            GROUP BY r.run_id
            ORDER BY r.created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cursor.fetchall()
        return [
            {
                "run_id": row[0],
                "source_type": row[1],
                "status": row[2],
                "csv_path": row[3],
                "created_at": row[4],
                "updated_at": row[5],
                "item_count": row[6],
                "classified": row[7],
            }
            for row in rows
        ]

    async def list_analysis_runs_detailed(self, limit: int = 20) -> List[Dict[str, Any]]:
        db = await self._get_conn()
        cursor = await db.execute(
            """
            SELECT r.run_id, r.source_type, r.status, r.csv_path, r.created_at, r.updated_at,
                   COUNT(i.id) AS item_count,
                   SUM(CASE WHEN i.frames_status = 'success' THEN 1 ELSE 0 END) AS framed,
                   SUM(CASE WHEN i.classify_status = 'success' THEN 1 ELSE 0 END) AS classified,
                   SUM(CASE WHEN i.export_status = 'success' THEN 1 ELSE 0 END) AS exported,
                   SUM(CASE WHEN i.organize_status IN ('success', 'skipped') THEN 1 ELSE 0 END)
                       AS organized,
                   SUM(CASE WHEN i.frames_status = 'failed'
                             OR i.classify_status = 'failed'
                             OR i.export_status = 'failed'
                             OR i.organize_status = 'failed'
                            THEN 1 ELSE 0 END) AS failed
            FROM analysis_run r
            LEFT JOIN analysis_item i ON i.run_id = r.run_id
            GROUP BY r.run_id
            ORDER BY r.created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cursor.fetchall()
        return [
            {
                "run_id": row[0],
                "source_type": row[1],
                "status": row[2],
                "csv_path": row[3],
                "created_at": row[4],
                "updated_at": row[5],
                "item_count": int(row[6] or 0),
                "framed": int(row[7] or 0),
                "classified": int(row[8] or 0),
                "exported": int(row[9] or 0),
                "organized": int(row[10] or 0),
                "failed": int(row[11] or 0),
            }
            for row in rows
        ]

    async def get_analysis_run_dashboard(self, run_id: str) -> Optional[Dict[str, Any]]:
        run = await self.get_analysis_run(run_id)
        if not run:
            return None

        db = await self._get_conn()
        cursor = await db.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN frames_status = 'pending' THEN 1 ELSE 0 END),
                SUM(CASE WHEN frames_status = 'success' THEN 1 ELSE 0 END),
                SUM(CASE WHEN frames_status = 'failed' THEN 1 ELSE 0 END),
                SUM(CASE WHEN classify_status = 'pending' THEN 1 ELSE 0 END),
                SUM(CASE WHEN classify_status = 'success' THEN 1 ELSE 0 END),
                SUM(CASE WHEN classify_status = 'failed' THEN 1 ELSE 0 END),
                SUM(CASE WHEN export_status = 'pending' THEN 1 ELSE 0 END),
                SUM(CASE WHEN export_status = 'success' THEN 1 ELSE 0 END),
                SUM(CASE WHEN export_status = 'failed' THEN 1 ELSE 0 END),
                SUM(CASE WHEN organize_status = 'pending' THEN 1 ELSE 0 END),
                SUM(CASE WHEN organize_status = 'success' THEN 1 ELSE 0 END),
                SUM(CASE WHEN organize_status = 'skipped' THEN 1 ELSE 0 END),
                SUM(CASE WHEN organize_status = 'failed' THEN 1 ELSE 0 END),
                SUM(CASE WHEN error_stage IS NOT NULL THEN 1 ELSE 0 END),
                SUM(retry_count)
            FROM analysis_item
            WHERE run_id = ?
            """,
            (run_id,),
        )
        row = await cursor.fetchone()
        if not row:
            row = [0] * 16

        failures_cursor = await db.execute(
            """
            SELECT aweme_id, author_name, error_stage, error_message, retry_count, updated_at
            FROM analysis_item
            WHERE run_id = ? AND error_stage IS NOT NULL
            ORDER BY updated_at DESC, id DESC
            LIMIT 20
            """,
            (run_id,),
        )
        failures = await failures_cursor.fetchall()
        run["stages"] = {
            "frames": {
                "pending": int(row[1] or 0),
                "success": int(row[2] or 0),
                "failed": int(row[3] or 0),
            },
            "classify": {
                "pending": int(row[4] or 0),
                "success": int(row[5] or 0),
                "failed": int(row[6] or 0),
            },
            "export": {
                "pending": int(row[7] or 0),
                "success": int(row[8] or 0),
                "failed": int(row[9] or 0),
            },
            "organize": {
                "pending": int(row[10] or 0),
                "success": int(row[11] or 0),
                "skipped": int(row[12] or 0),
                "failed": int(row[13] or 0),
            },
        }
        run["failed"] = int(row[14] or 0)
        run["retry_count"] = int(row[15] or 0)
        run["failures"] = [
            {
                "aweme_id": failure[0],
                "author_name": failure[1],
                "error_stage": failure[2],
                "error_message": failure[3],
                "retry_count": int(failure[4] or 0),
                "updated_at": failure[5],
            }
            for failure in failures
        ]
        return run

    async def get_analysis_score_dashboard(
        self,
        run_id: str,
        *,
        primary_attribute: str,
        buckets: List[Dict[str, Any]],
        limit: int = 20,
    ) -> Dict[str, Any]:
        db = await self._get_conn()

        dist_cursor = await db.execute(
            """
            SELECT attribute_key, score, COUNT(*)
            FROM analysis_score
            WHERE run_id = ?
            GROUP BY attribute_key, score
            ORDER BY attribute_key ASC, score ASC
            """,
            (run_id,),
        )
        distributions: Dict[str, Dict[str, int]] = {}
        for attribute_key, score, count in await dist_cursor.fetchall():
            distributions.setdefault(str(attribute_key), {})[str(int(score))] = int(count or 0)

        bucket_counts = []
        for bucket in buckets:
            label = str(bucket.get("label") or "")
            min_score = int(bucket.get("min_score", 0))
            max_score = int(bucket.get("max_score", 10))
            cursor = await db.execute(
                """
                SELECT COUNT(*)
                FROM analysis_score
                WHERE run_id = ? AND attribute_key = ? AND score BETWEEN ? AND ?
                """,
                (run_id, primary_attribute, min_score, max_score),
            )
            row = await cursor.fetchone()
            bucket_counts.append(
                {
                    "label": label,
                    "min_score": min_score,
                    "max_score": max_score,
                    "count": int((row or [0])[0] or 0),
                }
            )

        top_cursor = await db.execute(
            """
            SELECT i.aweme_id, i.author_name, i.video_path, s.score
            FROM analysis_score s
            JOIN analysis_item i
              ON i.run_id = s.run_id AND i.aweme_id = s.aweme_id
            WHERE s.run_id = ? AND s.attribute_key = ?
            ORDER BY s.score DESC, i.id ASC
            LIMIT ?
            """,
            (run_id, primary_attribute, int(limit or 20)),
        )
        top_items = [
            {
                "aweme_id": row[0],
                "author_name": row[1],
                "video_path": row[2],
                "score": int(row[3]),
            }
            for row in await top_cursor.fetchall()
        ]

        total_cursor = await db.execute(
            "SELECT COUNT(DISTINCT aweme_id) FROM analysis_score WHERE run_id = ?",
            (run_id,),
        )
        total_row = await total_cursor.fetchone()
        return {
            "run_id": run_id,
            "primary_attribute": primary_attribute,
            "scored_items": int((total_row or [0])[0] or 0),
            "distributions": distributions,
            "buckets": bucket_counts,
            "top_items": top_items,
        }

    async def reset_failed_analysis_items(self, run_id: str) -> int:
        """Reset all failed items so they can be retried. Cascades: a frames
        failure also resets classify/export/organize, etc. Returns count."""
        db = await self._get_conn()
        now_ts = int(datetime.now().timestamp())
        await db.execute(
            """
            UPDATE analysis_item
            SET frames_status = 'pending',
                classify_status = 'pending',
                export_status = 'pending',
                organize_status = 'pending',
                error_stage = NULL,
                error_message = NULL,
                updated_at = ?
            WHERE run_id = ? AND frames_status = 'failed'
            """,
            (now_ts, run_id),
        )
        await db.execute(
            """
            UPDATE analysis_item
            SET classify_status = 'pending',
                export_status = 'pending',
                organize_status = 'pending',
                error_stage = NULL,
                error_message = NULL,
                updated_at = ?
            WHERE run_id = ? AND classify_status = 'failed'
            """,
            (now_ts, run_id),
        )
        await db.execute(
            """
            UPDATE analysis_item
            SET export_status = 'pending',
                organize_status = 'pending',
                error_stage = NULL,
                error_message = NULL,
                updated_at = ?
            WHERE run_id = ? AND export_status = 'failed'
            """,
            (now_ts, run_id),
        )
        await db.execute(
            """
            UPDATE analysis_item
            SET organize_status = 'pending',
                error_stage = NULL,
                error_message = NULL,
                updated_at = ?
            WHERE run_id = ? AND organize_status = 'failed'
            """,
            (now_ts, run_id),
        )
        await db.commit()
        return db.total_changes

    async def get_latest_unfinished_run(self) -> Optional[Dict[str, Any]]:
        db = await self._get_conn()
        cursor = await db.execute(
            """
            SELECT run_id, source_type, status, csv_path, created_at, updated_at,
                   (SELECT COUNT(*) FROM analysis_item WHERE run_id = r.run_id) AS item_count,
                   (SELECT COUNT(*) FROM analysis_item WHERE run_id = r.run_id AND classify_status = 'success') AS classified
            FROM analysis_run r
            WHERE status IN ('prepared', 'running', 'partial')
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return {
            "run_id": row[0],
            "source_type": row[1],
            "status": row[2],
            "csv_path": row[3],
            "created_at": row[4],
            "updated_at": row[5],
            "item_count": row[6],
            "classified": row[7],
        }

    async def get_best_unfinished_run(self) -> Optional[Dict[str, Any]]:
        db = await self._get_conn()
        cursor = await db.execute(
            """
            SELECT run_id, source_type, status, csv_path, created_at, updated_at,
                   (SELECT COUNT(*) FROM analysis_item WHERE run_id = r.run_id) AS item_count,
                   (SELECT COUNT(*) FROM analysis_item
                    WHERE run_id = r.run_id AND frames_status = 'success') AS framed,
                   (SELECT COUNT(*) FROM analysis_item
                    WHERE run_id = r.run_id AND classify_status = 'success') AS classified
            FROM analysis_run r
            WHERE status IN ('prepared', 'running', 'partial')
            ORDER BY classified DESC, framed DESC, updated_at DESC, created_at DESC
            LIMIT 1
            """
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return {
            "run_id": row[0],
            "source_type": row[1],
            "status": row[2],
            "csv_path": row[3],
            "created_at": row[4],
            "updated_at": row[5],
            "item_count": row[6],
            "framed": row[7],
            "classified": row[8],
        }

    async def get_latest_classified_run(self) -> Optional[Dict[str, Any]]:
        db = await self._get_conn()
        cursor = await db.execute(
            """
            SELECT run_id, source_type, status, csv_path, created_at, updated_at,
                   (SELECT COUNT(*) FROM analysis_item WHERE run_id = r.run_id) AS item_count,
                   (SELECT COUNT(*) FROM analysis_item
                    WHERE run_id = r.run_id AND classify_status = 'success') AS classified
            FROM analysis_run r
            WHERE EXISTS (
                SELECT 1
                FROM analysis_item i
                WHERE i.run_id = r.run_id
                  AND i.classify_status = 'success'
            )
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return {
            "run_id": row[0],
            "source_type": row[1],
            "status": row[2],
            "csv_path": row[3],
            "created_at": row[4],
            "updated_at": row[5],
            "item_count": row[6],
            "classified": row[7],
        }

    async def add_analysis_items(self, run_id: str, items: List[Dict[str, Any]]) -> None:
        if not items:
            return
        now_ts = int(datetime.now().timestamp())
        rows = []
        seen = set()
        for item in items:
            aweme_id = str(item.get("aweme_id") or "").strip()
            video_path = str(item.get("video_path") or "").strip()
            if not aweme_id or not video_path or aweme_id in seen:
                continue
            seen.add(aweme_id)
            rows.append(
                (
                    run_id,
                    aweme_id,
                    item.get("author_name"),
                    video_path,
                    now_ts,
                    now_ts,
                )
            )
        if not rows:
            return
        db = await self._get_conn()
        await db.executemany(
            """
            INSERT OR IGNORE INTO analysis_item (
                run_id, aweme_id, author_name, video_path, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        await db.commit()

    async def list_analysis_items_for_stage(
        self,
        run_id: str,
        stage: str,
    ) -> List[Dict[str, Any]]:
        if stage not in {"frames", "classify"}:
            raise ValueError(f"unsupported analysis stage query: {stage}")
        where_clause = (
            "frames_status != 'success'"
            if stage == "frames"
            else "frames_status = 'success' AND classify_status != 'success'"
        )
        db = await self._get_conn()
        cursor = await db.execute(
            f"""
            SELECT aweme_id, author_name, video_path, grid_path,
                   frames_status, classify_status, export_status, organize_status
            FROM analysis_item
            WHERE run_id = ? AND {where_clause}
            ORDER BY id ASC
            """,
            (run_id,),
        )
        rows = await cursor.fetchall()
        return [
            {
                "aweme_id": row[0],
                "author_name": row[1],
                "video_path": row[2],
                "grid_path": row[3],
                "frames_status": row[4],
                "classify_status": row[5],
                "export_status": row[6],
                "organize_status": row[7],
            }
            for row in rows
        ]

    async def update_analysis_item_stage(
        self,
        run_id: str,
        aweme_id: str,
        *,
        stage: str,
        status: str,
        error_message: Optional[str] = None,
        extra_updates: Optional[Dict[str, Any]] = None,
    ) -> None:
        status_column = {
            "frames": "frames_status",
            "classify": "classify_status",
            "export": "export_status",
            "organize": "organize_status",
        }.get(stage)
        if status_column is None:
            raise ValueError(f"unsupported analysis stage: {stage}")

        updates = [f"{status_column} = ?", "updated_at = ?"]
        params: List[Any] = [status, int(datetime.now().timestamp())]
        if status == "failed":
            updates.extend(
                [
                    "error_stage = ?",
                    "error_message = ?",
                    "retry_count = retry_count + 1",
                ]
            )
            params.extend([stage, error_message])
        else:
            updates.extend(["error_stage = NULL", "error_message = NULL"])

        if extra_updates:
            allowed_columns = {"grid_path", "organized_path"}
            for key, value in extra_updates.items():
                if key not in allowed_columns:
                    raise ValueError(f"unsupported analysis item update column: {key}")
                updates.append(f"{key} = ?")
                params.append(value)

        params.extend([run_id, aweme_id])
        db = await self._get_conn()
        await db.execute(
            f"""
            UPDATE analysis_item
            SET {", ".join(updates)}
            WHERE run_id = ? AND aweme_id = ?
            """,
            params,
        )
        await db.commit()

    async def upsert_analysis_scores(
        self,
        run_id: str,
        aweme_id: str,
        scores: Dict[str, int],
    ) -> None:
        if not scores:
            return
        db = await self._get_conn()
        rows = [(run_id, aweme_id, key, int(value)) for key, value in scores.items()]
        await db.executemany(
            """
            INSERT INTO analysis_score (run_id, aweme_id, attribute_key, score)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(run_id, aweme_id, attribute_key) DO UPDATE SET
                score = excluded.score
            """,
            rows,
        )
        await db.commit()

    async def get_analysis_export_rows(self, run_id: str) -> List[Dict[str, Any]]:
        db = await self._get_conn()
        cursor = await db.execute(
            """
            SELECT i.aweme_id, i.author_name, i.video_path, i.grid_path,
                   i.organize_status, i.organized_path, s.attribute_key, s.score
            FROM analysis_item i
            LEFT JOIN analysis_score s
              ON s.run_id = i.run_id
             AND s.aweme_id = i.aweme_id
            WHERE i.run_id = ?
              AND i.classify_status = 'success'
            ORDER BY i.id ASC, s.attribute_key ASC
            """,
            (run_id,),
        )
        rows = await cursor.fetchall()
        grouped: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            aweme_id = row[0]
            item = grouped.setdefault(
                aweme_id,
                {
                    "aweme_id": aweme_id,
                    "author_name": row[1],
                    "video_path": row[2],
                    "grid_path": row[3],
                    "organize_status": row[4],
                    "organized_path": row[5],
                    "scores": {},
                },
            )
            if row[6] is not None:
                item["scores"][row[6]] = int(row[7])
        return list(grouped.values())

    async def reset_analysis_organize(self, run_id: str) -> int:
        db = await self._get_conn()
        cursor = await db.execute(
            """
            UPDATE analysis_item
            SET organize_status = 'pending',
                organized_path = NULL,
                error_stage = CASE WHEN error_stage = 'organize' THEN NULL ELSE error_stage END,
                error_message = CASE WHEN error_stage = 'organize' THEN NULL ELSE error_message END,
                updated_at = ?
            WHERE run_id = ?
              AND classify_status = 'success'
            """,
            (int(datetime.now().timestamp()), run_id),
        )
        await db.commit()
        return int(cursor.rowcount or 0)

    async def mark_analysis_exported(self, run_id: str, csv_path: str) -> None:
        now_ts = int(datetime.now().timestamp())
        db = await self._get_conn()
        await db.execute(
            """
            UPDATE analysis_item
            SET export_status = 'success',
                updated_at = ?
            WHERE run_id = ?
              AND classify_status = 'success'
            """,
            (now_ts, run_id),
        )
        await db.execute(
            """
            UPDATE analysis_run
            SET csv_path = ?,
                updated_at = ?
            WHERE run_id = ?
            """,
            (csv_path, now_ts, run_id),
        )
        await db.commit()

    async def refresh_analysis_run_status(self, run_id: str) -> str:
        db = await self._get_conn()
        cursor = await db.execute(
            """
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN classify_status = 'success' THEN 1 ELSE 0 END) AS classified_count,
                SUM(CASE WHEN frames_status = 'failed'
                          OR classify_status = 'failed'
                          OR organize_status = 'failed'
                         THEN 1 ELSE 0 END) AS failed_count,
                SUM(CASE WHEN organize_status IN ('success', 'skipped') THEN 1 ELSE 0 END)
                    AS organized_count
            FROM analysis_item
            WHERE run_id = ?
            """,
            (run_id,),
        )
        row = await cursor.fetchone()
        total_count = int(row[0] or 0)
        classified_count = int(row[1] or 0)
        failed_count = int(row[2] or 0)
        organized_count = int(row[3] or 0)
        if total_count == 0:
            status = "prepared"
        elif organized_count == total_count:
            status = "completed"
        elif failed_count > 0 and classified_count + failed_count >= total_count:
            status = "partial"
        else:
            status = "running"
        await db.execute(
            """
            UPDATE analysis_run
            SET status = ?, updated_at = ?
            WHERE run_id = ?
            """,
            (status, int(datetime.now().timestamp()), run_id),
        )
        await db.commit()
        return status

    async def get_transcript_job(self, aweme_id: str) -> Optional[Dict[str, Any]]:
        db = await self._get_conn()
        cursor = await db.execute(
            """
            SELECT aweme_id, video_path, transcript_dir, text_path, json_path,
                   model, status, skip_reason, error_message, created_at, updated_at
            FROM transcript_job
            WHERE aweme_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (aweme_id,),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return {
            "aweme_id": row[0],
            "video_path": row[1],
            "transcript_dir": row[2],
            "text_path": row[3],
            "json_path": row[4],
            "model": row[5],
            "status": row[6],
            "skip_reason": row[7],
            "error_message": row[8],
            "created_at": row[9],
            "updated_at": row[10],
        }

    async def delete_aweme_by_ids(self, aweme_ids: List[str]) -> int:
        """Delete aweme rows by their string id. Returns the number of rows removed.

        Empty input is a no-op that returns 0 without issuing any SQL.

        Uses a parameterized ``DELETE ... WHERE aweme_id IN (?,?,...)`` statement
        because ``aiosqlite.Cursor.rowcount`` is not reliably populated after
        ``executemany`` across all versions. Chunked at 500 ids per statement to
        stay well below SQLite's host-parameter limit (historically 999).
        """
        if not aweme_ids:
            return 0
        # De-duplicate input while preserving a stable order. Duplicate ids would
        # otherwise match the same row twice in different chunks and inflate the
        # returned count beyond the rows actually affected.
        seen: Dict[str, None] = {}
        for aid in aweme_ids:
            if aid not in seen:
                seen[aid] = None
        unique_ids = list(seen.keys())

        db = await self._get_conn()
        if self._conn_lock is None:
            self._conn_lock = asyncio.Lock()
        deleted = 0
        chunk_size = 500
        async with self._conn_lock:
            for start in range(0, len(unique_ids), chunk_size):
                chunk = unique_ids[start : start + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                cursor = await db.execute(
                    f"DELETE FROM aweme WHERE aweme_id IN ({placeholders})",
                    chunk,
                )
                if cursor.rowcount is not None and cursor.rowcount > 0:
                    deleted += cursor.rowcount
            await db.commit()
        return deleted

    async def truncate_history(self) -> None:
        """Delete every row from `aweme` and `download_history`.

        Does not touch disk files or any other table (e.g. transcript_job).
        """
        db = await self._get_conn()
        if self._conn_lock is None:
            self._conn_lock = asyncio.Lock()
        async with self._conn_lock:
            await db.execute("DELETE FROM aweme")
            await db.execute("DELETE FROM download_history")
            await db.commit()

    # ------------------------------------------------------------------
    # Task-center job persistence (see server/jobs.py)
    # ------------------------------------------------------------------

    async def upsert_job(self, job_dict: Dict[str, Any]) -> None:
        """Insert or replace a task-center job record.

        Accepts the dict produced by :py:meth:`server.jobs.DownloadJob.to_dict`
        plus an optional ``overrides`` key (the JobManager stores overrides
        separately on the in-memory job but we persist them too so future
        retries/re-runs can inherit them). Unknown keys are ignored — any
        renderer-only computed fields (``url_type``, ``duration_ms`` etc.)
        are recomputed from raw columns on read.
        """
        db = await self._get_conn()
        if self._conn_lock is None:
            self._conn_lock = asyncio.Lock()

        last_retry_summary = job_dict.get("last_retry_summary")
        retry_history = job_dict.get("retry_history")
        overrides = job_dict.get("overrides")
        params = (
            job_dict.get("job_id"),
            job_dict.get("url") or "",
            job_dict.get("status") or "",
            job_dict.get("created_at") or "",
            job_dict.get("started_at"),
            job_dict.get("finished_at"),
            int(job_dict.get("total") or 0),
            int(job_dict.get("success") or 0),
            int(job_dict.get("failed") or 0),
            int(job_dict.get("skipped") or 0),
            job_dict.get("error"),
            job_dict.get("author_nickname"),
            job_dict.get("author_sec_uid"),
            int(job_dict.get("retry_count") or 0),
            job_dict.get("last_retry_at"),
            json.dumps(last_retry_summary) if last_retry_summary else None,
            json.dumps(retry_history) if retry_history else None,
            json.dumps(overrides) if overrides else None,
        )
        async with self._conn_lock:
            await db.execute(
                """
                INSERT OR REPLACE INTO job (
                    job_id, url, status, created_at, started_at, finished_at,
                    total, success, failed, skipped, error,
                    author_nickname, author_sec_uid,
                    retry_count, last_retry_at, last_retry_summary,
                    retry_history, overrides
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                params,
            )
            await db.commit()

    async def delete_jobs(self, job_ids: List[str]) -> int:
        """Delete job rows by id. Returns the number of rows deleted."""
        if not job_ids:
            return 0
        seen: Dict[str, None] = {}
        for jid in job_ids:
            if jid and jid not in seen:
                seen[jid] = None
        unique_ids = list(seen.keys())
        if not unique_ids:
            return 0

        db = await self._get_conn()
        if self._conn_lock is None:
            self._conn_lock = asyncio.Lock()
        deleted = 0
        chunk_size = 500
        async with self._conn_lock:
            for start in range(0, len(unique_ids), chunk_size):
                chunk = unique_ids[start : start + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                cursor = await db.execute(
                    f"DELETE FROM job WHERE job_id IN ({placeholders})",
                    chunk,
                )
                if cursor.rowcount is not None and cursor.rowcount > 0:
                    deleted += cursor.rowcount
            await db.commit()
        return deleted

    async def load_terminal_jobs(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Load persisted terminal jobs ordered by created_at DESC.

        Only rows whose ``status`` is a terminal value (success / failed /
        cancelled) are returned. Running/pending rows shouldn't exist on
        disk — see server/jobs.py — but we filter defensively in case an
        older build left stale rows.
        """
        db = await self._get_conn()
        if self._conn_lock is None:
            self._conn_lock = asyncio.Lock()

        sql = (
            "SELECT job_id, url, status, created_at, started_at, finished_at, "
            "total, success, failed, skipped, error, author_nickname, "
            "author_sec_uid, retry_count, last_retry_at, last_retry_summary, "
            "retry_history, overrides FROM job "
            "WHERE status IN ('success', 'failed', 'cancelled') "
            "ORDER BY created_at DESC"
        )
        if limit is not None and limit > 0:
            sql += f" LIMIT {int(limit)}"

        async with self._conn_lock:
            cursor = await db.execute(sql)
            rows = await cursor.fetchall()

        result: List[Dict[str, Any]] = []
        for row in rows:
            summary_raw = row[15]
            history_raw = row[16]
            overrides_raw = row[17]
            try:
                summary = json.loads(summary_raw) if summary_raw else None
            except (TypeError, ValueError):
                summary = None
            try:
                history = json.loads(history_raw) if history_raw else []
                if not isinstance(history, list):
                    history = []
            except (TypeError, ValueError):
                history = []
            try:
                overrides = json.loads(overrides_raw) if overrides_raw else None
            except (TypeError, ValueError):
                overrides = None
            result.append(
                {
                    "job_id": row[0],
                    "url": row[1],
                    "status": row[2],
                    "created_at": row[3],
                    "started_at": row[4],
                    "finished_at": row[5],
                    "total": row[6] or 0,
                    "success": row[7] or 0,
                    "failed": row[8] or 0,
                    "skipped": row[9] or 0,
                    "error": row[10],
                    "author_nickname": row[11],
                    "author_sec_uid": row[12],
                    "retry_count": row[13] or 0,
                    "last_retry_at": row[14],
                    "last_retry_summary": summary,
                    "retry_history": history,
                    "overrides": overrides,
                }
            )
        return result

    async def close(self):
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
