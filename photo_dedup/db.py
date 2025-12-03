import sqlite3
from pathlib import Path
import json
import os

DB_NAME = "photo_dedup.db"
DB_PATH = "D:/"


class Database:
    def __init__(self, db_dir=None):
        if db_dir:
            if not Path(db_dir).exists():
                print(f"Folder {db_dir} does not exists")
                Path(db_dir).mkdir(parents=True, exist_ok=True)
            self.db_path = Path(db_dir, DB_NAME)
        else:
            self.db_path = Path(os.getcwd(), DB_NAME)
        print(self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._init()

    # ---------------------------
    # Initialization
    # ---------------------------
    def _init(self):
        # Photos table with folder
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS photos (
            id INTEGER PRIMARY KEY,
            path TEXT UNIQUE,
            folder TEXT,
            size INTEGER,
            hash TEXT,
            date_taken TEXT,
            camera_model TEXT,
            gps_lat REAL,
            gps_lon REAL,
            orientation TEXT
        );
        """)

        # Videos table with folder
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            id INTEGER PRIMARY KEY,
            path TEXT UNIQUE,
            folder TEXT,
            size INTEGER,
            hash TEXT
        );
        """)

        # Create tables indexes
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_photos_hash ON photos(hash);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_photos_folder ON photos(folder);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_hash ON videos(hash);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_folder ON videos(folder);")

        self.conn.commit()

    # ---------------------------
    # Insert photo(s)
    # ---------------------------
    def insert_photo(self, path, size, hash_value, exif=None):
        exif = exif or {}
        folder = str(Path(path).parent)
        self.conn.execute(
            """INSERT OR IGNORE INTO photos(
                path, folder, size, hash, date_taken, camera_model, gps_lat, gps_lon, orientation
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                path,
                folder,
                size,
                hash_value,
                exif.get("date_taken"),
                exif.get("camera_model"),
                exif.get("gps_lat"),
                exif.get("gps_lon"),
                exif.get("orientation")
            )
        )

    def insert_photos_batch(self, batch):
        """Batch: list of (path, size, hash, exif_dict)"""
        data = []
        for item in batch:
            path, size, hash_value = item[:3]
            exif = item[3] if len(item) > 3 else {}
            exif_json = json.dumps(exif, ensure_ascii=False)
            folder = str(Path(path).parent)
            data.append((path, folder, size, hash_value,
                         exif.get("date_taken"),
                         exif.get("camera_model"),
                         exif.get("gps_lat"),
                         exif.get("gps_lon"),
                         exif.get("orientation")))
        self.conn.executemany(
            """INSERT OR IGNORE INTO photos(
                path, folder, size, hash, date_taken, camera_model, gps_lat, gps_lon, orientation
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            data
        )

    # ---------------------------
    # Insert video(s)
    # ---------------------------
    def insert_videos_batch(self, batch):
        """Batch: list of (path, size, hash)."""
        data = [(p, str(Path(p).parent), s, h) for p, s, h in batch]
        self.conn.executemany(
            "INSERT OR IGNORE INTO videos(path, folder, size, hash) VALUES (?, ?, ?, ?)",
            data
        )

    # ---------------------------
    # Basic Queries
    # ---------------------------
    def list_all_photos(self):
        return self.conn.execute("SELECT * FROM photos ORDER BY id").fetchall()

    def list_all_videos(self):
        return self.conn.execute("SELECT * FROM videos ORDER BY id").fetchall()

    def duplicate_photo_groups(self):
        rows = self.conn.execute("""
            SELECT hash, path, size
            FROM photos
            WHERE hash IN (
                SELECT hash FROM photos GROUP BY hash HAVING COUNT(*) > 1
            )
            ORDER BY hash;
        """).fetchall()
        groups = {}
        for r in rows:
            groups.setdefault(r["hash"], []).append(dict(r))
        return groups

    def duplicate_video_groups(self):
        rows = self.conn.execute("""
            SELECT hash, path, size
            FROM videos
            WHERE hash IN (
                SELECT hash FROM videos GROUP BY hash HAVING COUNT(*) > 1
            )
            ORDER BY hash;
        """).fetchall()
        groups = {}
        for r in rows:
            groups.setdefault(r["hash"], []).append(dict(r))
        return groups

    # ---------------------------
    # Report Data
    # ---------------------------
    def get_global_stats(self):
        """Return global statistics for photos and videos."""
        cursor = self.conn.cursor()

        # Photos
        cursor.execute("SELECT COUNT(*) AS total_files FROM photos")
        total_photos_files = cursor.fetchone()["total_files"]

        cursor.execute("SELECT SUM(size) AS total_size FROM photos")
        total_photos_size = cursor.fetchone()["total_size"]

        cursor.execute("""
            SELECT hash, COUNT(*) AS cnt, SUM(size) AS total_size, MIN(size) AS min_size
            FROM photos
            WHERE hash IS NOT NULL
            GROUP BY hash
            HAVING cnt > 1
        """)
        dup_photo_groups = cursor.fetchall()
        lost_space_photos = sum(g["total_size"] - g["min_size"] for g in dup_photo_groups)

        # Videos
        cursor.execute("SELECT COUNT(*) AS total_files_videos FROM videos")
        total_videos_files = cursor.fetchone()["total_files_videos"]

        cursor.execute("SELECT SUM(size) AS total_size FROM videos")
        total_videos_size = cursor.fetchone()["total_size"]

        cursor.execute("""
            SELECT hash, COUNT(*) AS cnt, SUM(size) AS total_size, MIN(size) AS min_size
            FROM videos
            WHERE hash IS NOT NULL
            GROUP BY hash
            HAVING cnt > 1
        """)
        dup_video_groups = cursor.fetchall()
        lost_space_videos = sum(g["total_size"] - g["min_size"] for g in dup_video_groups)

        return {
            "total_files_photos": total_photos_files,
            "total_size_photos": total_photos_size,
            "lost_space_photos": lost_space_photos,
            "duplicate_photo_groups": len(dup_photo_groups),
            "total_files_videos": total_videos_files,
            "total_size_videos": total_videos_size,
            "lost_space_videos": lost_space_videos,
            "duplicate_video_groups": len(dup_video_groups)
        }

    def get_folders_stats(self):
        """Return per-folder stats for photos and videos."""
        cursor = self.conn.cursor()
        folders = {}

        # Photos
        cursor.execute("""
            SELECT folder,
                   COUNT(*) AS file_count,
                   SUM(CASE WHEN hash IN (
                       SELECT hash FROM photos GROUP BY hash HAVING COUNT(*) > 1
                   ) THEN 1 ELSE 0 END) AS dup_count,
                   SUM(CASE WHEN hash IN (
                       SELECT hash FROM photos GROUP BY hash HAVING COUNT(*) > 1
                   ) THEN size ELSE 0 END) AS lost_bytes
            FROM photos
            GROUP BY folder
        """)
        for row in cursor.fetchall():
            folders[row["folder"]] = {
                "photos_count": row["file_count"] or 0,
                "photos_dup_count": row["dup_count"] or 0,
                "photos_lost_bytes": row["lost_bytes"] or 0,
                "videos_count": 0,
                "videos_dup_count": 0,
                "videos_lost_bytes": 0
            }
        # Videos
        cursor.execute("""
            SELECT folder,
                   COUNT(*) AS file_count,
                   SUM(CASE WHEN hash IN (
                       SELECT hash FROM videos GROUP BY hash HAVING COUNT(*) > 1
                   ) THEN 1 ELSE 0 END) AS dup_count,
                   SUM(CASE WHEN hash IN (
                       SELECT hash FROM videos GROUP BY hash HAVING COUNT(*) > 1
                   ) THEN size ELSE 0 END) AS lost_bytes
            FROM videos
            GROUP BY folder
        """)
        for row in cursor.fetchall():
            if (row["folder"] not in folders.keys()):
                folders[row["folder"]] = {
                    "photos_count": 0,
                    "photos_dup_count": 0,
                    "photos_lost_bytes": 0,
                    "videos_count": row["file_count"] or 0,
                    "videos_dup_count": row["dup_count"] or 0,
                    "videos_lost_bytes": row["lost_bytes"] or 0
                }
            else:
                folders[row["folder"]].update(
                    {
                        "videos_count": row["file_count"] or 0,
                        "videos_dup_count": row["dup_count"] or 0,
                        "videos_lost_bytes": row["lost_bytes"] or 0
                    }
                )

        return folders


    # ---------------------------
    # Folder similarity map
    # ---------------------------
    def get_folder_hash_map(self):
        rows = self.conn.execute("SELECT path, hash FROM photos").fetchall()
        folder_map = {}
        for r in rows:
            folder = str(Path(r["path"]).parent)
            folder_map.setdefault(folder, set()).add(r["hash"])
        return folder_map


    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()
