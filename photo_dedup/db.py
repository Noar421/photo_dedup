"""
Database module for photo deduplication.

This module provides database operations for storing and querying
photo and video metadata, including hash values for duplicate detection.
"""

import sqlite3
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from contextlib import contextmanager

logger = logging.getLogger("photo_dedup")

DB_NAME = "photo_dedup.db"


class DatabaseError(Exception):
    """Base exception for database errors."""
    pass


class Database:
    """
    Database manager for photo deduplication.
    
    Handles storage and retrieval of photo/video metadata including
    file paths, sizes, hashes, and EXIF data.
    """
    
    def __init__(self, db_dir: Optional[str] = None):
        """
        Initialize database connection.
        
        Args:
            db_dir: Directory to store database file. If None, uses current directory.
                    Creates directory if it doesn't exist.
        """
        if db_dir:
            db_dir_path = Path(db_dir)
            if not db_dir_path.exists():
                logger.info(f"Creating database directory: {db_dir_path}")
                db_dir_path.mkdir(parents=True, exist_ok=True)
            self.db_path = db_dir_path / DB_NAME
        else:
            self.db_path = Path.cwd() / DB_NAME
        
        logger.info(f"Database path: {self.db_path}")
        
        self.conn = None
        self._connect()
        self._init_schema()
        self._create_indexes()
    
    def _connect(self):
        """Establish database connection."""
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            # Enable WAL mode for better concurrent access
            self.conn.execute("PRAGMA journal_mode=WAL")
            # Enable foreign keys
            self.conn.execute("PRAGMA foreign_keys=ON")
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to connect to database: {e}") from e
    
    def _init_schema(self):
        """Initialize database schema."""
        try:
            # Photos table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS photos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path TEXT UNIQUE NOT NULL,
                    folder TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    hash TEXT,
                    date_taken TEXT,
                    camera_model TEXT,
                    gps_lat REAL,
                    gps_lon REAL,
                    orientation INTEGER,
                    width INTEGER,
                    height INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Videos table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS videos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path TEXT UNIQUE NOT NULL,
                    folder TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    hash TEXT,
                    duration REAL,
                    width INTEGER,
                    height INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Metadata table for storing database info
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            self.conn.commit()
            logger.debug("Database schema initialized")
            
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to initialize schema: {e}") from e
    
    def _create_indexes(self):
        """Create indexes for better query performance."""
        try:
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_photos_hash ON photos(hash) WHERE hash IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS idx_photos_folder ON photos(folder)",
                "CREATE INDEX IF NOT EXISTS idx_photos_size ON photos(size)",
                "CREATE INDEX IF NOT EXISTS idx_videos_hash ON videos(hash) WHERE hash IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS idx_videos_folder ON videos(folder)",
                "CREATE INDEX IF NOT EXISTS idx_videos_size ON videos(size)",
            ]
            
            for index_sql in indexes:
                self.conn.execute(index_sql)
            
            self.conn.commit()
            logger.debug("Database indexes created")
            
        except sqlite3.Error as e:
            logger.warning(f"Failed to create indexes: {e}")
    
    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        try:
            yield self.conn
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Transaction failed, rolling back: {e}")
            raise
    
    # ---------------------------
    # Insert Operations
    # ---------------------------
    
    def insert_photo(self, path: str, size: int, hash_value: str, exif: Optional[Dict[str, Any]] = None) -> bool:
        """
        Insert a single photo record.
        
        Args:
            path: Full path to photo file
            size: File size in bytes
            hash_value: Hash of file content
            exif: Optional EXIF metadata dictionary
            
        Returns:
            True if inserted, False if already exists
        """
        exif = exif or {}
        folder = str(Path(path).parent)
        
        try:
            self.conn.execute("""
                INSERT OR IGNORE INTO photos(
                    path, folder, size, hash, date_taken, camera_model, 
                    gps_lat, gps_lon, orientation, width, height
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                path,
                folder,
                size,
                hash_value,
                exif.get("date_taken"),
                exif.get("camera_model"),
                exif.get("gps_lat"),
                exif.get("gps_lon"),
                exif.get("orientation"),
                exif.get("width"),
                exif.get("height")
            ))
            return self.conn.total_changes > 0
            
        except sqlite3.Error as e:
            logger.error(f"Failed to insert photo {path}: {e}")
            return False
    
    def insert_photos_batch(self, batch: List[Tuple]) -> int:
        """
        Insert multiple photo records in a batch.
        
        Args:
            batch: List of tuples (path, size, hash, exif_dict)
            
        Returns:
            Number of records inserted
        """
        if not batch:
            return 0
        
        data = []
        for item in batch:
            path, size, hash_value = item[:3]
            exif = item[3] if len(item) > 3 else {}
            folder = str(Path(path).parent)
            
            data.append((
                path, folder, size, hash_value,
                exif.get("date_taken"),
                exif.get("camera_model"),
                exif.get("gps_lat"),
                exif.get("gps_lon"),
                exif.get("orientation"),
                exif.get("width"),
                exif.get("height")
            ))
        
        try:
            cursor = self.conn.executemany("""
                INSERT OR IGNORE INTO photos(
                    path, folder, size, hash, date_taken, camera_model,
                    gps_lat, gps_lon, orientation, width, height
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, data)
            
            return cursor.rowcount
            
        except sqlite3.Error as e:
            logger.error(f"Failed to insert photo batch: {e}")
            return 0
    
    def insert_video(self, path: str, size: int, hash_value: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Insert a single video record.
        
        Args:
            path: Full path to video file
            size: File size in bytes
            hash_value: Hash of file content
            metadata: Optional video metadata dictionary
            
        Returns:
            True if inserted, False if already exists
        """
        metadata = metadata or {}
        folder = str(Path(path).parent)
        
        try:
            self.conn.execute("""
                INSERT OR IGNORE INTO videos(
                    path, folder, size, hash, duration, width, height
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                path,
                folder,
                size,
                hash_value,
                metadata.get("duration"),
                metadata.get("width"),
                metadata.get("height")
            ))
            return self.conn.total_changes > 0
            
        except sqlite3.Error as e:
            logger.error(f"Failed to insert video {path}: {e}")
            return False
    
    def insert_videos_batch(self, batch: List[Tuple]) -> int:
        """
        Insert multiple video records in a batch.
        
        Args:
            batch: List of tuples (path, size, hash) or (path, size, hash, metadata_dict)
            
        Returns:
            Number of records inserted
        """
        if not batch:
            return 0
        
        data = []
        for item in batch:
            path, size, hash_value = item[:3]
            metadata = item[3] if len(item) > 3 else {}
            folder = str(Path(path).parent)
            
            data.append((
                path, folder, size, hash_value,
                metadata.get("duration"),
                metadata.get("width"),
                metadata.get("height")
            ))
        
        try:
            cursor = self.conn.executemany("""
                INSERT OR IGNORE INTO videos(
                    path, folder, size, hash, duration, width, height
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, data)
            
            return cursor.rowcount
            
        except sqlite3.Error as e:
            logger.error(f"Failed to insert video batch: {e}")
            return 0
    
    def commit(self):
        """Commit pending transactions."""
        try:
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Failed to commit: {e}")
            raise DatabaseError(f"Commit failed: {e}") from e
    
    # ---------------------------
    # Query Operations
    # ---------------------------
    
    def list_all_photos(self) -> List[sqlite3.Row]:
        """Get all photo records."""
        try:
            return self.conn.execute("SELECT * FROM photos ORDER BY id").fetchall()
        except sqlite3.Error as e:
            logger.error(f"Failed to list photos: {e}")
            return []
    
    def list_all_videos(self) -> List[sqlite3.Row]:
        """Get all video records."""
        try:
            return self.conn.execute("SELECT * FROM videos ORDER BY id").fetchall()
        except sqlite3.Error as e:
            logger.error(f"Failed to list videos: {e}")
            return []
    
    def list_all(self) -> List[sqlite3.Row]:
        """Get all photo and video records combined."""
        return list(self.list_all_photos()) + list(self.list_all_videos())
    
    def get_photo_by_path(self, path: str) -> Optional[sqlite3.Row]:
        """Get photo record by file path."""
        try:
            return self.conn.execute(
                "SELECT * FROM photos WHERE path = ?", (path,)
            ).fetchone()
        except sqlite3.Error as e:
            logger.error(f"Failed to get photo by path: {e}")
            return None
    
    def get_video_by_path(self, path: str) -> Optional[sqlite3.Row]:
        """Get video record by file path."""
        try:
            return self.conn.execute(
                "SELECT * FROM videos WHERE path = ?", (path,)
            ).fetchone()
        except sqlite3.Error as e:
            logger.error(f"Failed to get video by path: {e}")
            return None
    
    # ---------------------------
    # Duplicate Detection
    # ---------------------------
    
    def duplicate_photo_groups(self) -> Dict[str, List[Dict]]:
        """
        Find duplicate photo groups.
        
        Returns:
            Dictionary mapping hash -> list of file records
        """
        try:
            # Use CTE for better performance
            rows = self.conn.execute("""
                WITH duplicate_hashes AS (
                    SELECT hash 
                    FROM photos 
                    WHERE hash IS NOT NULL
                    GROUP BY hash 
                    HAVING COUNT(*) > 1
                )
                SELECT p.hash, p.path, p.size, p.folder, p.date_taken
                FROM photos p
                INNER JOIN duplicate_hashes dh ON p.hash = dh.hash
                ORDER BY p.hash, p.path
            """).fetchall()
            
            groups = {}
            for r in rows:
                hash_key = r["hash"]
                groups.setdefault(hash_key, []).append(dict(r))
            
            return groups
            
        except sqlite3.Error as e:
            logger.error(f"Failed to find duplicate photos: {e}")
            return {}
    
    def duplicate_video_groups(self) -> Dict[str, List[Dict]]:
        """
        Find duplicate video groups.
        
        Returns:
            Dictionary mapping hash -> list of file records
        """
        try:
            # Use CTE for better performance
            rows = self.conn.execute("""
                WITH duplicate_hashes AS (
                    SELECT hash 
                    FROM videos 
                    WHERE hash IS NOT NULL
                    GROUP BY hash 
                    HAVING COUNT(*) > 1
                )
                SELECT v.hash, v.path, v.size, v.folder, v.duration
                FROM videos v
                INNER JOIN duplicate_hashes dh ON v.hash = dh.hash
                ORDER BY v.hash, v.path
            """).fetchall()
            
            groups = {}
            for r in rows:
                hash_key = r["hash"]
                groups.setdefault(hash_key, []).append(dict(r))
            
            return groups
            
        except sqlite3.Error as e:
            logger.error(f"Failed to find duplicate videos: {e}")
            return {}
    
    def count_duplicates_photos(self) -> int:
        """Count total duplicate photo files (excluding one original per group)."""
        try:
            result = self.conn.execute("""
                SELECT SUM(cnt - 1) as total_dups
                FROM (
                    SELECT COUNT(*) as cnt
                    FROM photos
                    WHERE hash IS NOT NULL
                    GROUP BY hash
                    HAVING cnt > 1
                )
            """).fetchone()
            
            return result["total_dups"] or 0
            
        except sqlite3.Error as e:
            logger.error(f"Failed to count duplicate photos: {e}")
            return 0
    
    def count_duplicates_videos(self) -> int:
        """Count total duplicate video files (excluding one original per group)."""
        try:
            result = self.conn.execute("""
                SELECT SUM(cnt - 1) as total_dups
                FROM (
                    SELECT COUNT(*) as cnt
                    FROM videos
                    WHERE hash IS NOT NULL
                    GROUP BY hash
                    HAVING cnt > 1
                )
            """).fetchone()
            
            return result["total_dups"] or 0
            
        except sqlite3.Error as e:
            logger.error(f"Failed to count duplicate videos: {e}")
            return 0
    
    # ---------------------------
    # Statistics and Reports
    # ---------------------------
    
    def get_global_stats(self) -> Dict[str, Any]:
        """
        Get global statistics for photos and videos.
        
        Returns:
            Dictionary with total files, sizes, duplicate counts, and lost space
        """
        try:
            cursor = self.conn.cursor()
            
            # Photos statistics
            cursor.execute("SELECT COUNT(*) AS total_files, COALESCE(SUM(size), 0) AS total_size FROM photos")
            photo_totals = cursor.fetchone()
            
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT hash) AS dup_groups,
                    COALESCE(SUM(total_size - min_size), 0) AS lost_space
                FROM (
                    SELECT hash, COUNT(*) AS cnt, SUM(size) AS total_size, MIN(size) AS min_size
                    FROM photos
                    WHERE hash IS NOT NULL
                    GROUP BY hash
                    HAVING cnt > 1
                )
            """)
            photo_dups = cursor.fetchone()
            
            # Videos statistics
            cursor.execute("SELECT COUNT(*) AS total_files, COALESCE(SUM(size), 0) AS total_size FROM videos")
            video_totals = cursor.fetchone()
            
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT hash) AS dup_groups,
                    COALESCE(SUM(total_size - min_size), 0) AS lost_space
                FROM (
                    SELECT hash, COUNT(*) AS cnt, SUM(size) AS total_size, MIN(size) AS min_size
                    FROM videos
                    WHERE hash IS NOT NULL
                    GROUP BY hash
                    HAVING cnt > 1
                )
            """)
            video_dups = cursor.fetchone()
            
            return {
                "total_files_photos": photo_totals["total_files"],
                "total_size_photos": photo_totals["total_size"],
                "duplicate_photo_groups": photo_dups["dup_groups"] or 0,
                "lost_space_photos": photo_dups["lost_space"] or 0,
                "total_files_videos": video_totals["total_files"],
                "total_size_videos": video_totals["total_size"],
                "duplicate_video_groups": video_dups["dup_groups"] or 0,
                "lost_space_videos": video_dups["lost_space"] or 0,
            }
            
        except sqlite3.Error as e:
            logger.error(f"Failed to get global stats: {e}")
            return {
                "total_files_photos": 0,
                "total_size_photos": 0,
                "duplicate_photo_groups": 0,
                "lost_space_photos": 0,
                "total_files_videos": 0,
                "total_size_videos": 0,
                "duplicate_video_groups": 0,
                "lost_space_videos": 0,
            }
    
    def get_folders_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Get per-folder statistics for photos and videos.
        
        Returns:
            Dictionary mapping folder path -> statistics dict
        """
        try:
            cursor = self.conn.cursor()
            folders = {}
            
            # Photos - using CTE for better performance
            cursor.execute("""
                WITH duplicate_hashes AS (
                    SELECT hash
                    FROM photos
                    WHERE hash IS NOT NULL
                    GROUP BY hash
                    HAVING COUNT(*) > 1
                )
                SELECT 
                    folder,
                    COUNT(*) AS file_count,
                    SUM(CASE WHEN hash IN (SELECT hash FROM duplicate_hashes) THEN 1 ELSE 0 END) AS dup_count,
                    SUM(CASE WHEN hash IN (SELECT hash FROM duplicate_hashes) THEN size ELSE 0 END) AS lost_bytes
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
                WITH duplicate_hashes AS (
                    SELECT hash
                    FROM videos
                    WHERE hash IS NOT NULL
                    GROUP BY hash
                    HAVING COUNT(*) > 1
                )
                SELECT 
                    folder,
                    COUNT(*) AS file_count,
                    SUM(CASE WHEN hash IN (SELECT hash FROM duplicate_hashes) THEN 1 ELSE 0 END) AS dup_count,
                    SUM(CASE WHEN hash IN (SELECT hash FROM duplicate_hashes) THEN size ELSE 0 END) AS lost_bytes
                FROM videos
                GROUP BY folder
            """)
            
            for row in cursor.fetchall():
                if row["folder"] not in folders:
                    folders[row["folder"]] = {
                        "photos_count": 0,
                        "photos_dup_count": 0,
                        "photos_lost_bytes": 0,
                        "videos_count": row["file_count"] or 0,
                        "videos_dup_count": row["dup_count"] or 0,
                        "videos_lost_bytes": row["lost_bytes"] or 0
                    }
                else:
                    folders[row["folder"]].update({
                        "videos_count": row["file_count"] or 0,
                        "videos_dup_count": row["dup_count"] or 0,
                        "videos_lost_bytes": row["lost_bytes"] or 0
                    })
            
            return folders
            
        except sqlite3.Error as e:
            logger.error(f"Failed to get folder stats: {e}")
            return {}
    
    def get_folder_hash_map(self) -> Dict[str, set]:
        """
        Get mapping of folders to their file hashes.
        
        Returns:
            Dictionary mapping folder path -> set of hashes
        """
        try:
            folder_map = {}
            
            # Get photo hashes
            rows = self.conn.execute(
                "SELECT path, hash FROM photos WHERE hash IS NOT NULL"
            ).fetchall()
            
            for r in rows:
                folder = str(Path(r["path"]).parent)
                folder_map.setdefault(folder, set()).add(r["hash"])
            
            # Get video hashes
            rows = self.conn.execute(
                "SELECT path, hash FROM videos WHERE hash IS NOT NULL"
            ).fetchall()
            
            for r in rows:
                folder = str(Path(r["path"]).parent)
                folder_map.setdefault(folder, set()).add(r["hash"])
            
            return folder_map
            
        except sqlite3.Error as e:
            logger.error(f"Failed to get folder hash map: {e}")
            return {}
    
    # ---------------------------
    # Utility Methods
    # ---------------------------
    
    def vacuum(self):
        """Optimize database by reclaiming unused space."""
        try:
            self.conn.execute("VACUUM")
            logger.info("Database vacuumed successfully")
        except sqlite3.Error as e:
            logger.warning(f"Failed to vacuum database: {e}")
    
    def get_database_size(self) -> int:
        """Get database file size in bytes."""
        try:
            return os.path.getsize(self.db_path)
        except OSError as e:
            logger.error(f"Failed to get database size: {e}")
            return 0
    
    def delete_all_data(self):
        """Delete all records from all tables (but keep schema)."""
        try:
            with self.transaction():
                self.conn.execute("DELETE FROM photos")
                self.conn.execute("DELETE FROM videos")
                self.conn.execute("DELETE FROM metadata")
                logger.info("All data deleted from database")
        except sqlite3.Error as e:
            logger.error(f"Failed to delete data: {e}")
            raise DatabaseError(f"Failed to delete data: {e}") from e
    
    def close(self):
        """Close database connection."""
        if self.conn:
            try:
                self.conn.close()
                logger.debug("Database connection closed")
            except sqlite3.Error as e:
                logger.error(f"Error closing database: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def __del__(self):
        """Destructor to ensure connection is closed."""
        self.close()


if __name__ == "__main__":
    # Example usage
    print("=== Database Module Test ===")
    
    with Database() as db:
        print(f"Database created at: {db.db_path}")
        print(f"Database size: {db.get_database_size()} bytes")
        
        # Test insert
        db.insert_photo("/test/photo.jpg", 1024000, "abc123", {
            "date_taken": "2024-01-01 12:00:00",
            "camera_model": "Test Camera"
        })
        db.commit()
        
        # Test query
        stats = db.get_global_stats()
        print(f"Total photos: {stats['total_files_photos']}")