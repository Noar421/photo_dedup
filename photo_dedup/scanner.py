"""
File scanning module for photo deduplication.

This module handles recursive directory scanning, multi-threaded file hashing,
and batch insertion into the database.
"""

import os
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Any, Optional, Set
from dataclasses import dataclass
from collections import defaultdict

from hashing import file_hash
from utils import extract_exif, is_photo, is_video

logger = logging.getLogger("photo_dedup")


@dataclass
class ScanStats:
    """Statistics for a scan operation."""
    total_photos: int = 0
    total_videos: int = 0
    processed_photos: int = 0
    processed_videos: int = 0
    skipped_files: int = 0
    failed_files: int = 0
    folders_scanned: int = 0
    
    @property
    def total_files(self) -> int:
        """Total number of media files found."""
        return self.total_photos + self.total_videos
    
    @property
    def total_processed(self) -> int:
        """Total number of files successfully processed."""
        return self.processed_photos + self.processed_videos
    
    def __str__(self) -> str:
        """String representation of scan statistics."""
        return (
            f"Scan Statistics:\n"
            f"  Photos: {self.processed_photos}/{self.total_photos} processed\n"
            f"  Videos: {self.processed_videos}/{self.total_videos} processed\n"
            f"  Folders: {self.folders_scanned}\n"
            f"  Skipped: {self.skipped_files}\n"
            f"  Failed: {self.failed_files}"
        )


class ScanError(Exception):
    """Exception raised for scan errors."""
    pass


class FileScanner:
    """
    File scanner for photo and video files.
    
    Handles recursive directory scanning, multi-threaded hashing,
    and batch database insertion.
    """
    
    def __init__(self, db, batch_size: int = 200, threads: int = 4, skip_existing: bool = True):
        """
        Initialize the file scanner.
        
        Args:
            db: Database instance
            batch_size: Number of files to batch before committing to database
            threads: Number of worker threads for hashing
            skip_existing: Skip files that are already in the database
        """
        self.db = db
        self.batch_size = max(1, batch_size)  # Ensure positive
        self.threads = max(1, min(threads, 32))  # Limit between 1-32
        self.skip_existing = skip_existing
        self.stats = ScanStats()
        
        # Cache of existing file paths in database
        self._existing_paths: Optional[Set[str]] = None
    
    def _load_existing_paths(self):
        """Load existing file paths from database into cache."""
        if self._existing_paths is None:
            self._existing_paths = set()
            
            try:
                # Load photo paths
                for row in self.db.list_all_photos():
                    self._existing_paths.add(row["path"])
                
                # Load video paths
                for row in self.db.list_all_videos():
                    self._existing_paths.add(row["path"])
                
                logger.info(f"Loaded {len(self._existing_paths)} existing file paths from database")
            except Exception as e:
                logger.error(f"Failed to load existing paths: {e}")
                self._existing_paths = set()
    
    def _is_path_indexed(self, path: str) -> bool:
        """Check if a file path is already indexed in the database."""
        if not self.skip_existing:
            return False
        
        if self._existing_paths is None:
            self._load_existing_paths()
        
        return path in self._existing_paths
    
    def _collect_files(self, folder: str) -> Tuple[List[str], List[str], Dict[str, int]]:
        """
        Collect photo and video files from folder recursively.
        
        Args:
            folder: Root folder to scan
            
        Returns:
            Tuple of (photo_paths, video_paths, folder_counts)
        """
        photo_files = []
        video_files = []
        folder_counts_photos = defaultdict(int)
        folder_counts_videos = defaultdict(int)
        
        logger.info(f"Collecting files from: {folder}")
        
        try:
            for root, dirs, files in os.walk(folder):
                # Skip hidden directories
                dirs[:] = [d for d in dirs if not d.startswith('.')]
                
                for filename in files:
                    # Skip hidden files
                    if filename.startswith('.'):
                        continue
                    
                    filepath = os.path.join(root, filename)
                    
                    # Skip if already indexed
                    if self._is_path_indexed(filepath):
                        self.stats.skipped_files += 1
                        continue
                    
                    try:
                        if is_photo(filepath):
                            photo_files.append(filepath)
                            folder_counts_photos[root] += 1
                        elif is_video(filepath):
                            video_files.append(filepath)
                            folder_counts_videos[root] += 1
                    except Exception as e:
                        logger.debug(f"Error checking file type for {filepath}: {e}")
            
            self.stats.total_photos = len(photo_files)
            self.stats.total_videos = len(video_files)
            self.stats.folders_scanned = len(set(list(folder_counts_photos.keys()) + list(folder_counts_videos.keys())))
            
            return photo_files, video_files, {
                'photos': dict(folder_counts_photos),
                'videos': dict(folder_counts_videos)
            }
            
        except PermissionError as e:
            logger.error(f"Permission denied accessing folder: {folder}")
            raise ScanError(f"Permission denied: {folder}") from e
        except Exception as e:
            logger.error(f"Error collecting files from {folder}: {e}")
            raise ScanError(f"Failed to collect files: {e}") from e
    
    def _process_photo(self, path: str) -> Optional[Tuple[str, int, str, Dict[str, Any]]]:
        """
        Process a single photo file.
        
        Args:
            path: Path to photo file
            
        Returns:
            Tuple of (path, size, hash, exif) or None if failed
        """
        try:
            # Check if file still exists
            if not os.path.exists(path):
                logger.warning(f"File disappeared: {path}")
                return None
            
            # Get file size
            size = os.path.getsize(path)
            
            # Compute hash
            hash_value = file_hash(path)
            
            # Extract EXIF data
            exif = extract_exif(path)
            
            return path, size, hash_value, exif
            
        except PermissionError:
            logger.warning(f"Permission denied: {path}")
            self.stats.failed_files += 1
            return None
        except Exception as e:
            logger.error(f"Failed to process photo {path}: {e}")
            self.stats.failed_files += 1
            return None
    
    def _process_video(self, path: str) -> Optional[Tuple[str, int, str]]:
        """
        Process a single video file.
        
        Args:
            path: Path to video file
            
        Returns:
            Tuple of (path, size, hash) or None if failed
        """
        try:
            # Check if file still exists
            if not os.path.exists(path):
                logger.warning(f"File disappeared: {path}")
                return None
            
            # Get file size
            size = os.path.getsize(path)
            
            # Compute hash
            hash_value = file_hash(path)
            
            return path, size, hash_value
            
        except PermissionError:
            logger.warning(f"Permission denied: {path}")
            self.stats.failed_files += 1
            return None
        except Exception as e:
            logger.error(f"Failed to process video {path}: {e}")
            self.stats.failed_files += 1
            return None
    
    def _process_photos_batch(self, photo_files: List[str]) -> int:
        """
        Process photo files in parallel with batched database inserts.
        
        Args:
            photo_files: List of photo file paths
            
        Returns:
            Number of photos processed
        """
        if not photo_files:
            return 0
        
        logger.info(f"Processing {len(photo_files)} photos with {self.threads} threads...")
        
        batch = []
        count = 0
        
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            # Submit all tasks
            futures = {executor.submit(self._process_photo, path): path for path in photo_files}
            
            # Process results as they complete
            for future in as_completed(futures):
                result = future.result()
                
                if result:
                    batch.append(result)
                    count += 1
                    
                    # Insert batch when it reaches batch_size
                    if len(batch) >= self.batch_size:
                        try:
                            inserted = self.db.insert_photos_batch(batch)
                            self.db.commit()
                            self.stats.processed_photos += inserted
                            
                            # Log progress
                            progress_pct = (count / len(photo_files)) * 100
                            logger.info(f"Photos: {count}/{len(photo_files)} ({progress_pct:.1f}%) - {inserted} inserted")
                            
                        except Exception as e:
                            logger.error(f"Failed to insert photo batch: {e}")
                        
                        batch = []
        
        # Insert remaining batch
        if batch:
            try:
                inserted = self.db.insert_photos_batch(batch)
                self.db.commit()
                self.stats.processed_photos += inserted
                logger.info(f"Photos: {count}/{len(photo_files)} (100%) - {inserted} inserted")
            except Exception as e:
                logger.error(f"Failed to insert final photo batch: {e}")
        
        return count
    
    def _process_videos_batch(self, video_files: List[str]) -> int:
        """
        Process video files in parallel with batched database inserts.
        
        Args:
            video_files: List of video file paths
            
        Returns:
            Number of videos processed
        """
        if not video_files:
            return 0
        
        logger.info(f"Processing {len(video_files)} videos with {self.threads} threads...")
        
        batch = []
        count = 0
        
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            # Submit all tasks
            futures = {executor.submit(self._process_video, path): path for path in video_files}
            
            # Process results as they complete
            for future in as_completed(futures):
                result = future.result()
                
                if result:
                    batch.append(result)
                    count += 1
                    
                    # Insert batch when it reaches batch_size
                    if len(batch) >= self.batch_size:
                        try:
                            inserted = self.db.insert_videos_batch(batch)
                            self.db.commit()
                            self.stats.processed_videos += inserted
                            
                            # Log progress
                            progress_pct = (count / len(video_files)) * 100
                            logger.info(f"Videos: {count}/{len(video_files)} ({progress_pct:.1f}%) - {inserted} inserted")
                            
                        except Exception as e:
                            logger.error(f"Failed to insert video batch: {e}")
                        
                        batch = []
        
        # Insert remaining batch
        if batch:
            try:
                inserted = self.db.insert_videos_batch(batch)
                self.db.commit()
                self.stats.processed_videos += inserted
                logger.info(f"Videos: {count}/{len(video_files)} (100%) - {inserted} inserted")
            except Exception as e:
                logger.error(f"Failed to insert final video batch: {e}")
        
        return count
    
    def _log_folder_summary(self, folder_counts: Dict[str, Dict[str, int]]):
        """Log summary of files found per subfolder."""
        photo_counts = folder_counts.get('photos', {})
        video_counts = folder_counts.get('videos', {})
        
        if photo_counts:
            logger.info("\n=== Photos by Subfolder ===")
            max_len = max(len(str(p)) for p in photo_counts.keys())
            for subfolder, count in sorted(photo_counts.items()):
                logger.info(f"  {str(subfolder).ljust(max_len)} | {str(count).rjust(5)} files")
        
        if video_counts:
            logger.info("\n=== Videos by Subfolder ===")
            max_len = max(len(str(p)) for p in video_counts.keys())
            for subfolder, count in sorted(video_counts.items()):
                logger.info(f"  {str(subfolder).ljust(max_len)} | {str(count).rjust(5)} files")
    
    def scan(self, folder: str) -> ScanStats:
        """
        Scan a folder and index all photos and videos.
        
        Args:
            folder: Root folder to scan
            
        Returns:
            ScanStats object with scan results
            
        Raises:
            ScanError: If scan fails
        """
        folder_path = Path(folder).resolve()
        
        if not folder_path.exists():
            raise ScanError(f"Folder does not exist: {folder}")
        
        if not folder_path.is_dir():
            raise ScanError(f"Path is not a directory: {folder}")
        
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"Starting scan: {folder_path}")
        logger.info("=" * 60)
        
        # Collect all files
        photo_files, video_files, folder_counts = self._collect_files(str(folder_path))
        
        logger.info(f"\nFound {len(photo_files)} photos and {len(video_files)} videos")
        if self.stats.skipped_files > 0:
            logger.info(f"Skipped {self.stats.skipped_files} already indexed files")
        
        # Log subfolder summary
        self._log_folder_summary(folder_counts)
        
        # Process photos
        if photo_files:
            logger.info("\n--- Processing Photos ---")
            self._process_photos_batch(photo_files)
        
        # Process videos
        if video_files:
            logger.info("\n--- Processing Videos ---")
            self._process_videos_batch(video_files)
        
        # Final statistics
        logger.info("")
        logger.info("=" * 60)
        logger.info("Scan Complete")
        logger.info("=" * 60)
        logger.info(str(self.stats))
        logger.info("=" * 60)
        
        return self.stats


def scan_folder(folder: str, db, batch_size: int = 200, threads: int = 4) -> int:
    """
    Legacy function for backward compatibility.
    
    Scan folder recursively, compute hashes in threads, write to DB.
    
    Args:
        folder: Root folder to scan
        db: Database instance
        batch_size: Number of files per batch commit
        threads: Number of hashing threads
        
    Returns:
        Total number of files processed
    """
    scanner = FileScanner(db, batch_size=batch_size, threads=threads)
    stats = scanner.scan(folder)
    return stats.total_processed


def scan_multiple_folders(folders: List[str], db, batch_size: int = 200, threads: int = 4) -> Dict[str, ScanStats]:
    """
    Scan multiple folders sequentially.
    
    Args:
        folders: List of folder paths to scan
        db: Database instance
        batch_size: Number of files per batch commit
        threads: Number of hashing threads
        
    Returns:
        Dictionary mapping folder path -> ScanStats
    """
    results = {}
    scanner = FileScanner(db, batch_size=batch_size, threads=threads)
    
    for folder in folders:
        try:
            stats = scanner.scan(folder)
            results[folder] = stats
        except ScanError as e:
            logger.error(f"Failed to scan {folder}: {e}")
            results[folder] = scanner.stats
    
    return results


if __name__ == "__main__":
    # Example usage
    import sys
    from db import Database
    
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        datefmt="%H:%M:%S"
    )
    
    if len(sys.argv) < 2:
        print("Usage: python scanner.py <folder_path>")
        sys.exit(1)
    
    folder_to_scan = sys.argv[1]
    
    with Database() as db:
        scanner = FileScanner(db, batch_size=100, threads=4)
        try:
            stats = scanner.scan(folder_to_scan)
            print(f"\n{stats}")
        except ScanError as e:
            print(f"Scan failed: {e}")
            sys.exit(1)