"""
Advanced file comparison module for photo deduplication.

This module provides perceptual hashing and similarity detection
for finding near-duplicate images beyond exact hash matching.
"""

import logging
from typing import List, Tuple, Dict, Optional, Set
from pathlib import Path
from dataclasses import dataclass
from collections import defaultdict

logger = logging.getLogger("photo_dedup")

# Try to import optional dependencies for perceptual hashing
try:
    from PIL import Image
    import imagehash
    PERCEPTUAL_HASH_AVAILABLE = True
except ImportError:
    PERCEPTUAL_HASH_AVAILABLE = False
    logger.debug("Perceptual hashing not available (install imagehash: pip install imagehash)")


@dataclass
class DuplicateGroup:
    """Represents a group of duplicate files."""
    hash_value: str
    files: List[Dict]
    total_size: int
    wasted_space: int
    file_type: str  # 'photo' or 'video'
    
    def __len__(self) -> int:
        """Return number of files in the group."""
        return len(self.files)
    
    @property
    def duplicate_count(self) -> int:
        """Number of duplicate files (excluding master)."""
        return len(self.files) - 1


@dataclass
class SimilarityMatch:
    """Represents a similarity match between two files."""
    file1: str
    file2: str
    similarity: float
    hash1: Optional[str] = None
    hash2: Optional[str] = None
    distance: int = 0


class FileComparer:
    """
    Advanced file comparison for finding duplicates and similar files.
    
    Provides both exact matching (content hash) and perceptual matching
    (visual similarity for images).
    """
    
    def __init__(self, db):
        """
        Initialize the file comparer.
        
        Args:
            db: Database instance
        """
        self.db = db
        self._cache_exact_duplicates = None
    
    def find_exact_duplicates(self, file_type: str = "all") -> List[DuplicateGroup]:
        """
        Find exact duplicate files based on content hash.
        
        Args:
            file_type: Type of files to check ('photo', 'video', or 'all')
            
        Returns:
            List of DuplicateGroup objects
        """
        groups = []
        
        # Get photo duplicates
        if file_type in ("photo", "all"):
            photo_groups = self.db.duplicate_photo_groups()
            for hash_value, files in photo_groups.items():
                total_size = sum(f["size"] for f in files)
                min_size = min(f["size"] for f in files)
                wasted = total_size - min_size
                
                groups.append(DuplicateGroup(
                    hash_value=hash_value,
                    files=files,
                    total_size=total_size,
                    wasted_space=wasted,
                    file_type="photo"
                ))
        
        # Get video duplicates
        if file_type in ("video", "all"):
            video_groups = self.db.duplicate_video_groups()
            for hash_value, files in video_groups.items():
                total_size = sum(f["size"] for f in files)
                min_size = min(f["size"] for f in files)
                wasted = total_size - min_size
                
                groups.append(DuplicateGroup(
                    hash_value=hash_value,
                    files=files,
                    total_size=total_size,
                    wasted_space=wasted,
                    file_type="video"
                ))
        
        return groups
    
    def get_duplicate_statistics(self) -> Dict[str, int]:
        """
        Get comprehensive duplicate statistics.
        
        Returns:
            Dictionary with statistics about duplicates
        """
        groups = self.find_exact_duplicates()
        
        photo_groups = [g for g in groups if g.file_type == "photo"]
        video_groups = [g for g in groups if g.file_type == "video"]
        
        return {
            "total_duplicate_groups": len(groups),
            "photo_duplicate_groups": len(photo_groups),
            "video_duplicate_groups": len(video_groups),
            "total_duplicate_files": sum(g.duplicate_count for g in groups),
            "photo_duplicate_files": sum(g.duplicate_count for g in photo_groups),
            "video_duplicate_files": sum(g.duplicate_count for g in video_groups),
            "total_wasted_space": sum(g.wasted_space for g in groups),
            "photo_wasted_space": sum(g.wasted_space for g in photo_groups),
            "video_wasted_space": sum(g.wasted_space for g in video_groups),
        }
    
    def find_duplicates_in_folder(self, folder: str) -> List[DuplicateGroup]:
        """
        Find duplicates within a specific folder.
        
        Args:
            folder: Path to folder
            
        Returns:
            List of duplicate groups where all files are in the folder
        """
        all_groups = self.find_exact_duplicates()
        folder_groups = []
        
        for group in all_groups:
            # Check if all files in the group are in the specified folder
            if all(Path(f["path"]).parent == Path(folder) for f in group.files):
                folder_groups.append(group)
        
        return folder_groups
    
    def find_duplicates_across_folders(self) -> List[DuplicateGroup]:
        """
        Find duplicates that span multiple folders.
        
        Returns:
            List of duplicate groups where files are in different folders
        """
        all_groups = self.find_exact_duplicates()
        cross_folder_groups = []
        
        for group in all_groups:
            # Check if files are in different folders
            folders = {Path(f["path"]).parent for f in group.files}
            if len(folders) > 1:
                cross_folder_groups.append(group)
        
        return cross_folder_groups
    
    def get_largest_duplicates(self, limit: int = 10) -> List[DuplicateGroup]:
        """
        Get duplicate groups with the most wasted space.
        
        Args:
            limit: Maximum number of groups to return
            
        Returns:
            List of duplicate groups sorted by wasted space
        """
        groups = self.find_exact_duplicates()
        sorted_groups = sorted(groups, key=lambda g: g.wasted_space, reverse=True)
        return sorted_groups[:limit]
    
    def compare_folders_by_content(self, folder1: str, folder2: str) -> Dict:
        """
        Compare two folders to find shared and unique files.
        
        Args:
            folder1: Path to first folder
            folder2: Path to second folder
            
        Returns:
            Dictionary with comparison results
        """
        # Get all files from each folder
        folder1_hashes = set()
        folder2_hashes = set()
        
        for row in self.db.list_all_photos():
            if str(Path(row["path"]).parent) == folder1:
                folder1_hashes.add(row["hash"])
            elif str(Path(row["path"]).parent) == folder2:
                folder2_hashes.add(row["hash"])
        
        for row in self.db.list_all_videos():
            if str(Path(row["path"]).parent) == folder1:
                folder1_hashes.add(row["hash"])
            elif str(Path(row["path"]).parent) == folder2:
                folder2_hashes.add(row["hash"])
        
        shared = folder1_hashes & folder2_hashes
        unique_to_1 = folder1_hashes - folder2_hashes
        unique_to_2 = folder2_hashes - folder1_hashes
        
        total_unique = len(folder1_hashes | folder2_hashes)
        similarity = len(shared) / total_unique if total_unique > 0 else 0
        
        return {
            "folder1": folder1,
            "folder2": folder2,
            "folder1_total": len(folder1_hashes),
            "folder2_total": len(folder2_hashes),
            "shared_files": len(shared),
            "unique_to_folder1": len(unique_to_1),
            "unique_to_folder2": len(unique_to_2),
            "similarity": similarity
        }


class PerceptualComparer:
    """
    Perceptual comparison for finding visually similar images.
    
    Uses perceptual hashing to find images that look similar but may have
    different file sizes due to compression, format, or editing.
    """
    
    def __init__(self, db):
        """
        Initialize the perceptual comparer.
        
        Args:
            db: Database instance
            
        Raises:
            ImportError: If imagehash library is not available
        """
        if not PERCEPTUAL_HASH_AVAILABLE:
            raise ImportError(
                "Perceptual hashing requires imagehash library. "
                "Install with: pip install imagehash"
            )
        
        self.db = db
        self._phash_cache: Dict[str, str] = {}
    
    def compute_perceptual_hash(self, image_path: str, hash_size: int = 8) -> Optional[str]:
        """
        Compute perceptual hash for an image.
        
        Args:
            image_path: Path to image file
            hash_size: Size of hash (default: 8, higher = more precise)
            
        Returns:
            Perceptual hash as string, or None if failed
        """
        # Check cache first
        if image_path in self._phash_cache:
            return self._phash_cache[image_path]
        
        try:
            with Image.open(image_path) as img:
                # Use average hash (aHash) - fast and good for duplicates
                phash = imagehash.average_hash(img, hash_size=hash_size)
                phash_str = str(phash)
                self._phash_cache[image_path] = phash_str
                return phash_str
        
        except Exception as e:
            logger.debug(f"Failed to compute perceptual hash for {image_path}: {e}")
            return None
    
    def hamming_distance(self, hash1: str, hash2: str) -> int:
        """
        Calculate Hamming distance between two hashes.
        
        Args:
            hash1: First hash string
            hash2: Second hash string
            
        Returns:
            Hamming distance (number of different bits)
        """
        if len(hash1) != len(hash2):
            raise ValueError("Hashes must be same length")
        
        return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))
    
    def find_visual_duplicates(self, max_distance: int = 5) -> List[List[str]]:
        """
        Find visually similar images using perceptual hashing.
        
        Args:
            max_distance: Maximum Hamming distance to consider similar (default: 5)
                         Lower = more strict, higher = more permissive
                         
        Returns:
            List of groups, where each group is a list of similar image paths
        """
        logger.info("Computing perceptual hashes for all photos...")
        
        # Get all photos
        photos = self.db.list_all_photos()
        
        if not photos:
            return []
        
        # Compute perceptual hashes
        photo_hashes = []
        for i, photo in enumerate(photos):
            if i % 100 == 0:
                logger.debug(f"Processing {i}/{len(photos)} photos...")
            
            phash = self.compute_perceptual_hash(photo["path"])
            if phash:
                photo_hashes.append((photo["path"], phash))
        
        logger.info(f"Computed hashes for {len(photo_hashes)} photos")
        
        # Find similar groups using clustering
        groups = []
        used_indices = set()
        
        for i, (path1, hash1) in enumerate(photo_hashes):
            if i in used_indices:
                continue
            
            group = [path1]
            used_indices.add(i)
            
            for j in range(i + 1, len(photo_hashes)):
                if j in used_indices:
                    continue
                
                path2, hash2 = photo_hashes[j]
                distance = self.hamming_distance(hash1, hash2)
                
                if distance <= max_distance:
                    group.append(path2)
                    used_indices.add(j)
            
            if len(group) > 1:
                groups.append(group)
        
        logger.info(f"Found {len(groups)} groups of visually similar images")
        return groups
    
    def find_similar_to_image(self, target_path: str, max_distance: int = 10) -> List[SimilarityMatch]:
        """
        Find images similar to a specific target image.
        
        Args:
            target_path: Path to target image
            max_distance: Maximum Hamming distance
            
        Returns:
            List of SimilarityMatch objects sorted by similarity
        """
        target_hash = self.compute_perceptual_hash(target_path)
        if not target_hash:
            logger.error(f"Could not compute hash for target image: {target_path}")
            return []
        
        matches = []
        photos = self.db.list_all_photos()
        
        for photo in photos:
            if photo["path"] == target_path:
                continue
            
            phash = self.compute_perceptual_hash(photo["path"])
            if not phash:
                continue
            
            distance = self.hamming_distance(target_hash, phash)
            
            if distance <= max_distance:
                # Convert distance to similarity score (0-1)
                max_possible = len(target_hash)
                similarity = 1.0 - (distance / max_possible)
                
                matches.append(SimilarityMatch(
                    file1=target_path,
                    file2=photo["path"],
                    similarity=similarity,
                    hash1=target_hash,
                    hash2=phash,
                    distance=distance
                ))
        
        # Sort by similarity (highest first)
        matches.sort(key=lambda m: m.similarity, reverse=True)
        return matches


# Backward compatibility functions
def find_exact_duplicates(db) -> List[Tuple[str, List[str]]]:
    """
    Legacy function for finding exact duplicates.
    
    Args:
        db: Database instance
        
    Returns:
        List of tuples (hash, [paths])
    """
    comparer = FileComparer(db)
    groups = comparer.find_exact_duplicates()
    
    return [(g.hash_value, [f["path"] for f in g.files]) for g in groups]


def find_visual_duplicates(db, max_distance: int = 5) -> List[List[str]]:
    """
    Legacy function for finding visual duplicates.
    
    Args:
        db: Database instance
        max_distance: Maximum Hamming distance
        
    Returns:
        List of groups of similar image paths
    """
    if not PERCEPTUAL_HASH_AVAILABLE:
        logger.error("Perceptual hashing not available. Install imagehash library.")
        return []
    
    comparer = PerceptualComparer(db)
    return comparer.find_visual_duplicates(max_distance)


if __name__ == "__main__":
    import sys
    from db import Database
    
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s"
    )
    
    if len(sys.argv) < 2:
        print("Usage: python comparer.py <command> [options]")
        print("\nCommands:")
        print("  exact [db_path]              - Find exact duplicates")
        print("  visual [db_path] [distance]  - Find visually similar images")
        print("  stats [db_path]              - Show duplicate statistics")
        sys.exit(1)
    
    command = sys.argv[1]
    db_path = sys.argv[2] if len(sys.argv) > 2 else None
    
    with Database(db_path) as db:
        if command == "exact":
            comparer = FileComparer(db)
            groups = comparer.find_exact_duplicates()
            
            print(f"\nFound {len(groups)} duplicate groups:\n")
            for group in groups[:10]:  # Show first 10
                print(f"Hash: {group.hash_value}")
                print(f"  Files: {len(group.files)}")
                print(f"  Wasted space: {group.wasted_space:,} bytes")
                for f in group.files:
                    print(f"    - {f['path']}")
                print()
        
        elif command == "visual":
            if not PERCEPTUAL_HASH_AVAILABLE:
                print("Error: imagehash library not installed")
                print("Install with: pip install imagehash")
                sys.exit(1)
            
            max_distance = int(sys.argv[3]) if len(sys.argv) > 3 else 5
            comparer = PerceptualComparer(db)
            groups = comparer.find_visual_duplicates(max_distance)
            
            print(f"\nFound {len(groups)} groups of similar images:\n")
            for i, group in enumerate(groups[:10], 1):
                print(f"Group {i}: {len(group)} similar images")
                for path in group:
                    print(f"  - {path}")
                print()
        
        elif command == "stats":
            comparer = FileComparer(db)
            stats = comparer.get_duplicate_statistics()
            
            print("\n=== Duplicate Statistics ===")
            print(f"Total duplicate groups: {stats['total_duplicate_groups']}")
            print(f"  Photo groups: {stats['photo_duplicate_groups']}")
            print(f"  Video groups: {stats['video_duplicate_groups']}")
            print(f"\nTotal duplicate files: {stats['total_duplicate_files']}")
            print(f"  Photo duplicates: {stats['photo_duplicate_files']}")
            print(f"  Video duplicates: {stats['video_duplicate_files']}")
            print(f"\nTotal wasted space: {stats['total_wasted_space']:,} bytes")
            print(f"  Photos: {stats['photo_wasted_space']:,} bytes")
            print(f"  Videos: {stats['video_wasted_space']:,} bytes")
        
        else:
            print(f"Unknown command: {command}")
            sys.exit(1)