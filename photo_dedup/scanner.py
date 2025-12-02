import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from hashing import file_hash
from pathlib import Path
from utils import extract_exif

# Allowed photo extensions
PHOTO_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".tiff", ".bmp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".3gp", ".wma", "mpeg4"}

# Global logger
logger = logging.getLogger("photo_dedup")
logger.setLevel(logging.INFO)


def setup_console_logging():
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter("[%(asctime)s] %(message)s", "%H:%M:%S")
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)


def setup_file_logging(db_path):
    log_file = Path(db_path).with_suffix(".log")
    if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)


def is_photo(path):
    # return os.path.splitext(path)[1].lower() in PHOTO_EXTENSIONS
    return Path(path).suffix.lower() in PHOTO_EXTENSIONS

def is_video(path):
    return Path(path).suffix.lower() in VIDEO_EXTENSIONS


def process_file(path):
    """Worker function: compute file size and hash."""
    size = os.path.getsize(path)
    h = file_hash(path)
    return path, size, h


def scan_folder(folder, db, batch_size=200, threads=4):
    """Scan folder recursively, compute hashes in threads, write to DB."""
    setup_console_logging()
    setup_file_logging(db.db_path)

    # Step 1: collect image and video files per subfolder
    image_files = []
    video_files = []
    folder_counts_images = {}
    folder_counts_videos = {}

    for root, dirs, files in os.walk(folder):
        imgs = [os.path.join(root, f) for f in files if is_photo(os.path.join(root, f))]
        vids = [os.path.join(root, f) for f in files if is_video(os.path.join(root, f))]

        if imgs:
            folder_counts_images[root] = len(imgs)
            image_files.extend(imgs)
        if vids:
            folder_counts_videos[root] = len(vids)
            video_files.extend(vids)

    # Log main folder summary
    logger.info(f"Scanning main folder: {folder}")
    logger.info(f"Total number of image files found: {len(image_files)}")
    logger.info(f"Total number of video files found: {len(video_files)}")

    # Log subfolder counts
    max_len = max((len(p) for p in folder_counts_images.keys()), default=0)
    for subfolder, count in folder_counts_images.items():
        logger.info(f"Subfolder (images): {subfolder.ljust(max_len)} | {str(count).rjust(5)} files")
    max_len = max((len(p) for p in folder_counts_images.keys()), default=0)
    for subfolder, count in folder_counts_videos.items():
        logger.info(f"Subfolder (videos): {subfolder.ljust(max_len)} | {str(count).rjust(5)} files")

    # --- Existing image processing code remains unchanged ---
    count = 0
    batch = []
    if image_files:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = {executor.submit(process_file, path): path for path in image_files}
            for future in as_completed(futures):
                path, size, h = future.result()
                exif = extract_exif(path)
                batch.append((path, size, h, exif))
                count += 1
                if len(batch) >= batch_size:
                    for item in batch:
                        db.insert_photo(item[0], item[1], item[2], item[3])
                    db.commit()
                    logger.info(f"Processed {count}/{len(image_files)} image files...")
                    batch = []
        if batch:
            for item in batch:
                db.insert_photo(item[0], item[1], item[2], item[3])
            db.commit()

    count_videos = 0
    batch = []
    if video_files:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = {executor.submit(process_file, path): path for path in video_files}
            for future in as_completed(futures):
                path, size, h = future.result()
                batch.append((path, size, h))
                count_videos += 1
                if len(batch) >= batch_size:
                    db.insert_videos_batch(batch)  # insert into separate video table
                    db.commit()
                    logger.info(f"Processed {count_videos}/{len(video_files)} video files...")
                    batch = []
        if batch:
            db.insert_videos_batch(batch)
            db.commit()

    # --- Subfolder summary ---
    logger.info("\n=== Subfolder Summary (Images) ===")
    max_len = max((len(p) for p in folder_counts_images.keys()), default=0)
    for subfolder, fcount in folder_counts_images.items():
        logger.info(f"{subfolder.ljust(max_len)} | {str(fcount).rjust(5)} files")

    logger.info("\n=== Subfolder Summary (Videos) ===")
    max_len = max((len(p) for p in folder_counts_videos.keys()), default=0)
    for subfolder, fcount in folder_counts_videos.items():
        logger.info(f"{subfolder.ljust(max_len)} | {str(fcount).rjust(5)} files")

    # --- Total summary ---
    total_folders = len(set(list(folder_counts_images.keys()) + list(folder_counts_videos.keys())))
    duplicates_images = db.count_duplicates() if hasattr(db, "count_duplicates") else 0
    duplicates_videos = sum(len(v)-1 for v in db.duplicate_video_groups().values()) if hasattr(db, "duplicate_video_groups") else 0

    logger.info("\n=== Total Summary ===")
    logger.info(f"Total folders scanned : {str(total_folders).rjust(6)}")
    logger.info(f"Total image files     : {len(image_files)}")
    logger.info(f"Total video files     : {len(video_files)}")
    logger.info(f"Duplicate images      : {duplicates_images}")
    logger.info(f"Duplicate videos      : {duplicates_videos}\n")

    logger.info(f"Scan complete. {count} images and {count_videos} videos indexed.")
    return count + count_videos