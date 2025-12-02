import click
from pathlib import Path
from .scanner import scan_folder
from .db import Database
from .utils import human_size
import logging
import csv

# Configure console logging globally
logger = logging.getLogger("photo_dedup")
logger.setLevel(logging.INFO)
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter("[%(asctime)s] %(message)s", "%H:%M:%S")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)


#==============================================================================================
# Main
#==============================================================================================
@click.group()
@click.option("--log-file", type=click.Path(), default=None,
              help="Path to log file. If not set, defaults next to DB.")
@click.option("--no-file-log", is_flag=True, help="Disable file logging entirely.")
@click.pass_context
def main(ctx, log_file, no_file_log):
    """Photo Deduplication Tool"""
    ctx.ensure_object(dict)
    ctx.obj["log_file"] = log_file
    ctx.obj["no_file_log"] = no_file_log

#==============================================================================================
# Scan
#==============================================================================================
@main.command()
@click.argument("folders", type=click.Path(exists=True), nargs=-1)
@click.option("--db-path", type=click.Path(dir_okay=True, file_okay=False), help="Path for DB (current directory if not specified)")
@click.option("--folder-list", type=click.Path(exists=True),
              help="Text file with folder paths")
@click.option("--batch", default=200, help="DB commit batch size")
@click.option("--threads", default=4, help="Number of hashing threads")
@click.option("--fresh", is_flag=True, help="Delete DB and perform a full clean scan")
@click.option("--no-file-log", is_flag=True, help="Disable file logging")
@click.pass_context
def scan(ctx, folders, db_path, folder_list, batch, threads, fresh, no_file_log):
    """Scan a list of folders and store hashes of files in database"""
    from .logger_setup import setup_logger
    from .scanner import scan_folder
    from .db import Database
    import os

    # Normal DB open (or new DB created here)
    db = Database(db_path)

    # Initialize logger with timestamped log file
    global logger
    logger = setup_logger(db_path=db.db_path, no_file_log=no_file_log)

    # Handle fresh full reset
    if fresh:
        db_path = db.db_path
        if os.path.exists(db_path):
            logger.info("")
            logger.info("===========================================")
            logger.info("   FRESH SCAN REQUESTED — DELETING DB")
            logger.info(f"   Removing existing database: {db_path}")
            logger.info("===========================================")
            logger.info("")

            # CLOSE THE DATABASE BEFORE DELETING
            if hasattr(db, "conn") and db.conn:
                db.conn.close()

            os.remove(db_path)

        # Recreate empty DB
        db = Database()

    # Combine folders from CLI args and folder list file
    if folder_list:
        with open(folder_list, "r", encoding="utf-8") as f:
            file_folders = [line.strip() for line in f if line.strip()]
        folders = folders + tuple(file_folders)

    if not folders:
        print("No folders specified for scanning.")
        return

    total_count = 0
    for folder in folders:
        folder = str(folder)
        logger.info("")
        logger.info("===========================================")
        logger.info(f"=== Starting scan for folder: {folder} ===")
        logger.info("===========================================")
        count = scan_folder(folder, db, batch_size=batch, threads=threads)
        total_count += count
        logger.info("")
        logger.info("===========================================")
        logger.info(f"=== Finished scan for folder: {folder} ===")
        logger.info("===========================================")

    print("")
    print("")
    print(f"Total files indexed from all folders: {total_count}")

#==============================================================================================
# Dedup
#==============================================================================================
@main.command()
@click.option("--db-path", type=click.Path(dir_okay=True, file_okay=False), help="Path for DB (current directory if not specified)")
@click.option("--export", type=click.Path(), default=None, help="Export duplicates report to CSV")
@click.option("--keep", type=click.Choice(["first", "largest", "newest"]), default="first")
@click.option("--no-file-log", is_flag=True, help="Disable file logging")
@click.pass_context
def dedup(ctx, db_path, export, keep, no_file_log):
    from .logger_setup import setup_logger
    from .db import Database
    import csv
    from .utils import human_size
    from pathlib import Path

    db = Database(db_path)
    global logger
    logger = setup_logger(db_path=db.db_path, no_file_log=no_file_log)
    rows_to_export = []

    logger.info("==============================================")
    logger.info("=== Searching photos duplicates            ===")
    logger.info("==============================================")
    groups_photos = db.duplicate_photo_groups()
    if not groups_photos:
        logger.info("No photos duplicates found.")
    else:
        for h, files in groups_photos.items():
            if keep == "largest":
                master = max(files, key=lambda f: f["size"])
            elif keep == "newest":
                master = max(files, key=lambda f: f.get("ctime", 0))
            else:
                master = files[0]  # first

            logger.info(f"Hash: {h}")
            logger.info(f"  KEEP -> {master['path']} ({human_size(master['size'])})")

            for f in files:
                if f["path"] != master["path"]:
                    logger.info(f"  DUP  -> {f['path']} ({human_size(f['size'])})")
                    rows_to_export.append({
                        "type":"photo",
                        "hash": h,
                        "master": master["path"],
                        "duplicate": f["path"],
                        "size": human_size(f['size'])
                    })
    logger.info("")
    logger.info("==============================================")
    logger.info("=== Searching videos duplicates            ===")
    logger.info("==============================================")
    groups_videos = db.duplicate_video_groups()
    if not groups_photos:
        logger.info("No videos duplicates found.")
    else:
        for h, files in groups_videos.items():
            if keep == "largest":
                master = max(files, key=lambda f: f["size"])
            elif keep == "newest":
                master = max(files, key=lambda f: f.get("ctime", 0))
            else:
                master = files[0]  # first

            logger.info(f"Hash: {h}")
            logger.info(f"  KEEP -> {master['path']} ({human_size(master['size'])})")

            for f in files:
                if f["path"] != master["path"]:
                    logger.info(f"  DUP  -> {f['path']} ({human_size(f['size'])})")
                    rows_to_export.append({
                        "type":"video",
                        "hash": h,
                        "master": master["path"],
                        "duplicate": f["path"],
                        "size": human_size(f['size'])
                    })

    if export:
        export_path = Path(export)
        with open(export_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["type", "hash", "master", "duplicate", "size"])
            writer.writeheader()
            writer.writerows(rows_to_export)
        logger.info(f"Duplicates report exported to {export_path}")

#==============================================================================================
# Folder summary
#==============================================================================================
@main.command()
@click.option("--db-path", type=click.Path(dir_okay=True, file_okay=False), help="Path for DB (current directory if not specified)")
@click.option("--export", type=click.Path(), default=None, help="Export folder summary to CSV")
@click.option("--no-file-log", is_flag=True, help="Disable file logging")
@click.pass_context
def folder_summary(ctx, db_path, export, no_file_log):
    """List all folders from database and show duplicates count"""

    from .logger_setup import setup_logger
    from .db import Database
    from pathlib import Path
    from collections import defaultdict
    import csv

    db = Database(db_path)
    global logger
    logger = setup_logger(db_path=db.db_path, no_file_log=no_file_log)

    groups = db.duplicate_photo_groups()
    dup_paths = set(f["path"] for files in groups.values() for f in files[1:])

    folder_info = defaultdict(lambda: {"total": 0, "dups": 0})
    for row in db.list_all():
        folder = str(Path(row["path"]).parent)
        folder_info[folder]["total"] += 1
        if row["path"] in dup_paths:
            folder_info[folder]["dups"] += 1

    max_len = max(len(f) for f in folder_info.keys())
    logger.info("=== Folder Summary with Duplicates ===")
    logger.info("")
    logger.info(f"{'Folder'.ljust(max_len)} | Total | Duplicates")
    logger.info("-" * (max_len + 20))
    for folder, info in folder_info.items():
        logger.info(f"{folder.ljust(max_len)} | {info['total']:5} | {info['dups']:5}")
    logger.info("======================================\n")

    if export:
        export_path = Path(export)
        with open(export_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["folder", "total_files", "duplicate_files"])
            writer.writeheader()
            for folder, info in folder_info.items():
                writer.writerow({"folder": folder, "total_files": info["total"], "duplicate_files": info["dups"]})
        logger.info(f"Folder summary exported to {export_path}")

#==============================================================================================
# Folder similarity detection
#==============================================================================================
@main.command(context_settings=dict(ignore_unknown_options=True))
@click.option("--db-path", type=click.Path(dir_okay=True, file_okay=False), help="Path for DB (current directory if not specified)")
@click.option("--export", type=click.Path(), default=None,
              help="Export similarity report to CSV")
@click.option("--threshold", default=0.5, show_default=True,
              help="Minimum similarity score (0–1) to display")
@click.option("--no-file-log", is_flag=True, help="Disable file logging")
@click.pass_context
def folder_similar(ctx, db_path, export, threshold, no_file_log):
    """
    Detect folders with similar content based on file hashes.
    Similarity = intersection / union of file-hash sets.
    """

    from .logger_setup import setup_logger
    from .db import Database
    from pathlib import Path
    import csv
    from collections import defaultdict

    db = Database(db_path)
    global logger
    logger = setup_logger(db_path=db.db_path, no_file_log=no_file_log)

    logger.info("")
    logger.info("===========================================")
    logger.info("=== Folder Similarity Analysis Started ===")
    logger.info("===========================================")

    # 1) Build mapping: folder_path → set of hashes
    try:
        folder_hashes = defaultdict(set)
        for row in db.list_all():
            folder = str(Path(row["path"]).parent)
            if row["hash"]:
                folder_hashes[folder].add(row["hash"])
        folders = [k for k in folder_hashes.keys()]
        results = []
    except Exception as e:
        print(f"Build mapping: folder_path → set of hashes: {e}")

    # 2) Compare every folder pair
    logger.info("Compare folder pairs")
    for i in range(len(folders)):
        for j in range(i + 1, len(folders)):
            f1, f2 = folders[i], folders[j]
            s1, s2 = folder_hashes[f1], folder_hashes[f2]

            if not s1 or not s2:
                continue

            intersection = len(s1 & s2)
            union = len(s1 | s2)
            score = intersection / union if union else 0

            if score >= threshold:
                results.append((f1, f2, score, intersection, union))

    # 3) Log results
    if not results:
        logger.info(f"No similar folders found (threshold={threshold}).")
        return

    logger.info("")
    logger.info(f"=== Similar Folders (threshold = {threshold}) ===")
    logger.info("")

    max_len = max(len(f) for f in folders)

    for f1, f2, score, inter, uni in sorted(results, key=lambda x: -x[2]):
        logger.info(f"{f1.ljust(max_len)} <--> {f2.ljust(max_len)} | "
                    f"Similarity: {score:.3f} ({inter}/{uni} shared hashes)")

    logger.info("")
    logger.info("===========================================")
    logger.info("=== Folder Similarity Analysis Finished ===")
    logger.info("===========================================")

    # 4) Export if needed
    if export:
        export_path = Path(export)
        with open(export_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["folder1", "folder2", "similarity", "intersection", "union"])
            for f1, f2, score, inter, uni in results:
                writer.writerow([f1, f2, score, inter, uni])

        logger.info(f"Similarity report exported to {export_path}")

#==============================================================================================
# List
#==============================================================================================
@main.command()
@click.option("--db-path", type=click.Path(dir_okay=True, file_okay=False), help="Path for DB (current directory if not specified)")
@click.pass_context
def list(ctx, db_path):
    """List all files in DB."""
    db = Database(db_path)
    rows = db.list_all()

    for r in rows:
        click.echo(f"{r['id']:6} | {human_size(r['size']):10} | {r['path']}")

#==============================================================================================
# Report
#==============================================================================================
@main.command()
@click.option("--db-path", type=click.Path(dir_okay=True, file_okay=False), help="Path for DB (current directory if not specified)")
@click.option("--no-file-log", is_flag=True, help="Disable file logging")
@click.pass_context
def report(ctx, db_path, no_file_log):
    """Show global and per-folder duplicate statistics for photos and videos."""

    from .logger_setup import setup_logger
    from .db import Database
    from .utils import human_size

    db = Database(db_path)

    global logger
    logger = setup_logger(db_path=db.db_path, no_file_log=no_file_log)

    logger.info("")
    logger.info("====================================")
    logger.info("          DEDUP REPORT")
    logger.info("====================================")

    # --- Global statistics ---
    stats = db.get_global_stats()
    logger.info("=== Global Statistics ===")
    logger.info(f"Photos:              {stats['total_files_photos']} files")
    logger.info(f"  Total size:        {human_size(stats['total_size_photos'])}")
    logger.info(f"  Duplicate groups:  {stats['duplicate_photo_groups']}")
    logger.info(f"  Lost space:        {human_size(stats['lost_space_photos'])}")
    logger.info(f"Videos:              {stats['total_files_videos']} files")
    logger.info(f"  Total size:        {human_size(stats['total_size_videos'])}")
    logger.info(f"  Duplicate groups:  {stats['duplicate_video_groups']}")
    logger.info(f"  Lost space:        {human_size(stats['lost_space_videos'])}")
    logger.info("")

    # --- Per-folder statistics ---
    folder_stats = db.get_folders_stats()
    if not folder_stats:
        logger.info("No folders found in database.")
        return

    logger.info("=== Per-folder Statistics ===")
    max_len = max(len(f) for f in folder_stats.keys())

    for folder, info in sorted(folder_stats.items()):
        logger.info(
            f"{folder.ljust(max_len)}  |  "
            f"Photos: {info['photos_count']:5}  |  "
            f"Photos Dups: {info['photos_dup_count']:5}  |  "
            f"Photos Lost: {human_size(info['photos_lost_bytes']):8}  |  "
            f"Videos: {info['videos_count']:5}  |  "
            f"Videos Dups: {info['videos_dup_count']:5}  |  "
            f"Videos Lost: {human_size(info['videos_lost_bytes']):8}  |  "
        )
    logger.info("")
    logger.info("====================================")
    logger.info("            REPORT END")
    logger.info("====================================")
