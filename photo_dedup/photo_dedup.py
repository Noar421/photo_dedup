#!/usr/bin/env python3
"""
Photo Deduplication Tool

A CLI tool for detecting and managing duplicate photos and videos
using content-based hashing.
"""

import click
import sys
import os
import csv
import logging
from pathlib import Path
from typing import List, Dict, Optional

from scanner import FileScanner, ScanError
from db import Database, DatabaseError
from utils import human_size, validate_threshold
from logger_setup import setup_logger

# Version
__version__ = "0.2.0"

# Global logger (will be configured by commands)
logger = logging.getLogger("photo_dedup")


# ==============================================================================================
# Helper Functions
# ==============================================================================================

def get_database(ctx, db_path: Optional[str] = None) -> Database:
    """
    Get or create database instance.
    
    Args:
        ctx: Click context
        db_path: Optional database directory path
        
    Returns:
        Database instance
    """
    try:
        logger.debug(f"[get_database] : Initialize database")
        db = Database(db_path)
        ctx.obj["db"] = db
        return db

    except DatabaseError as e:
        logger.error(f"Failed to initialize database: {e}")
        click.echo(f"Error: Failed to initialize database: {e}", err=True)
        sys.exit(1)


def setup_logging(ctx, db_path: Optional[Path] = None, no_file_log: bool = False):
    """
    Setup logging for the command.
    
    Args:
        ctx: Click context
        db_path: Database path for log file location
        no_file_log: Disable file logging
    """
    log_file = ctx.obj.get("log_file")
    global logger
    logger = setup_logger(db_path=db_path, log_file=log_file, no_file_log=no_file_log)


def read_folder_list(file_path: str) -> List[str]:
    """
    Read folder paths from a text file.
    
    Args:
        file_path: Path to text file with one folder per line
        
    Returns:
        List of folder paths
    """
    folders = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):  # Skip empty and comment lines
                    folders.append(line)
        return folders
    except FileNotFoundError:
        logger.error(f"Folder list file not found: {file_path}")
        click.echo(f"Error: File not found: {file_path}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to read folder list: {e}")
        click.echo(f"Error: Failed to read folder list: {e}", err=True)
        sys.exit(1)


def select_master_file(files: List[Dict], keep_strategy: str) -> Dict:
    """
    Select which file to keep as master based on strategy.
    
    Args:
        files: List of duplicate file records
        keep_strategy: Strategy ('first', 'largest', 'newest')
        
    Returns:
        Selected master file record
    """
    if keep_strategy == "largest":
        return max(files, key=lambda f: f["size"])
    elif keep_strategy == "newest":
        # Use date_taken if available (photos), otherwise use first
        dated_files = [f for f in files if f.get("date_taken")]
        if dated_files:
            return max(dated_files, key=lambda f: f["date_taken"])
        return files[0]
    else:  # 'first'
        return files[0]


def process_duplicates(groups: Dict[str, List[Dict]], file_type: str, keep_strategy: str) -> List[Dict]:
    """
    Process duplicate groups and generate report rows.
    
    Args:
        groups: Dictionary of hash -> list of files
        file_type: Type of files ('photo' or 'video')
        keep_strategy: Which file to keep ('first', 'largest', 'newest')
        
    Returns:
        List of rows for CSV export
    """
    rows = []
    
    for hash_value, files in groups.items():
        master = select_master_file(files, keep_strategy)
        
        logger.info(f"Hash: {hash_value}")
        logger.info(f"  KEEP -> {master['path']} ({human_size(master['size'])})")
        
        for f in files:
            if f["path"] != master["path"]:
                logger.info(f"  DUP  -> {f['path']} ({human_size(f['size'])})")
                rows.append({
                    "type": file_type,
                    "hash": hash_value,
                    "master": master["path"],
                    "duplicate": f["path"],
                    "size": f["size"]
                })
    
    return rows


# ==============================================================================================
# Main CLI Group
# ==============================================================================================

@click.group()
@click.version_option(version=__version__)
@click.option("--log-file", type=click.Path(), default=None,
              help="Path to log file. If not set, defaults next to DB.")
@click.option("--no-file-log", is_flag=True, 
              help="Disable file logging entirely.")
@click.pass_context
def main(ctx, log_file, no_file_log):
    """
    Photo Deduplication Tool
    
    A fast tool for detecting duplicate photos and videos using content hashing.
    """
    ctx.ensure_object(dict)
    ctx.obj["log_file"] = log_file
    ctx.obj["no_file_log"] = no_file_log
    ctx.obj["db"] = None

# ==============================================================================================
# Scan Command
# ==============================================================================================

@main.command()
@click.argument("folders", type=click.Path(exists=True), nargs=-1)
@click.option("--db-path", type=click.Path(dir_okay=True, file_okay=False), 
              help="Directory for database (current directory if not specified)")
@click.option("--folder-list", type=click.Path(exists=True),
              help="Text file with folder paths (one per line)")
@click.option("--batch", default=200, type=click.IntRange(1, 10000),
              help="Database commit batch size")
@click.option("--threads", default=4, type=click.IntRange(1, 32),
              help="Number of hashing threads")
@click.option("--fresh", is_flag=True, 
              help="Delete database and perform full clean scan")
@click.pass_context
def scan(ctx, folders, db_path, folder_list, batch, threads, fresh):
    """
    Scan folders and index photos/videos in database.
    
    FOLDERS: One or more folder paths to scan
    
    Example:
        photo-dedup scan "/home/user/Pictures" --threads 8
        photo-dedup scan --folder-list folders.txt --fresh
    """

    # Combine folders from arguments and file
    all_folders = list(folders)
    if folder_list:
        all_folders.extend(read_folder_list(folder_list))
    
    # if not all_folders:
    if len(all_folders) == 0:
        click.echo("Error: No folders specified for scanning.", err=True)
        click.echo("Specify folders as arguments or use --folder-list option.")
        sys.exit(1)
    
    # Validate all folders exist
    for folder in all_folders:
        if not Path(folder).exists():
            click.echo(f"Error: Folder does not exist: {folder}", err=True)
            sys.exit(1)
    
    # Handle fresh scan
    if fresh:
        if db_path:
            db_file = Path(db_path) / "photo_dedup.db"
        else:
            db_file = Path.cwd() / "photo_dedup.db"
        
        if db_file.exists():
            click.echo(f"\n{'='*60}")
            click.echo("   FRESH SCAN REQUESTED — DELETING DATABASE")
            click.echo(f"   Removing: {db_file}")
            click.echo(f"{'='*60}\n")
            
            try:
                os.remove(db_file)
            except Exception as e:
                click.echo(f"Error: Failed to delete database: {e}", err=True)
                sys.exit(1)
    
    db = get_database(ctx, db_path)
    setup_logging(ctx, db_path=db.db_path, no_file_log=ctx.obj.get("no_file_log", False))
    
    # Create scanner
    scanner = FileScanner(db, batch_size=batch, threads=threads, skip_existing=not fresh)
    
    # Scan all folders
    total_stats = {
        "total_photos": 0,
        "total_videos": 0,
        "processed_photos": 0,
        "processed_videos": 0,
        "failed": 0,
        "skipped": 0
    }
    
    for folder in all_folders:
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"Scanning folder: {folder}")
        logger.info("=" * 60)
        
        try:
            stats = scanner.scan(folder)
            total_stats["total_photos"] += stats.total_photos
            total_stats["total_videos"] += stats.total_videos
            total_stats["processed_photos"] += stats.processed_photos
            total_stats["processed_videos"] += stats.processed_videos
            total_stats["failed"] += stats.failed_files
            total_stats["skipped"] += stats.skipped_files
            
        except ScanError as e:
            logger.error(f"Failed to scan {folder}: {e}")
            click.echo(f"Error scanning {folder}: {e}", err=True)
    
    # Print summary
    click.echo("\n" + "=" * 60)
    click.echo("SCAN COMPLETE - SUMMARY")
    click.echo("=" * 60)
    click.echo(f"Photos:    {total_stats['processed_photos']:6} / {total_stats['total_photos']:6} processed")
    click.echo(f"Videos:    {total_stats['processed_videos']:6} / {total_stats['total_videos']:6} processed")
    click.echo(f"Skipped:   {total_stats['skipped']:6} already indexed")
    click.echo(f"Failed:    {total_stats['failed']:6}")
    click.echo("=" * 60)
    
    db.close()


# ==============================================================================================
# Dedup Command
# ==============================================================================================

@main.command()
@click.option("--db-path", type=click.Path(dir_okay=True, file_okay=False),
              help="Directory containing database")
@click.option("--export", type=click.Path(), default=None,
              help="Export duplicates report to CSV")
@click.option("--keep", type=click.Choice(["first", "largest", "newest"]), 
              default="first", show_default=True,
              help="Which file to keep: first seen, largest size, or newest")
@click.pass_context
def dedup(ctx, db_path, export, keep):
    """
    Find and report duplicate files.
    
    Identifies duplicate photos and videos based on content hash.
    Optionally exports a CSV report with master/duplicate relationships.
    
    Example:
        photo-dedup dedup --export duplicates.csv --keep largest
    """
    db = get_database(ctx, db_path)
    setup_logging(ctx, db_path=db.db_path, no_file_log=ctx.obj.get("no_file_log", False))
    
    all_rows = []
    
    # Process photos
    logger.info("=" * 60)
    logger.info("Searching for duplicate photos...")
    logger.info("=" * 60)
    
    groups_photos = db.duplicate_photo_groups()
    if not groups_photos:
        logger.info("No duplicate photos found.")
    else:
        logger.info(f"Found {len(groups_photos)} duplicate photo groups")
        rows = process_duplicates(groups_photos, "photo", keep)
        all_rows.extend(rows)
    
    # Process videos
    logger.info("")
    logger.info("=" * 60)
    logger.info("Searching for duplicate videos...")
    logger.info("=" * 60)
    
    groups_videos = db.duplicate_video_groups()
    if not groups_videos:
        logger.info("No duplicate videos found.")
    else:
        logger.info(f"Found {len(groups_videos)} duplicate video groups")
        rows = process_duplicates(groups_videos, "video", keep)
        all_rows.extend(rows)
    
    # Summary
    click.echo(f"\nFound {len(all_rows)} duplicate files")
    click.echo(f"  Photos: {sum(1 for r in all_rows if r['type'] == 'photo')}")
    click.echo(f"  Videos: {sum(1 for r in all_rows if r['type'] == 'video')}")
    
    # Export to CSV
    if export and all_rows:
        try:
            export_path = Path(export)
            with open(export_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["type", "hash", "master", "duplicate", "size"])
                writer.writeheader()
                writer.writerows(all_rows)
            
            logger.info(f"Duplicates report exported to {export_path}")
            click.echo(f"Report exported to: {export_path}")
        except Exception as e:
            logger.error(f"Failed to export report: {e}")
            click.echo(f"Error: Failed to export report: {e}", err=True)
    
    db.close()


# ==============================================================================================
# Folder Summary Command
# ==============================================================================================

@main.command()
@click.option("--db-path", type=click.Path(dir_okay=True, file_okay=False),
              help="Directory containing database")
@click.option("--export", type=click.Path(), default=None,
              help="Export folder summary to CSV")
@click.pass_context
def folder_summary(ctx, db_path, export):
    """
    Show summary of files and duplicates per folder.
    
    Lists all folders in the database with total file counts
    and duplicate file counts.
    
    Example:
        photo-dedup folder-summary --export folder_report.csv
    """
    db = get_database(ctx, db_path)
    setup_logging(ctx, db_path=db.db_path, no_file_log=ctx.obj.get("no_file_log", False))
    
    logger.info("=" * 60)
    logger.info("Generating folder summary...")
    logger.info("=" * 60)
    
    folder_stats = db.get_folders_stats()
    
    if not folder_stats:
        click.echo("No folders found in database.")
        db.close()
        return
    
    # Display summary
    max_len = max(len(f) for f in folder_stats.keys())
    
    logger.info("")
    logger.info(f"{'Folder'.ljust(max_len)} | Photos | Photo Dups | Videos | Video Dups")
    logger.info("-" * (max_len + 50))
    
    for folder, info in sorted(folder_stats.items()):
        logger.info(
            f"{folder.ljust(max_len)} | "
            f"{str(info['photos_count']).rjust(6)} | "
            f"{str(info['photos_dup_count']).rjust(10)} | "
            f"{str(info['videos_count']).rjust(6)} | "
            f"{str(info['videos_dup_count']).rjust(10)}"
        )
    
    # Export to CSV
    if export:
        try:
            export_path = Path(export)
            with open(export_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=[
                    "folder", "total_photos", "duplicate_photos", 
                    "total_videos", "duplicate_videos"
                ])
                writer.writeheader()
                
                for folder, info in folder_stats.items():
                    writer.writerow({
                        "folder": folder,
                        "total_photos": info["photos_count"],
                        "duplicate_photos": info["photos_dup_count"],
                        "total_videos": info["videos_count"],
                        "duplicate_videos": info["videos_dup_count"]
                    })
            
            logger.info(f"Folder summary exported to {export_path}")
            click.echo(f"Report exported to: {export_path}")
        except Exception as e:
            logger.error(f"Failed to export report: {e}")
            click.echo(f"Error: Failed to export report: {e}", err=True)
    
    db.close()


# ==============================================================================================
# Folder Similarity Command
# ==============================================================================================

@main.command()
@click.option("--db-path", type=click.Path(dir_okay=True, file_okay=False),
              help="Directory containing database")
@click.option("--export", type=click.Path(), default=None,
              help="Export similarity report to CSV")
@click.option("--threshold", default=0.5, type=click.FloatRange(0.0, 1.0),
              show_default=True,
              help="Minimum similarity score (0-1) to display")
@click.pass_context
def folder_similar(ctx, db_path, export, threshold):
    """
    Detect folders with similar content based on shared files.
    
    Calculates similarity between folder pairs using Jaccard similarity
    (intersection / union of file hashes).
    
    Example:
        photo-dedup folder-similar --threshold 0.3
    """
    db = get_database(ctx, db_path)
    setup_logging(ctx, db_path=db.db_path, no_file_log=ctx.obj.get("no_file_log", False))
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("Folder Similarity Analysis")
    logger.info("=" * 60)
    
    # Validate threshold
    try:
        threshold = validate_threshold(threshold, 0.0, 1.0)
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    
    # Get folder hash mapping
    folder_hashes = db.get_folder_hash_map()
    folders = list(folder_hashes.keys())
    
    if len(folders) < 2:
        click.echo("Need at least 2 folders to compare.")
        db.close()
        return
    
    logger.info(f"Comparing {len(folders)} folders with threshold {threshold}")
    
    # Compare all folder pairs
    results = []
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
    
    # Display results
    if not results:
        logger.info(f"No similar folders found (threshold={threshold})")
        click.echo(f"No similar folders found with similarity >= {threshold}")
    else:
        logger.info("")
        logger.info(f"Found {len(results)} similar folder pairs:")
        logger.info("")
        
        max_len = max(len(f) for f in folders)
        
        for f1, f2, score, inter, uni in sorted(results, key=lambda x: -x[2]):
            logger.info(
                f"{f1.ljust(max_len)} <-> {f2.ljust(max_len)} | "
                f"Similarity: {score:.3f} ({inter}/{uni} shared)"
            )
        
        click.echo(f"\nFound {len(results)} similar folder pairs")
    
    # Export to CSV
    if export and results:
        try:
            export_path = Path(export)
            with open(export_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["folder1", "folder2", "similarity", "intersection", "union"])
                for f1, f2, score, inter, uni in results:
                    writer.writerow([f1, f2, f"{score:.4f}", inter, uni])
            
            logger.info(f"Similarity report exported to {export_path}")
            click.echo(f"Report exported to: {export_path}")
        except Exception as e:
            logger.error(f"Failed to export report: {e}")
            click.echo(f"Error: Failed to export report: {e}", err=True)
    
    db.close()


# ==============================================================================================
# List Command
# ==============================================================================================

@main.command()
@click.option("--db-path", type=click.Path(dir_okay=True, file_okay=False),
              help="Directory containing database")
@click.option("--type", "file_type", type=click.Choice(["all", "photos", "videos"]),
              default="all", show_default=True,
              help="Type of files to list")
@click.option("--limit", type=int, default=None,
              help="Maximum number of files to display")
@click.pass_context
def list_files(ctx, db_path, file_type, limit):
    """
    List all files in database.
    
    Example:
        photo-dedup list --type photos --limit 100
    """
    db = get_database(ctx, db_path)
    
    # Get files based on type
    if file_type == "photos":
        rows = db.list_all_photos()
    elif file_type == "videos":
        rows = db.list_all_videos()
    else:
        rows = db.list_all()
    
    if not rows:
        click.echo("No files found in database.")
        db.close()
        return
    
    # Apply limit
    if limit:
        rows = rows[:limit]
    
    # Display files
    click.echo(f"\n{'ID':>6} | {'Size':>12} | {'Path'}")
    click.echo("-" * 80)
    
    for r in rows:
        click.echo(f"{r['id']:6} | {human_size(r['size']):>12} | {r['path']}")
    
    click.echo(f"\nTotal: {len(rows)} files")
    
    db.close()


# ==============================================================================================
# Report Command
# ==============================================================================================

@main.command()
@click.option("--db-path", type=click.Path(dir_okay=True, file_okay=False),
              help="Directory containing database")
@click.pass_context
def report(ctx, db_path):
    """
    Show comprehensive duplicate statistics.
    
    Displays global and per-folder statistics including:
    - Total files and sizes
    - Duplicate counts
    - Wasted space estimates
    
    Example:
        photo-dedup report
    """
    db = get_database(ctx, db_path)
    setup_logging(ctx, db_path=db.db_path, no_file_log=ctx.obj.get("no_file_log", False))
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("          DEDUPLICATION REPORT")
    logger.info("=" * 60)
    
    # Global statistics
    stats = db.get_global_stats()
    
    logger.info("")
    logger.info("=== Global Statistics ===")
    logger.info(f"Photos:              {str(stats['total_files_photos']).rjust(7)} files")
    logger.info(f"  Total size:        {human_size(stats['total_size_photos']).rjust(12)}")
    logger.info(f"  Duplicate groups:  {str(stats['duplicate_photo_groups']).rjust(7)}")
    logger.info(f"  Wasted space:      {human_size(stats['lost_space_photos']).rjust(12)}")
    logger.info("")
    logger.info(f"Videos:              {str(stats['total_files_videos']).rjust(7)} files")
    logger.info(f"  Total size:        {human_size(stats['total_size_videos']).rjust(12)}")
    logger.info(f"  Duplicate groups:  {str(stats['duplicate_video_groups']).rjust(7)}")
    logger.info(f"  Wasted space:      {human_size(stats['lost_space_videos']).rjust(12)}")
    logger.info("")
    
    # Per-folder statistics
    folder_stats = db.get_folders_stats()
    
    if not folder_stats:
        logger.info("No folders found in database.")
    else:
        logger.info("=== Per-Folder Statistics ===")
        max_len = max(len(f) for f in folder_stats.keys())
        
        logger.info(f"{'Folder'.ljust(max_len)} | {"Photos".rjust(6)} | {"Dups".rjust(6)} | {"Wasted".rjust(9)} | {"Videos".rjust(6)} | {"Dups".rjust(6)} | {"Wasted".rjust(9)}")
        logger.info("-" * (max_len + 60))
        
        for folder, info in sorted(folder_stats.items()):
            logger.info(
                f"{folder.ljust(max_len)} | "
                f"{str(info['photos_count']).rjust(6)} | "
                f"{str(info['photos_dup_count']).rjust(6)} | "
                f"{human_size(info['photos_lost_bytes']).rjust(8)} | "
                f"{str(info['videos_count']).rjust(6)} | "
                f"{str(info['videos_dup_count']).rjust(6)} | "
                f"{human_size(info['videos_lost_bytes']).rjust(8)}"
            )
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("            END OF REPORT")
    logger.info("=" * 60)
    
    # Also print to console
    total_wasted = stats['lost_space_photos'] + stats['lost_space_videos']
    total_dups = stats['duplicate_photo_groups'] + stats['duplicate_video_groups']
    
    logger.info(f"Summary:")
    logger.info(f"  Total duplicate groups: {total_dups}")
    logger.info(f"  Total wasted space: {human_size(total_wasted)}")
    
    db.close()


# ==============================================================================================
# Vacuum Command (New)
# ==============================================================================================

@main.command()
@click.option("--db-path", type=click.Path(dir_okay=True, file_okay=False),
              help="Directory containing database")
@click.pass_context
def vacuum(ctx, db_path):
    """
    Optimize database by reclaiming unused space.
    
    Run this after deleting many files to reduce database size.
    
    Example:
        photo-dedup vacuum
    """
    db = get_database(ctx, db_path)
    
    size_before = db.get_database_size()
    click.echo(f"Database size before: {human_size(size_before)}")
    
    try:
        db.vacuum()
        size_after = db.get_database_size()
        saved = size_before - size_after
        
        click.echo(f"Database size after:  {human_size(size_after)}")
        click.echo(f"Space reclaimed:      {human_size(saved)}")
    except Exception as e:
        click.echo(f"Error: Failed to vacuum database: {e}", err=True)
        sys.exit(1)
    
    db.close()


# ==============================================================================================
# Entry Point
# ==============================================================================================

if __name__ == "__main__":
    main(obj={})