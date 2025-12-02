# photo-dedup

A fast photo & video deduplication tool using Python + SQLite + xxhash. Scans folders, stores file hashes in an SQLite DB, and helps find/export duplicate photos and videos. Includes folder similarity analysis and per-folder statistics.

## Features
- Scan folders and index file hashes into an SQLite database.
- Detect duplicate photos and videos.
- Export duplicate reports to CSV.
- Per-folder summary with duplicate counts.
- Folder similarity analysis (based on sets of file hashes).
- Command-line interface with configurable logging and multi-threaded hashing.

## Requirements
- Python 3.8+
- click
- xxhash
- (Other dependencies may be listed in requirements.txt)

Install:
- If repository includes requirements.txt:
  pip install -r requirements.txt
- Or install minimal runtime deps:
  pip install click xxhash

## Installation
Clone the repo:

    git clone https://github.com/Noar421/photo_dedup.git
    cd photo_dedup

Run via Python:

    python -m photo_dedup.photo_dedup [COMMAND] [OPTIONS]

You can also add a console script entrypoint if desired.

## Basic usage
Global options (apply when running the top-level command):

- --log-file PATH       Path to log file. If not set, defaults next to DB.
- --no-file-log         Disable file logging entirely.

Commands:
- scan [FOLDERS...]           Scan one or more folders and index files into the DB.
- dedup                      Find duplicate photos and videos and optionally export a report.
- list                       List all files in the database.
- report                     Show global and per-folder duplicate statistics.
- folder-summary             List folders and duplicate counts (can export to CSV).
- folder-similar             Detect folders with similar content (based on shared hashes).

## Command examples and options

1) Scan

Scan folders and index files:

    photo-dedup scan "/path/to/Photos"

Options:
- --db-path PATH              Path for DB (directory containing DB; current dir if not specified)
- --folder-list PATH          Text file containing folder paths to scan (one per line)
- --batch N                   DB commit batch size (default 200)
- --threads N                 Number of hashing threads (default 4)
- --fresh                     Delete DB and perform a full clean scan
- --no-file-log               Disable file logging for this run

Examples:

    photo-dedup scan "/home/user/Pictures" --threads 8
    photo-dedup scan --folder-list my_folders.txt --fresh

2) Dedup

Find duplicates and optionally export a CSV report.

Options:
- --db-path PATH
- --export PATH               Export duplicates report to CSV
- --keep {first,largest,newest}
- --no-file-log

The --keep option determines which file of a duplicate group is considered the "master":
- first — keep the first seen entry (default)
- largest — keep the largest file
- newest — keep the newest file (based on ctime where available)

CSV export fields: type, hash, master, duplicate, size

Example:

    photo-dedup dedup --export duplicates.csv --keep largest

3) Folder summary

List folders with total files and duplicate counts.

Options:
- --db-path PATH
- --export PATH               Export folder summary to CSV (folder, total_files, duplicate_files)
- --no-file-log

Example:

    photo-dedup folder-summary --export folder_summary.csv

4) Folder similarity

Detect folders with similar content based on shared file hashes. Similarity = intersection / union of hash sets.

Options:
- --db-path PATH
- --export PATH               Export similarity report to CSV (folder1, folder2, similarity, intersection, union)
- --threshold FLOAT           Minimum similarity score (0–1) to display (default 0.5)
- --no-file-log

Example:

    photo-dedup folder-similar --threshold 0.3 --export similarity.csv

5) List

List all files recorded in the DB:

    photo-dedup list

6) Report

Show global and per-folder duplicate statistics (counts and lost space estimates):

    photo-dedup report

## Logging
- By default logs are written to console and (unless disabled) to a file next to the DB.
- Use --log-file to specify a custom log file.
- Use --no-file-log to disable writing log files.

## Database
- Uses an SQLite DB to store indexed file metadata and hashes.
- The DB path can be set with --db-path. If not provided, DB is created in the current directory.
- Use --fresh on scan to delete and recreate the DB before indexing.

## CSV Export formats
- Duplicates (dedup --export): columns - type, hash, master, duplicate, size
- Folder summary (folder-summary --export): columns - folder, total_files, duplicate_files
- Folder similarity (folder-similar --export): columns - folder1, folder2, similarity, intersection, union

## Notes
- The tool differentiates photos and videos where possible (see db module for how file types are classified).
- Hashing is multi-threaded; increase --threads for faster hashing on many-core machines.
- The script exposes logging configuration options and will create a timestamped log file when scanning.

## Contributing
Contributions, bug reports and PRs are welcome. Please open an issue describing the problem or feature first.
