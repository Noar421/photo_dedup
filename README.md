# photo-dedup

A fast photo & video deduplication tool using Python + SQLite + xxhash. It scans folders, stores file hashes in an SQLite DB, and helps find/export duplicate photos and videos. It also provides per-folder summaries and folder-similarity analysis.

## Highlights
- Fast multi-threaded hashing with xxhash
- Small, portable SQLite database to store indexed files
- Detects duplicate photos and videos and exports CSV reports
- Per-folder duplicate summaries and folder-to-folder similarity analysis
- Simple command-line interface with configurable logging

## Requirements
- Python 3.8+
- click
- xxhash
- (See requirements.txt for full dependency list)

## Installation
1. Clone the repo:

    git clone https://github.com/Noar421/photo_dedup.git
    cd photo_dedup

2. Install dependencies:

    pip install -r requirements.txt

or for minimal runtime:

    pip install click xxhash

## Running
- This project is intended to be used as a script (top-level photo_dedup.py) and not as an importable module.
- Run with Python:

    python photo_dedup.py [COMMAND] [OPTIONS]

- Or make the script executable and run directly:

    chmod +x photo_dedup.py
    ./photo_dedup.py [COMMAND] [OPTIONS]

## Global options
- --db-path PATH         Path for DB (directory containing DB; current dir if not specified)
- --log-file PATH        Path to log file. If not set, defaults next to DB
- --no-file-log          Disable file logging entirely

## Commands

1) scan

- Description: Index files by hashing and store metadata in the SQLite DB.
- Usage:

    python photo_dedup.py scan "/path/to/Photos"

    or

    ./photo_dedup.py scan "/path/to/Photos"

- Options:
  --db-path PATH
  --folder-list PATH      Text file containing folder paths to scan (one per line)
  --batch N               DB commit batch size (default 200)
  --threads N             Number of hashing threads (default 4)
  --fresh                 Delete DB and perform a full clean scan
  --no-file-log

- Examples:

    python photo_dedup.py scan "/home/user/Pictures" --threads 8
    python photo_dedup.py scan --folder-list my_folders.txt --fresh

2) dedup

- Description: Find duplicate files and optionally export a report.
- Usage:

    python photo_dedup.py dedup

- Options:
  --db-path PATH
  --export PATH           Export duplicates report to CSV
  --keep {first,largest,newest}
  --no-file-log

- Keep options:
  - first — keep the first seen entry (default)
  - largest — keep the largest file
  - newest — keep the newest file (based on ctime where available)

- CSV export fields: type, hash, master, duplicate, size

- Example:

    python photo_dedup.py dedup --export duplicates.csv --keep largest

3) folder-summary

- Description: List folders with total files and duplicate counts
- Usage:

    python photo_dedup.py folder-summary

- Options:
  --db-path PATH
  --export PATH           Export folder summary to CSV (folder, total_files, duplicate_files)
  --no-file-log

- Example:

    python photo_dedup.py folder-summary --export folder_summary.csv

4) folder-similar

- Description: Detect folders with similar content based on shared file hashes.
- Similarity = intersection_size / union_size
- Usage:

    python photo_dedup.py folder-similar

- Options:
  --db-path PATH
  --export PATH           Export similarity report to CSV (folder1, folder2, similarity, intersection, union)
  --threshold FLOAT       Minimum similarity score (0–1) to display (default 0.5)
  --no-file-log

- Example:

    python photo_dedup.py folder-similar --threshold 0.3 --export similarity.csv

5) list

- Description: List all files recorded in the DB
- Usage:

    python photo_dedup.py list

6) report

- Description: Show global and per-folder duplicate statistics (counts and lost space estimates)
- Usage:

    python photo_dedup.py report

## Logging
- By default logs are written to console and, unless disabled, to a file next to the DB.
- Use --log-file to specify a custom log file.
- Use --no-file-log to disable writing log files.

## Database
- Uses an SQLite DB to store indexed file metadata and hashes.
- The DB path can be set with --db-path. If not provided, the DB is created in the current directory.
- Use --fresh on scan to delete and recreate the DB before indexing.

## CSV Export formats
- Duplicates (dedup --export): columns - type, hash, master, duplicate, size
- Folder summary (folder-summary --export): columns - folder, total_files, duplicate_files
- Folder similarity (folder-similar --export): columns - folder1, folder2, similarity, intersection, union

## Notes & Tips
- The tool differentiates photos and videos where possible (see db module for how file types are classified).
- Increase --threads for faster hashing on many-core machines.
- Hashing is multi-threaded; batch DB commits with --batch to tune performance.
- The default --keep strategy is 'first' to preserve the first-seen file; pick largest/newest to preserve file properties you care about.
- Test a small folder first to confirm behavior before running large scans.

## Contributing
- Contributions, bug reports, and PRs are welcome. Please open an issue describing the problem or feature before sending a PR.
- Suggested workflow:
  1. Fork the repo
  2. Create a topic branch (e.g., update-readme)
  3. Open a pull request with a clear description of changes

## License
- Add your license here (e.g., MIT). If you want me to add a license file, tell me which license to use.

## Contact
- Maintainer: Noar421 (GitHub)
- Open an issue for bugs or feature requests.

## Changelog / TODO (optional)
- Add CI (pytest, lint)
- Package for pip with console entrypoint (if you later want one)
- Add progress reporting and resume support for very large scans
