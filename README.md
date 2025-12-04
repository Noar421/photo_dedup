# Photo Dedup

> A fast, efficient photo and video deduplication tool using content-based hashing

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Photo Dedup is a powerful command-line tool for detecting and managing duplicate photos and videos in your collection. It uses fast content-based hashing (xxhash) to identify exact duplicates, even if files have different names or are in different folders.

## ✨ Features

- **Fast Multi-threaded Hashing** - Process thousands of files quickly using parallel workers
- **Content-Based Detection** - Identifies duplicates by file content, not filenames
- **Portable SQLite Database** - Lightweight, single-file database for all metadata
- **Comprehensive Reports** - Global and per-folder duplicate statistics
- **Folder Similarity Analysis** - Discover folders with overlapping content
- **EXIF Metadata Extraction** - Preserves photo metadata (date, location, camera)
- **Flexible Duplicate Handling** - Keep first/largest/newest file per duplicate group
- **CSV Export** - Export reports for further analysis in Excel or other tools
- **Incremental Scanning** - Skip already-indexed files for faster re-scans
- **Smart Progress Reporting** - Real-time feedback during long operations

## 📋 Table of Contents

- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Usage](#-usage)
  - [Scan Command](#scan-command)
  - [Dedup Command](#dedup-command)
  - [Report Command](#report-command)
  - [Folder Summary](#folder-summary)
  - [Folder Similarity](#folder-similarity)
  - [List Files](#list-files)
  - [Vacuum Database](#vacuum-database)
- [Configuration](#-configuration)
- [Examples](#-examples)
- [Performance Tips](#-performance-tips)
- [How It Works](#-how-it-works)
- [Project Structure](#-project-structure)
- [Contributing](#-contributing)
- [License](#-license)

## 🚀 Installation

### Requirements

- Python 3.8 or higher
- pip (Python package installer)

### Install Dependencies

```bash
# Clone the repository
git clone https://github.com/Noar421/photo_dedup.git
cd photo_dedup

# Install required packages
pip install -r requirements.txt
```

### Required Packages

- `click` - Command-line interface framework
- `Pillow` - Image processing and EXIF extraction
- `xxhash` - Fast hashing algorithm

### Optional: Make Script Executable

On Linux/macOS:

```bash
chmod +x photo_dedup/photo_dedup.py
```

## 🎯 Quick Start

### 1. Scan Your Photos

```bash
# Basic scan
python photo_dedup/photo_dedup.py scan "/path/to/photos"

# Faster scan with more threads
python photo_dedup/photo_dedup.py scan "/path/to/photos" --threads 8
```

### 2. Find Duplicates

```bash
# Show duplicates in console
python photo_dedup/photo_dedup.py dedup

# Export to CSV
python photo_dedup/photo_dedup.py dedup --export duplicates.csv
```

### 3. View Report

```bash
# See statistics and wasted space
python photo_dedup/photo_dedup.py report
```

## 📖 Usage

### Global Options

All commands support these global options:

```bash
--log-file PATH      # Custom log file path
--no-file-log        # Disable file logging
--version            # Show version
--help               # Show help
```

### Scan Command

Index photos and videos in folders by computing content hashes.

```bash
python photo_dedup/photo_dedup.py scan [OPTIONS] [FOLDERS]...
```

**Options:**

- `--db-path PATH` - Directory for database (default: current directory)
- `--folder-list FILE` - Text file with folder paths (one per line)
- `--batch N` - Database commit batch size (default: 200, range: 1-10000)
- `--threads N` - Number of hashing threads (default: 4, range: 1-32)
- `--fresh` - Delete database and perform clean scan

**Examples:**

```bash
# Scan single folder
python photo_dedup/photo_dedup.py scan "/home/user/Pictures"

# Scan multiple folders
python photo_dedup/photo_dedup.py scan "/path/to/photos" "/path/to/more/photos"

# Scan with 8 threads for faster processing
python photo_dedup/photo_dedup.py scan "/path/to/photos" --threads 8

# Fresh scan (delete existing database first)
python photo_dedup/photo_dedup.py scan "/path/to/photos" --fresh

# Scan folders from a list file
python photo_dedup/photo_dedup.py scan --folder-list folders.txt
```

**Folder List File Format:**

```text
/home/user/Pictures/2023
/home/user/Pictures/2024
/media/backup/photos
# Lines starting with # are comments
```

### Dedup Command

Find and report duplicate files.

```bash
python photo_dedup/photo_dedup.py dedup [OPTIONS]
```

**Options:**

- `--db-path PATH` - Directory containing database
- `--export FILE` - Export duplicates report to CSV
- `--keep STRATEGY` - Which file to keep: `first` (default), `largest`, or `newest`

**Keep Strategies:**

- `first` - Keep the first file encountered during scan
- `largest` - Keep the largest file (highest quality)
- `newest` - Keep the file with the newest date_taken (from EXIF)

**Examples:**

```bash
# Show duplicates in console
python photo_dedup/photo_dedup.py dedup

# Export to CSV
python photo_dedup/photo_dedup.py dedup --export duplicates.csv

# Keep largest files (highest quality)
python photo_dedup/photo_dedup.py dedup --keep largest --export report.csv

# Keep newest photos
python photo_dedup/photo_dedup.py dedup --keep newest
```

**CSV Export Format:**

| type  | hash      | master           | duplicate        | size    |
|-------|-----------|------------------|------------------|---------|
| photo | abc123... | /path/photo1.jpg | /path/photo2.jpg | 1234567 |

### Report Command

Show comprehensive statistics about your collection.

```bash
python photo_dedup/photo_dedup.py report [OPTIONS]
```

**Options:**

- `--db-path PATH` - Directory containing database

**Output Includes:**

- Total files and sizes (photos and videos)
- Number of duplicate groups
- Estimated wasted space
- Per-folder statistics

**Example:**

```bash
python photo_dedup/photo_dedup.py report
```

**Sample Output:**

```
====================================
          DEDUPLICATION REPORT
====================================

=== Global Statistics ===
Photos:                 12,543 files
  Total size:             45.3 GB
  Duplicate groups:          234
  Wasted space:            8.7 GB

Videos:                  1,829 files
  Total size:            123.4 GB
  Duplicate groups:           18
  Wasted space:            5.2 GB

=== Per-Folder Statistics ===
/home/user/Pictures/2023  |  3,421 |  45 |  1.2 GB | ...
/home/user/Pictures/2024  |  5,678 |  89 |  3.1 GB | ...
```

### Folder Summary

List all folders with file and duplicate counts.

```bash
python photo_dedup/photo_dedup.py folder-summary [OPTIONS]
```

**Options:**

- `--db-path PATH` - Directory containing database
- `--export FILE` - Export summary to CSV

**Example:**

```bash
python photo_dedup/photo_dedup.py folder-summary --export summary.csv
```

### Folder Similarity

Detect folders with similar content using Jaccard similarity.

```bash
python photo_dedup/photo_dedup.py folder-similar [OPTIONS]
```

**Options:**

- `--db-path PATH` - Directory containing database
- `--export FILE` - Export similarity report to CSV
- `--threshold FLOAT` - Minimum similarity score (0.0-1.0, default: 0.5)

**Similarity Calculation:**

```
Similarity = (Shared Files) / (Total Unique Files)
           = Intersection / Union
```

**Examples:**

```bash
# Find folders with 50%+ overlap
python photo_dedup/photo_dedup.py folder-similar

# Lower threshold to find more matches
python photo_dedup/photo_dedup.py folder-similar --threshold 0.3

# Export results
python photo_dedup/photo_dedup.py folder-similar --export similarity.csv
```

### List Files

Display all indexed files in the database.

```bash
python photo_dedup/photo_dedup.py list [OPTIONS]
```

**Options:**

- `--db-path PATH` - Directory containing database
- `--type TYPE` - Filter by type: `all`, `photos`, or `videos` (default: all)
- `--limit N` - Maximum number of files to display

**Examples:**

```bash
# List all files
python photo_dedup/photo_dedup.py list

# List only photos
python photo_dedup/photo_dedup.py list --type photos

# List first 100 files
python photo_dedup/photo_dedup.py list --limit 100
```

### Vacuum Database

Optimize database by reclaiming unused space.

```bash
python photo_dedup/photo_dedup.py vacuum [OPTIONS]
```

**Options:**

- `--db-path PATH` - Directory containing database

**When to Use:**

- After deleting many duplicate files
- Database file has grown large
- Periodic maintenance

**Example:**

```bash
python photo_dedup/photo_dedup.py vacuum
```

## ⚙️ Configuration

### Database Location

By default, the database is created in the current directory as `photo_dedup.db`. You can specify a custom location:

```bash
# Store database in a specific folder
python photo_dedup/photo_dedup.py scan "/photos" --db-path "/home/user/databases"
```

### Logging

**Console Logging:**
- Always enabled
- Shows progress and important messages

**File Logging:**
- Enabled by default
- Creates timestamped log files next to the database
- Format: `photo_dedup_YYYYMMDD_HHMMSS.log`

**Disable File Logging:**

```bash
python photo_dedup/photo_dedup.py scan "/photos" --no-file-log
```

**Custom Log File:**

```bash
python photo_dedup/photo_dedup.py scan "/photos" --log-file "/var/log/photo_dedup.log"
```

### Performance Tuning

**Thread Count:**
- Default: 4 threads
- Recommended: Number of CPU cores
- Maximum: 32 threads

```bash
# Use 8 threads for faster hashing
python photo_dedup/photo_dedup.py scan "/photos" --threads 8
```

**Batch Size:**
- Default: 200 files per database commit
- Larger batches: Faster, but more memory
- Smaller batches: Slower, but safer

```bash
# Larger batches for better performance
python photo_dedup/photo_dedup.py scan "/photos" --batch 500
```

## 💡 Examples

### Example 1: First-Time Setup

```bash
# Scan your photo library
python photo_dedup/photo_dedup.py scan "/home/user/Pictures" --threads 8

# View the report
python photo_dedup/photo_dedup.py report

# Find duplicates and export
python photo_dedup/photo_dedup.py dedup --export duplicates.csv --keep largest
```

### Example 2: Multiple Folders

```bash
# Scan multiple locations
python photo_dedup/photo_dedup.py scan \
  "/home/user/Pictures" \
  "/media/backup/photos" \
  "/mnt/nas/family_photos" \
  --threads 8

# Find duplicates across all folders
python photo_dedup/photo_dedup.py dedup --export all_duplicates.csv
```

### Example 3: Incremental Updates

```bash
# Initial scan
python photo_dedup/photo_dedup.py scan "/photos" --fresh

# Later, scan only new files (much faster)
python photo_dedup/photo_dedup.py scan "/photos"
# Automatically skips already-indexed files
```

### Example 4: Finding Similar Folders

```bash
# Find folders with overlapping content
python photo_dedup/photo_dedup.py folder-similar --threshold 0.3

# Example output:
# /backup/photos <-> /cloud/photos | Similarity: 0.85 (450/529 shared)
# This shows 85% of files are duplicated between these folders
```

### Example 5: Cleaning Up After Review

```bash
# 1. Export duplicates
python photo_dedup/photo_dedup.py dedup --export to_delete.csv --keep largest

# 2. Review the CSV file manually
# 3. Delete duplicates (manual or scripted)
# 4. Optimize database
python photo_dedup/photo_dedup.py vacuum
```

## ⚡ Performance Tips

### Scanning Performance

1. **Use More Threads** - Set `--threads` to match your CPU core count
2. **Increase Batch Size** - Use `--batch 500` or higher for large collections
3. **Use SSD** - Store database on SSD for faster access
4. **Skip Existing Files** - Don't use `--fresh` for incremental scans

### Memory Usage

- **Thread Count** - More threads = more memory
- **Batch Size** - Larger batches = more memory
- **Recommendation**: For 8GB RAM, use 8 threads and batch size 200

### Disk Space

- **Database Size** - Approximately 1KB per file indexed
- **Log Files** - Can grow large; delete old logs periodically
- **Example**: 10,000 files ≈ 10MB database

### Large Collections (100,000+ files)

```bash
# Recommended settings
python photo_dedup/photo_dedup.py scan "/photos" \
  --threads 8 \
  --batch 500 \
  --db-path "/fast/ssd/location"
```

## 🔍 How It Works

### 1. Scanning Phase

```
┌─────────────┐
│  Scan Files │
└──────┬──────┘
       │
       ├─→ Find all photos/videos recursively
       ├─→ Skip hidden files and folders
       └─→ Check if already indexed
```

### 2. Hashing Phase

```
┌──────────────┐
│ Hash Files   │ (Multi-threaded)
└──────┬───────┘
       │
       ├─→ Read file in 256KB chunks
       ├─→ Compute xxHash (128-bit)
       ├─→ Extract EXIF metadata (photos)
       └─→ Store in database (batched)
```

### 3. Duplicate Detection

```
┌─────────────────┐
│ Find Duplicates │
└────────┬────────┘
         │
         ├─→ Group files by hash
         ├─→ Identify duplicate groups
         ├─→ Apply keep strategy
         └─→ Generate report
```

### Why xxHash?

- **Very Fast** - 10-20x faster than MD5/SHA1
- **Good Distribution** - Excellent for hash tables
- **Collision Resistant** - Extremely low collision probability for file sizes
- **Non-cryptographic** - Optimized for speed, not security (perfect for deduplication)

### Database Schema

**Photos Table:**
```sql
- id (primary key)
- path (unique)
- folder
- size
- hash (indexed)
- date_taken, camera_model, gps_lat, gps_lon
- orientation, width, height
- created_at, updated_at
```

**Videos Table:**
```sql
- id (primary key)
- path (unique)
- folder
- size
- hash (indexed)
- duration, width, height
- created_at, updated_at
```

## 📁 Project Structure

```
photo_dedup/
├── photo_dedup/
│   ├── __init__.py
│   ├── photo_dedup.py      # Main CLI application
│   ├── scanner.py           # File scanning and hashing
│   ├── db.py                # Database operations
│   ├── hashing.py           # File hashing utilities
│   ├── utils.py             # Helper functions (EXIF, formatting)
│   ├── logger_setup.py      # Logging configuration
│   └── comparer.py          # (Legacy, not used)
├── requirements.txt         # Python dependencies
├── pyproject.toml          # Project metadata
├── README.md               # This file
├── .gitignore              # Git ignore rules
└── LICENSE                 # License file
```

## 🤝 Contributing

Contributions are welcome! Here's how you can help:

### Reporting Bugs

1. Check if the bug has already been reported in [Issues](https://github.com/Noar421/photo_dedup/issues)
2. Create a new issue with:
   - Clear description of the problem
   - Steps to reproduce
   - Expected vs actual behavior
   - Your environment (OS, Python version)

### Suggesting Features

1. Open an issue describing the feature
2. Explain the use case and benefits
3. Discuss implementation approach

### Submitting Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests if applicable
5. Commit with clear messages (`git commit -m 'Add amazing feature'`)
6. Push to your fork (`git push origin feature/amazing-feature`)
7. Open a Pull Request

### Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/photo_dedup.git
cd photo_dedup

# Install in development mode
pip install -e .

# Install development dependencies
pip install pytest black flake8
```

### Code Style

- Follow PEP 8 guidelines
- Use type hints where appropriate
- Add docstrings to functions
- Keep functions focused and small

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **xxHash** - Fast hashing algorithm by Yann Collet
- **Click** - Beautiful command-line interfaces
- **Pillow** - Python Imaging Library
- **SQLite** - Embedded database engine

## 📞 Contact

- **Author**: Noar421
- **GitHub**: [@Noar421](https://github.com/Noar421)
- **Issues**: [GitHub Issues](https://github.com/Noar421/photo_dedup/issues)

## 📊 Changelog

### Version 0.2.0 (Current)

**Added:**
- Object-oriented scanner with `FileScanner` class
- Comprehensive error handling throughout
- Safe hashing with `file_hash_safe()`
- Database optimization with `vacuum` command
- Enhanced folder similarity analysis
- Progress reporting during scans
- Skip already-indexed files for incremental scans
- Type hints across all modules
- Extensive logging improvements

**Improved:**
- Performance optimizations (CTEs in SQL queries)
- Database schema with proper indexes
- Better memory usage for large collections
- CSV export formats
- Documentation and help text

**Fixed:**
- Database path handling in fresh scans
- Missing `list_all()` method
- Video extension typos
- Inefficient duplicate detection queries
- Race conditions in database operations

### Version 0.1.0

- Initial release
- Basic scanning and duplicate detection
- SQLite database storage
- CSV export functionality

## 🗺️ Roadmap

### Planned Features

- [ ] GUI interface (web-based)
- [ ] Automatic duplicate deletion with safety checks
- [ ] Perceptual hashing for similar (not identical) photos
- [ ] Video thumbnail extraction
- [ ] Cloud storage integration (Google Photos, iCloud)
- [ ] Duplicate file preview before deletion
- [ ] Undo/restore deleted files
- [ ] Progress bars with ETA
- [ ] Multi-database comparison
- [ ] Docker container
- [ ] Configuration file support (YAML/TOML)

### Future Improvements

- [ ] Unit test suite
- [ ] Integration tests
- [ ] Continuous Integration (CI/CD)
- [ ] PyPI package distribution
- [ ] Pre-built executables for Windows/macOS/Linux
- [ ] Plugin system for extensibility
- [ ] REST API for programmatic access

---

**Note**: This is an active project. Star the repository to follow updates!
