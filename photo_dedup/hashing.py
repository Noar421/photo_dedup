"""
File hashing module for photo deduplication.

This module provides fast file hashing using xxhash algorithm.
Handles large files efficiently with buffered reading.
"""

import xxhash
import logging
import os
from pathlib import Path
from typing import Optional, BinaryIO
from dataclasses import dataclass

logger = logging.getLogger("photo_dedup")

# Buffer size for file reading (256 KB is optimal for most systems)
DEFAULT_BUFFER_SIZE = 256 * 1024

# Maximum file size to hash (default: no limit)
MAX_FILE_SIZE = None  # Set to value like 10 * 1024**3 for 10GB limit


@dataclass
class HashResult:
    """Result of a file hashing operation."""
    hash_value: Optional[str] = None
    file_size: int = 0
    error: Optional[str] = None
    success: bool = False
    
    def __bool__(self) -> bool:
        """Allow boolean checking of result."""
        return self.success


class HashingError(Exception):
    """Exception raised for hashing errors."""
    pass


def _validate_file_path(file_path: str | Path) -> Path:
    """
    Validate that a file path exists and is a file.
    
    Args:
        file_path: Path to validate
        
    Returns:
        Resolved Path object
        
    Raises:
        HashingError: If path is invalid
    """
    try:
        path = Path(file_path).resolve()
        
        if not path.exists():
            raise HashingError(f"File does not exist: {file_path}")
        
        if not path.is_file():
            raise HashingError(f"Path is not a file: {file_path}")
        
        return path
        
    except (OSError, RuntimeError) as e:
        raise HashingError(f"Invalid file path: {file_path}") from e


def _check_file_size(file_path: Path) -> int:
    """
    Check file size and validate against limits.
    
    Args:
        file_path: Path to file
        
    Returns:
        File size in bytes
        
    Raises:
        HashingError: If file is too large or size cannot be determined
    """
    try:
        size = os.path.getsize(file_path)
        
        if size == 0:
            logger.warning(f"Empty file: {file_path}")
        
        if MAX_FILE_SIZE is not None and size > MAX_FILE_SIZE:
            raise HashingError(
                f"File too large: {file_path} ({size} bytes, max: {MAX_FILE_SIZE})"
            )
        
        return size
        
    except OSError as e:
        raise HashingError(f"Cannot determine file size: {file_path}") from e


def file_hash(file_path: str | Path, buffer_size: int = DEFAULT_BUFFER_SIZE) -> str:
    """
    Compute xxhash (128-bit) hash of a file.
    
    Reads file in chunks to handle large files efficiently.
    Uses xxh3_128 which is very fast and suitable for deduplication.
    
    Args:
        file_path: Path to file to hash
        buffer_size: Size of read buffer in bytes (default: 256KB)
        
    Returns:
        Hexadecimal hash string
        
    Raises:
        HashingError: If file cannot be read or hashed
        PermissionError: If permission denied to read file
        
    Examples:
        >>> hash1 = file_hash("photo.jpg")
        >>> hash2 = file_hash("photo_copy.jpg")
        >>> hash1 == hash2  # True if files are identical
        True
    """
    # Validate inputs
    if buffer_size <= 0:
        raise ValueError(f"Buffer size must be positive, got {buffer_size}")
    
    # Validate file path
    path = _validate_file_path(file_path)
    
    # Check file size
    _check_file_size(path)
    
    # Compute hash
    try:
        hasher = xxhash.xxh3_128()
        
        with open(path, "rb") as f:
            while True:
                chunk = f.read(buffer_size)
                if not chunk:
                    break
                hasher.update(chunk)
        
        return hasher.hexdigest()
        
    except PermissionError:
        logger.warning(f"Permission denied: {path}")
        raise
    except OSError as e:
        raise HashingError(f"Failed to read file: {path}") from e
    except Exception as e:
        raise HashingError(f"Hashing failed for {path}: {type(e).__name__}: {e}") from e


def file_hash_safe(file_path: str | Path, buffer_size: int = DEFAULT_BUFFER_SIZE) -> HashResult:
    """
    Safely compute file hash without raising exceptions.
    
    This is a safe wrapper around file_hash() that catches all exceptions
    and returns a HashResult object instead.
    
    Args:
        file_path: Path to file to hash
        buffer_size: Size of read buffer in bytes
        
    Returns:
        HashResult object with hash_value or error information
        
    Examples:
        >>> result = file_hash_safe("photo.jpg")
        >>> if result:
        ...     print(f"Hash: {result.hash_value}")
        ... else:
        ...     print(f"Error: {result.error}")
    """
    try:
        path = Path(file_path).resolve()
        size = os.path.getsize(path)
        hash_value = file_hash(path, buffer_size)
        
        return HashResult(
            hash_value=hash_value,
            file_size=size,
            success=True
        )
        
    except PermissionError as e:
        logger.debug(f"Permission denied: {file_path}")
        return HashResult(
            file_size=0,
            error=f"Permission denied: {file_path}",
            success=False
        )
    except HashingError as e:
        logger.debug(f"Hashing error: {e}")
        return HashResult(
            file_size=0,
            error=str(e),
            success=False
        )
    except Exception as e:
        logger.error(f"Unexpected error hashing {file_path}: {type(e).__name__}: {e}")
        return HashResult(
            file_size=0,
            error=f"Unexpected error: {type(e).__name__}: {e}",
            success=False
        )


def hash_bytes(data: bytes) -> str:
    """
    Compute xxhash of bytes data.
    
    Useful for testing or hashing small data in memory.
    
    Args:
        data: Bytes to hash
        
    Returns:
        Hexadecimal hash string
        
    Examples:
        >>> hash_bytes(b"Hello, World!")
        '...'
    """
    hasher = xxhash.xxh3_128()
    hasher.update(data)
    return hasher.hexdigest()


def hash_stream(stream: BinaryIO, buffer_size: int = DEFAULT_BUFFER_SIZE) -> str:
    """
    Compute xxhash of a binary stream.
    
    Useful for hashing file-like objects or network streams.
    
    Args:
        stream: Binary stream to hash (must be readable)
        buffer_size: Size of read buffer in bytes
        
    Returns:
        Hexadecimal hash string
        
    Raises:
        IOError: If stream cannot be read
        
    Examples:
        >>> import io
        >>> stream = io.BytesIO(b"test data")
        >>> hash_stream(stream)
        '...'
    """
    try:
        hasher = xxhash.xxh3_128()
        
        while True:
            chunk = stream.read(buffer_size)
            if not chunk:
                break
            hasher.update(chunk)
        
        return hasher.hexdigest()
        
    except Exception as e:
        raise IOError(f"Failed to read stream: {e}") from e


def verify_file_hash(file_path: str | Path, expected_hash: str) -> bool:
    """
    Verify that a file matches an expected hash.
    
    Args:
        file_path: Path to file
        expected_hash: Expected hash value
        
    Returns:
        True if hash matches, False otherwise
        
    Examples:
        >>> hash_val = file_hash("photo.jpg")
        >>> verify_file_hash("photo.jpg", hash_val)
        True
    """
    try:
        actual_hash = file_hash(file_path)
        return actual_hash.lower() == expected_hash.lower()
    except Exception as e:
        logger.error(f"Failed to verify hash for {file_path}: {e}")
        return False


def compare_files(file1: str | Path, file2: str | Path) -> bool:
    """
    Compare two files by hash to check if they're identical.
    
    Args:
        file1: Path to first file
        file2: Path to second file
        
    Returns:
        True if files have same content, False otherwise
        
    Examples:
        >>> compare_files("photo.jpg", "photo_backup.jpg")
        True
    """
    try:
        # Quick size check first (much faster than hashing)
        path1 = Path(file1).resolve()
        path2 = Path(file2).resolve()
        
        size1 = os.path.getsize(path1)
        size2 = os.path.getsize(path2)
        
        if size1 != size2:
            return False
        
        # If sizes match, compare hashes
        hash1 = file_hash(path1)
        hash2 = file_hash(path2)
        
        return hash1 == hash2
        
    except Exception as e:
        logger.error(f"Failed to compare files: {e}")
        return False


def get_optimal_buffer_size(file_size: int) -> int:
    """
    Calculate optimal buffer size based on file size.
    
    For very small files, use smaller buffer.
    For large files, use larger buffer.
    
    Args:
        file_size: Size of file in bytes
        
    Returns:
        Optimal buffer size in bytes
        
    Examples:
        >>> get_optimal_buffer_size(1024)  # 1KB file
        8192
        >>> get_optimal_buffer_size(100 * 1024**2)  # 100MB file
        1048576
    """
    if file_size < 1024:  # < 1KB
        return 1024
    elif file_size < 1024**2:  # < 1MB
        return 8 * 1024  # 8KB
    elif file_size < 100 * 1024**2:  # < 100MB
        return 256 * 1024  # 256KB
    else:  # >= 100MB
        return 1024 * 1024  # 1MB


def hash_with_metadata(file_path: str | Path) -> dict:
    """
    Hash a file and return hash with metadata.
    
    Args:
        file_path: Path to file
        
    Returns:
        Dictionary with hash, size, and path information
        
    Examples:
        >>> result = hash_with_metadata("photo.jpg")
        >>> print(result['hash'])
        >>> print(result['size'])
    """
    try:
        path = Path(file_path).resolve()
        size = os.path.getsize(path)
        
        # Use optimal buffer size
        buffer_size = get_optimal_buffer_size(size)
        hash_value = file_hash(path, buffer_size)
        
        return {
            'hash': hash_value,
            'size': size,
            'path': str(path),
            'filename': path.name,
            'extension': path.suffix.lower(),
            'success': True,
            'error': None
        }
        
    except Exception as e:
        return {
            'hash': None,
            'size': 0,
            'path': str(file_path),
            'filename': Path(file_path).name,
            'extension': Path(file_path).suffix.lower(),
            'success': False,
            'error': str(e)
        }


# Backward compatibility alias
def compute_hash(file_path: str | Path) -> str:
    """
    Deprecated: Use file_hash() instead.
    
    Compute hash of a file (backward compatibility wrapper).
    """
    logger.warning("compute_hash() is deprecated, use file_hash() instead")
    return file_hash(file_path)


# Module configuration
def set_max_file_size(size_bytes: Optional[int]):
    """
    Set maximum file size to hash.
    
    Args:
        size_bytes: Maximum size in bytes, or None for no limit
        
    Examples:
        >>> set_max_file_size(10 * 1024**3)  # 10GB limit
    """
    global MAX_FILE_SIZE
    MAX_FILE_SIZE = size_bytes
    if size_bytes:
        logger.info(f"Max file size set to {size_bytes} bytes")
    else:
        logger.info("Max file size limit disabled")


def get_hash_info() -> dict:
    """
    Get information about the hashing configuration.
    
    Returns:
        Dictionary with configuration details
    """
    return {
        'algorithm': 'xxh3_128',
        'default_buffer_size': DEFAULT_BUFFER_SIZE,
        'max_file_size': MAX_FILE_SIZE,
        'hash_length': 32,  # hex characters
        'hash_bits': 128
    }


if __name__ == "__main__":
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s"
    )
    
    if len(sys.argv) < 2:
        print("Usage: python hashing.py <file_path> [file_path2 ...]")
        print("\nExamples:")
        print("  python hashing.py photo.jpg")
        print("  python hashing.py file1.jpg file2.jpg  # Compare files")
        sys.exit(1)
    
    files = sys.argv[1:]
    
    if len(files) == 1:
        # Hash single file
        print(f"Hashing: {files[0]}")
        result = file_hash_safe(files[0])
        
        if result:
            print(f"Hash:    {result.hash_value}")
            print(f"Size:    {result.file_size:,} bytes")
        else:
            print(f"Error:   {result.error}")
            sys.exit(1)
    
    elif len(files) == 2:
        # Compare two files
        print(f"Comparing files:")
        print(f"  File 1: {files[0]}")
        print(f"  File 2: {files[1]}")
        print()
        
        result1 = file_hash_safe(files[0])
        result2 = file_hash_safe(files[1])
        
        if result1 and result2:
            print(f"Hash 1: {result1.hash_value}")
            print(f"Hash 2: {result2.hash_value}")
            print()
            
            if result1.hash_value == result2.hash_value:
                print("✓ Files are IDENTICAL")
            else:
                print("✗ Files are DIFFERENT")
        else:
            if not result1:
                print(f"Error with file 1: {result1.error}")
            if not result2:
                print(f"Error with file 2: {result2.error}")
            sys.exit(1)
    
    else:
        # Hash multiple files
        print(f"Hashing {len(files)} files...\n")
        
        for file_path in files:
            result = file_hash_safe(file_path)
            if result:
                print(f"{result.hash_value}  {file_path}")
            else:
                print(f"ERROR: {result.error}  {file_path}")