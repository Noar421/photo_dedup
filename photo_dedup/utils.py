"""
Utility functions for photo deduplication.

This module provides helper functions for:
- Human-readable file size formatting
- EXIF metadata extraction from images
- File type detection
"""

import logging
from pathlib import Path
from typing import Dict, Optional, Any
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS

logger = logging.getLogger("photo_dedup")

# Supported file extensions
PHOTO_EXTENSIONS = frozenset({
    ".jpg", ".jpeg", ".png", ".webp", ".tiff", ".tif", 
    ".bmp", ".gif", ".heic", ".heif", ".raw", ".cr2", 
    ".nef", ".arw", ".dng"
})

VIDEO_EXTENSIONS = frozenset({
    ".mp4", ".mov", ".avi", ".mkv", ".webm", ".flv",
    ".wmv", ".m4v", ".mpg", ".mpeg", ".3gp", ".ogv"
})


def human_size(num_bytes: int) -> str:
    """
    Convert bytes to human-readable format.
    
    Args:
        num_bytes: Size in bytes
        
    Returns:
        Formatted string like "1.5 MB" or "245.0 KB"
        
    Examples:
        >>> human_size(1024)
        '  1.0 KB'
        >>> human_size(1536000)
        '  1.5 MB'
    """
    if num_bytes < 0:
        return "  0.0 B "
    
    for unit in ["B ", "KB", "MB", "GB", "TB", "PB"]:
        if num_bytes < 1024.0:
            return f"{num_bytes:6.1f} {unit}"
        num_bytes /= 1024.0
    
    return f"{num_bytes:6.1f} EB"


def is_photo(file_path: str | Path) -> bool:
    """
    Check if file is a supported photo format.
    
    Args:
        file_path: Path to file
        
    Returns:
        True if file extension matches photo formats
    """
    return Path(file_path).suffix.lower() in PHOTO_EXTENSIONS


def is_video(file_path: str | Path) -> bool:
    """
    Check if file is a supported video format.
    
    Args:
        file_path: Path to file
        
    Returns:
        True if file extension matches video formats
    """
    return Path(file_path).suffix.lower() in VIDEO_EXTENSIONS


def is_media_file(file_path: str | Path) -> bool:
    """
    Check if file is either a photo or video.
    
    Args:
        file_path: Path to file
        
    Returns:
        True if file is a supported media format
    """
    return is_photo(file_path) or is_video(file_path)


def _convert_gps_coordinate(coord_tuple: tuple) -> float:
    """
    Convert GPS coordinate from degrees/minutes/seconds to decimal.
    
    Args:
        coord_tuple: Tuple of (degrees, minutes, seconds) where each
                     is a ratio (numerator, denominator)
                     
    Returns:
        Decimal coordinate value
        
    Examples:
        >>> _convert_gps_coordinate(((40, 1), (26, 1), (46, 1)))
        40.44611111111111
    """
    try:
        degrees, minutes, seconds = coord_tuple
        d = degrees[0] / degrees[1] if degrees[1] != 0 else 0
        m = minutes[0] / minutes[1] if minutes[1] != 0 else 0
        s = seconds[0] / seconds[1] if seconds[1] != 0 else 0
        return d + m / 60.0 + s / 3600.0
    except (TypeError, ZeroDivisionError, IndexError) as e:
        logger.debug(f"Error converting GPS coordinate: {e}")
        return 0.0


def _extract_gps_info(exif_data: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    """
    Extract GPS latitude and longitude from EXIF data.
    
    Args:
        exif_data: Dictionary of EXIF tags and values
        
    Returns:
        Tuple of (latitude, longitude) or (None, None) if not available
    """
    gps_info = exif_data.get("GPSInfo")
    if not gps_info:
        return None, None
    
    try:
        # Convert GPS tags to readable names
        gps_data = {GPSTAGS.get(k, k): v for k, v in gps_info.items()}
        
        # Check if we have the required fields
        if 'GPSLatitude' not in gps_data or 'GPSLongitude' not in gps_data:
            return None, None
        
        # Extract and convert latitude
        lat = _convert_gps_coordinate(gps_data['GPSLatitude'])
        if gps_data.get('GPSLatitudeRef', 'N') == 'S':
            lat = -lat
        
        # Extract and convert longitude
        lon = _convert_gps_coordinate(gps_data['GPSLongitude'])
        if gps_data.get('GPSLongitudeRef', 'E') == 'W':
            lon = -lon
        
        return lat, lon
    
    except (KeyError, TypeError, AttributeError) as e:
        logger.debug(f"Error extracting GPS info: {e}")
        return None, None


def extract_exif(file_path: str | Path) -> Dict[str, Any]:
    """
    Extract key EXIF metadata from an image file.
    
    Args:
        file_path: Path to image file
        
    Returns:
        Dictionary containing:
        - date_taken: DateTime the photo was taken (str or None)
        - camera_model: Camera model name (str or None)
        - orientation: Image orientation code (int or None)
        - gps_lat: GPS latitude (float or None)
        - gps_lon: GPS longitude (float or None)
        - width: Image width in pixels (int or None)
        - height: Image height in pixels (int or None)
        - error: Error message if extraction failed (str or None)
        
    Examples:
        >>> exif = extract_exif("photo.jpg")
        >>> print(exif['camera_model'])
        'Canon EOS 5D Mark IV'
    """
    result = {
        "date_taken": None,
        "camera_model": None,
        "orientation": None,
        "gps_lat": None,
        "gps_lon": None,
        "width": None,
        "height": None,
        "error": None
    }
    
    try:
        with Image.open(file_path) as img:
            # Get image dimensions
            result["width"], result["height"] = img.size
            
            # Get EXIF data
            exif_data = img.getexif()
            if not exif_data:
                logger.debug(f"No EXIF data found in {file_path}")
                return result
            
            # Convert numeric tags to readable names
            exif_dict = {}
            for tag_id, value in exif_data.items():
                tag_name = TAGS.get(tag_id, tag_id)
                exif_dict[tag_name] = value
            
            # Extract date taken (try multiple fields)
            result["date_taken"] = (
                exif_dict.get("DateTimeOriginal") or
                exif_dict.get("DateTime") or
                exif_dict.get("DateTimeDigitized")
            )
            
            # Extract camera model
            result["camera_model"] = exif_dict.get("Model")
            
            # Extract orientation
            result["orientation"] = exif_dict.get("Orientation")
            
            # Extract GPS coordinates
            lat, lon = _extract_gps_info(exif_dict)
            result["gps_lat"] = lat
            result["gps_lon"] = lon
            
    except FileNotFoundError:
        error_msg = f"File not found: {file_path}"
        logger.warning(error_msg)
        result["error"] = error_msg
    except PermissionError:
        error_msg = f"Permission denied: {file_path}"
        logger.warning(error_msg)
        result["error"] = error_msg
    except Image.UnidentifiedImageError:
        error_msg = f"Cannot identify image file: {file_path}"
        logger.debug(error_msg)
        result["error"] = error_msg
    except Exception as e:
        error_msg = f"Error extracting EXIF from {file_path}: {type(e).__name__}: {str(e)}"
        logger.error(error_msg)
        result["error"] = error_msg
    
    return result


def format_file_info(file_path: str | Path, size: int, file_hash: str) -> str:
    """
    Format file information for display.
    
    Args:
        file_path: Path to file
        size: File size in bytes
        file_hash: Hash value of file
        
    Returns:
        Formatted string with file information
    """
    path_obj = Path(file_path)
    return f"{path_obj.name:40} | {human_size(size):12} | {file_hash[:16]}..."


def validate_threshold(value: float, min_val: float = 0.0, max_val: float = 1.0) -> float:
    """
    Validate that a threshold value is within acceptable range.
    
    Args:
        value: The threshold value to validate
        min_val: Minimum acceptable value
        max_val: Maximum acceptable value
        
    Returns:
        The validated value
        
    Raises:
        ValueError: If value is outside acceptable range
    """
    if not min_val <= value <= max_val:
        raise ValueError(f"Threshold must be between {min_val} and {max_val}, got {value}")
    return value


def sanitize_path(path: str | Path) -> Path:
    """
    Sanitize and validate a file path.
    
    Args:
        path: Path to sanitize
        
    Returns:
        Resolved Path object
        
    Raises:
        ValueError: If path is invalid or unsafe
    """
    try:
        path_obj = Path(path).resolve()
        return path_obj
    except (OSError, RuntimeError) as e:
        raise ValueError(f"Invalid path: {path}") from e


def get_file_type_display(file_path: str | Path) -> str:
    """
    Get display name for file type.
    
    Args:
        file_path: Path to file
        
    Returns:
        "Photo", "Video", or "Unknown"
    """
    if is_photo(file_path):
        return "Photo"
    elif is_video(file_path):
        return "Video"
    else:
        return "Unknown"


# Backward compatibility with old function name
def human_readable_size(num_bytes: int) -> str:
    """Deprecated: Use human_size() instead."""
    logger.warning("human_readable_size() is deprecated, use human_size() instead")
    return human_size(num_bytes)


if __name__ == "__main__":
    # Example usage and tests
    print("=== File Size Formatting ===")
    test_sizes = [0, 512, 1024, 1536000, 5242880, 1073741824]
    for size in test_sizes:
        print(f"{size:12} bytes = {human_size(size)}")
    
    print("\n=== File Type Detection ===")
    test_files = ["photo.jpg", "video.mp4", "document.pdf", "image.PNG"]
    for file in test_files:
        print(f"{file:20} -> Photo: {is_photo(file)}, Video: {is_video(file)}, Type: {get_file_type_display(file)}")
    
    print("\n=== GPS Coordinate Conversion ===")
    test_coord = ((40, 1), (26, 1), (46, 1))
    print(f"DMS {test_coord} = {_convert_gps_coordinate(test_coord):.6f} decimal")