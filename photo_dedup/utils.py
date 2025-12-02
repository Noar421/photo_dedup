from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS


def human_size(num):
    for unit in ["B ", "KB", "MB", "GB", "TB"]:
        if num < 1024:
            ret = f"{num:6.1f}"
            ret = ret.rjust(6)
            ret +=  f" {unit}"
            return ret
        num /= 1024

from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS

def extract_exif(path):
    """Extract key EXIF data from an image"""
    try:
        img = Image.open(path)
        exif_data = img.getexif()
        if not exif_data:
            return {}

        result = {}

        for tag_id, value in exif_data.items():
            tag = TAGS.get(tag_id, tag_id)
            result[tag] = value

        # Date taken
        date_taken = result.get("DateTimeOriginal") or result.get("DateTime")
        # Camera model
        camera_model = result.get("Model")
        # Orientation
        orientation = result.get("Orientation")

        # GPS extraction
        gps_info = result.get("GPSInfo")
        lat = lon = None
        if gps_info:
            def convert(v):
                d, m, s = v
                return d[0]/d[1] + m[0]/m[1]/60 + s[0]/s[1]/3600

            gps_data = {GPSTAGS.get(k, k): v for k, v in gps_info.items()}
            if 'GPSLatitude' in gps_data and 'GPSLongitude' in gps_data:
                lat = convert(gps_data['GPSLatitude'])
                if gps_data.get('GPSLatitudeRef', 'N') == 'S':
                    lat = -lat
                lon = convert(gps_data['GPSLongitude'])
                if gps_data.get('GPSLongitudeRef', 'E') == 'W':
                    lon = -lon

        return {
            "date_taken": date_taken,
            "camera_model": camera_model,
            "orientation": orientation,
            "gps_lat": lat,
            "gps_lon": lon
        }

    except Exception as e:
        return {"error": str(e)}
