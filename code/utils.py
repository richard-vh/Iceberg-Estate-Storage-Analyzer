# code/utils.py

import base64
from pathlib import Path
import numpy as np

def img_to_bytes(img_path):
    """Encodes a local image file to a base64 string."""
    img_bytes = Path(img_path).read_bytes()
    encoded = base64.b64encode(img_bytes).decode()
    return encoded

def format_bytes(size_bytes):
    """Converts bytes to a human-readable format (KB, MB, GB, etc.)."""
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(np.floor(np.log(size_bytes) / np.log(1024)))
    p = np.power(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"