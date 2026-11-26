"""Bounded validation of optional Telegram JPEG thumbnails."""
from pathlib import Path
import struct

MAX_TELEGRAM_THUMB_BYTES = 20 * 1024
MAX_TELEGRAM_THUMB_SIDE = 320


def jpeg_dimensions(path: Path) -> tuple[int, int]:
    try:
        with path.open("rb") as stream:
            data = stream.read(MAX_TELEGRAM_THUMB_BYTES)
        if not data.startswith(b"\xff\xd8\xff"):
            return 0, 0
        pos = 2
        while pos + 9 < len(data):
            if data[pos] != 0xFF:
                pos += 1
                continue
            marker = data[pos + 1]
            pos += 2
            while marker == 0xFF and pos < len(data):
                marker = data[pos]
                pos += 1
            if marker in {0xD8, 0xD9, 0x01} or 0xD0 <= marker <= 0xD7:
                continue
            if pos + 2 > len(data):
                break
            segment_len = struct.unpack(">H", data[pos:pos + 2])[0]
            if segment_len < 2 or pos + segment_len > len(data):
                break
            if marker in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}:
                if segment_len >= 7:
                    height = struct.unpack(">H", data[pos + 3:pos + 5])[0]
                    width = struct.unpack(">H", data[pos + 5:pos + 7])[0]
                    return int(width), int(height)
                break
            pos += segment_len
    except (OSError, ValueError, struct.error):
        pass
    return 0, 0


def valid_telegram_thumbnail(path: Path | None) -> bool:
    if path is None:
        return False
    try:
        if not path.is_file() or not 0 < path.stat().st_size < MAX_TELEGRAM_THUMB_BYTES:
            return False
        width, height = jpeg_dimensions(path)
        return 0 < width <= MAX_TELEGRAM_THUMB_SIDE and 0 < height <= MAX_TELEGRAM_THUMB_SIDE
    except OSError:
        return False
