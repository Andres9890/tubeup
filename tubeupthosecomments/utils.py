import os
import re
import threading
import json
from collections import defaultdict
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from typing import Dict, Optional


EMPTY_ANNOTATION_FILE = ('<?xml version="1.0" encoding="UTF-8" ?>'
                         '<document><annotations></annotations></document>')

# YouTube video ID regex pattern
YT_ID_RE = re.compile(r"^[0-9A-Za-z_-]{11}$")


def key_value_to_dict(lst):
    """
    Convert many key:value pair strings into a python dictionary
    """
    if not isinstance(lst, list):
        lst = [lst]

    result = defaultdict(list)
    for item in lst:
        key, value = item.split(":", 1)
        assert value, f"Expected a value! for {key}"
        if result[key] and value not in result[key]:
            result[key].append(value)
        else:
            result[key] = [value]

    # Convert single-item lists back to strings for non-list values
    return {k: v if len(v) > 1 else v[0] for k, v in result.items()}


def sanitize_identifier(identifier, replacement='-'):
    return re.sub(r'[^\w-]', replacement, identifier)


def get_itemname(infodict):
    # Remove illegal characters in identifier
    return sanitize_identifier('%s-%s' % (
        infodict.get('extractor'),
        infodict.get('display_id', infodict.get('id')),
    ))


def check_is_file_empty(filepath):
    """
    Check whether file is empty or not.

    :param filepath:  Path of a file that will be checked.
    :return:          True if the file empty.
    """
    if os.path.exists(filepath):
        return os.stat(filepath).st_size == 0
    else:
        raise FileNotFoundError("Path '%s' doesn't exist" % filepath)


def extract_video_id(url_or_id: str) -> Optional[str]:
    """
    Return the canonical 11-char video ID from any YouTube URL or from a bare ID.
    Falls back to None if it cannot detect an ID.
    
    Args:
        url_or_id: YouTube URL or video ID
        
    Returns:
        Extracted video ID or None
    """
    s = url_or_id.strip()

    # Already an ID?
    if YT_ID_RE.fullmatch(s):
        return s

    # Parse URL variants
    if s.startswith(("http://", "https://")):
        parsed = urlparse(s)
        host = parsed.netloc.lower()
        path = parsed.path

        # Legacy embed case sometimes seen
        if host.endswith("googleusercontent.com"):
            if path.startswith("/youtube.com/v/"):
                candidate = path.split("/")[3]
                if YT_ID_RE.fullmatch(candidate):
                    return candidate

        if 'youtube.com' in host:
            if path == '/watch':
                qs = parse_qs(parsed.query)
                if 'v' in qs and YT_ID_RE.fullmatch(qs['v'][0]):
                    return qs['v'][0]
            for prefix in ("/shorts/", "/embed/", "/watch/"):
                if path.startswith(prefix):
                    candidate = path[len(prefix):].split("/")[0].split("?")[0]
                    if YT_ID_RE.fullmatch(candidate):
                        return candidate
        elif 'youtu.be' in host:
            candidate = path.lstrip('/')
            if YT_ID_RE.fullmatch(candidate):
                return candidate

    return None


def normalize_comment(comment: Dict, platform: str) -> Dict:
    """
    Normalize a comment to a consistent schema across platforms.
    
    Args:
        comment: Raw comment data from yt-dlp
        platform: Platform name (youtube, tiktok, twitch, etc.)
        
    Returns:
        Normalized comment dictionary
    """
    normalized = {
        "id": "",
        "parent": "",
        "text": "",
        "like_count": 0,
        "author_id": "",
        "author": "",
        "author_is_uploader": False,
        "author_is_verified": False,
        "is_favorited": False,
        "is_pinned": False,
        "timestamp": 0,
        "edited": False
    }
    
    try:
        if platform == 'youtube':
            normalized["id"] = comment.get("id", "")
            normalized["parent"] = comment.get("parent", "")
            normalized["text"] = comment.get("text", "")
            normalized["like_count"] = comment.get("like_count", 0) or 0
            normalized["author_id"] = comment.get("author_id", "")
            normalized["author"] = comment.get("author", "")
            normalized["author_is_uploader"] = comment.get("author_is_uploader", False)
            normalized["author_is_verified"] = comment.get("author_is_verified", False)
            normalized["is_favorited"] = comment.get("is_favorited", False)
            normalized["is_pinned"] = comment.get("is_pinned", False)
            normalized["timestamp"] = comment.get("timestamp", 0) or 0
            normalized["edited"] = bool(comment.get("_time_text", "").endswith("(edited)"))
            
        elif platform == 'tiktok':
            normalized["id"] = str(comment.get("cid", ""))
            normalized["text"] = comment.get("text", "")
            normalized["like_count"] = comment.get("digg_count", 0) or 0
            normalized["author"] = comment.get("user", {}).get("nickname", "")
            normalized["author_id"] = str(comment.get("user", {}).get("uid", ""))
            normalized["timestamp"] = comment.get("create_time", 0) or 0
            
        elif platform == 'twitch':
            normalized["id"] = comment.get("_id", "")
            normalized["text"] = comment.get("message", {}).get("body", "")
            normalized["author"] = comment.get("commenter", {}).get("display_name", "")
            normalized["author_id"] = comment.get("commenter", {}).get("_id", "")
            normalized["timestamp"] = int(comment.get("content_offset_seconds", 0))
            
        elif platform == 'soundcloud':
            normalized["id"] = str(comment.get("id", ""))
            normalized["text"] = comment.get("body", "")
            normalized["author"] = comment.get("user", {}).get("username", "")
            normalized["author_id"] = str(comment.get("user", {}).get("id", ""))
            normalized["timestamp"] = comment.get("created_at", 0)
            
        else:
            # Generic fallback for other platforms
            normalized["id"] = str(comment.get("id", comment.get("comment_id", "")))
            normalized["text"] = comment.get("text", comment.get("content", comment.get("body", "")))
            normalized["author"] = comment.get("author", comment.get("user", comment.get("username", "")))
            normalized["like_count"] = comment.get("like_count", comment.get("likes", 0)) or 0
            normalized["timestamp"] = comment.get("timestamp", comment.get("created_at", 0)) or 0
        
        # Ensure all values are of correct type
        normalized["like_count"] = int(normalized["like_count"]) if normalized["like_count"] else 0
        normalized["timestamp"] = int(normalized["timestamp"]) if normalized["timestamp"] else 0
        
    except Exception as e:
        # Return None if normalization fails
        return None
    
    return normalized


def atomic_write_text(final_path: Path, text: str) -> None:
    """
    Write text atomically using a temporary file and atomic rename.
    
    This prevents partial/corrupted files if the process is interrupted.
    
    Args:
        final_path: Destination file path
        text: Text content to write
    """
    tmp_path = final_path.with_suffix(final_path.suffix + ".partial")
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, final_path)


class CommentStats:
    """Thread-safe statistics tracker for comment download progress."""
    
    def __init__(self):
        self._lock = threading.Lock()
        self.successful = 0
        self.failed = 0
        self.skipped_exists = 0
        self.skipped_no_comments = 0
        self.skipped_incomplete = 0
        self.skipped_disabled = 0
        
    def increment(self, category: str):
        with self._lock:
            if hasattr(self, category):
                setattr(self, category, getattr(self, category) + 1)
    
    def get_summary(self) -> Dict[str, int]:
        with self._lock:
            return {
                'successful': self.successful,
                'failed': self.failed,
                'skipped_exists': self.skipped_exists,
                'skipped_no_comments': self.skipped_no_comments,
                'skipped_incomplete': self.skipped_incomplete,
                'skipped_disabled': self.skipped_disabled,
            }


def read_lines_from_file(filepath: str) -> list:
    """
    Read non-empty, non-comment lines from a file.
    
    Args:
        filepath: Path to the file
        
    Returns:
        List of lines (stripped)
    """
    lines = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                lines.append(line)
    return lines


def dedupe_preserve_order(items: list) -> list:
    """
    Remove duplicates from a list while preserving order
    
    Args:
        items: List with potential duplicates
        
    Returns:
        List with duplicates removed, and order preserved
    """
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result