from datetime import datetime, timezone

def now_iso8601_utc() -> str:
    """Get the current time in ISO 8601 format in UTC timezone."""
    return datetime.now(timezone.utc).isoformat()

def parse_to_iso8601_utc(date_str: str) -> str:
    """Parse a formatted date string into a datetime string in ISO 8601."""
    if not date_str:
        return date_str

    if date_str.endswith("Z"):
        return date_str[:-1] + "+00:00"

    dt = datetime.fromisoformat(date_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()