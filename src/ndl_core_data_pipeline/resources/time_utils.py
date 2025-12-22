from datetime import datetime, timezone


def now_iso8601_utc() -> str:
    """Get the current time in ISO 8601 format in UTC timezone."""
    return datetime.now(timezone.utc).isoformat()


def _format_dt_iso(dt: datetime) -> str:
    """Format a datetime with UTC tzinfo into ISO 8601 string.

    - If there are fractional seconds, trim trailing zeros (so 123000 -> .123)
    - Always include timezone offset as +00:00
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    base = dt.strftime('%Y-%m-%dT%H:%M:%S')
    micro = dt.microsecond
    if micro:
        frac = str(micro).rjust(6, '0')
        # trim trailing zeros to produce minimal fractional digits
        frac = frac.rstrip('0')
        return f"{base}.{frac}+00:00"
    return f"{base}+00:00"


def parse_to_iso8601_utc(date_str: str) -> str:
    """Parse a formatted date string into a datetime string in ISO 8601 (UTC) format.

    This function accepts:
    - ISO 8601 strings (with or without timezone, with 'Z' suffix, with fractional seconds)
    - Common English human-readable date formats such as "1 Mar 2023", "01 March 2023"
    - Space-separated ISO strings like "2025-01-27 10:26:06"

    On empty input the function returns the empty string.
    """
    if not date_str:
        return date_str

    # Normalize trailing Z to explicit UTC offset so fromisoformat can parse it
    if date_str.endswith("Z"):
        # replace trailing Z with +00:00
        try:
            iso = date_str[:-1] + "+00:00"
            dt = datetime.fromisoformat(iso)
            return _format_dt_iso(dt)
        except Exception:
            # fallthrough to more flexible parsing below
            pass

    # First try direct ISO parsing (accepts both 'T' and space separators)
    try:
        dt = datetime.fromisoformat(date_str)
        return _format_dt_iso(dt)
    except Exception:
        # Try common human-friendly English date formats as fallback
        fmts = [
            "%d %b %Y",
            "%d %B %Y",
            "%d %b %Y %H:%M:%S",
            "%d %B %Y %H:%M:%S",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y-%m-%d",
        ]
        for fmt in fmts:
            try:
                dt = datetime.strptime(date_str, fmt)
                # treat naive parsed times as UTC (repo convention)
                dt = dt.replace(tzinfo=timezone.utc)
                return _format_dt_iso(dt)
            except Exception:
                continue

    # As a last resort, raise ValueError so callers can handle it
    raise ValueError(f"Unsupported date format for parsing to ISO 8601 UTC: {date_str!r}")


def parse_iso_to_ts(ts: str) -> datetime:
    """
    Helper to parse ISO 8601 string to datetime object.
    :param ts: input timestamp string
    :return: timestamp as datetime object
    """
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)