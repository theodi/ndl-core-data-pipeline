from dagster import ConfigurableResource
import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Tuple
import os
import tempfile
import re
import mimetypes
import urllib.parse


class RateLimitedApiClient(ConfigurableResource):
    base_url: str
    # If None or <= 0, rate limiting is disabled
    rate_limit_per_second: Optional[float] = None

    def get_session(self):
        session = requests.Session()
        # Set browser-like User-Agent and Accept headers
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        })
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def get(self, endpoint, params=None):
        if getattr(self, "rate_limit_per_second", None) and self.rate_limit_per_second > 0:
            time.sleep(1.0 / self.rate_limit_per_second)

        url = endpoint if endpoint.startswith("http") else f"{self.base_url}{endpoint}"

        session = self.get_session()
        print(f"Fetching: {url} | Params: {params}")
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()


    def _filename_from_content_disposition(self, cd: Optional[str]) -> Optional[str]:
        """
        Extract filename from Content-Disposition header.
        :param cd: Content-Disposition header value
        :return: extracted filename or None
        """
        if not cd:
            return None
        # RFC 5987 filename*
        m = re.search(r"filename\*\s*=\s*([^;]+)", cd, flags=re.IGNORECASE)
        if m:
            val = m.group(1).strip().strip('"\'')
            # format: charset'lang'%encoded
            if "'" in val:
                parts = val.split("'", 2)
                if len(parts) == 3:
                    try:
                        return urllib.parse.unquote(parts[2])
                    except Exception:
                        return parts[2]
            try:
                return urllib.parse.unquote(val)
            except Exception:
                return val
        # filename="..."
        m = re.search(r'filename\s*=\s*"([^"]+)"', cd, flags=re.IGNORECASE)
        if m:
            return m.group(1)
        # filename=token
        m = re.search(r'filename\s*=\s*([^;]+)', cd, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip().strip('"\'')
        return None

    def _safe_filename(self, name: Optional[str]) -> Optional[str]:
        """
        Sanitize a filename by removing unsafe characters.
        :param name: input filename
        :return: safe filename or None
        """
        if not name:
            return None
        name = os.path.basename(name)
        # normalize whitespace to underscores
        name = re.sub(r"[\s]+", "_", name)
        # keep only characters with ord >= 32 and not in the forbidden set
        forbidden = set('<>:"/\\|?*')
        filtered = ''.join(ch for ch in name if ord(ch) >= 32 and ch not in forbidden)
        # collapse repeated underscores
        filtered = re.sub(r"_+", "_", filtered)
        # strip leading/trailing underscores or dots
        filtered = filtered.strip('_.')
        return filtered or None

    def _guess_extension_from_content_type(self, content_type: Optional[str]) -> Optional[str]:
        if not content_type:
            return None
        ct = content_type.split(";", 1)[0].strip()
        ext = mimetypes.guess_extension(ct)
        if ext:
            return ext.lstrip('.')
        # common mappings not always covered by mimetypes
        common = {
            "text/csv": "csv",
            "text/tab-separated-values": "tsv",
            "text/tsv": "tsv",
            "application/tsv": "tsv",
            "text/x-tab-separated-values": "tsv",
            "application/pdf": "pdf",
            "application/json": "json",
            "application/ld+json": "json",
            "application/vnd.api+json": "json",
            "application/xml": "xml",
            "text/xml": "xml",
            "text/plain": "txt",
            "text/html": "html",
            "text/markdown": "md",
            "application/zip": "zip",
            "application/x-7z-compressed": "7z",
            "application/gzip": "gz",
            "application/x-gzip": "gz",
            "application/x-tar": "tar",
            "application/x-bzip2": "bz2",
            "application/x-xz": "xz",
            "application/x-rar-compressed": "rar",
            "application/msword": "doc",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
            "application/vnd.ms-excel": "xls",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
            "application/vnd.ms-powerpoint": "ppt",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
            "application/vnd.oasis.opendocument.spreadsheet": "ods",
            "application/vnd.oasis.opendocument.text": "odt",
            "image/png": "png",
            "image/jpeg": "jpg",
            "image/jpg": "jpg",
            "image/gif": "gif",
            "image/svg+xml": "svg",
            "image/tiff": "tif",
            "image/webp": "webp",
            "image/bmp": "bmp",
            "audio/mpeg": "mp3",
            "audio/mp3": "mp3",
            "audio/wav": "wav",
            "audio/x-wav": "wav",
            "audio/ogg": "ogg",
            "audio/opus": "opus",
            "audio/aac": "aac",
            "audio/flac": "flac",
            "audio/x-flac": "flac",
            "video/mp4": "mp4",
            "video/mpeg": "mpeg",
            "video/quicktime": "mov",
            "video/x-msvideo": "avi",
            "video/x-ms-wmv": "wmv",
            "video/x-flv": "flv",
            "application/rtf": "rtf",
            "application/x-iso9660-image": "iso",
            "font/ttf": "ttf",
            "application/vnd.rar": "rar",
        }
        if ct in common:
            return common[ct]

        # fallback for text/* -> take subtype (e.g. text/markdown -> markdown or txt)
        if ct.startswith("text/"):
            subtype = ct.split("/", 1)[1]
            # prefer known small set, otherwise default to 'txt'
            text_map = {"csv": "csv", "tsv": "tsv", "tab-separated-values": "tsv", "x-tab-separated-values": "tsv", "plain": "txt", "markdown": "md", "x-markdown": "md"}
            return text_map.get(subtype, "txt")

        # last-resort: if content-type indicates a vendor+suffix (e.g. application/ld+json)
        if "+" in ct:
            suffix = ct.split("+", 1)[1]
            suffix_map = {"json": "json", "xml": "xml", "zip": "zip", "csv": "csv", "tsv": "tsv"}
            return suffix_map.get(suffix, None)
        return None

    def download_file(self, url: str, folder: str, preferred_name: str, timeout: int = 60) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Download a file from `url` into `folder` with given name.

        If `preferred_name` is provided the file will be saved using that name (extension appended if known).
        The function still derives the actual resource filename (from Content-Disposition or final URL)
        and returns it alongside the saved file path and the format/extension.

        Returns: (saved_full_path or None on failure, actual_resource_filename or None, format extension like 'csv' or 'pdf' or None)
        """
        os.makedirs(folder, exist_ok=True)
        sess = self.get_session()

        # First attempt a lightweight HEAD to follow redirects and pick up headers
        head_headers = None
        final_url = url
        try:
            h = sess.head(url, timeout=10, allow_redirects=True)
            h.raise_for_status()
            head_headers = h.headers
            final_url = getattr(h, "url", url) or url
        except Exception:
            # Some servers don't support HEAD or block it; we'll proceed with GET
            head_headers = None

        # Attempt GET with streaming. If it fails the first time, log and retry once.
        r = None
        try:
            r = sess.get(url, timeout=timeout, stream=True, allow_redirects=True)
            r.raise_for_status()
        except Exception as exc:
            print(f"[download_file] GET failed for {url}: {exc}. Retrying once...")
            try:
                time.sleep(1)
                r = sess.get(url, timeout=timeout, stream=True, allow_redirects=True)
                r.raise_for_status()
            except Exception as exc2:
                print(f"[download_file] Retry GET failed for {url}: {exc2}. Attempting fallback with fresh session...")
                # Attempt a final try with a fresh session (some servers behave differently per connection)
                try:
                    fresh = self.get_session()
                    time.sleep(1)
                    r = fresh.get(url, timeout=timeout, stream=True, allow_redirects=True)
                    r.raise_for_status()
                except Exception as exc3:
                    print(f"[download_file] Fallback GET with fresh session failed for {url}: {exc3}")
                    return None, None, None

        # Derive the actual resource filename from Content-Disposition (prefer HEAD headers if present)
        # or the final redirected URL. This is the filename the server advertises for the resource.
        cd = None
        if head_headers and head_headers.get("content-disposition"):
            cd = head_headers.get("content-disposition")
        else:
            cd = r.headers.get("content-disposition")
        actual_filename = self._filename_from_content_disposition(cd)
        if actual_filename:
            actual_filename = self._safe_filename(actual_filename)

        if not actual_filename:
            final_url = getattr(r, "url", None) or final_url or url
            parsed = urllib.parse.urlparse(final_url)
            actual_filename = self._safe_filename(os.path.basename(parsed.path))

        if not actual_filename:
            actual_filename = None

        # Determine extension from actual filename or content-type
        ext = None
        if actual_filename:
            _, ext = os.path.splitext(actual_filename)
            ext = ext.lstrip('.') if ext else None

        if not ext:
            ext = self._guess_extension_from_content_type(r.headers.get("content-type"))


        save_name = self._safe_filename(preferred_name) or 'resource'

        name, _ = os.path.splitext(save_name)
        final_name = f"{name}.{ext}" if ext else name
        target_file = os.path.join(folder, final_name)

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile("wb", delete=False, dir=folder) as tmp:
                tmp_path = tmp.name
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        tmp.write(chunk)
            os.replace(tmp_path, target_file)
            # return saved path, actual resource filename (may be None), and extension
            return target_file, actual_filename, ext
        except Exception:
            try:
                if tmp_path and os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            return None, None, None
