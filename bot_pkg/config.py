from dotenv import load_dotenv
load_dotenv()

import os, re, math

TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_API      = int(os.environ.get("TELEGRAM_API", "0"))
TELEGRAM_HASH     = os.environ.get("TELEGRAM_HASH", "").strip()
DRIVE_FOLDER_ID   = os.environ.get("DRIVE_FOLDER_ID", "").strip()
GCP_CLIENT_ID     = os.environ.get("GCP_CLIENT_ID", "").strip()
GCP_CLIENT_SECRET = os.environ.get("GCP_CLIENT_SECRET", "").strip()
GCP_REFRESH_TOKEN = os.environ.get("GCP_REFRESH_TOKEN", "").strip()
ADMIN_ID          = os.environ.get("ADMIN_ID", "").strip()

if not TELEGRAM_TOKEN:
    print("CRITICAL: TELEGRAM_TOKEN missing!"); exit(1)
if not TELEGRAM_API:
    print("CRITICAL: TELEGRAM_API missing (Pyrogram needs api_id)!"); exit(1)
if not TELEGRAM_HASH:
    print("CRITICAL: TELEGRAM_HASH missing (Pyrogram needs api_hash)!"); exit(1)

_BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOWNLOAD_DIR = os.path.join(_BASE_DIR, "downloads")
DATA_DIR     = os.path.join(_BASE_DIR, "data")
MIN_FREE_MB  = 300
TG_PART_BYTES = 1_900_000_000
DRIVE_CHUNK   = 10 * 1024 * 1024
DRIVE_PAGE_SIZE = 15
UPLOAD_WORKERS = 3

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(DATA_DIR,     exist_ok=True)

SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")
COOKIES_DIR   = os.path.join(DATA_DIR, "cookies")
USERS_FILE    = os.path.join(DATA_DIR, "users.json")
os.makedirs(COOKIES_DIR, exist_ok=True)

# ── Instagram ──────────────────────────────────────────────────────────
IG_CONTENT_TYPES = ["posts", "reels", "stories", "highlights", "tagged"]
IG_LABELS = {
    "posts":      "📸 Posts",
    "reels":      "🎬 Reels",
    "stories":    "📖 Stories",
    "highlights": "⭐ Highlights",
    "tagged":     "🏷 Tagged",
}
_IG_URL = {
    "posts":      "https://www.instagram.com/{u}/posts/",
    "reels":      "https://www.instagram.com/{u}/reels/",
    "stories":    "https://www.instagram.com/stories/{u}/",
    "highlights": "https://www.instagram.com/{u}/highlights/",
    "tagged":     "https://www.instagram.com/{u}/tagged/",
}
_AUTH_TYPES = {"stories", "highlights"}

# ── Host lists ─────────────────────────────────────────────────────────
YT_HOSTS = ("youtube.com", "youtu.be", "vimeo.com", "dailymotion.com", "twitch.tv",
            "tiktok.com", "twitter.com", "x.com", "reddit.com", "bilibili.com",
            "soundcloud.com", "nicovideo.jp", "streamable.com", "rumble.com", "odysee.com")
IG_HOSTS = ("instagram.com",)
GALLERY_HOSTS = ("pixiv.", "deviantart.", "gelbooru.", "danbooru.", "rule34.",
                 "nhentai.", "imgur.", "flickr.", "e621.", "artstation.", "bunkr.",
                 "cyberdrop.", "coomer.", "kemono.")

DEST_CYCLE = ["drive", "telegram", "both"]
DEST_LABEL = {"drive": "☁️ Drive", "telegram": "📱 Telegram", "both": "☁️+📱 Both"}

# ── File extensions ────────────────────────────────────────────────────
IMG_EXT = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
VID_EXT = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".m4v"}
AUD_EXT = {".mp3", ".flac", ".aac", ".ogg", ".opus", ".m4a", ".wav"}
ARC_EXT = {".zip", ".tar", ".gz", ".bz2", ".xz", ".tgz", ".rar", ".7z"}
