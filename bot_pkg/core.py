import os, asyncio, time, json, shutil, threading, queue
from pyrogram import Client, filters
from pyrogram.types import BotCommand, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode, LinkPreviewOptions

from . import config

# ── Pyrogram Client ────────────────────────────────────────────────────
app = Client(
    name="azudl_bot",
    api_id=config.TELEGRAM_API,
    api_hash=config.TELEGRAM_HASH,
    bot_token=config.TELEGRAM_TOKEN,
    parse_mode=ParseMode.HTML,
)

# ── Persistence ────────────────────────────────────────────────────────
def _load_json(path, default):
    try:
        if os.path.exists(path):
            with open(path) as f: return json.load(f)
    except: pass
    return default

def _save_json(path, data):
    with open(path, "w") as f: json.dump(data, f, indent=2)

_server_settings = _load_json(config.SETTINGS_FILE, {})
_allowed_users   = set(_load_json(config.USERS_FILE,
                                  [config.ADMIN_ID] if config.ADMIN_ID else []))

def save_settings(): _save_json(config.SETTINGS_FILE, _server_settings)
def save_users():    _save_json(config.USERS_FILE, list(_allowed_users))

# ── User/Chat preferences ──────────────────────────────────────────────
def get_prefs(user_id):
    key = str(user_id)
    if key not in _server_settings:
        _server_settings[key] = {"active_cookie": "global"}
    p = _server_settings[key]
    p.setdefault("active_cookie", "global")
    p.pop("destination", None)
    return p

def get_chat_prefs(chat_id):
    key = f"chat_{chat_id}"
    if key not in _server_settings:
        _server_settings[key] = {"destination": "drive"}
    p = _server_settings[key]
    p.setdefault("destination", "drive")
    return p

def save_prefs(user_id):
    save_settings()

def cookie_path(name="global"):
    import re
    safe = re.sub(r'[^\w\-]', '_', name)
    return os.path.join(config.COOKIES_DIR, f"{safe}.txt")

def list_cookies():
    return [os.path.splitext(f)[0]
            for f in os.listdir(config.COOKIES_DIR) if f.endswith(".txt")]

def get_active_cookie(user_id):
    name = get_prefs(user_id).get("active_cookie", "global")
    p    = cookie_path(name)
    return p if os.path.exists(p) else None

def get_job_prefs(user_id, cmd, chat_id=None):
    p = get_prefs(user_id).copy()
    cp = get_chat_prefs(chat_id if chat_id is not None else user_id)
    p["destination"] = cp.get("destination", "drive")
    if cmd in ("/l", "/zl", "/ytl", "/ytzl", "/unzipl"):
        p["destination"] = "telegram"
    if cmd in ("/zm", "/zl", "/ytzm", "/ytzl", "/unzipm"):
        p["force_zip"] = True
    return p

# ── Core state ─────────────────────────────────────────────────────────
PENDING_TASKS   = {}
ACTIVE_JOBS     = {}
DRIVE_NAV       = {}
DRIVE_CACHE     = {}
DRIVE_FILE_REFS = {}
CACHE_TTL       = 90
jobs_lock       = threading.Lock()
task_queue      = queue.Queue()

def disk_free_mb():  return shutil.disk_usage(config.DOWNLOAD_DIR).free  // (1024*1024)
def disk_total_mb(): return shutil.disk_usage(config.DOWNLOAD_DIR).total // (1024*1024)

def ensure_free(mb=300):
    if disk_free_mb() >= mb: return True
    for e in sorted(os.scandir(config.DOWNLOAD_DIR), key=lambda x: x.stat().st_mtime):
        if e.is_dir(): shutil.rmtree(e.path, ignore_errors=True)
        if disk_free_mb() >= mb: return True
    return disk_free_mb() >= mb

def dir_size_bytes(path):
    t = 0
    for r, _, fs in os.walk(path):
        for f in fs:
            try: t += os.path.getsize(os.path.join(r, f))
            except: pass
    return t

def dir_size_mb(path): return dir_size_bytes(path) // (1024*1024)

def purge_stale():
    if not os.path.isdir(config.DOWNLOAD_DIR): return
    now = time.time()
    for e in os.scandir(config.DOWNLOAD_DIR):
        if e.is_dir() and now - e.stat().st_mtime > 3600:
            shutil.rmtree(e.path, ignore_errors=True)

purge_stale()

# ── Background worker pool (for blocking tasks) ───────────────────────
def bg_worker():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        fn, args = task_queue.get()
        try:
            if asyncio.iscoroutinefunction(fn):
                loop.run_until_complete(fn(*args))
            else:
                fn(*args)
        except Exception as e: print(f"[worker] {e}")
        finally: task_queue.task_done()

for _ in range(8):
    threading.Thread(target=bg_worker, daemon=True).start()

# ── Telegram helper (rate-limit-free Pyrogram sends) ──────────────────
LPO = LinkPreviewOptions(is_disabled=True)

async def send_msg(chat_id, text, reply_markup=None, reply_to_message_id=None,
                   message_thread_id=None, parse_mode=ParseMode.HTML):
    """Safe send with FloodWait retry."""
    kwargs = dict(chat_id=chat_id, text=text, reply_markup=reply_markup,
                  link_preview=LPO, parse_mode=parse_mode)
    if reply_to_message_id:
        kwargs["reply_to_message_id"] = reply_to_message_id
    if message_thread_id:
        kwargs["message_thread_id"] = message_thread_id
    for attempt in range(5):
        try:
            return await app.send_message(**kwargs)
        except Exception as e:
            err = str(e)
            if "429" in err or "flood" in err.lower():
                import re
                m = re.search(r"retry.after\D*(\d+)", err, re.IGNORECASE)
                wait = int(m.group(1)) + 1 if m else min(30, 4 * (2 ** attempt))
                print(f"[send] FloodWait {wait}s (attempt {attempt+1})")
                await asyncio.sleep(wait); continue
            if attempt < 4: await asyncio.sleep(1); continue
            raise
    return None

async def edit_msg(chat_id, message_id, text, reply_markup=None,
                   parse_mode=ParseMode.HTML):
    """Safe edit — swallow MessageNotModified."""
    for attempt in range(4):
        try:
            return await app.edit_message_text(
                chat_id=chat_id, message_id=message_id, text=text,
                reply_markup=reply_markup, link_preview=LPO, parse_mode=parse_mode)
        except Exception as e:
            err = str(e).lower()
            if "not modified" in err: return None
            if "429" in err or "flood" in err:
                import re
                m = re.search(r"retry.after\D*(\d+)", str(e), re.IGNORECASE)
                wait = int(m.group(1)) + 1 if m else min(30, 4 * (2 ** attempt))
                await asyncio.sleep(wait); continue
            if attempt < 3: await asyncio.sleep(1); continue
            print(f"[edit] {e}"); return None
    return None

async def safe_answer(callback_id, text="", show_alert=False):
    try:
        await app.answer_callback_query(callback_id, text=text, show_alert=show_alert)
    except: pass

async def safe_delete(chat_id, message_id):
    try:
        await app.delete_messages(chat_id, message_id)
    except: pass

# ── Import handler modules ─────────────────────────────────────────────
from . import auth
from . import settings
from . import youtube
from . import instagram
from . import download
from . import admin
from . import drive

# ── Boot ───────────────────────────────────────────────────────────────
import logging
logging.getLogger("pyrogram").setLevel(logging.WARNING)

async def _set_commands():
    cmds = [
        BotCommand("/start",      "Help & intro"),
        BotCommand("/help",       "Interactive help"),
        BotCommand("/settings",   "Destination & cookies"),
        BotCommand("/m",          "Smart mirror"),
        BotCommand("/zm",         "Zip mirror"),
        BotCommand("/l",          "Leech → Telegram"),
        BotCommand("/zl",         "Zip leech → Telegram"),
        BotCommand("/yt",         "YouTube quality picker"),
        BotCommand("/ytl",        "YouTube → Telegram"),
        BotCommand("/ytzm",       "YouTube → zip → dest"),
        BotCommand("/ytzl",       "YouTube → zip → Telegram"),
        BotCommand("/torrent",    "Torrent/magnet"),
        BotCommand("/gallery",    "Gallery-dl"),
        BotCommand("/clone",      "Mirror website"),
        BotCommand("/unzipl",     "Extract → Telegram"),
        BotCommand("/unzipm",     "Extract → Drive"),
        BotCommand("/unzip",      "Extract → Drive (alias)"),
        BotCommand("/ig",         "Instagram"),
        BotCommand("/drive",      "Browse Drive"),
        BotCommand("/drivesearch","Search Drive"),
        BotCommand("/cookie",     "Cookie profile"),
        BotCommand("/stats",      "Status"),
        BotCommand("/clean",      "Purge temp"),
        BotCommand("/sh",         "Shell (admin)"),
        BotCommand("/cancelall",  "Kill all (admin)"),
    ]
    for i in range(5):
        try:
            await app.set_bot_commands(cmds)
            print("Commands synced."); return
        except Exception as e:
            print(f"Commands ({i+1}/5): {e}"); await asyncio.sleep(3)

def start():
    print(f"{'='*55}\n  AzuDL Pyro  |  {time.strftime('%Y-%m-%d %H:%M:%S')}\n{'='*55}")
    print(f"Online | Free disk: {disk_free_mb()} MB")

    async def _boot():
        await _set_commands()
        # warm Drive service
        try:
            from .drive import get_drive_service
            srv = get_drive_service()
            if srv:
                await asyncio.to_thread(
                    srv.files().list(pageSize=1, fields="files(id)").execute)
        except: pass
        print("Polling...")

    app.run(_boot())
