import os, re, time, math, threading, subprocess, zipfile, shutil
from pyrogram.enums import ParseMode, LinkPreviewOptions

from . import config

LPO = LinkPreviewOptions(is_disabled=True)

def bar(done, total, L=12):
    if total <= 0: return "[" + "□" * L + "] 0.0%"
    pct = min(100.0, 100 * done / total)
    f = int(L * pct / 100)
    r = (L * pct / 100) - f
    blocks = '■' * f
    if r > 0.3:
        blocks += '▨'; f += 1
    blocks += '□' * (L - f)
    return f"[{blocks}] {pct:.2f}%"

def fmtsz(b):
    if not b or b <= 0: return "0 B"
    n = ("B","KB","MB","GB","TB")
    i = min(int(math.floor(math.log(b, 1024))), 4)
    return f"{round(b / math.pow(1024,i), 2)} {n[i]}"

def fmt_time(s):
    s = int(max(0, s)); m, s = divmod(s, 60); h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m}m {s}s"
    if m > 0: return f"{m}m {s}s"
    return f"{s}s"

def smooth(samples):
    if len(samples) < 2: return "…"
    dt = samples[-1][1] - samples[0][1]
    if dt <= 0: return "…"
    return fmtsz(max(0, (samples[-1][0] - samples[0][0]) / dt)) + "/s"

def hms2s(ts):
    p = [int(x) for x in ts.split(":")]
    return p[0]*3600 + p[1]*60 + p[2] if len(p) == 3 else p[0]*60 + p[1]

# ── Throttled status editor (async-compatible) ────────────────────────
_status_lock      = threading.Lock()
_status_last_edit = {}
_status_pending   = {}
_status_timers    = {}
_EDIT_MIN_GAP = 3.0
_EDIT_FORCE_DELAY = 0.5

async def sedit(chat_id, msg_id, text, markup=None, force=False):
    """Throttled message edit — queues edits, skips if too frequent."""
    if not msg_id: return
    from . import core
    key = (chat_id, msg_id)
    now = time.time()
    with _status_lock:
        last = _status_last_edit.get(key, 0)
        gap  = now - last
        _status_pending[key] = (text, markup)
        existing = _status_timers.pop(key, None)
        if existing: existing.cancel()
        if force:
            delay = _EDIT_FORCE_DELAY
        elif gap >= _EDIT_MIN_GAP:
            delay = 0.05
        else:
            delay = _EDIT_MIN_GAP - gap + 0.05

    async def _do_edit():
        with _status_lock:
            pending = _status_pending.pop(key, None)
            _status_timers.pop(key, None)
            if pending is None: return
            _text, _markup = pending
            _status_last_edit[key] = time.time()
        await core.edit_msg(chat_id, msg_id, _text, _markup)

    loop = asyncio.get_event_loop()
    t = threading.Timer(delay, lambda: asyncio.run_coroutine_threadsafe(_do_edit(), loop))
    t.daemon = True
    with _status_lock:
        _status_timers[key] = t
    t.start()

import asyncio

def get_thread_id(message):
    if message.is_topic_message:
        return message.message_thread_id
    return None

async def ssend(chat_id, text, thread_id=None, reply_markup=None):
    from .core import send_msg
    return await send_msg(chat_id, text, reply_markup=reply_markup,
                          message_thread_id=thread_id)

def cbtn(task_id):
    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    m = InlineKeyboardMarkup()
    m.add(InlineKeyboardButton("🛑 Stop", callback_data=f"cancel:{task_id}"))
    return m

def zip_dir(src, out):
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        for root, _, files in os.walk(src):
            for f in files:
                fp = os.path.join(root, f)
                if fp == out: continue
                zf.write(fp, os.path.relpath(fp, src))
    return out

def unzip_file(fpath, dest_dir):
    ext = os.path.splitext(fpath)[1].lower()
    os.makedirs(dest_dir, exist_ok=True)
    if ext == ".zip":
        with zipfile.ZipFile(fpath) as z: z.extractall(dest_dir)
    elif ext in (".tar", ".gz", ".bz2", ".xz", ".tgz"):
        subprocess.run(["tar", "xf", fpath, "-C", dest_dir], timeout=600, check=True)
    elif ext in (".rar", ".7z"):
        subprocess.run(["7z", "x", fpath, f"-o{dest_dir}", "-y"], timeout=600, check=True)

def split_file(fpath, chunk=config.TG_PART_BYTES):
    parts = []; n = 1
    with open(fpath, "rb") as src:
        while True:
            data = src.read(chunk)
            if not data: break
            pp = f"{fpath}.part{n:03d}"
            with open(pp, "wb") as dst: dst.write(data)
            parts.append(pp); n += 1
    return parts

def prepare_payload(task_dir, do_zip, custom_name=None):
    if not do_zip: return
    zname = (custom_name or "archive")
    if not zname.endswith(".zip"): zname += ".zip"
    zip_path = os.path.join(task_dir, zname)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        for root, _, files in os.walk(task_dir):
            for f in files:
                fp = os.path.join(root, f)
                if fp == zip_path: continue
                zf.write(fp, os.path.relpath(fp, task_dir))
    for item in os.listdir(task_dir):
        ip = os.path.join(task_dir, item)
        if ip != zip_path:
            shutil.rmtree(ip) if os.path.isdir(ip) else os.remove(ip)

def parse_cmd(message):
    raw   = (message.text or message.caption or "")
    parts = raw.split(None, 1)
    cmd   = parts[0].lower() if parts else ""
    if "@" in cmd: cmd = cmd.split("@")[0]
    link  = parts[1].strip() if len(parts) > 1 else ""
    thread_id = get_thread_id(message)
    target = message.reply_to_message
    file_id = file_name = None
    if target:
        if target.document:  file_id, file_name = target.document.file_id, target.document.file_name
        elif target.video:   file_id, file_name = target.video.file_id, target.video.file_name or "video.mp4"
        elif target.audio:   file_id, file_name = target.audio.file_id, target.audio.file_name or "audio.mp3"
        elif target.photo:   file_id, file_name = target.photo[-1].file_id, "photo.jpg"
        if not link and target.text: link = target.text.strip()
    raw_flags = custom = folder = t_range = None
    if link and not file_id:
        if "--" in link:
            idx = link.index("--"); raw_flags = link[idx:].strip(); link = link[:idx].strip()
        t = re.search(r"(\d{1,2}:\d{2}(?::\d{2})?-\d{1,2}:\d{2}(?::\d{2})?)", link)
        f = re.search(r"#(\w+)", link)
        n = re.search(r"\|\s*(.+?)(?=\s*#|\s*\d{1,2}:\d{2}|$)", link)
        link    = (link.split("|")[0].split("#")[0].split()[0].strip() if link.split() else None)
        t_range = t.group(1) if t else None
        folder  = f.group(1) if f else None
        custom  = n.group(1).strip() if n else None
    user_id = message.from_user.id if message.from_user else None
    return cmd, link, custom, folder, t_range, raw_flags, file_id, file_name, thread_id, user_id
