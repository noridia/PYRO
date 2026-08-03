import os, re, math, threading, time, json, tempfile, shutil, queue, uuid, asyncio
from pyrogram import filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from . import config
from .core import (app, ACTIVE_JOBS, PENDING_TASKS, jobs_lock, task_queue,
                   ensure_free, get_job_prefs, get_prefs, get_active_cookie,
                   cookie_path, dir_size_mb, send_msg, edit_msg, safe_answer, LPO)
from .utils import (sedit, cbtn, fmtsz, bar, fmt_time, parse_cmd, get_thread_id,
                    prepare_payload, zip_dir)
from .drive import (get_drive_service, get_or_create_folder, drive_quota,
                    upload_file_to_drive, _send_single_file_tg,
                    dispatch, _invalidate_cache)

import gallery_dl.config as gdl_config
from gallery_dl.job import DownloadJob

# ── IG Index (Drive-backed) ────────────────────────────────────────────
USERSETTING_FOLDER_NAME = "usersetting"
_usersetting_folder_id  = None
_usersetting_lock       = threading.Lock()

def _get_usersetting_folder():
    global _usersetting_folder_id
    with _usersetting_lock:
        if _usersetting_folder_id: return _usersetting_folder_id
        try:
            fid = get_or_create_folder(USERSETTING_FOLDER_NAME, config.DRIVE_FOLDER_ID)
            _usersetting_folder_id = fid; return fid
        except Exception as e: print(f"[usersetting] folder create failed: {e}"); return None

def _drive_ig_filename(username): return f"ig_index_{username}.json"

def _drive_load_index(username) -> set:
    try:
        srv = get_drive_service()
        if not srv: return set()
        parent = _get_usersetting_folder()
        if not parent: return set()
        fname = _drive_ig_filename(username)
        q = f"name='{fname}' and '{parent}' in parents and trashed=false"
        res = srv.files().list(q=q, spaces="drive", fields="files(id)").execute()
        files = res.get("files", [])
        if not files: return set()
        fid = files[0]["id"]
        data = srv.files().get_media(fileId=fid).execute()
        parsed = json.loads(data.decode("utf-8") if isinstance(data, bytes) else data)
        result = set(parsed) if isinstance(parsed, list) else set()
        print(f"[ig_index] Loaded {len(result)} IDs for @{username} from Drive"); return result
    except Exception as e: print(f"[ig_index] load failed for {username}: {e}"); return set()

def _drive_save_index(username, seen: set):
    if not hasattr(_drive_save_index, "_locks"):
        _drive_save_index._locks = {}
        _drive_save_index._locks_lock = threading.Lock()
    with _drive_save_index._locks_lock:
        if username not in _drive_save_index._locks:
            _drive_save_index._locks[username] = threading.Lock()
        ulock = _drive_save_index._locks[username]
    seen_snapshot = set(seen)
    def _upload():
        with ulock:
            tmp_path = None
            try:
                srv = get_drive_service()
                if not srv: return
                parent = _get_usersetting_folder()
                if not parent: return
                fname = _drive_ig_filename(username)
                data = json.dumps(sorted(list(seen_snapshot)), indent=2).encode("utf-8")
                with tempfile.NamedTemporaryFile(mode="wb", suffix=".json", delete=False) as tf:
                    tf.write(data); tmp_path = tf.name
                q = f"name='{fname}' and '{parent}' in parents and trashed=false"
                res = srv.files().list(q=q, spaces="drive", fields="files(id)").execute()
                files = res.get("files", [])
                from googleapiclient.http import MediaFileUpload
                media = MediaFileUpload(tmp_path, mimetype="application/json",
                                        chunksize=config.DRIVE_CHUNK, resumable=False)
                if files: srv.files().update(fileId=files[0]["id"], media_body=media).execute()
                else:
                    meta = {"name": fname, "parents": [parent]}
                    srv.files().create(body=meta, media_body=media, fields="id").execute()
                print(f"[ig_index] Saved {len(seen_snapshot)} IDs for @{username} to Drive")
            except Exception as e: print(f"[ig_index] save failed for {username}: {e}")
            finally:
                if tmp_path:
                    try: os.remove(tmp_path)
                    except: pass
    threading.Thread(target=_upload, daemon=True).start()

def _drive_delete_index(username) -> bool:
    try:
        srv = get_drive_service()
        if not srv: return False
        parent = _get_usersetting_folder()
        if not parent: return False
        fname = _drive_ig_filename(username)
        q = f"name='{fname}' and '{parent}' in parents and trashed=false"
        res = srv.files().list(q=q, spaces="drive", fields="files(id)").execute()
        deleted = False
        for f in res.get("files", []):
            srv.files().delete(fileId=f["id"]).execute(); deleted = True
        return deleted
    except Exception as e: print(f"[ig_index] delete failed for {username}: {e}"); return False

def _drive_list_ig_indexes() -> list:
    try:
        srv = get_drive_service()
        if not srv: return []
        parent = _get_usersetting_folder()
        if not parent: return []
        q = f"'{parent}' in parents and trashed=false and name contains 'ig_index_'"
        res = srv.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=100).execute()
        names = []
        for f in res.get("files", []):
            m = re.match(r"ig_index_(.+)\.json$", f["name"])
            if m: names.append(m.group(1))
        return sorted(names)
    except Exception as e: print(f"[ig_index] list failed: {e}"); return []

# ── IG URL helpers ─────────────────────────────────────────────────────
def _ig_is_single(url):
    return bool(re.search(
        r"instagram\.com/(p|reel|tv)/[A-Za-z0-9_-]+|"
        r"instagram\.com/stories/[A-Za-z0-9_.]+/\d+", url))

def _ig_is_stories_page(url):
    return bool(re.search(r"instagram\.com/stories/[A-Za-z0-9_.]+/?$", url)) and not bool(
        re.search(r"instagram\.com/stories/[A-Za-z0-9_.]+/\d+", url))

def _ig_is_highlights_single(url):
    return bool(re.search(r"instagram\.com/stories/highlights/\d+", url))

def _ig_username(url):
    ms = re.search(r"instagram\.com/stories/([A-Za-z0-9_.]+)(?:/(?!\d)|/?$)", url)
    if ms: return ms.group(1)
    mh = re.search(r"instagram\.com/([A-Za-z0-9_.]+)/highlights", url)
    if mh and mh.group(1) not in ("p","reel","tv","explore","accounts","reels","tagged","stories"):
        return mh.group(1)
    m = re.search(r"instagram\.com/([A-Za-z0-9_.]+)/?", url)
    return (m.group(1) if m and m.group(1) not in
            ("p","reel","stories","tv","explore","accounts","reels","highlights","tagged") else None)

# ── gallery-dl helpers ─────────────────────────────────────────────────
def _gdl_apply_config(task_dir, user_id):
    gdl_config.clear()
    gdl_config.set((), "base-directory", task_dir)
    gdl_config.set(("output",), "mode", "null")
    gdl_config.set(("downloader",), "retries", 6)
    gdl_config.set(("downloader",), "timeout", 30)
    gdl_config.set(("downloader",), "part", True)
    gdl_config.set(("downloader",), "part-filter", True)
    gdl_config.set(("downloader",), "rate", None)
    gdl_config.set(("extractor", "bunkr"), "sleep-request", 0.3)
    gdl_config.set(("extractor", "bunkralbum"), "sleep-request", 0.3)
    gdl_config.set(("extractor",), "retries", 6)
    gdl_config.set(("extractor",), "timeout", 30)
    gdl_config.set(("extractor", "instagram"), "sleep-request", 1.5)
    gdl_config.set(("extractor", "instagram"), "sleep-extractor", 1.0)
    gdl_config.set(("extractor", "instagram"), "include", "posts,reels,stories,highlights,tagged")
    gdl_config.set(("extractor", "instagram"), "videos", True)
    gdl_config.set(("extractor", "instagram"), "headers", {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    ck = get_active_cookie(user_id)
    if ck and os.path.exists(ck):
        gdl_config.set(("extractor",), "cookies", ck)
        gdl_config.set(("extractor", "instagram"), "cookies", ck)

class _GdlOut:
    def __init__(self, on_file=None):
        self._cb = on_file; self.lock = threading.Lock()
        self.done = 0; self.skipped = 0; self.errors = 0; self.bytes = 0; self.current = ""
        self._current_path = None
    def start(self, path):
        with self.lock: self._current_path = path; self.current = os.path.basename(path)
    def skip(self, *args):
        with self.lock: self.skipped += 1; self._current_path = None
    def success(self, path):
        sz = os.path.getsize(path) if os.path.exists(path) else 0
        with self.lock: self.done += 1; self.bytes += sz; self.current = os.path.basename(path); self._current_path = None
        if self._cb:
            try: self._cb(path)
            except Exception as e: print(f"[gdl cb] {e}")
    def error(self, *args):
        with self.lock: self.errors += 1; self._current_path = None
    def progress(self, *a): pass
    def wait(self, *a): pass

def _run_gdl(url, task_dir, user_id, out_obj):
    _gdl_apply_config(task_dir, user_id)
    try: job = DownloadJob(url); job.out = out_obj; job.run(); return True
    except Exception as e: print(f"[gdl] {e}"); return False

def _run_gdl_highlights(username, task_dir, user_id, out_obj):
    import gallery_dl.config as _gc
    from gallery_dl.job import UrlJob
    import io, contextlib
    profile_hl_url = f"https://www.instagram.com/{username}/highlights/"
    _gdl_apply_config(task_dir, user_id)
    reel_ids = []
    class _UrlCollector:
        def __init__(self): self.lock = threading.Lock()
        def start(self, path): pass
        def skip(self): pass
        def success(self, path): pass
        def error(self): pass
        def progress(self, *a): pass
        def wait(self, *a): pass
    try:
        _gc.set(("output",), "mode", "null")
        url_job = UrlJob(profile_hl_url)
        url_job.out = _UrlCollector()
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf): url_job.run()
        for line in buf.getvalue().splitlines():
            line = line.strip()
            m = re.search(r"highlights/(\d+)", line)
            if m: reel_ids.append(m.group(1))
    except Exception as e:
        print(f"[hl_enum] URL listing failed ({e}), falling back to single-pass")
    if not reel_ids:
        print(f"[hl_enum] No reel IDs found via UrlJob, using single-pass for @{username}")
        return _run_gdl(profile_hl_url, task_dir, user_id, out_obj)
    print(f"[hl_enum] Found {len(reel_ids)} highlight reels for @{username}")
    for rid in reel_ids:
        reel_url = f"https://www.instagram.com/stories/highlights/{rid}/"
        _gdl_apply_config(task_dir, user_id)
        try:
            job = DownloadJob(reel_url); job.out = out_obj; job.run()
        except Exception as e: print(f"[hl_enum] reel {rid} failed: {e}")
        time.sleep(2.0)
    return True

# ── IG Index UI ────────────────────────────────────────────────────────
async def render_igindex(chat_id, msg_id, page=0):
    await sedit(chat_id, msg_id, "📋 <b>Fetching tracked profiles from Drive…</b>")
    usernames = await asyncio.to_thread(_drive_list_ig_indexes)
    if not usernames:
        return await sedit(chat_id, msg_id,
            "📋 <b>IG Archive Tracker</b>\n\n<i>No tracked profiles yet.</i>\n\n"
            "Profiles appear here after running <code>/ig &lt;profile_url&gt;</code>.")
    PAGE = 10; total_pages = max(1, math.ceil(len(usernames) / PAGE))
    page = max(0, min(page, total_pages - 1))
    page_users = usernames[page*PAGE:(page+1)*PAGE]
    text = (f"📋 <b>IG Archive Tracker</b>\n<i>{len(usernames)} profile(s) tracked • "
            f"page {page+1}/{total_pages}</i>\n\nTap a profile to view details.")
    m = InlineKeyboardMarkup(row_width=1)
    for u in page_users: m.add(InlineKeyboardButton(f"👤 @{u}", callback_data=f"igidx:user:{u}:{page}"))
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"igidx:pg:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="igidx:noop"))
    if page < total_pages - 1: nav.append(InlineKeyboardButton("▶️", callback_data=f"igidx:pg:{page+1}"))
    if nav: m.row(*nav)
    await sedit(chat_id, msg_id, text, m)

async def render_igindex_user(chat_id, msg_id, username, back_page=0):
    await sedit(chat_id, msg_id, f"👤 <b>@{username}</b>\n\n⏳ <i>Fetching index from Drive…</i>")
    idx = await asyncio.to_thread(_drive_load_index, username)
    total = len(idx)
    text = (f"👤 <b>@{username}</b>\n\n📊 <code>{total}</code> tracked post IDs\n\n"
            f"<i>Delete to allow re-downloading all posts next archive run.</i>")
    m = InlineKeyboardMarkup(row_width=1)
    m.add(InlineKeyboardButton("🗑 Delete ALL tracked IDs for this profile",
          callback_data=f"igidx:delall:{username}:{back_page}"))
    m.add(InlineKeyboardButton("⬅️ Back", callback_data=f"igidx:pg:{back_page}"))
    await sedit(chat_id, msg_id, text, m)

async def render_igindex_confirm(chat_id, msg_id, username, back_page=0):
    text = (f"⚠️ <b>Confirm Delete</b>\n\nRemove all tracked IDs for <b>@{username}</b>?\n\n"
            f"Next archive run will re-download everything.")
    m = InlineKeyboardMarkup(row_width=2)
    m.row(InlineKeyboardButton("✅ Yes, delete", callback_data=f"igidx:delconfirm:{username}:{back_page}"),
          InlineKeyboardButton("❌ Cancel", callback_data=f"igidx:user:{username}:{back_page}"))
    await sedit(chat_id, msg_id, text, m)

@app.on_callback_query(filters.regex(r"^igidx:"))
async def igidx_cb(client, call):
    parts = call.data.split(":"); action = parts[1]
    chat_id = call.message.chat.id; msg_id = call.message.id
    await safe_answer(call.id)
    if action == "noop": return
    if action == "pg":
        page = int(parts[2]) if len(parts) > 2 else 0
        await render_igindex(chat_id, msg_id, page)
    elif action == "user":
        username = parts[2]; back_page = int(parts[3]) if len(parts) > 3 else 0
        await render_igindex_user(chat_id, msg_id, username, back_page)
    elif action == "delall":
        username = parts[2]; back_page = int(parts[3]) if len(parts) > 3 else 0
        await render_igindex_confirm(chat_id, msg_id, username, back_page)
    elif action == "delconfirm":
        username = parts[2]; back_page = int(parts[3]) if len(parts) > 3 else 0
        await sedit(chat_id, msg_id, f"🗑 <b>Deleting index for @{username}…</b>")
        await asyncio.to_thread(_drive_delete_index, username)
        await sedit(chat_id, msg_id,
            f"✅ <b>@{username}</b> tracking cleared!\n<i>Items will re-download on next archive run.</i>")
        await asyncio.sleep(1.5); await render_igindex(chat_id, msg_id, back_page)

# ── IG Single Post ─────────────────────────────────────────────────────
def process_ig_single(chat_id, msg_id, url, custom, folder, task_id, job_prefs, user_id):
    import asyncio
    loop = asyncio.get_event_loop()
    task_dir = os.path.join(config.DOWNLOAD_DIR, task_id)
    os.makedirs(task_dir, exist_ok=True)
    with jobs_lock:
        ACTIVE_JOBS[task_id] = {"type": "ig", "dir": task_dir, "chat_id": chat_id, "msg_id": msg_id,
                                "start_time": time.time(), "cancelled": False}
    try:
        asyncio.run_coroutine_threadsafe(
            sedit(chat_id, msg_id, "⬇️ <b>Downloading Instagram post…</b>", cbtn(task_id)), loop)
        counter = _GdlOut(); prog_stop = threading.Event()
        def _prog():
            while not prog_stop.is_set():
                with counter.lock: d = counter.done; b = counter.bytes; cur = counter.current
                asyncio.run_coroutine_threadsafe(
                    sedit(chat_id, msg_id,
                        f"⬇️ <b>Downloading Instagram post…</b>\n✅ <code>{d}</code> file(s) • 📦 <code>{fmtsz(b)}</code>\n📄 <code>{cur[:40]}</code>",
                        cbtn(task_id)), loop)
                prog_stop.wait(4)
        pt = threading.Thread(target=_prog, daemon=True); pt.start()
        _run_gdl(url, task_dir, user_id, counter)
        prog_stop.set(); pt.join(timeout=2)
        if ACTIVE_JOBS.get(task_id, {}).get("cancelled"): return
        real = [os.path.join(r,f) for r,_,fs in os.walk(task_dir) for f in fs if os.path.getsize(os.path.join(r,f)) > 0]
        if not real:
            return asyncio.run_coroutine_threadsafe(
                sedit(chat_id, msg_id, "❌ Nothing downloaded. Post may be private — add cookies via <code>/cookie</code>."), loop)
        prepare_payload(task_dir, job_prefs.get("force_zip", False), custom)
        dispatch(chat_id, msg_id, task_dir, job_prefs, task_id, folder, dl_mb=dir_size_mb(task_dir))
    except Exception as e:
        if "cancelled" not in str(e).lower() and task_id in ACTIVE_JOBS:
            asyncio.run_coroutine_threadsafe(
                sedit(chat_id, msg_id, f"❌ <b>Instagram failed:</b>\n<code>{str(e)[:300]}</code>"), loop)
    finally:
        with jobs_lock: ACTIVE_JOBS.pop(task_id, None)

# ── IG Archive ─────────────────────────────────────────────────────────
def process_ig_archive(chat_id, msg_id, username, content_types, folder, task_id, job_prefs, user_id):
    import asyncio
    loop = asyncio.get_event_loop()
    dest = job_prefs.get("destination", "drive")
    thread_id = job_prefs.get("thread_id")
    tmp_root = os.path.join(config.DOWNLOAD_DIR, task_id)
    os.makedirs(tmp_root, exist_ok=True)
    with jobs_lock:
        ACTIVE_JOBS[task_id] = {"type": "ig", "dir": tmp_root, "chat_id": chat_id, "msg_id": msg_id,
                                "start_time": time.time(), "cancelled": False}
    has_cookie = bool(get_active_cookie(user_id))
    start_time = ACTIVE_JOBS[task_id]["start_time"]
    asyncio.run_coroutine_threadsafe(
        sedit(chat_id, msg_id, f"📋 <b>Loading archive index for @{username}…</b>"), loop)
    previous_seen = _drive_load_index(username)
    session_new = set()
    seen_lock = threading.Lock()
    print(f"[ig_archive] @{username}: {len(previous_seen)} previously-seen IDs")
    drive_folder_ids = {}
    if dest in ("drive", "both"):
        try:
            ig_root = get_or_create_folder("instagram", config.DRIVE_FOLDER_ID)
            arch_root = get_or_create_folder(f"{username}_archive", ig_root)
            for ct in content_types: drive_folder_ids[ct] = get_or_create_folder(ct, arch_root)
        except Exception as e: print(f"[ig_folders] {e}")
    phase_stats = {ct: {"done": 0, "skip": 0, "bytes": 0, "uploaded": 0, "status": "⏳", "current": ""}
                   for ct in content_types}
    stats_lock = threading.Lock()
    current_ct = [content_types[0]]
    total_bytes = [0]
    last_render = [0.0]
    upload_q = queue.Queue(maxsize=50)
    def _cancelled():
        return (task_id not in ACTIVE_JOBS or ACTIVE_JOBS.get(task_id, {}).get("cancelled", False))
    def _render(force=False):
        now = time.time()
        if not force and now - last_render[0] < 3: return
        last_render[0] = now
        elapsed = fmt_time(int(now - start_time))
        with stats_lock: ct = current_ct[0]; tb = total_bytes[0]
        q_size = upload_q.qsize()
        total_done = sum(v["done"] for v in phase_stats.values())
        total_skip = sum(v["skip"] for v in phase_stats.values())
        total_up = sum(v.get("uploaded", 0) for v in phase_stats.values())
        lines = [f"📷 <b>Archiving @{username}</b>", "",
                 f"<code>{elapsed}</code> elapsed  {'☁️ Uploading…' if q_size > 0 else '⬇️ Downloading…'}",
                 f"📤 <code>{fmtsz(tb)}</code>  ⏳ queue: <code>{q_size}</code>", ""]
        for _ct in config.IG_CONTENT_TYPES:
            if _ct not in phase_stats: continue
            v = phase_stats[_ct]; status = v.get("status", "⏳")
            if status == "🔑 skipped": lines.append(f"{config.IG_LABELS[_ct]}: 🔑 <i>skipped (no cookie)</i>")
            else: lines.append(f"{config.IG_LABELS[_ct]}:  {status} <code>{v['done']}</code> ↓ • <code>{v['skip']}</code> ⏭️ • <code>{v.get('uploaded',0)}</code> ☁️")
        with stats_lock: cur = phase_stats.get(ct, {}).get("current", "")
        if cur: lines.append(f"\n📄 <code>{cur[:45]}</code>")
        asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, "\n".join(lines), cbtn(task_id)), loop)
    def _upload_worker():
        while True:
            item = upload_q.get()
            if item is None: upload_q.task_done(); break
            if _cancelled():
                fpath, ct, fname = item
                try: os.remove(fpath)
                except: pass
                upload_q.task_done(); continue
            fpath, ct, fname = item; fsize = 0
            try: fsize = os.path.getsize(fpath)
            except: pass
            uploaded = False
            try:
                if dest in ("drive", "both"):
                    parent = drive_folder_ids.get(ct, config.DRIVE_FOLDER_ID)
                    def _sfn(done, total, done_str, total_str, spd, _fn, _ct=ct): _render()
                    fid, _ = upload_file_to_drive(fpath, parent, chat_id, msg_id, task_id, status_fn=_sfn)
                    if fid:
                        with stats_lock: phase_stats[ct]["uploaded"] += 1
                        uploaded = True
                if dest in ("telegram", "both"):
                    ok = asyncio.run_coroutine_threadsafe(
                        _send_single_file_tg(chat_id, fpath, fname, thread_id=thread_id), loop).result()
                    if ok:
                        with stats_lock: phase_stats[ct]["uploaded"] += 1
                        uploaded = True
            except Exception as e: print(f"[ig_upload] {fname}: {e}")
            finally:
                try: os.remove(fpath)
                except: pass
                _render(); upload_q.task_done()
    n_workers = 1 if dest == "telegram" else config.UPLOAD_WORKERS
    workers = []
    for _ in range(n_workers):
        t = threading.Thread(target=_upload_worker, daemon=True); t.start(); workers.append(t)
    last_index_push = [time.time()]
    INDEX_PUSH_INTERVAL = 30
    def _maybe_push_index():
        now = time.time()
        if now - last_index_push[0] >= INDEX_PUSH_INTERVAL:
            last_index_push[0] = now
            with seen_lock: snap = previous_seen | session_new
            _drive_save_index(username, snap)
    def _make_on_file(ct):
        def _on_file(fpath):
            if not os.path.exists(fpath): return
            fname = os.path.basename(fpath)
            unique_id = re.sub(r'\.[a-z0-9]+$', '', fname, flags=re.IGNORECASE)
            if not unique_id: return
            with seen_lock:
                if unique_id in previous_seen:
                    with stats_lock: phase_stats[ct]["skip"] += 1
                    try: os.remove(fpath)
                    except: pass
                    _render(); return
                session_new.add(unique_id)
            _maybe_push_index()
            fsize = 0
            try: fsize = os.path.getsize(fpath)
            except: pass
            with stats_lock:
                phase_stats[ct]["done"] += 1
                phase_stats[ct]["bytes"] += fsize
                phase_stats[ct]["current"] = fname
                total_bytes[0] += fsize
            _render()
            upload_q.put((fpath, ct, fname))
        return _on_file
    for ct in content_types:
        if _cancelled(): break
        if ct in config._AUTH_TYPES and not has_cookie:
            with stats_lock: phase_stats[ct]["status"] = "🔑 skipped"
            _render(force=True); continue
        with stats_lock: current_ct[0] = ct; phase_stats[ct]["status"] = "⬇️"
        _render(force=True)
        ct_tmp = os.path.join(tmp_root, ct)
        os.makedirs(ct_tmp, exist_ok=True)
        url = config._IG_URL[ct].format(u=username)
        out = _GdlOut(on_file=_make_on_file(ct))
        try:
            if ct == "highlights": _run_gdl_highlights(username, ct_tmp, user_id, out)
            else: _run_gdl(url, ct_tmp, user_id, out)
        except Exception as e: print(f"[ig_archive] gdl error for {ct}: {e}")
        with stats_lock: phase_stats[ct]["skip"] += out.skipped; phase_stats[ct]["status"] = "✅"
        _render(force=True)
        if not _cancelled(): time.sleep(1.0)
    asyncio.run_coroutine_threadsafe(
        sedit(chat_id, msg_id, f"⏳ <b>Waiting for uploads to finish…</b>\nQueue: <code>{upload_q.qsize()}</code> remaining"), loop)
    def _drain_monitor():
        while True:
            q = upload_q.qsize()
            if q == 0: break
            _render(force=True); time.sleep(4)
    monitor = threading.Thread(target=_drain_monitor, daemon=True); monitor.start()
    upload_q.join()
    for _ in workers: upload_q.put(None)
    for t in workers: t.join(timeout=15)
    monitor.join(timeout=5)
    with seen_lock: final_index = previous_seen | session_new
    _drive_save_index(username, final_index)
    print(f"[ig_archive] Final save: {len(final_index)} IDs for @{username} → Drive")
    if _cancelled():
        asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, "🛑 <b>Cancelled.</b>"), loop)
        with jobs_lock: ACTIVE_JOBS.pop(task_id, None)
        shutil.rmtree(tmp_root, ignore_errors=True); return
    total_done = sum(v["done"] for v in phase_stats.values())
    total_skip = sum(v["skip"] for v in phase_stats.values())
    total_up = sum(v.get("uploaded", 0) for v in phase_stats.values())
    elapsed = fmt_time(int(time.time() - start_time))
    if total_done == 0:
        asyncio.run_coroutine_threadsafe(
            sedit(chat_id, msg_id,
                f"⚠️ <b>Nothing new for @{username}.</b>\n<code>{total_skip}</code> item(s) already archived.\n"
                f"Cookies may be missing or profile is empty."), loop)
    else:
        lines = [f"✅ <b>@{username} archive complete!</b>", "",
                 f"📥 <code>{total_done}</code> new files", f"☁️  <code>{total_up}</code> uploaded",
                 f"📦 <code>{fmtsz(total_bytes[0])}</code>", f"⏭️  <code>{total_skip}</code> skipped", f"⏱  <code>{elapsed}</code>", ""]
        for ct in content_types:
            v = phase_stats.get(ct, {})
            lines.append(f"{config.IG_LABELS[ct]}: <code>{v.get('done',0)}</code> ↓ • <code>{v.get('skip',0)}</code> ⏭️ • <code>{v.get('uploaded',0)}</code> ☁️")
        asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, "\n".join(lines)), loop)
    with jobs_lock: ACTIVE_JOBS.pop(task_id, None)
    shutil.rmtree(tmp_root, ignore_errors=True)

# ── IG command + picker ────────────────────────────────────────────────
@app.on_message(filters.command("ig") & filters.private)
async def cmd_ig(client, message):
    from .auth import is_auth
    if not is_auth(message): return
    _c, link, custom, folder, _r, _rf, _fid, _fn, thread_id, user_id = parse_cmd(message)
    job_prefs = get_job_prefs(user_id, "/ig", message.chat.id)
    job_prefs["user_id"] = user_id; job_prefs["thread_id"] = thread_id
    if not link: return await message.reply("⚠️ `/ig <post_url>` or `/ig <profile_url>`", parse_mode="Markdown")
    if not ensure_free(config.MIN_FREE_MB): return await message.reply("❌ Disk full.")
    tid = str(uuid.uuid4())[:6]
    if _ig_is_single(link):
        msg = await message.reply("🔍 <b>Processing Instagram post…</b>")
        task_queue.put((process_ig_single, (message.chat.id, msg.id, link, custom, folder, tid, job_prefs, user_id)))
    else:
        username = _ig_username(link)
        if not username: return await message.reply("❌ Could not extract username from URL.")
        PENDING_TASKS[tid] = {"type": "ig_choose", "username": username, "selected": {"stories"},
            "chat_id": message.chat.id, "msg_id": None, "custom": custom, "folder": folder,
            "job_prefs": job_prefs, "user_id": user_id}
        try:
            await _render_ig_picker(message.chat.id, None, tid, new_msg=True)
        except Exception as e:
            PENDING_TASKS.pop(tid, None)
            return await message.reply(f"❌ IG picker failed: <code>{e}</code>")

@app.on_callback_query(filters.regex(r"^igq:"))
async def handle_ig_picker_cb(client, call):
    parts = call.data.split(":"); tid = parts[1]; action = parts[2]
    task = PENDING_TASKS.get(tid)
    if not task or task.get("type") != "ig_choose":
        return await safe_answer(call.id, "⚠️ Session expired.")
    if call.from_user.id != task["user_id"]:
        return await safe_answer(call.id, "⛔ Not your selection.", show_alert=True)
    if action == "toggle":
        ct = parts[3]
        if ct in task["selected"]: task["selected"].discard(ct)
        else: task["selected"].add(ct)
        await safe_answer(call.id)
        await _render_ig_picker(call.message.chat.id, call.message.id, tid, new_msg=False)
    elif action == "start":
        if not task["selected"]: return await safe_answer(call.id, "⚠️ Select at least one type.", show_alert=True)
        await safe_answer(call.id, "✅ Starting…")
        PENDING_TASKS.pop(tid, None)
        tid_job = str(uuid.uuid4())[:6]
        msg = await client.send_message(call.message.chat.id, f"📋 <b>Archiving @{task['username']}…</b>")
        task_queue.put((process_ig_archive, (call.message.chat.id, msg.id, task["username"],
            list(task["selected"]), task["folder"], tid_job, task["job_prefs"], task["user_id"])))
    elif action == "cancel":
        PENDING_TASKS.pop(tid, None)
        await safe_answer(call.id, "❌ Cancelled.")
        await client.edit_message_text(call.message.chat.id, call.message.id, "❌ Cancelled.")

async def _render_ig_picker(chat_id, msg_id, tid, new_msg=False):
    task = PENDING_TASKS.get(tid)
    if not task: return
    username = task["username"]; selected = task["selected"]
    lines = [f"📷 <b>Select content for @{username}</b>", ""]
    for ct in config.IG_CONTENT_TYPES:
        check = "✅" if ct in selected else "⬜"
        label = f"{check} {config.IG_LABELS.get(ct, ct)}"
        if ct in config._AUTH_TYPES and ct not in selected: label += " 🔑"
        lines.append(label)
    lines.append(""); lines.append(f"<code>{len(selected)}/{len(config.IG_CONTENT_TYPES)}</code> selected")
    m = InlineKeyboardMarkup(row_width=2)
    for ct in config.IG_CONTENT_TYPES:
        check = "✅" if ct in selected else "⬜"
        m.add(InlineKeyboardButton(f"{check} {config.IG_LABELS.get(ct, ct)}", callback_data=f"igq:{tid}:toggle:{ct}"))
    m.row(InlineKeyboardButton("▶️ Start Archive", callback_data=f"igq:{tid}:start"),
          InlineKeyboardButton("❌ Cancel", callback_data=f"igq:{tid}:cancel"))
    text = "\n".join(lines)
    if new_msg:
        msg = await send_msg(chat_id, text, reply_markup=m)
        task["msg_id"] = msg.id
    else: await sedit(chat_id, msg_id, text, m, force=True)
