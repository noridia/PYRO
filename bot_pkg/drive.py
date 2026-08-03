import os, re, time, math, threading, subprocess, sys, json, uuid, asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import queue

from pyrogram import filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from . import config
from .core import (app, ACTIVE_JOBS, PENDING_TASKS, DRIVE_CACHE, DRIVE_NAV,
                   DRIVE_FILE_REFS, CACHE_TTL, jobs_lock, task_queue,
                   ensure_free, get_job_prefs, get_prefs, get_chat_prefs,
                   get_active_cookie, disk_free_mb, _load_json, _save_json,
                   cookie_path, send_msg, edit_msg, safe_answer, safe_delete, LPO, get_loop)
from .utils import (sedit, cbtn, fmtsz, bar, fmt_time, smooth, hms2s,
                    parse_cmd, prepare_payload, zip_dir, unzip_file, split_file)

# ── Split confirmation ──────────────────────────────────────────────────
_PENDING_SPLIT = {}
_SPLIT_LOCK = threading.Lock()

@app.on_callback_query(filters.regex(r"^split:"))
async def handle_split_cb(client, call):
    parts = call.data.split(":", 2)
    key, decision = parts[1], parts[2]
    with _SPLIT_LOCK:
        entry = _PENDING_SPLIT.get(key)
        if entry:
            entry["decision"] = decision
            entry["event"].set()
    await safe_answer(call.id)
    try:
        await client.edit_message_text(
            call.message.chat.id, call.message.id,
            "✅ Splitting…" if decision == "yes" else "🛑 Skipped.")
    except: pass

# ── Drive Service ──────────────────────────────────────────────────────
def get_drive_service():
    try:
        creds = Credentials(
            token=None, refresh_token=config.GCP_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=config.GCP_CLIENT_ID, client_secret=config.GCP_CLIENT_SECRET,
            scopes=["https://www.googleapis.com/auth/drive"])
        try:
            return build("drive", "v3", credentials=creds, static_discovery=True)
        except Exception:
            import httplib2
            http = httplib2.Http(timeout=15)
            creds.refresh(http)
            return build("drive", "v3", credentials=creds,
                        static_discovery=False, cache_discovery=False)
    except Exception as e:
        print(f"Drive auth failed: {e}")
        return None

def get_or_create_folder(name, parent_id):
    srv = get_drive_service()
    if not srv: return None
    name_esc = name.replace("'", "\\'")
    q = (f"name='{name_esc}' and mimeType='application/vnd.google-apps.folder'"
         f" and '{parent_id}' in parents and trashed=false")
    res = srv.files().list(q=q, spaces="drive", fields="files(id)").execute()
    files = res.get("files", [])
    if files: return files[0]["id"]
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id]}
    return srv.files().create(body=meta, fields="id").execute().get("id")

def drive_quota():
    srv = get_drive_service()
    if not srv: return 0, 0
    try:
        q = srv.about().get(fields="storageQuota").execute().get("storageQuota", {})
        return (int(q.get("usage", 0)) // (1024*1024),
                int(q.get("limit",  0)) // (1024*1024))
    except: return 0, 0

def _drive_fetch_all(folder_id):
    srv = get_drive_service()
    if not srv: return []
    all_files = []; token = None; page_retries = 0
    MAX_PAGE_RETRIES = 3; MAX_ITEMS = 5000
    while len(all_files) < MAX_ITEMS:
        kwargs = dict(
            q=f"'{folder_id}' in parents and trashed=false",
            spaces="drive",
            fields="nextPageToken, files(id,name,mimeType,size,starred)",
            pageSize=min(1000, MAX_ITEMS - len(all_files)),
            orderBy="folder,name",
            supportsAllDrives=True, includeItemsFromAllDrives=True)
        if token: kwargs["pageToken"] = token
        try:
            res = srv.files().list(**kwargs).execute()
            page_files = res.get("files", [])
            room = MAX_ITEMS - len(all_files)
            if len(page_files) > room: page_files = page_files[:room]
            all_files.extend(page_files)
            if len(all_files) >= MAX_ITEMS: break
            token = res.get("nextPageToken")
            page_retries = 0
            if not token: break
        except Exception as e:
            page_retries += 1
            print(f"[drive_fetch] Error (page attempt {page_retries}/{MAX_PAGE_RETRIES}): {e}")
            if page_retries >= MAX_PAGE_RETRIES: break
            time.sleep(2)
    return all_files

def _get_cached_drive_files(folder_id):
    now = time.time()
    if folder_id in DRIVE_CACHE:
        ts, files = DRIVE_CACHE[folder_id]
        if now - ts < CACHE_TTL: return files
    files = _drive_fetch_all(folder_id)
    DRIVE_CACHE[folder_id] = (now, files)
    return files

def _invalidate_cache(*folder_ids):
    for fid in folder_ids: DRIVE_CACHE.pop(fid, None)

def _extract_drive_id(link):
    if not link: return None
    m = re.search(r"/folders/([a-zA-Z0-9_-]{10,})", link)
    if m: return m.group(1)
    m = re.search(r"/file/d/([a-zA-Z0-9_-]{10,})", link)
    if m: return m.group(1)
    m = re.search(r"(?:id=|/d/)([a-zA-Z0-9_-]{10,})", link)
    if m: return m.group(1)
    return None

def _drive_fetch_recursive_with_paths(root_folder_id, max_total=10000, on_progress=None):
    srv = get_drive_service()
    if not srv: return []
    out = []; pending = [(root_folder_id, "")]
    while pending and len(out) < max_total:
        folder_id, path_prefix = pending.pop(0)
        token = None
        while True:
            try:
                kwargs = dict(
                    q=f"'{folder_id}' in parents and trashed=false",
                    spaces="drive",
                    fields="nextPageToken, files(id,name,mimeType,size)",
                    pageSize=min(1000, max_total - len(out)),
                    supportsAllDrives=True, includeItemsFromAllDrives=True)
                if token: kwargs["pageToken"] = token
                res = srv.files().list(**kwargs).execute()
            except Exception as e:
                print(f"[drive_recurse] list err folder={folder_id}: {e}")
                if on_progress: on_progress(len(out), max_total, f"⚠️ list err: {str(e)[:60]}")
                break
            for f in res.get("files", []):
                mime = f.get("mimeType", "")
                if mime == "application/vnd.google-apps.folder":
                    pending.append((f["id"], path_prefix + f["name"] + "/"))
                else:
                    out.append((f, path_prefix + f["name"]))
                if len(out) >= max_total: return out
            token = res.get("nextPageToken")
            if not token: break
            if on_progress: on_progress(len(out), max_total, f"listed {len(pending)} folders")
    return out

def _reg_ref(file_id, folder_id, page=0):
    key = str(uuid.uuid4())[:8]
    DRIVE_FILE_REFS[key] = {"fid": file_id, "folder": folder_id, "page": page}
    if len(DRIVE_FILE_REFS) > 500:
        oldest = list(DRIVE_FILE_REFS.keys())[:100]
        for k in oldest: DRIVE_FILE_REFS.pop(k, None)
    return key

def _get_ref(key):
    return DRIVE_FILE_REFS.get(key)

# ── Drive Service Pool ─────────────────────────────────────────────────
_drive_service_pool = []; _drive_pool_lock = threading.Lock(); _DRIVE_POOL_SIZE = 5

def get_drive_service_pooled():
    with _drive_pool_lock:
        if _drive_service_pool: return _drive_service_pool.pop()
    return get_drive_service()

def release_drive_service(srv):
    with _drive_pool_lock:
        if len(_drive_service_pool) < _DRIVE_POOL_SIZE:
            _drive_service_pool.append(srv)

# ── Drive Upload ───────────────────────────────────────────────────────
def upload_file_to_drive(fpath, parent_id, chat_id, msg_id, task_id, status_fn=None):
    def _prog(done, total):
        if status_fn:
            fname = os.path.basename(fpath)
            fsize = os.path.getsize(fpath)
            status_fn(done, fsize, fmtsz(done), fmtsz(fsize), "…", fname)
        else:
            import asyncio
            loop = get_loop()
            asyncio.run_coroutine_threadsafe(
                sedit(chat_id, msg_id,
                    f"☁️ <b>Uploading to Drive…</b>\n{bar(done, total)}\n"
                    f"📦 <code>{fmtsz(done)} / {fmtsz(total)}</code>\n"
                    f"📄 <code>{os.path.basename(fpath)[:50]}</code>",
                    cbtn(task_id)), loop)
    return upload_file_to_drive_fast(fpath, parent_id, task_id, on_progress=_prog)

def upload_file_to_drive_fast(fpath, parent_id, task_id, on_progress=None, retries=5):
    CHUNK = 32 * 1024 * 1024
    for attempt in range(retries):
        srv = get_drive_service_pooled()
        if not srv: return None, None
        try:
            fname = os.path.basename(fpath)
            fsize = os.path.getsize(fpath)
            media = MediaFileUpload(fpath, chunksize=CHUNK, resumable=True)
            req = srv.files().create(
                body={"name": fname, "parents": [parent_id]},
                media_body=media, fields="id,webViewLink", supportsAllDrives=True)
            response = None
            while response is None:
                if ACTIVE_JOBS.get(task_id, {}).get("cancelled"):
                    release_drive_service(srv); return None, None
                try:
                    status, response = req.next_chunk()
                    if status and on_progress: on_progress(status.resumable_progress, fsize)
                except HttpError as e:
                    if e.resp.status in (500, 502, 503, 504):
                        wait = 2 ** attempt
                        print(f"[drive_fast] chunk HTTP {e.resp.status}, retry in {wait}s")
                        time.sleep(wait); continue
                    raise
            if not response: release_drive_service(srv); return None, None
            fid = response.get("id", ""); link = response.get("webViewLink", "")
            try:
                srv.permissions().create(fileId=fid, body={"type": "anyone", "role": "reader"}).execute()
            except Exception: pass
            release_drive_service(srv)
            return fid, link
        except HttpError as e:
            release_drive_service(srv)
            if e.resp.status == 429:
                wait = min(60, 4 * (2 ** attempt))
                print(f"[drive_fast] 429, wait {wait}s"); time.sleep(wait); continue
            if attempt < retries - 1: time.sleep(2 ** attempt); continue
            print(f"[drive_fast] failed after {retries}: {e}"); return None, None
        except Exception as e:
            release_drive_service(srv)
            if attempt < retries - 1: time.sleep(2 ** attempt); continue
            print(f"[drive_fast] {e}"); return None, None
    return None, None

def upload_dir_to_drive(task_dir, base_folder_id, chat_id, msg_id, task_id, label="☁️"):
    top_items = []; all_links = []
    all_files = [os.path.join(r,f) for r,_,fs in os.walk(task_dir)
                 for f in fs if os.path.getsize(os.path.join(r,f)) > 0]
    total_bytes = sum(os.path.getsize(p) for p in all_files)
    done_bytes = 0; file_count = 0; total_files = len(all_files)
    def _walk(cur_dir, parent_id, is_top=False):
        nonlocal done_bytes, file_count
        items = sorted(os.listdir(cur_dir))
        files = [i for i in items if os.path.isfile(os.path.join(cur_dir,i))
                 and os.path.getsize(os.path.join(cur_dir,i)) > 0]
        subs = [i for i in items if os.path.isdir(os.path.join(cur_dir,i))]
        for f in files:
            if ACTIVE_JOBS.get(task_id,{}).get("cancelled"): return
            fpath = os.path.join(cur_dir,f); fsize = os.path.getsize(fpath)
            file_count += 1
            def _status_fn(done, total, done_str, total_str, spd, fname,
                           _db=done_bytes, _tb=total_bytes,
                           _fc=file_count, _tf=total_files):
                import asyncio
                loop = get_loop()
                asyncio.run_coroutine_threadsafe(
                    sedit(chat_id, msg_id,
                        f"{label} <b>Uploading to Drive…</b>\n📁 <code>{_fc}/{_tf}</code> files\n"
                        f"{bar(_db+done, _tb)}\n"
                        f"📦 <code>{fmtsz(_db+done)} / {fmtsz(_tb)}</code>\n"
                        f"🚀 <code>{spd}</code> • 📄 <code>{fname[:40]}</code>",
                        cbtn(task_id)), loop)
            fid, link = upload_file_to_drive(fpath, parent_id, chat_id, msg_id, task_id, _status_fn)
            if fid: all_links.append(link)
            if is_top: top_items.append((fid, f))
            done_bytes += fsize
        for sub in subs:
            if ACTIVE_JOBS.get(task_id,{}).get("cancelled"): return
            sub_id = get_or_create_folder(sub, parent_id)
            if is_top: top_items.append((sub_id, sub))
            _walk(os.path.join(cur_dir,sub), sub_id)
    _walk(task_dir, base_folder_id, is_top=True)
    return top_items, all_links

def _finalize_drive(chat_id, msg_id, task_id, top_items, all_links, start_time, size_mb):
    used, total = drive_quota()
    elapsed = fmt_time(int(time.time() - start_time))
    quota = f"{used} MB / {total} MB" if total else "Unknown"
    names_str = ", ".join(n for _,n in top_items[:2])
    if len(top_items) > 2: names_str += f" (+{len(top_items)-2} more)"
    base_text = (f"✅ <b>Saved to Drive</b>\n📄 <code>{names_str or 'items'}</code>\n"
                 f"📦 <code>{size_mb} MB</code> • ⏱ <code>{elapsed}</code>\n☁️ <code>{quota}</code>")
    if all_links:
        links = "\n".join(f"🔗 <a href='{l}'>View</a>" for l in all_links[:3])
        if len(all_links) > 3: links += f"\n_…and {len(all_links)-3} more_"
        base_text += f"\n\n{links}"
    single_files = [(fid,name) for fid,name in top_items if not _drive_item_is_folder(fid)]
    if len(single_files) == 1:
        _render_drive_success(chat_id, msg_id, base_text, single_files)
    else:
        import asyncio
        loop = get_loop()
        asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, base_text), loop)

def _drive_item_is_folder(file_id):
    try:
        srv = get_drive_service()
        if not srv: return False
        meta = srv.files().get(fileId=file_id, fields="mimeType").execute()
        return meta.get("mimeType") == "application/vnd.google-apps.folder"
    except: return False

def _render_drive_success(chat_id, msg_id, text, items):
    if not items:
        import asyncio
        loop = get_loop()
        asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, text), loop)
        return
    pid = str(uuid.uuid4())[:6]
    PENDING_TASKS[f"act_{pid}"] = {"items": items, "base_text": text}
    m = InlineKeyboardMarkup(row_width=2)
    m.row(InlineKeyboardButton("✏️ Rename", callback_data=f"act:{pid}:ren"),
          InlineKeyboardButton("🗑 Delete", callback_data=f"act:{pid}:del"))
    import asyncio
    loop = get_loop()
    asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, text, m), loop)

@app.on_callback_query(filters.regex(r"^act:"))
async def final_action_cb(client, call):
    parts = call.data.split(":"); pid = parts[1]; action = parts[2]
    key = f"act_{pid}"
    if key not in PENDING_TASKS: return await safe_answer(call.id, "Expired.")
    rec = PENDING_TASKS[key]
    chat_id = call.message.chat.id; msg_id = call.message.id
    if action == "del":
        PENDING_TASKS.pop(key)
        await safe_answer(call.id, "Deleting…")
        srv = get_drive_service()
        for item_id, _ in rec["items"]:
            try:
                if srv: srv.files().delete(fileId=item_id).execute()
            except: pass
        await sedit(chat_id, msg_id, rec["base_text"] + "\n\n🗑 <i>(Deleted from Drive)</i>")
    elif action == "ren":
        if len(rec["items"]) > 1:
            return await safe_answer(call.id, "Only for single-file uploads.", show_alert=True)
        await safe_answer(call.id)
        item_id, old_name = rec["items"][0]
        PENDING_TASKS[f"ren_{call.message.chat.id}"] = {
            "id": item_id, "msg_id": msg_id, "base_text": rec["base_text"]}
        m = InlineKeyboardMarkup().add(
            InlineKeyboardButton("❌ Cancel", callback_data=f"ren_cancel:{call.message.chat.id}"))
        await send_msg(call.message.chat.id,
            f"✏️ <b>Rename:</b> <code>{old_name}</code>\n\nReply with new name:",
            reply_markup=m)

@app.on_message(filters.text & filters.private)
async def process_rename(client, message):
    key = f"ren_{message.chat.id}"
    if key not in PENDING_TASKS: return
    rec = PENDING_TASKS.pop(key)
    new_name = message.text.strip()
    try:
        srv = get_drive_service()
        if srv: srv.files().update(fileId=rec["id"], body={"name": new_name}).execute()
        await message.reply(f"✅ Renamed to <code>{new_name}</code>")
        await sedit(message.chat.id, rec["msg_id"],
                    rec["base_text"] + f"\n\n✏️ <i>(Renamed to: {new_name})</i>")
    except Exception as e:
        await message.reply(f"❌ <code>{e}</code>")

@app.on_callback_query(filters.regex(r"^ren_cancel:"))
async def cancel_rename(client, call):
    PENDING_TASKS.pop(f"ren_{call.message.chat.id}", None)
    await safe_answer(call.id, "Cancelled")
    await sedit(call.message.chat.id, call.message.id, "❌ Rename cancelled.")

# ── Dispatch + StreamingDispatcher + Telegram Sender ──────────────────
def dispatch(chat_id, msg_id, task_dir, job_prefs, task_id,
             folder_name=None, dl_mb=0, delete_after=True):
    dest = job_prefs.get("destination", "drive")
    with jobs_lock:
        start_time = ACTIVE_JOBS.get(task_id, {}).get("start_time", time.time())
    if ACTIVE_JOBS.get(task_id, {}).get("cancelled"):
        shutil.rmtree(task_dir, ignore_errors=True); return
    with jobs_lock:
        if task_id in ACTIVE_JOBS:
            ACTIVE_JOBS[task_id]["type"] = "upload"
            ACTIVE_JOBS[task_id]["dir"] = task_dir
    all_files = sorted([os.path.join(r, f) for r, _, fs in os.walk(task_dir)
                        for f in fs if os.path.getsize(os.path.join(r, f)) > 0])
    total_files = len(all_files)
    total_bytes = sum(os.path.getsize(p) for p in all_files)
    sd = StreamingDispatcher(chat_id, msg_id, task_id, job_prefs,
        folder_name=folder_name, total_files=total_files,
        total_bytes=total_bytes, base_dir=task_dir)
    try:
        for fpath in all_files:
            if ACTIVE_JOBS.get(task_id, {}).get("cancelled"): break
            sd.submit(fpath)
        sd.wait_done()
        if not ACTIVE_JOBS.get(task_id, {}).get("cancelled"):
            sd.finalize()
    finally:
        if delete_after: shutil.rmtree(task_dir, ignore_errors=True)
        with jobs_lock: ACTIVE_JOBS.pop(task_id, None)

async def _send_single_file_tg(chat_id, fpath, fname, thread_id=None):
    import asyncio
    ext = os.path.splitext(fname)[1].lower()
    fsize = os.path.getsize(fpath)
    del_m = InlineKeyboardMarkup()
    del_m.add(InlineKeyboardButton("🗑 Delete", callback_data="delup"))
    base_kw = dict(caption=f"<code>{fname}</code>", reply_markup=del_m)
    if thread_id: base_kw["reply_to_message_id"] = thread_id

    if fsize > 2 * 1024 * 1024 * 1024:
        # Pyrogram supports up to 2GB, but let's split at 1.9GB for safety
        parts = split_file(fpath, chunk=1_900_000_000)
        all_ok = True
        for i, part in enumerate(parts):
            pname = f"{fname}.part{i+1:03d}"
            pkw = dict(caption=f"<code>{pname}</code>", reply_markup=del_m)
            if thread_id: pkw["reply_to_message_id"] = thread_id
            try:
                with open(part, "rb") as fh:
                    await app.send_document(chat_id, fh, **pkw)
            except Exception as e:
                print(f"[tg_send] part {pname}: {e}"); all_ok = False
            finally:
                try: os.remove(part)
                except: pass
        return all_ok

    try:
        with open(fpath, "rb") as fh:
            if ext in config.VID_EXT:
                await app.send_video(chat_id, fh, supports_streaming=True, **base_kw)
            elif ext in config.AUD_EXT:
                await app.send_audio(chat_id, fh, **base_kw)
            elif ext in config.IMG_EXT:
                await app.send_photo(chat_id, fh, **base_kw)
            else:
                await app.send_document(chat_id, fh, **base_kw)
        return True
    except Exception as e:
        print(f"[tg_send] {fname}: {e}")
        try:
            with open(fpath, "rb") as fh:
                await app.send_document(chat_id, fh, **base_kw)
            return True
        except Exception as e2:
            print(f"[tg_send] fallback failed {fname}: {e2}")
            await ssend(chat_id, f"❌ Failed to send <code>{fname}</code>:\n<code>{str(e2)[:200]}</code>",
                        thread_id=thread_id)
            return False

class StreamingDispatcher:
    def __init__(self, chat_id, msg_id, task_id, job_prefs,
                 folder_name=None, total_files=None, total_bytes=None, base_dir=None):
        self.chat_id = chat_id; self.msg_id = msg_id; self.task_id = task_id
        self.job_prefs = job_prefs; self.dest = job_prefs.get("destination", "drive")
        self.thread_id = job_prefs.get("thread_id"); self.folder_name = folder_name
        self.total_files = total_files; self.total_bytes = total_bytes; self.base_dir = base_dir
        self.done_files = 0; self.done_bytes = 0; self.skip_files = 0; self.err_files = 0
        self._drive_links = []; self.start_time = time.time()
        self._lock = threading.Lock(); self._last_render = 0.0
        self._speed_samples = []
        self._upload_q = queue.Queue(maxsize=24); self._workers = []
        self._drive_parent = None; self._drive_ready = threading.Event(); self._drive_error = None
        if self.dest == "drive": n_workers = 6
        elif self.dest == "both": n_workers = 4
        else: n_workers = 1
        for _ in range(n_workers):
            t = threading.Thread(target=self._worker, daemon=True); t.start(); self._workers.append(t)
        if self.dest in ("drive", "both"):
            threading.Thread(target=self._init_drive, daemon=True).start()
        else: self._drive_ready.set()

    def _init_drive(self):
        try:
            if self.folder_name:
                self._drive_parent = get_or_create_folder(self.folder_name, config.DRIVE_FOLDER_ID)
            else: self._drive_parent = config.DRIVE_FOLDER_ID
        except Exception as e:
            self._drive_error = str(e); self._drive_parent = config.DRIVE_FOLDER_ID
            print(f"[StreamDispatch] Drive init failed: {e}")
        self._drive_ready.set()

    def _cancelled(self):
        return (self.task_id not in ACTIVE_JOBS or ACTIVE_JOBS.get(self.task_id, {}).get("cancelled", False))

    def submit(self, fpath):
        while not self._cancelled():
            try: self._upload_q.put(fpath, timeout=2.0); return
            except queue.Full: continue
        try: os.remove(fpath)
        except: pass

    def _worker(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while True:
            try: item = self._upload_q.get(timeout=5.0)
            except queue.Empty: continue
            if item is None: self._upload_q.task_done(); break
            try:
                loop.run_until_complete(self._process_one(item))
            except Exception as e:
                print(f"[StreamDispatch._worker] {e}")
                with self._lock: self.err_files += 1
            finally: self._upload_q.task_done()

    async def _process_one(self, fpath):
        if self._cancelled():
            try: os.remove(fpath); return
            except: pass
            return
        fname = (os.path.relpath(fpath, self.base_dir) if self.base_dir else os.path.basename(fpath))
        try: fsize = os.path.getsize(fpath)
        except: fsize = 0
        parts = split_file(fpath) if fsize > config.TG_PART_BYTES else [fpath]
        is_split = len(parts) > 1
        uploaded_ok = False
        try:
            if self.dest in ("drive", "both"):
                self._drive_ready.wait(timeout=30)
                parent = self._drive_parent or config.DRIVE_FOLDER_ID
                for part in parts:
                    if self._cancelled(): break
                    pname = os.path.basename(part)
                    psize = os.path.getsize(part)
                    def _prog(done, total, _pname=pname, _psize=psize):
                        self._render_progress(phase="☁️ Uploading", current_file=_pname,
                                              current_done=done, current_total=_psize)
                    fid, link = upload_file_to_drive_fast(part, parent, self.task_id, on_progress=_prog)
                    if fid:
                        uploaded_ok = True
                        if link:
                            with self._lock: self._drive_links.append((os.path.basename(part), fid, link))
                    if is_split:
                        try: os.remove(part)
                        except: pass
            if self.dest in ("telegram", "both"):
                for part in parts:
                    if self._cancelled(): break
                    pname = (os.path.relpath(part, self.base_dir) if self.base_dir else os.path.basename(part))
                    self._render_progress(phase="📤 Sending", current_file=pname,
                                          current_done=0, current_total=os.path.getsize(part))
                    ok = await _send_single_file_tg(self.chat_id, part, pname, thread_id=self.thread_id)
                    if ok: uploaded_ok = True
                    if is_split:
                        try: os.remove(part)
                        except: pass
        finally:
            if uploaded_ok:
                try: os.remove(fpath)
                except: pass
                if is_split:
                    for p in parts:
                        try: os.remove(p)
                        except: pass
        with self._lock:
            if uploaded_ok: self.done_files += 1; self.done_bytes += fsize
            else: self.err_files += 1
        self._render_progress(force=True)

    def _render_progress(self, phase="", current_file="", current_done=0, current_total=0, speed="", force=False):
        now = time.time()
        if not force and now - self._last_render < 4.0: return
        self._last_render = now
        with self._lock:
            df = self.done_files; db = self.done_bytes
            ef = self.err_files; sf = self.skip_files; tf = self.total_files; tb = self.total_bytes
            self._speed_samples.append((db, now))
            if len(self._speed_samples) > 10: self._speed_samples.pop(0)
            if not speed and len(self._speed_samples) > 1:
                dt = self._speed_samples[-1][1] - self._speed_samples[0][1]
                if dt > 1:
                    delta = self._speed_samples[-1][0] - self._speed_samples[0][0]
                    if delta > 0: speed = f"{fmtsz(delta / dt)}/s"
        elapsed = now - self.start_time
        file_bar = bar(current_done, current_total) if current_total > 0 else ""
        if tf and tf > 0:
            overall_bar = bar(df, tf)
        else:
            overall_bar = bar(db, tb or db or 1)
        lines = [f"{df+1}. {current_file[:60]}" if current_file else f"{overall_bar}"]
        lines.append(f"┟ {file_bar or overall_bar}")
        lines.append(f"┠ Processed → {fmtsz(db)}" + (f" of {fmtsz(tb)}" if tb else ""))
        if phase:
            lines.append(f"┠ Status → {phase}")
        if speed:
            lines.append(f"┠ Speed → {speed}")
        total_pct = (db / tb * 100) if tb and tb > 0 else (df / tf * 100) if tf and tf > 0 else 0
        if total_pct > 1:
            est_total = elapsed / (total_pct / 100)
            eta = max(0, est_total - elapsed)
            lines.append(f"┠ Time → {fmt_time(elapsed)} of {fmt_time(est_total)} ( {fmt_time(eta)} )")
        else:
            lines.append(f"┠ Time → {fmt_time(elapsed)}")
        lines.append(f"┖ Stop → /c_{self.task_id}")
        extras = []
        if ef: extras.append(f"❌ {ef} err")
        if sf: extras.append(f"⏭️ {sf} skip")
        if extras:
            lines.append(f"  {' | '.join(extras)}")
        import asyncio
        loop = get_loop()
        asyncio.run_coroutine_threadsafe(
            sedit(self.chat_id, self.msg_id, "\n".join(lines), cbtn(self.task_id)), loop)

    def wait_done(self):
        self._upload_q.join()
        for _ in self._workers: self._upload_q.put(None)
        for t in self._workers: t.join(timeout=30)

    def finalize(self, extra_text=""):
        elapsed = fmt_time(int(time.time() - self.start_time))
        with self._lock: df = self.done_files; db = self.done_bytes; ef = self.err_files; sf = self.skip_files
        dest_icon = {"drive": "☁️ Drive", "telegram": "📱 Telegram", "both": "☁️+📱 Both"}.get(self.dest, self.dest)
        lines = [f"✅ <b>Done!</b>", f"📬 <code>{dest_icon}</code>",
                 f"📥 <code>{df}</code> file(s) • 📦 <code>{fmtsz(db)}</code>",
                 f"⏱ <code>{elapsed}</code>"]
        if ef: lines.append(f"❌ <code>{ef}</code> error(s)")
        if sf: lines.append(f"⏭️ <code>{sf}</code> skipped")
        if extra_text: lines.append(extra_text)
        with self._lock: dlinks = list(self._drive_links)
        if dlinks and self.dest in ("drive", "both"):
            items = [(fid, n) for n, fid, _ in dlinks]
            if len(dlinks) == 1: lines.append(f"\n🔗 <a href='{dlinks[0][2]}'>View on Drive</a>")
            else:
                shown = dlinks[:3]
                lines += [f"🔗 <a href='{l}'>{n[:28]}</a>" for n, _, l in shown]
                if len(dlinks) > 3: lines.append(f"_…and {len(dlinks)-3} more_")
            _render_drive_success(self.chat_id, self.msg_id, "\n".join(lines), items)
        else:
            import asyncio
            loop = get_loop()
            asyncio.run_coroutine_threadsafe(
                sedit(self.chat_id, self.msg_id, "\n".join(lines)), loop)
        if self.dest in ("drive", "both"):
            _invalidate_cache(self._drive_parent or config.DRIVE_FOLDER_ID)

# ── Drive Browser ──────────────────────────────────────────────────────
@app.on_message(filters.command("drive") & filters.private)
async def cmd_drive(client, message):
    from .auth import is_auth
    if not is_auth(message): return
    msg = await message.reply("📂 <b>Connecting to Drive…</b>")
    def _start():
        srv = get_drive_service()
        if not srv:
            asyncio.run_coroutine_threadsafe(
                sedit(message.chat.id, msg.id, "❌ Drive not configured or auth failed."),
                get_loop())
            return
        DRIVE_NAV[message.chat.id] = [(config.DRIVE_FOLDER_ID, "🏠 Root")]
        asyncio.run_coroutine_threadsafe(
            render_drive(message.chat.id, msg.id, config.DRIVE_FOLDER_ID, 0),
            get_loop())
    threading.Thread(target=_start, daemon=True).start()

async def render_drive(chat_id, msg_id, folder_id, page=0):
    try:
        files = await asyncio.to_thread(_get_cached_drive_files, folder_id)
        total = len(files)
        total_pages = max(1, math.ceil(total / config.DRIVE_PAGE_SIZE))
        page = max(0, min(page, total_pages - 1))
        page_files = files[page*config.DRIVE_PAGE_SIZE:(page+1)*config.DRIVE_PAGE_SIZE]
        nav = DRIVE_NAV.get(chat_id, [(config.DRIVE_FOLDER_ID, "🏠 Root")])
        breadcrumb = " › ".join(n for _, n in nav[-3:])
        text = (f"📂 <b>Drive</b> › <code>{breadcrumb}</code>\n"
                f"<i>{total} item{'s' if total != 1 else ''} • page {page+1}/{total_pages}</i>")
        m = InlineKeyboardMarkup(row_width=1)
        if len(nav) > 1:
            m.add(InlineKeyboardButton(f"⬆️ Back to {nav[-2][1][:18]}", callback_data="drv:up:0"))
        for f in page_files:
            is_folder = f["mimeType"] == "application/vnd.google-apps.folder"
            icon = "📁" if is_folder else "📄"
            star = "⭐ " if f.get("starred") else ""
            sz = fmtsz(int(f.get("size", 0))) if f.get("size") else ""
            label = f"{star}{icon} {f['name'][:22]} {sz}".strip()
            if is_folder:
                ref_key = _reg_ref(f["id"], folder_id, page)
                m.row(InlineKeyboardButton(label, callback_data=f"drv:cd:{f['id']}:{f['name'][:10].replace(':','').replace('|','')}"),
                      InlineKeyboardButton("🔗", callback_data=f"drv:folderlink:{ref_key}"))
            else:
                ref_key = _reg_ref(f["id"], folder_id, page)
                m.row(InlineKeyboardButton(label, callback_data="drv:noop"),
                      InlineKeyboardButton("🔗", callback_data=f"drv:link:{ref_key}"),
                      InlineKeyboardButton("🗑", callback_data=f"drv:del:{ref_key}"))
        nav_row = []
        if page > 0: nav_row.append(InlineKeyboardButton("◀️", callback_data=f"drv:pg:{folder_id}:{page-1}"))
        nav_row.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="drv:noop"))
        if page < total_pages - 1: nav_row.append(InlineKeyboardButton("▶️", callback_data=f"drv:pg:{folder_id}:{page+1}"))
        if nav_row: m.row(*nav_row)
        cur_ref_key = _reg_ref(folder_id, folder_id, page)
        m.row(InlineKeyboardButton("🔗 This Folder", callback_data=f"drv:folderlink:{cur_ref_key}"),
              InlineKeyboardButton("🗑 Del folder", callback_data=f"drv:delfolder:{folder_id}"),
              InlineKeyboardButton("🔄 Refresh", callback_data=f"drv:refresh:{folder_id}:{page}"))
        await sedit(chat_id, msg_id, text, m)
    except Exception as e:
        await sedit(chat_id, msg_id, f"❌ Drive error: <code>{e}</code>")

@app.on_callback_query(filters.regex(r"^drv:"))
async def drive_nav_cb(client, call):
    parts = call.data.split(":")
    action = parts[1]
    chat_id = call.message.chat.id; msg_id = call.message.id
    if action == "noop": return await safe_answer(call.id)
    await safe_answer(call.id)
    if action == "cd":
        folder_id = parts[2]; fname = parts[3] if len(parts) > 3 else "Folder"
        nav = DRIVE_NAV.setdefault(chat_id, [(config.DRIVE_FOLDER_ID, "🏠 Root")])
        nav.append((folder_id, fname))
        await sedit(chat_id, msg_id, f"📂 <b>Loading</b> <code>{fname}</code>…")
        threading.Thread(target=asyncio.run_coroutine_threadsafe,
            args=(render_drive(chat_id, msg_id, folder_id, 0), get_loop()),
            daemon=True).start()
    elif action == "up":
        nav = DRIVE_NAV.get(chat_id, [(config.DRIVE_FOLDER_ID, "🏠 Root")])
        if len(nav) > 1: nav.pop()
        await sedit(chat_id, msg_id, "📂 <b>Going back…</b>")
        threading.Thread(target=asyncio.run_coroutine_threadsafe,
            args=(render_drive(chat_id, msg_id, nav[-1][0], 0), get_loop()),
            daemon=True).start()
    elif action == "pg":
        await sedit(chat_id, msg_id, "📂 <b>Loading…</b>")
        threading.Thread(target=asyncio.run_coroutine_threadsafe,
            args=(render_drive(chat_id, msg_id, parts[2], int(parts[3])), get_loop()),
            daemon=True).start()
    elif action == "refresh":
        _invalidate_cache(parts[2])
        await sedit(chat_id, msg_id, "🔄 <b>Refreshing…</b>")
        threading.Thread(target=asyncio.run_coroutine_threadsafe,
            args=(render_drive(chat_id, msg_id, parts[2], int(parts[3])), get_loop()),
            daemon=True).start()
    elif action == "del":
        ref_key = parts[2]; ref = _get_ref(ref_key)
        if not ref: return await sedit(chat_id, msg_id, "❌ Reference expired. Please refresh.")
        file_id = ref["fid"]; folder_id = ref["folder"]; page = ref["page"]
        try:
            srv = get_drive_service()
            if srv: srv.files().delete(fileId=file_id).execute()
            _invalidate_cache(folder_id)
        except Exception as e: return await sedit(chat_id, msg_id, f"❌ <code>{e}</code>")
        threading.Thread(target=asyncio.run_coroutine_threadsafe,
            args=(render_drive(chat_id, msg_id, folder_id, page), get_loop()),
            daemon=True).start()
    elif action == "delfolder":
        folder_id = parts[2]
        if folder_id == config.DRIVE_FOLDER_ID:
            return await safe_answer(call.id, "❌ Cannot delete root.", show_alert=True)
        m = InlineKeyboardMarkup(row_width=2)
        m.row(InlineKeyboardButton("⚠️ Yes", callback_data=f"drv:delfolder_ok:{folder_id}"),
              InlineKeyboardButton("❌ Cancel", callback_data=f"drv:pg:{folder_id}:0"))
        await sedit(chat_id, msg_id, "⚠️ <b>Delete this entire folder?</b> Cannot be undone.", m)
    elif action == "delfolder_ok":
        folder_id = parts[2]
        try:
            srv = get_drive_service()
            if srv: srv.files().delete(fileId=folder_id).execute()
            _invalidate_cache(folder_id)
        except Exception as e: return await sedit(chat_id, msg_id, f"❌ <code>{e}</code>")
        nav = DRIVE_NAV.get(chat_id, [(config.DRIVE_FOLDER_ID, "🏠 Root")])
        if len(nav) > 1: nav.pop()
        _invalidate_cache(nav[-1][0])
        threading.Thread(target=asyncio.run_coroutine_threadsafe,
            args=(render_drive(chat_id, msg_id, nav[-1][0], 0), get_loop()),
            daemon=True).start()
    elif action == "link":
        ref_key = parts[2]; ref = _get_ref(ref_key)
        if not ref: return await sedit(chat_id, msg_id, "❌ Reference expired. Please refresh.")
        file_id = ref["fid"]; folder_id = ref["folder"]; page = ref["page"]
        try:
            srv = get_drive_service()
            if srv:
                srv.permissions().create(fileId=file_id, body={"type": "anyone", "role": "reader"}).execute()
                link = srv.files().get(fileId=file_id, fields="webViewLink").execute().get("webViewLink", "?")
                back_m = InlineKeyboardMarkup().add(
                    InlineKeyboardButton("⬅️ Back", callback_data=f"drv:pg:{folder_id}:{page}"))
                await sedit(chat_id, msg_id, f"🔗 <b>Public File Link:</b>\n<code>{link}</code>", back_m)
        except Exception as e: await sedit(chat_id, msg_id, f"❌ <code>{e}</code>")
    elif action == "folderlink":
        ref_key = parts[2]; ref = _get_ref(ref_key)
        if not ref: return await sedit(chat_id, msg_id, "❌ Reference expired. Please refresh.")
        folder_id_target = ref["fid"]; parent_folder = ref["folder"]; page = ref["page"]
        def _get_folder_link():
            try:
                srv = get_drive_service()
                if not srv:
                    asyncio.run_coroutine_threadsafe(
                        sedit(chat_id, msg_id, "❌ Drive not configured."), get_loop())
                    return
                try: srv.permissions().create(fileId=folder_id_target, body={"type": "anyone", "role": "reader"}).execute()
                except Exception as e: print(f"[folderlink perm] {e}")
                meta = srv.files().get(fileId=folder_id_target, fields="name,webViewLink").execute()
                fname = meta.get("name", "Folder"); link = meta.get("webViewLink", "?")
                is_current = (folder_id_target == parent_folder)
                nav = DRIVE_NAV.get(chat_id, [(config.DRIVE_FOLDER_ID, "🏠 Root")])
                back_folder = nav[-2][0] if is_current and len(nav) > 1 else parent_folder
                back_page = 0 if is_current else page
                back_m = InlineKeyboardMarkup().add(
                    InlineKeyboardButton("⬅️ Back", callback_data=f"drv:pg:{back_folder}:{back_page}"))
                asyncio.run_coroutine_threadsafe(
                    sedit(chat_id, msg_id,
                        f"📁 <b>Folder:</b> <code>{fname}</code>\n\n🔗 <b>Public Link:</b>\n<code>{link}</code>", back_m),
                    get_loop())
            except Exception as e:
                asyncio.run_coroutine_threadsafe(
                    sedit(chat_id, msg_id, f"❌ <code>{e}</code>"), get_loop())
        threading.Thread(target=_get_folder_link, daemon=True).start()

@app.on_message(filters.command("drivesearch") & filters.private)
async def cmd_drivesearch(client, message):
    from .auth import is_auth
    if not is_auth(message): return
    query = message.text.split(None, 1)[1].strip() if " " in message.text else ""
    if not query: return await message.reply("⚠️ `/drivesearch <query>`", parse_mode="Markdown")
    try:
        srv = get_drive_service()
        if not srv: return await message.reply("❌ Drive not configured.")
        q_esc = query.replace("'", "\\'")
        res = await asyncio.to_thread(
            lambda: srv.files().list(q=f"name contains '{q_esc}' and trashed=false",
                spaces="drive", fields="files(id,name,mimeType,size)", pageSize=20).execute())
        files = res.get("files", [])
        if not files: return await message.reply(f"🔍 No results for <code>{query}</code>.")
        m = InlineKeyboardMarkup(row_width=1)
        for f in files:
            icon = "📁" if f["mimeType"] == "application/vnd.google-apps.folder" else "📄"
            sz = fmtsz(int(f.get("size", 0))) if f.get("size") else ""
            label = f"{icon} {f['name'][:28]} {sz}".strip()
            ref_key = _reg_ref(f["id"], config.DRIVE_FOLDER_ID, 0)
            m.add(InlineKeyboardButton(label, callback_data=f"drv:link:{ref_key}"))
        await message.reply(f"🔍 <b>Results for</b> <code>{query}</code>:", reply_markup=m)
    except Exception as e: await message.reply(f"❌ <code>{e}</code>")

# ── GDrive Download ───────────────────────────────────────────────────
@app.on_message(filters.command("gdrive") & filters.private)
async def cmd_gdrive(client, message):
    from .auth import is_auth
    if not is_auth(message): return
    _, link, custom, folder, _, _, _, _, thread_id, user_id = parse_cmd(message)
    if not link: return await message.reply("⚠️ `/gdrive <url>`", parse_mode="Markdown")
    if not ensure_free(config.MIN_FREE_MB): return await message.reply("❌ Disk full.")
    job_prefs = get_job_prefs(user_id, "/gdrive", message.chat.id)
    job_prefs["user_id"] = user_id; job_prefs["thread_id"] = thread_id
    tid = str(uuid.uuid4())[:6]
    msg = await message.reply("⬇️ <b>Queued Google Drive download…</b>")
    task_queue.put((process_gdown, (message.chat.id, msg.id, link, custom, folder, tid, job_prefs)))

def process_gdown(chat_id, msg_id, link, custom, folder, task_id, job_prefs):
    import asyncio
    loop = get_loop()
    task_dir = os.path.join(config.DOWNLOAD_DIR, task_id)
    os.makedirs(task_dir, exist_ok=True)
    with jobs_lock:
        ACTIVE_JOBS[task_id] = {"type": "gdown", "obj": None, "dir": task_dir,
            "chat_id": chat_id, "msg_id": msg_id, "start_time": time.time(), "cancelled": False}
    force_zip = bool(job_prefs.get("force_zip"))
    def _walk_done():
        files = []
        for r, _, fs in os.walk(task_dir):
            for f in fs:
                if f.endswith((".part", ".tmp", ".crdownload")): continue
                fp = os.path.join(r, f)
                try:
                    if os.path.getsize(fp) > 0: files.append(fp)
                except OSError: pass
        return files
    def _render_progress(extra=""):
        files = _walk_done()
        n = len(files); b = sum(os.path.getsize(p) for p in files)
        body = f"☁️ <b>Downloading from Drive…</b>\n📁 <code>{n}</code> files • 📦 <code>{fmtsz(b)}</code>"
        if extra: body += f"\n<code>{extra}</code>"
        asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, body, cbtn(task_id)), loop)
    def _drive_fallback_subprocess(target_link):
        proc = subprocess.Popen([sys.executable, "-m", "gdown", "--folder", target_link, "-O", task_dir + "/"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, bufsize=1)
        with jobs_lock:
            if task_id in ACTIVE_JOBS: ACTIVE_JOBS[task_id]["obj"] = proc
        last_t = 0.0
        for line in iter(proc.stdout.readline, ""):
            if task_id not in ACTIVE_JOBS:
                try: proc.terminate()
                except: pass; break
            now = time.time()
            if now - last_t > 4: last_t = now; _render_progress(extra=line.strip()[:80])
        proc.wait()
    try:
        _render_progress("Starting…")
        is_folder = ("folder" in link.lower() or "drive.google.com/drive" in link)
        if is_folder:
            folder_id = _extract_drive_id(link)
            srv = get_drive_service()
            if folder_id and srv:
                def _list_prog(done, total, extra):
                    asyncio.run_coroutine_threadsafe(
                        sedit(chat_id, msg_id,
                            f"☁️ <b>Listing folder…</b>\n📁 <code>{done}</code> files found" +
                            (f"\n<code>{extra}</code>" if extra else ""), cbtn(task_id)), loop)
                file_list = _drive_fetch_recursive_with_paths(folder_id, max_total=10000, on_progress=_list_prog)
                if not file_list:
                    asyncio.run_coroutine_threadsafe(
                        sedit(chat_id, msg_id, "❌ Folder empty or no access."), loop); return
                total_files = len(file_list)
                total_bytes = sum(int(f.get("size", 0) or 0) for f, _ in file_list)
                asyncio.run_coroutine_threadsafe(
                    sedit(chat_id, msg_id,
                        f"☁️ <b>Found <code>{total_files}</code> files in Drive folder</b>\n"
                        f"📦 <code>{fmtsz(total_bytes)}</code>\n⏳ <i>Starting…</i>", cbtn(task_id)), loop)
                sd = StreamingDispatcher(chat_id, msg_id, task_id, job_prefs, folder_name=folder,
                    total_files=total_files, total_bytes=total_bytes, base_dir=task_dir)
                used_paths = set()
                def _unique(fpath):
                    nonlocal used_paths
                    if fpath not in used_paths: used_paths.add(fpath); return fpath
                    base, ext = os.path.splitext(fpath); i = 1
                    while True:
                        cand = f"{base}_{i}{ext}"
                        if cand not in used_paths: used_paths.add(cand); return cand
                        i += 1
                workspace_export = {
                    "application/vnd.google-apps.document": ("application/pdf", ".pdf"),
                    "application/vnd.google-apps.spreadsheet":
                        ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"),
                    "application/vnd.google-apps.presentation": ("application/pdf", ".pdf"),
                }
                done_n = 0; err_n = 0
                def _build_path(relpath, fname=None, ext_override=None):
                    safe_parts = [re.sub(r'[<>:"\\|?*\x00]', '_', p) for p in relpath.split("/") if p]
                    if fname is not None: safe_parts = safe_parts[:-1] + [fname]
                    elif ext_override:
                        last = safe_parts[-1]; base, _ = os.path.splitext(last); safe_parts[-1] = base + ext_override
                    fpath = _unique(os.path.join(task_dir, *safe_parts))
                    os.makedirs(os.path.dirname(fpath), exist_ok=True)
                    return fpath
                def _download_one(meta, relpath):
                    if task_id not in ACTIVE_JOBS: return (False, None, meta.get("name", "?"))
                    mime = meta.get("mimeType", ""); fname = meta.get("name", "file")
                    wsrv = get_drive_service_pooled()
                    try:
                        if mime in workspace_export:
                            export_mime, export_ext = workspace_export[mime]
                            fpath = _build_path(relpath, ext_override=export_ext)
                            req = wsrv.files().export_media(fileId=meta["id"], mimeType=export_mime)
                        else:
                            fpath = _build_path(relpath)
                            req = wsrv.files().get_media(fileId=meta["id"])
                    except Exception as e:
                        release_drive_service(wsrv); print(f"[drive_dl] req {fname}: {e}"); return (False, None, fname)
                    try:
                        with open(fpath, "wb") as fh:
                            downloader = _DriveDownloader(req, fh)
                            while True:
                                if task_id not in ACTIVE_JOBS: break
                                status, done = downloader.next_chunk()
                                if done: break
                    except Exception as e:
                        print(f"[drive_dl] {fname}: {e}"); release_drive_service(wsrv)
                        try: os.remove(fpath)
                        except: pass
                        return (False, None, fname)
                    release_drive_service(wsrv)
                    ok = os.path.exists(fpath) and os.path.getsize(fpath) > 0
                    if not ok:
                        try: os.remove(fpath)
                        except: pass
                    return (ok, fpath if ok else None, fname)
                with ThreadPoolExecutor(max_workers=6) as pool:
                    futures = {pool.submit(_download_one, meta, relpath): (meta, relpath) for meta, relpath in file_list}
                    for future in as_completed(futures):
                        if task_id not in ACTIVE_JOBS: break
                        ok, fpath, fname = future.result()
                        if ok:
                            sd.submit(fpath); done_n += 1
                        else: err_n += 1
                        if done_n % 10 == 0:
                            asyncio.run_coroutine_threadsafe(
                                sedit(chat_id, msg_id,
                                    f"☁️ <b>Drive folder download</b>\n✅ <code>{done_n}</code> done • ❌ <code>{err_n}</code> err\n"
                                    f"📤 <code>{done_n+err_n}/{total_files}</code>", cbtn(task_id)), loop)
                sd.wait_done()
                if task_id in ACTIVE_JOBS:
                    if force_zip:
                        all_files = _walk_done()
                        if all_files:
                            zip_name = _zip_and_collect(task_dir, all_files, folder or custom or task_id,
                                                        chat_id, msg_id, task_id)
                            if zip_name: _dispatch_files(task_dir, [os.path.join(task_dir, zip_name)],
                                                         chat_id, msg_id, task_id, job_prefs, folder, force_zip_done=True)
                    else: sd.finalize()
                return
            if not folder_id:
                asyncio.run_coroutine_threadsafe(
                    sedit(chat_id, msg_id, "⚠️ Couldn't parse folder ID — falling back to gdown…", cbtn(task_id)), loop)
            _drive_fallback_subprocess(link)
        else:
            file_id = _extract_drive_id(link)
            if file_id:
                fpath = _gdown_single_with_api(file_id, task_dir, custom, chat_id, msg_id, task_id, job_prefs)
                if fpath:
                    if force_zip:
                        all_files = [fpath]
                        zip_name = _zip_and_collect(task_dir, all_files, folder or custom or task_id,
                                                    chat_id, msg_id, task_id)
                        if zip_name and task_id in ACTIVE_JOBS:
                            _dispatch_files(task_dir, [os.path.join(task_dir, zip_name)],
                                            chat_id, msg_id, task_id, job_prefs, folder, force_zip_done=True)
                    else: _dispatch_files(task_dir, [fpath], chat_id, msg_id, task_id, job_prefs, folder)
                return
            else:
                out = (os.path.join(task_dir, custom) if custom else task_dir + "/")
                proc = subprocess.Popen([sys.executable, "-m", "gdown", link, "-O", out],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                with jobs_lock:
                    if task_id in ACTIVE_JOBS: ACTIVE_JOBS[task_id]["obj"] = proc; ACTIVE_JOBS[task_id]["type"] = "gdown"
                proc.wait()
                if task_id not in ACTIVE_JOBS: return
        time.sleep(0.5)
        all_files = _walk_done()
        if not all_files:
            asyncio.run_coroutine_threadsafe(
                sedit(chat_id, msg_id, "❌ Download failed (private or invalid link)."), loop); return
        if force_zip and is_folder:
            zip_name = _zip_and_collect(task_dir, all_files, folder or custom or task_id, chat_id, msg_id, task_id)
            if not zip_name: return
            all_files = [os.path.join(task_dir, zip_name)]
        _dispatch_files(task_dir, all_files, chat_id, msg_id, task_id, job_prefs, folder,
                        force_zip_done=(force_zip and is_folder))
    except Exception as e:
        if task_id in ACTIVE_JOBS:
            asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, f"❌ <code>{e}</code>"), loop)
    finally:
        with jobs_lock: ACTIVE_JOBS.pop(task_id, None)
        shutil.rmtree(task_dir, ignore_errors=True)

def _zip_and_collect(task_dir, all_files, base_name, chat_id, msg_id, task_id):
    import asyncio
    loop = get_loop()
    zip_base = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', (base_name or "").strip())
    if not zip_base: zip_base = "archive"
    zip_name = zip_base + ".zip"
    zip_path = os.path.join(task_dir, zip_name)
    asyncio.run_coroutine_threadsafe(
        sedit(chat_id, msg_id, f"🗜️ <b>Zipping</b> <code>{zip_name}</code>…\n📁 <code>{len(all_files)}</code> files", cbtn(task_id)), loop)
    try: zip_dir(task_dir, zip_path)
    except Exception as e:
        asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, f"❌ Zip failed: <code>{e}</code>"), loop); return None
    for r, _, fs in os.walk(task_dir):
        for f in fs:
            fp = os.path.join(r, f)
            if fp != zip_path:
                try: os.remove(fp)
                except: pass
    return zip_name

def _dispatch_files(task_dir, all_files, chat_id, msg_id, task_id, job_prefs, folder, force_zip_done=False):
    total_bytes = sum(os.path.getsize(p) for p in all_files)
    sd = StreamingDispatcher(chat_id, msg_id, task_id, job_prefs, folder_name=folder,
        total_files=len(all_files), total_bytes=total_bytes, base_dir=task_dir)
    for fp in all_files:
        if ACTIVE_JOBS.get(task_id, {}).get("cancelled"): break
        sd.submit(fp)
    sd.wait_done()
    if task_id in ACTIVE_JOBS: sd.finalize(extra_text="🗜️ Zipped" if force_zip_done else "")

def _gdown_single_with_api(file_id, task_dir, custom, chat_id, msg_id, task_id, job_prefs):
    srv = get_drive_service()
    if not srv:
        out = os.path.join(task_dir, custom or "file")
        try:
            proc = subprocess.Popen([sys.executable, "-m", "gdown", f"https://drive.google.com/uc?id={file_id}", "-O", out],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            with jobs_lock:
                if task_id in ACTIVE_JOBS: ACTIVE_JOBS[task_id]["obj"] = proc; ACTIVE_JOBS[task_id]["type"] = "gdown"
            proc.wait()
            if os.path.exists(out) and os.path.getsize(out) > 0: return out
        except Exception as e: print(f"[gdown_fallback] {e}")
        return None
    try:
        meta = srv.files().get(fileId=file_id, fields="name,size,mimeType").execute()
        fname = custom or meta.get("name", "file")
        fsize = int(meta.get("size", 0))
        fpath = os.path.join(task_dir, fname)
        mime = meta.get("mimeType", "")
        export_map = {
            "application/vnd.google-apps.document": ("application/pdf", fname + ".pdf"),
            "application/vnd.google-apps.spreadsheet":
                ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", fname + ".xlsx"),
            "application/vnd.google-apps.presentation": ("application/pdf", fname + ".pdf"),
        }
        if mime in export_map:
            export_mime, export_fname = export_map[mime]
            fpath = os.path.join(task_dir, export_fname)
            req = srv.files().export_media(fileId=file_id, mimeType=export_mime)
        else: req = srv.files().get_media(fileId=file_id)
        samples = []; last_t = [0.0]; done_b = [0]
        import asyncio
        loop = get_loop()
        with open(fpath, "wb") as fh:
            downloader = _DriveDownloader(req, fh)
            while True:
                if ACTIVE_JOBS.get(task_id, {}).get("cancelled"): return None
                status, done = downloader.next_chunk()
                if status:
                    dl = status.resumable_progress; done_b[0] = dl; now = time.time()
                    samples.append((dl, now))
                    if len(samples) > 10: samples.pop(0)
                    if now - last_t[0] > 3:
                        spd = smooth(samples)
                        asyncio.run_coroutine_threadsafe(
                            sedit(chat_id, msg_id,
                                f"⬇️ <b>Downloading from Drive…</b>\n{bar(dl, fsize)}\n"
                                f"📦 <code>{fmtsz(dl)} / {fmtsz(fsize)}</code>\n🚀 <code>{spd}</code>\n📄 <code>{fname[:50]}</code>",
                                cbtn(task_id)), loop)
                        last_t[0] = now
                if done: break
        if os.path.exists(fpath) and os.path.getsize(fpath) > 0: return fpath
    except Exception as e:
        print(f"[gdown_api] {e}")
        out = os.path.join(task_dir, custom or "file")
        try:
            proc = subprocess.Popen([sys.executable, "-m", "gdown", f"https://drive.google.com/uc?id={file_id}", "-O", out],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            with jobs_lock:
                if task_id in ACTIVE_JOBS: ACTIVE_JOBS[task_id]["obj"] = proc; ACTIVE_JOBS[task_id]["type"] = "gdown"
            proc.wait()
            if os.path.exists(out) and os.path.getsize(out) > 0: return out
        except Exception as e2: print(f"[gdown_fallback] {e2}")
    return None

class _DriveDownloader:
    def __init__(self, request, fh):
        from googleapiclient.http import MediaIoBaseDownload
        self._dl = MediaIoBaseDownload(fh, request, chunksize=config.DRIVE_CHUNK)
    def next_chunk(self):
        return self._dl.next_chunk()
