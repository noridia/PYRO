import os, re, threading, time, shutil, subprocess, sys, uuid, shlex, asyncio
from pyrogram import filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from . import config
from .core import (app, ACTIVE_JOBS, PENDING_TASKS, jobs_lock, task_queue,
                   ensure_free, get_job_prefs, get_active_cookie, cookie_path,
                   dir_size_mb, send_msg, edit_msg, safe_answer, LPO, get_loop)
from .utils import (sedit, ssend, cbtn, bar, fmtsz, fmt_time, smooth,
                    parse_cmd, get_thread_id, unzip_file, split_file)

class BatchTracker:
    def __init__(self, chat_id, msg_id, cmd, task_ids, urls):
        self.chat_id = chat_id; self.msg_id = msg_id; self.cmd = cmd
        self.total = len(urls)
        self.tasks = list(zip(task_ids, urls, ["queued"] * len(urls)))
        self.lock = threading.Lock()
        loop = get_loop()
        asyncio.run_coroutine_threadsafe(self._render(), loop)

    def notify(self, task_id, status):
        with self.lock:
            for i, (tid, url, _) in enumerate(self.tasks):
                if tid == task_id:
                    self.tasks[i] = (tid, url, status); break
        loop = get_loop()
        asyncio.run_coroutine_threadsafe(self._render(), loop)

    async def _render(self):
        done = sum(1 for _, _, s in self.tasks if s in ("done", "error", "cancelled"))
        pct = done * 100 // self.total if self.total else 0
        lines = [f"📋 <b>Batch {self.cmd}</b>  <code>{done}/{self.total}</code>", bar(pct, 100)]
        status_icon = {"queued": "⏳", "downloading": "⬇️", "uploading": "☁️",
                       "done": "✅", "error": "❌", "cancelled": "🚫"}
        for tid, url, s in self.tasks:
            lines.append(f"{status_icon.get(s, '⏳')} <code>{url[:50]}</code>")
        await sedit(self.chat_id, self.msg_id, "\n".join(lines))

    async def finalize(self):
        success = sum(1 for _, _, s in self.tasks if s == "done")
        failed = sum(1 for _, _, s in self.tasks if s == "error")
        cancelled = sum(1 for _, _, s in self.tasks if s == "cancelled")
        lines = [f"📋 <b>Batch {self.cmd} Complete</b>"]
        if success: lines.append(f"✅ <code>{success}</code> succeeded")
        if failed: lines.append(f"❌ <code>{failed}</code> failed")
        if cancelled: lines.append(f"🚫 <code>{cancelled}</code> cancelled")
        await sedit(self.chat_id, self.msg_id, "\n".join(lines))

def _batch_notify(job_prefs, success, cancelled=False):
    bid = job_prefs.get("batch_id")
    if not bid: return
    batch_tid = job_prefs.get("batch_tid")
    with jobs_lock:
        tracker = PENDING_TASKS.get(bid)
    if not tracker: return
    if cancelled: status = "cancelled"
    elif success: status = "done"
    else: status = "error"
    tracker.notify(batch_tid, status)
    with jobs_lock:
        if all(s != "queued" for _, _, s in tracker.tasks):
            loop = get_loop()
            asyncio.run_coroutine_threadsafe(tracker.finalize(), loop)
            PENDING_TASKS.pop(bid, None)

# ── Mirror / Leech ─────────────────────────────────────────────────────
@app.on_message(filters.command(["m", "zm", "l", "zl"]) & filters.private)
async def cmd_mirror(client, message):
    from .auth import is_auth
    if not is_auth(message): return
    cmd, link, custom, folder, t_range, raw_flags, file_id, file_name, thread_id, user_id = parse_cmd(message)
    job_prefs = get_job_prefs(user_id, cmd, message.chat.id)
    job_prefs["user_id"] = user_id; job_prefs["thread_id"] = thread_id
    if file_id:
        if not ensure_free(config.MIN_FREE_MB): return await message.reply("❌ Disk full.")
        tid = str(uuid.uuid4())[:6]
        msg = await message.reply("⬇️ Downloading from Telegram…")
        task_queue.put((process_tg_file, (message.chat.id, msg.id, file_id, file_name,
                         cmd, custom, folder, tid, job_prefs)))
        return
    if not link: return await message.reply(f"⚠️ <code>{cmd} &lt;url&gt;</code> or reply to a file.")
    ll = link.lower()
    if any(d in ll for d in config.IG_HOSTS):
        from .instagram import cmd_ig
        return await cmd_ig(message)
    if "drive.google.com" in ll:
        from .drive import cmd_gdrive
        return await cmd_gdrive(message, mirror_cmd=cmd)
    if any(d in ll for d in config.YT_HOSTS) or ll.endswith((".m3u8", ".mpd")):
        from .youtube import cmd_yt
        yt_map = {"/m": "/yt", "/zm": "/ytzm", "/l": "/ytl", "/zl": "/ytzl"}
        return await cmd_yt(message, forced_cmd=yt_map.get(cmd, "/yt"))
    if ll.startswith("magnet:") or ll.endswith(".torrent"):
        return await cmd_dl(message, forced_cmd="/torrent", mirror_cmd=cmd)
    if any(d in ll for d in config.GALLERY_HOSTS):
        return await cmd_dl(message, forced_cmd="/gallery", mirror_cmd=cmd)
    await cmd_dl(message, forced_cmd="/dl", mirror_cmd=cmd)

# ── TG File Download ───────────────────────────────────────────────────
def process_tg_file(chat_id, msg_id, file_id, file_name, cmd, custom, folder, task_id, job_prefs):
    import asyncio
    loop = get_loop()
    task_dir = os.path.join(config.DOWNLOAD_DIR, task_id)
    os.makedirs(task_dir, exist_ok=True)
    with jobs_lock:
        ACTIVE_JOBS[task_id] = {"type": "tg", "dir": task_dir, "chat_id": chat_id, "msg_id": msg_id,
                                "start_time": time.time(), "cancelled": False}
    from .drive import StreamingDispatcher
    sd = StreamingDispatcher(chat_id, msg_id, task_id, job_prefs, folder_name=folder,
                             total_files=1, total_bytes=None, base_dir=task_dir)
    try:
        # Pyrogram can download files up to 2GB directly via MTProto
        fpath = os.path.join(task_dir, custom or file_name)
        loop.run_until_complete(
            app.download_media(file_id, file_name=fpath))
        # Check if download was cancelled
        if ACTIVE_JOBS.get(task_id, {}).get("cancelled"): raise Exception("Cancelled")
        # Update progress
        if os.path.exists(fpath) and os.path.getsize(fpath) > 0:
            ext = os.path.splitext(fpath)[1].lower()
            if ext in config.ARC_EXT and job_prefs.get("unzip_first"):
                asyncio.run_coroutine_threadsafe(
                    sedit(chat_id, msg_id, "📦 <b>Extracting…</b>", cbtn(task_id)), loop)
                extract_dir = fpath + "_extracted"; os.makedirs(extract_dir, exist_ok=True)
                unzip_file(fpath, extract_dir); os.remove(fpath)
                for r2, _, fs in os.walk(extract_dir):
                    for f2 in fs:
                        fp2 = os.path.join(r2, f2)
                        if os.path.getsize(fp2) > 0: sd.submit(fp2)
            else: sd.submit(fpath)
        sd.wait_done()
        if not ACTIVE_JOBS.get(task_id, {}).get("cancelled"): sd.finalize()
    except Exception as e:
        if task_id in ACTIVE_JOBS:
            asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, f"❌ <code>{e}</code>"), loop)
        try: sd.wait_done()
        except: pass
    finally:
        with jobs_lock: ACTIVE_JOBS.pop(task_id, None)
        shutil.rmtree(task_dir, ignore_errors=True)

# ── Subprocess Downloader (aria2c / gallery-dl / wget) ─────────────────
@app.on_message(filters.command(["torrent", "gallery", "clone"]) & filters.private)
async def cmd_dl(client, message, forced_cmd=None, mirror_cmd=None):
    from .auth import is_auth
    if not is_auth(message): return
    cmd = forced_cmd or message.text.split()[0].lower()
    if "@" in cmd: cmd = cmd.split("@")[0]
    _, link, custom, folder, _, raw_flags, _, _, thread_id, user_id = parse_cmd(message)
    job_prefs = get_job_prefs(user_id, mirror_cmd or cmd, message.chat.id)
    job_prefs["user_id"] = user_id; job_prefs["thread_id"] = thread_id
    if cmd == "/dl" and message.text and " " in message.text:
        body = message.text.split(None, 1)[1]
        if "\n" in body:
            urls = [u.strip() for u in body.splitlines() if u.strip()]
            total = len(urls); ids = []; batch_key = f"batch_{uuid.uuid4().hex[:8]}"
            bm = await message.reply(f"📋 <b>Batch {cmd}:</b> 0/{total}")
            for u in urls:
                tid = str(uuid.uuid4())[:6]; ids.append(tid)
                sm = await message.reply(f"📋 <code>{tid}</code> <code>{u[:50]}</code>\n⏳ queued")
                bp = dict(job_prefs, batch_id=batch_key, batch_tid=tid)
                task_queue.put((process_subprocess,
                    (message.chat.id, sm.id, cmd, u, None, None, tid, bp, None)))
            with jobs_lock:
                PENDING_TASKS[batch_key] = BatchTracker(
                    message.chat.id, bm.id, cmd, ids, urls)
            return
    if not link: return await message.reply(f"⚠️ <code>{cmd} &lt;url&gt;</code>")
    if not ensure_free(config.MIN_FREE_MB): return await message.reply("❌ Disk full.")
    tid = str(uuid.uuid4())[:6]
    msg = await message.reply(f"⚡ <b>Queued</b> <code>{cmd}</code>…")
    task_queue.put((process_subprocess, (message.chat.id, msg.id, cmd, link, custom, folder, tid, job_prefs, raw_flags)))

def _build_args(cmd, link, custom, task_dir, raw_flags, user_id):
    ck = get_active_cookie(user_id)
    if cmd == "/dl":
        args = ["aria2c", "-x", "16", "-s", "16", "--file-allocation=none", "--summary-interval=1",
                "--max-connection-per-server=16", "-d", task_dir]
        if raw_flags:
            try: args += shlex.split(raw_flags)
            except: pass
        args.append(link)
        if custom: args += ["-o", custom]
        return args
    if cmd == "/torrent":
        args = ["aria2c", "--seed-time=0", "--file-allocation=none", "--summary-interval=1", "-d", task_dir]
        if raw_flags:
            try: args += shlex.split(raw_flags)
            except: pass
        args.append(link); return args
    if cmd == "/gallery":
        args = ["gallery-dl", "--dest", task_dir]
        if ck: args += ["--cookies", ck]
        if raw_flags:
            try: args += shlex.split(raw_flags)
            except: pass
        args.append(link); return args
    if cmd == "/clone":
        args = ["wget", "--mirror", "--convert-links", "--adjust-extension",
                "--page-requisites", "--no-parent", "-P", task_dir]
        if raw_flags:
            try: args += shlex.split(raw_flags)
            except: pass
        args.append(link); return args
    return []

def _parse_aria2_error(log_lines):
    if not log_lines: return None
    seen_status = None; seen_errcd = None
    for line in log_lines:
        m = re.search(r"status=(\d{3})", line)
        if m: seen_status = m.group(1)
        m = re.search(r"errorCode=(\d+)", line)
        if m: seen_errcd = m.group(1)
    HTTP_MSGS = {"400":"Bad request (400)","401":"Unauthorized (401)","403":"Forbidden (403)",
                 "404":"Not found (404)","408":"Request timeout (408)","410":"Gone (410)",
                 "429":"Rate limited (429)","500":"Server error (500)","502":"Bad gateway (502)",
                 "503":"Service unavailable (503)","504":"Gateway timeout (504)"}
    if seen_status and seen_status in HTTP_MSGS: return HTTP_MSGS[seen_status]
    if seen_status: return f"HTTP {seen_status} error"
    if seen_errcd in ("3",): return "Resource not found"
    if seen_errcd in ("22",): return "HTTP error — link unreachable"
    if seen_errcd: return f"aria2c error code {seen_errcd}"
    for line in log_lines:
        if "[ERROR]" in line or "Exception:" in line: return line.strip()[:200]
    return None

def process_subprocess(chat_id, msg_id, cmd, link, custom, folder, task_id, job_prefs, raw_flags=None):
    import asyncio
    loop = get_loop()
    task_dir = os.path.join(config.DOWNLOAD_DIR, task_id)
    os.makedirs(task_dir, exist_ok=True)
    user_id = job_prefs.get("user_id", chat_id)
    _success = False; _cancelled = False
    with jobs_lock:
        ACTIVE_JOBS[task_id] = {"type": "subp", "obj": None, "dir": task_dir, "chat_id": chat_id,
                                "msg_id": msg_id, "start_time": time.time(), "cancelled": False}
    from .drive import StreamingDispatcher
    sd = StreamingDispatcher(chat_id, msg_id, task_id, job_prefs, folder_name=folder,
                             total_files=None, total_bytes=None, base_dir=task_dir)
    _seen_files = set(); _watch_stop = threading.Event()
    def _file_watcher():
        while not _watch_stop.is_set():
            try:
                now = time.time()
                for r, _, fs in os.walk(task_dir):
                    for f in fs:
                        fp = os.path.join(r, f)
                        if fp in _seen_files: continue
                        if f.endswith((".part", ".aria2", ".tmp", ".crdownload", ".ytdl")): continue
                        try:
                            st = os.stat(fp)
                            if st.st_size > 0 and now - st.st_mtime > 2:
                                _seen_files.add(fp); sd.submit(fp)
                        except FileNotFoundError: pass
            except Exception as e: print(f"[file_watcher] {e}")
            _watch_stop.wait(2)
    watcher = threading.Thread(target=_file_watcher, daemon=True); watcher.start()
    try:
        args = _build_args(cmd, link, custom, task_dir, raw_flags, user_id)
        if msg_id:
            asyncio.run_coroutine_threadsafe(
                sedit(chat_id, msg_id, f"⬇️ <b>{cmd}…</b>\n<code>{link[:60]}</code>", cbtn(task_id)), loop)
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                universal_newlines=True, bufsize=1)
        with jobs_lock:
            if task_id in ACTIVE_JOBS: ACTIVE_JOBS[task_id]["obj"] = proc
        last_t = 0.0; err_last_t = 0.0; full_log = []; err_seen = set()
        for line in iter(proc.stdout.readline, ""):
            if task_id not in ACTIVE_JOBS: _cancelled = True; break
            full_log.append(line.rstrip())
            now = time.time()
            if now - last_t < 3: continue
            last_t = now
            m2 = re.search(r"\[#\w+\s+([\d.]+\w+)/([\d.]+\w+)\((\d+)%\).*?DL:([\d.]+\w+)", line)
            if m2 and msg_id:
                ds, ts, pct, spd = m2.groups()
                eta_m = re.search(r"ETA:(\S+)", line)
                eta = eta_m.group(1) if eta_m else "N/A"
                asyncio.run_coroutine_threadsafe(
                    sedit(chat_id, msg_id,
                        f"⬇️ <b>{cmd}</b>\n{bar(float(pct), 100)}\n"
                        f"📦 <code>{ds}/{ts}</code> • 🚀 <code>{spd}/s</code>\n⏳ <code>{eta}</code>",
                        cbtn(task_id)), loop)
            elif cmd == "/gallery" and msg_id and any(kw in line for kw in ("Downloading","Saved","Skipped","error")):
                asyncio.run_coroutine_threadsafe(
                    sedit(chat_id, msg_id, f"🖼️ <b>gallery-dl</b>\n<code>{line.strip()[:120]}</code>", cbtn(task_id)), loop)
            elif msg_id and ("[ERROR]" in line or "Exception:" in line or "status=5" in line or "status=4" in line):
                fp = line.strip()[:120]
                if fp in err_seen: continue
                err_seen.add(fp)
                if now - err_last_t > 4: err_last_t = now
                asyncio.run_coroutine_threadsafe(
                    sedit(chat_id, msg_id, f"⚠️ <b>{cmd} error:</b>\n<code>{fp}</code>", cbtn(task_id)), loop)
        proc.wait()
        _watch_stop.set(); watcher.join(timeout=5)
        for r, _, fs in os.walk(task_dir):
            for f in fs:
                fp = os.path.join(r, f)
                if fp not in _seen_files and not f.endswith((".part",".aria2",".tmp",".crdownload",".ytdl")) and os.path.getsize(fp) > 0:
                    _seen_files.add(fp); sd.submit(fp)
        sd.wait_done()
        job_alive = task_id in ACTIVE_JOBS
        if job_alive:
            has = any(True for _ in _seen_files)
            friendly_err = _parse_aria2_error(full_log)
            if has:
                sd.finalize()
                _success = True
                if friendly_err:
                    asyncio.run_coroutine_threadsafe(
                        sedit(chat_id, msg_id, f"⚠️ <b>Partial — aria2c reported:</b> <code>{friendly_err}</code>"), loop)
            elif msg_id:
                if friendly_err:
                    asyncio.run_coroutine_threadsafe(
                        sedit(chat_id, msg_id,
                            f"❌ <b>Download failed</b>\n<code>{friendly_err}</code>\n\n_Link:_ <code>{link[:80]}</code>"), loop)
                else:
                    log_lines = [l for l in full_log if l][-40:]
                    log_text = "\n".join(log_lines)
                    if len(log_text) > 3500: log_text = "…\n" + log_text[-3300:]
                    import html as _html
                    log_text = _html.escape(log_text)
                    loop.run_until_complete(
                        app.edit_message_text(chat_id, msg_id,
                            f"<b>📝 {cmd} output:</b>\n<pre>{log_text or '(none)'}</pre>"))
    except Exception as e:
        _watch_stop.set()
        if task_id in ACTIVE_JOBS and msg_id:
            asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, f"❌ <code>{e}</code>"), loop)
        _cancelled = _cancelled or task_id not in ACTIVE_JOBS
        try: sd.wait_done()
        except: pass
    finally:
        _watch_stop.set()
        _batch_notify(job_prefs, _success, _cancelled)
        with jobs_lock: ACTIVE_JOBS.pop(task_id, None)
        shutil.rmtree(task_dir, ignore_errors=True)

# ── Unzip ──────────────────────────────────────────────────────────────
@app.on_message(filters.command(["unzip", "unzipl", "unzipm"]) & filters.private)
async def cmd_unzip(client, message):
    from .auth import is_auth
    if not is_auth(message): return
    raw_cmd = message.text.split()[0].lower()
    cmd = raw_cmd.split("@")[0]
    _, link, custom, folder, _, _, file_id, file_name, thread_id, user_id = parse_cmd(message)
    job_prefs = get_job_prefs(user_id, cmd, message.chat.id)
    job_prefs["user_id"] = user_id; job_prefs["thread_id"] = thread_id
    if file_id:
        if not ensure_free(config.MIN_FREE_MB): return await message.reply("❌ Disk full.")
        tid = str(uuid.uuid4())[:6]
        msg = await message.reply("⬇️ Downloading archive…")
        task_queue.put((process_unzip_tg, (message.chat.id, msg.id, file_id, file_name, tid, job_prefs, folder)))
        return
    if link:
        if not ensure_free(config.MIN_FREE_MB): return await message.reply("❌ Disk full.")
        tid = str(uuid.uuid4())[:6]
        msg = await message.reply("⬇️ Downloading archive…")
        task_queue.put((process_unzip_url, (message.chat.id, msg.id, link, tid, job_prefs, folder, custom)))
        return
    await message.reply(
        "⚠️ Reply to a <code>.zip/.tar/.rar/.7z</code> file with <code>/unzipl</code> (→ Telegram) or <code>/unzipm</code> (→ Drive)\n"
        "or <code>/unzip &lt;url&gt;</code>")

def process_unzip_tg(chat_id, msg_id, file_id, file_name, task_id, job_prefs, folder):
    import asyncio
    loop = get_loop()
    task_dir = os.path.join(config.DOWNLOAD_DIR, task_id); os.makedirs(task_dir, exist_ok=True)
    try:
        with jobs_lock:
            ACTIVE_JOBS[task_id] = {"type": "tg", "dir": task_dir, "chat_id": chat_id, "msg_id": msg_id,
                                    "start_time": time.time(), "cancelled": False}
        asyncio.run_coroutine_threadsafe(
            sedit(chat_id, msg_id, f"⬇️ <b>Downloading</b> <code>{file_name}</code>…", cbtn(task_id)), loop)
        arc = os.path.join(task_dir, file_name)
        loop.run_until_complete(app.download_media(file_id, file_name=arc))
        if ACTIVE_JOBS.get(task_id, {}).get("cancelled"): return
        asyncio.run_coroutine_threadsafe(
            sedit(chat_id, msg_id, f"📂 <b>Extracting</b> <code>{file_name}</code>…", cbtn(task_id)), loop)
        ext_dir = os.path.join(task_dir, "extracted"); unzip_file(arc, ext_dir); os.remove(arc)
        for item in os.listdir(ext_dir): shutil.move(os.path.join(ext_dir, item), task_dir)
        shutil.rmtree(ext_dir, ignore_errors=True)
        from .drive import dispatch
        dispatch(chat_id, msg_id, task_dir, job_prefs, task_id, folder, dl_mb=dir_size_mb(task_dir))
    except Exception as e:
        if task_id in ACTIVE_JOBS:
            asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, f"❌ <code>{e}</code>"), loop)
    finally:
        with jobs_lock: ACTIVE_JOBS.pop(task_id, None)

def process_unzip_url(chat_id, msg_id, url, task_id, job_prefs, folder, custom):
    import asyncio
    loop = get_loop()
    task_dir = os.path.join(config.DOWNLOAD_DIR, task_id); os.makedirs(task_dir, exist_ok=True)
    try:
        with jobs_lock:
            ACTIVE_JOBS[task_id] = {"type": "subp", "obj": None, "dir": task_dir, "chat_id": chat_id, "msg_id": msg_id,
                                    "start_time": time.time(), "cancelled": False}
        args = ["aria2c", "-x", "16", "-s", "16", "--file-allocation=none", "--summary-interval=1", "-d", task_dir, url]
        asyncio.run_coroutine_threadsafe(
            sedit(chat_id, msg_id, "⬇️ <b>Downloading archive…</b>", cbtn(task_id)), loop)
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        with jobs_lock: ACTIVE_JOBS[task_id]["obj"] = proc
        last_t = 0
        for line in iter(proc.stdout.readline, ""):
            if ACTIVE_JOBS.get(task_id, {}).get("cancelled"): break
            m2 = re.search(r"\[#\w+\s+([\d.]+\w+)/([\d.]+\w+)\((\d+)%\).*?DL:([\d.]+\w+)", line)
            if m2 and time.time() - last_t > 3:
                ds, ts, pct, spd = m2.groups()
                asyncio.run_coroutine_threadsafe(
                    sedit(chat_id, msg_id,
                        f"⬇️ <b>Downloading archive…</b>\n{bar(float(pct), 100)}\n"
                        f"📦 <code>{ds}/{ts}</code> • 🚀 <code>{spd}/s</code>", cbtn(task_id)), loop)
                last_t = time.time()
        proc.wait()
        if ACTIVE_JOBS.get(task_id, {}).get("cancelled"): return
        arcs = [os.path.join(r,f) for r,_,fs in os.walk(task_dir) for f in fs if os.path.splitext(f)[1].lower() in config.ARC_EXT]
        if not arcs:
            asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, "❌ No archive found after download."), loop); return
        for arc in arcs:
            asyncio.run_coroutine_threadsafe(
                sedit(chat_id, msg_id, f"📂 <b>Extracting</b> <code>{os.path.basename(arc)}</code>…", cbtn(task_id)), loop)
            ext_dir = arc + "_ext"; unzip_file(arc, ext_dir); os.remove(arc)
            for item in os.listdir(ext_dir): shutil.move(os.path.join(ext_dir, item), task_dir)
            shutil.rmtree(ext_dir, ignore_errors=True)
        from .drive import dispatch
        dispatch(chat_id, msg_id, task_dir, job_prefs, task_id, folder, dl_mb=dir_size_mb(task_dir))
    except Exception as e:
        if task_id in ACTIVE_JOBS:
            asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, f"❌ <code>{e}</code>"), loop)
    finally:
        with jobs_lock: ACTIVE_JOBS.pop(task_id, None)
