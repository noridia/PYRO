import os, re, threading, time, shlex, shutil, uuid, asyncio
from pyrogram import filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from yt_dlp import YoutubeDL

from . import config
from .core import (app, ACTIVE_JOBS, PENDING_TASKS, jobs_lock, task_queue,
                   ensure_free, get_job_prefs, get_prefs, get_active_cookie,
                   send_msg, edit_msg, safe_answer, LPO, get_loop)
from .utils import (sedit, cbtn, fmtsz, bar, fmt_time, hms2s, parse_cmd, get_thread_id)

FORMAT_TIMEOUT = 120

# ── YT-DLP probe helpers ───────────────────────────────────────────────
def _ydl_base_opts(user_id):
    opts = {
        "quiet": True, "no_warnings": True, "noprogress": True,
        "nocheckcertificate": True, "geo_bypass": True,
        "source_address": "0.0.0.0", "socket_timeout": 30,
        "retries": 10, "fragment_retries": 10, "extractor_retries": 5,
        "file_access_retries": 3, "concurrent_fragment_downloads": 4,
        "http_chunk_size": 5 * 1024 * 1024, "ignore_no_formats_error": True,
        "http_headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    }
    ck = get_active_cookie(user_id)
    if ck and os.path.exists(ck): opts["cookiefile"] = ck
    return opts

def _probe_full(url, user_id):
    ck = get_active_cookie(user_id)
    minimal = {"quiet": True, "no_warnings": True, "noplaylist": True,
               "skip_download": True, "extract_flat": False,
               "ignore_no_formats_error": True, "nocheckcertificate": True,
               "geo_bypass": True, "socket_timeout": 30, "retries": 5}
    if ck and os.path.exists(ck): minimal["cookiefile"] = ck
    strategies = [
        ("default", minimal),
        ("web", {**minimal, "extractor_args": {"youtube": {"player_client": ["web"]}, "youtubetab": {"skip": ["authcheck"]}}}),
        ("android", {**minimal, "extractor_args": {"youtube": {"player_client": ["android"]}, "youtubetab": {"skip": ["authcheck"]}}}),
        ("ios", {**minimal, "extractor_args": {"youtube": {"player_client": ["ios"]}, "youtubetab": {"skip": ["authcheck"]}}}),
        ("tv", {**minimal, "extractor_args": {"youtube": {"player_client": ["tv_embedded"]}, "youtubetab": {"skip": ["authcheck"]}}}),
    ]
    last_error = "Unknown error"
    for name, opts in strategies:
        try:
            with YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
            if info: info["_working_client"] = name; return info, None
            last_error = f"Strategy '{name}': returned None"
        except Exception as e: last_error = f"[{name}] {type(e).__name__}: {e}"; continue
    return None, last_error

def _probe_playlist_flat(url, user_id):
    opts = {**_ydl_base_opts(user_id), "skip_download": True,
            "extract_flat": "in_playlist", "noplaylist": False}
    try:
        with YoutubeDL(opts) as ydl: return ydl.extract_info(url, download=False)
    except: return None

def _parse_yt_formats(info):
    formats = {}; is_m4a = False
    for item in (info.get("formats") or []):
        tbr = (item.get("tbr") or item.get("vbr") or item.get("abr") or 0)
        if not tbr or tbr <= 0:
            size_only = (item.get("filesize") or item.get("filesize_approx") or 0)
            if size_only > 0: tbr = round(size_only / 1024.0)
            else: continue
        format_id = item["format_id"]
        size = (item.get("filesize") or item.get("filesize_approx") or 0)
        if (item.get("video_ext") == "none" and (item.get("resolution") == "audio only" or item.get("acodec") != "none")):
            if item.get("audio_ext") == "m4a": is_m4a = True
            b_name = f"{item.get('acodec') or format_id}-{item['ext']}"
            v_format = format_id
        elif item.get("height"):
            height = item["height"]; ext = item["ext"]
            fps = item["fps"] if item.get("fps") else ""
            b_name = f"{height}p{fps}-{ext}"
            ba_ext = "[ext=m4a]" if is_m4a and ext == "mp4" else ""
            v_format = f"{format_id}+ba{ba_ext}/b[height=?{height}]"
        else: continue
        formats.setdefault(b_name, {})[f"{int(tbr)}"] = [size, v_format]
    return formats, is_m4a

def _yt_format_label(b_name, size=0):
    if size > 0: return f"{b_name} ({fmtsz(size)})"
    return b_name

def _yt_sort_key(b_name):
    m = re.match(r"(\d+)", b_name)
    if m: return (-int(m.group(1)), b_name)
    return (0, b_name)

def _merge_ext_for(fmt_spec):
    s = fmt_spec or ""
    if "ba/b-" in s or s.startswith("ba/"): return None
    if "webm" in s: return "webm"
    if "+ba" in s or "bv*" in s: return "mp4"
    return None

# ── Format selection ───────────────────────────────────────────────────
class YtFormatPicker:
    def __init__(self, chat_id, msg_id, tid, task):
        self.chat_id = chat_id; self.msg_id = msg_id; self.tid = tid; self.task = task
        self._start = time.time(); self._main_buttons = None

    def _remaining(self): return max(0, FORMAT_TIMEOUT - (time.time() - self._start))
    def _time_str(self): return f"⏱ {int(self._remaining())}s remaining"
    def _make_menu(self, buttons, row_width=2):
        m = InlineKeyboardMarkup(row_width=row_width)
        for row in buttons:
            if isinstance(row, list): m.add(*row)
            else: m.add(row)
        return m

    async def render_main(self):
        task = self.task; title = task["title"]; uploader = task.get("uploader", "")
        dur_s = task.get("duration", 0) or 0; views = task.get("views"); likes = task.get("likes")
        count = task.get("count", 1); is_pl = task.get("is_playlist", False)
        ck_status = task.get("ck_status", ""); job_prefs = task["job_prefs"]
        formats = task.get("formats", {})
        lines = [f"🎬 <b>{title}</b>"]
        if uploader: lines.append(f"👤 <code>{uploader}</code>")
        if dur_s: lines.append(f"⏱ <code>{fmt_time(int(dur_s))}</code>")
        if views: lines.append(f"👁 <code>{views:,}</code>")
        if likes: lines.append(f"👍 <code>{likes:,}</code>")
        if is_pl: lines.append(f"📋 Playlist: <code>{count}</code> items")
        if ck_status: lines.append(f"🍪 Cookie: {ck_status}")
        if job_prefs.get("force_zip"): lines.append("🗜️ <i>Will be zipped</i>")
        lines.append(f"⏱ <code>{int(self._remaining())}s</code> timeout")
        lines.append("")
        buttons = []
        if is_pl:
            for h in ["144","240","360","480","720","1080","1440","2160"]:
                mp4_spec = f"bv*[height<=?{h}][ext=mp4]+ba[ext=m4a]/b[height<=?{h}]"
                webm_spec = f"bv*[height<=?{h}][ext=webm]+ba/b[height<=?{h}]"
                buttons.append([
                    InlineKeyboardButton(f"{h}-mp4", callback_data=f"ytq:{self.tid}:pl:{h}|mp4:{mp4_spec}"),
                    InlineKeyboardButton(f"{h}-webm", callback_data=f"ytq:{self.tid}:pl:{h}|webm:{webm_spec}"),
                ])
        else:
            for b_name in sorted(formats.keys(), key=_yt_sort_key):
                tbr_dict = formats[b_name]
                if len(tbr_dict) == 1:
                    tbr, (size, _) = next(iter(tbr_dict.items()))
                    buttons.append(InlineKeyboardButton(_yt_format_label(b_name, size),
                        callback_data=f"ytq:{self.tid}:qual:{b_name}:{tbr}"))
                else:
                    buttons.append(InlineKeyboardButton(b_name, callback_data=f"ytq:{self.tid}:dict:{b_name}"))
        buttons.append([
            InlineKeyboardButton("MP3", callback_data=f"ytq:{self.tid}:mp3"),
            InlineKeyboardButton("Audio", callback_data=f"ytq:{self.tid}:audio"),
        ])
        buttons.append([
            InlineKeyboardButton("🌟 Best Video", callback_data=f"ytq:{self.tid}:bv"),
            InlineKeyboardButton("🎵 Best Audio", callback_data=f"ytq:{self.tid}:ba"),
        ])
        buttons.append(InlineKeyboardButton("❌ Cancel", callback_data=f"ytq:{self.tid}:cancel"))
        self._main_buttons = buttons
        await sedit(self.chat_id, self.msg_id, "\n".join(lines), self._make_menu(buttons))

    async def render_dict(self, b_name):
        tbr_dict = self.task.get("formats", {}).get(b_name, {})
        buttons = []
        for tbr, (size, _) in tbr_dict.items():
            buttons.append(InlineKeyboardButton(_yt_format_label(f"{tbr}K", size),
                callback_data=f"ytq:{self.tid}:qual:{b_name}:{tbr}"))
        buttons.append([
            InlineKeyboardButton("⬅️ Back", callback_data=f"ytq:{self.tid}:back"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"ytq:{self.tid}:cancel"),
        ])
        await sedit(self.chat_id, self.msg_id,
            f"Choose Bit rate for <b>{b_name}</b>:\n{self._time_str()}",
            self._make_menu(buttons))

    async def render_mp3(self):
        buttons = [[InlineKeyboardButton(f"{q}K-mp3", callback_data=f"ytq:{self.tid}:mp3q:{q}") for q in [64, 128, 320]]]
        buttons.append([
            InlineKeyboardButton("⬅️ Back", callback_data=f"ytq:{self.tid}:back"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"ytq:{self.tid}:cancel"),
        ])
        await sedit(self.chat_id, self.msg_id,
            f"Choose MP3 Bitrate:\n{self._time_str()}", self._make_menu(buttons, row_width=3))

    async def render_audio_format(self):
        buttons = [[InlineKeyboardButton(fmt, callback_data=f"ytq:{self.tid}:audioq:{fmt}") for fmt in ["aac","alac","flac","m4a","opus","vorbis","wav"]]]
        buttons.append([
            InlineKeyboardButton("⬅️ Back", callback_data=f"ytq:{self.tid}:back"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"ytq:{self.tid}:cancel"),
        ])
        await sedit(self.chat_id, self.msg_id,
            f"Choose Audio Format:\n{self._time_str()}", self._make_menu(buttons, row_width=3))

    async def render_audio_quality(self, fmt):
        buttons = [[InlineKeyboardButton(str(q), callback_data=f"ytq:{self.tid}:audioql:{fmt}:{q}") for q in range(11)]]
        buttons.append([
            InlineKeyboardButton("⬅️ Back", callback_data=f"ytq:{self.tid}:audio"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"ytq:{self.tid}:cancel"),
        ])
        await sedit(self.chat_id, self.msg_id,
            f"Choose Audio Quality: <code>0</code>=best, <code>10</code>=worst\n{self._time_str()}",
            self._make_menu(buttons, row_width=5))

# ── YT Command ─────────────────────────────────────────────────────────
@app.on_message(filters.command(["yt", "ytl", "ytzm", "ytzl"]))
async def cmd_yt(client, message, forced_cmd=None):
    from .auth import is_auth
    if not is_auth(message): return
    cmd, link, custom, folder, t_range, raw_flags, _, _, thread_id, user_id = parse_cmd(message)
    cmd = forced_cmd or cmd
    chat_id = message.chat.id
    job_prefs = get_job_prefs(user_id, cmd, message.chat.id)
    job_prefs["user_id"] = user_id; job_prefs["thread_id"] = thread_id
    if not link: return await message.reply("⚠️ `/yt <url> [| Name] [#Folder] [HH:MM-HH:MM] [-- flags]`", parse_mode="Markdown")
    if not ensure_free(config.MIN_FREE_MB): return await message.reply("❌ Disk full.")
    tid = str(uuid.uuid4())[:6]
    msg = await message.reply("🔍 <b>Fetching formats…</b>")
    def _fetch():
        loop = get_loop()
        ck = get_active_cookie(user_id)
        ck_status = (f"🍪 <code>{get_prefs(user_id).get('active_cookie','?')}</code>" if ck else "⚠️ <i>no cookie</i>")
        try:
            flat = _probe_playlist_flat(link, user_id)
            is_pl = bool(flat and flat.get("_type") == "playlist")
            entries = ([e for e in (flat.get("entries") or [])] if flat else [])
            count = len(entries) if is_pl else 1
            info, probe_err = _probe_full(link, user_id)
            if not info:
                asyncio.run_coroutine_threadsafe(
                    sedit(chat_id, msg.id,
                        f"❌ <b>Could not fetch video info.</b>\n\nCookie: {ck_status}\nError: <code>{probe_err[:300] if probe_err else 'unknown'}</code>"), loop)
                return
            formats, is_m4a = _parse_yt_formats(info)
            PENDING_TASKS[tid] = {"url": link, "custom_name": custom, "folder": folder, "time": t_range,
                "raw_flags": raw_flags, "is_playlist": is_pl, "is_m4a": is_m4a, "working_client": info.get("_working_client", ""),
                "chat_id": chat_id, "job_prefs": job_prefs, "formats": formats, "title": (info.get("title") or "Media")[:50],
                "uploader": info.get("uploader") or info.get("channel") or "",
                "duration": info.get("duration") or 0, "views": info.get("view_count"), "likes": info.get("like_count"),
                "count": count, "ck_status": ck_status}
            picker = YtFormatPicker(chat_id, msg.id, tid, PENDING_TASKS[tid])
            asyncio.run_coroutine_threadsafe(picker.render_main(), loop)
        except Exception as e:
            asyncio.run_coroutine_threadsafe(sedit(chat_id, msg.id, f"❌ <b>Probe failed:</b> <code>{e}</code>"), loop)
    threading.Thread(target=_fetch, daemon=True).start()

def _finalize_yt(call, tid, fmt_spec):
    import asyncio
    loop = get_loop()
    if tid not in PENDING_TASKS:
        asyncio.run_coroutine_threadsafe(safe_answer(call.id, "⚠️ Session expired."), loop); return
    task = PENDING_TASKS.pop(tid)
    asyncio.run_coroutine_threadsafe(safe_answer(call.id, "⚡ Queued…"), loop)
    asyncio.run_coroutine_threadsafe(sedit(call.message.chat.id, call.message.id, "⚡ <b>Queued…</b>"), loop)
    new_tid = str(uuid.uuid4())[:6]
    merge_ext = _merge_ext_for(fmt_spec)
    task_queue.put((process_yt, (call.message.chat.id, call.message.id, task, fmt_spec, merge_ext, new_tid)))

@app.on_callback_query(filters.regex(r"^ytq:"))
async def yt_quality_cb(client, call):
    parts = call.data.split(":"); tid = parts[1]; action = parts[2]
    await safe_answer(call.id)
    task = PENDING_TASKS.get(tid)
    if not task: return await sedit(call.message.chat.id, call.message.id, "⚠️ Session expired.")
    picker = YtFormatPicker(call.message.chat.id, call.message.id, tid, task)
    if action == "cancel": PENDING_TASKS.pop(tid, None); return await sedit(call.message.chat.id, call.message.id, "🛑 Cancelled.")
    if action == "back": return await picker.render_main()
    if action == "dict": return await picker.render_dict(parts[3])
    if action == "qual":
        entry = task.get("formats", {}).get(parts[3], {}).get(parts[4])
        if not entry: return await sedit(call.message.chat.id, call.message.id, "⚠️ Format not found.")
        return _finalize_yt(call, tid, entry[1])
    if action == "pl": return _finalize_yt(call, tid, parts[4])
    if action == "mp3": return await picker.render_mp3()
    if action == "mp3q": return _finalize_yt(call, tid, f"ba/b-mp3-{parts[3]}")
    if action == "audio": return await picker.render_audio_format()
    if action == "audioq": return await picker.render_audio_quality(parts[3])
    if action == "audioql": return _finalize_yt(call, tid, f"ba/b-{parts[3]}-{parts[4]}")
    if action == "bv": return _finalize_yt(call, tid, "bv*+ba/b")
    if action == "ba": return _finalize_yt(call, tid, "ba/b")

def _make_dl_opts_for_client(base_opts, client_name):
    opts = base_opts.copy()
    if client_name and client_name != "bare":
        opts["extractor_args"] = {"youtube": {"player_client": [client_name], "player_skip": []}, "youtubetab": {"skip": ["authcheck"]}}
    return opts

def process_yt(chat_id, msg_id, task, fmt_spec, merge_ext, task_id):
    import asyncio
    loop = get_loop()
    task_dir = os.path.join(config.DOWNLOAD_DIR, task_id)
    os.makedirs(task_dir, exist_ok=True)
    job_prefs = task["job_prefs"]; url = task["url"]
    last_t = [0.0]; samples = []
    with jobs_lock:
        ACTIVE_JOBS[task_id] = {"type": "ytdl", "obj": None, "dir": task_dir, "chat_id": chat_id,
                                "msg_id": msg_id, "start_time": time.time(), "cancelled": False}
    working_client = task.get("working_client", "")
    user_id = job_prefs.get("user_id", chat_id)
    from .drive import StreamingDispatcher
    sd = StreamingDispatcher(chat_id, msg_id, task_id, job_prefs, folder_name=task.get("folder"),
                             total_files=None, total_bytes=None, base_dir=task_dir)
    _dispatched = set(); _dispatch_lock = threading.Lock()
    def _maybe_dispatch(fpath):
        if not fpath or not os.path.exists(fpath): return
        with _dispatch_lock:
            if fpath in _dispatched: return
            _dispatched.add(fpath)
        if os.path.getsize(fpath) > 0: sd.submit(fpath)
    def hook(d):
        if ACTIVE_JOBS.get(task_id, {}).get("cancelled"): raise SystemExit("cancelled")
        status = d.get("status")
        if status == "downloading" and time.time() - last_t[0] > 3:
            done = d.get("downloaded_bytes", 0) or 0
            total = (d.get("total_bytes") or d.get("total_bytes_estimate") or max(done, 1))
            spd = d.get("speed") or 0; eta = d.get("eta") or 0
            fi = d.get("fragment_index"); fc = d.get("fragment_count")
            fname = os.path.basename(d.get("filename", ""))[:40]
            samples.append((done, time.time()))
            if len(samples) > 10: samples.pop(0)
            text = (f"⬇️ <b>Downloading…</b>\n{bar(done, total)}\n"
                    f"📦 <code>{fmtsz(done)} / {fmtsz(total)}</code>\n"
                    f"🚀 <code>{fmtsz(spd)}/s</code> • ⏳ <code>{fmt_time(eta)}</code>")
            if fi and fc: text += f"\n🧩 Fragment <code>{fi}/{fc}</code>"
            if fname: text += f"\n📄 <code>{fname}</code>"
            asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, text, cbtn(task_id)), loop)
            last_t[0] = time.time()
        elif status == "finished":
            fname = d.get("filename", "")
            asyncio.run_coroutine_threadsafe(
                sedit(chat_id, msg_id, f"🔄 <b>Post-processing…</b>\n📄 <code>{os.path.basename(fname)[:40]}</code>",
                      cbtn(task_id)), loop)
        elif status == "post_processing_finished":
            fname = d.get("filename") or d.get("info_dict", {}).get("filepath", "")
            if fname: _maybe_dispatch(fname)
    out_name = task.get("custom_name") or "%(title).120B"
    out_tpl = os.path.join(task_dir, f"{out_name}.%(ext)s")
    def _make_opts(fmt, merge, client_override=None):
        base = _ydl_base_opts(user_id)
        client = client_override or working_client
        o = _make_dl_opts_for_client(base, client)
        is_audio = bool(fmt and ("ba/b-" in fmt or fmt == "ba/b"))
        o.update({"format": fmt, "outtmpl": out_tpl, "progress_hooks": [hook],
                  "noplaylist": not task.get("is_playlist", False),
                  "writethumbnail": not is_audio, "overwrites": True, "trim_file_name": 200})
        if merge and not is_audio: o["merge_output_format"] = merge
        if is_audio:
            if "mp3" in fmt:
                q = re.search(r"mp3-(\d+)", fmt)
                bitrate = q.group(1) if q else "320"
                o["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": bitrate}]
            else:
                m = re.search(r"ba/b-([a-z]+)-(\d+)", fmt)
                acodec, q = (m.group(1), m.group(2)) if m else ("m4a", "0")
                o["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": acodec, "preferredquality": q}]
            o["keepvideo"] = False
        else:
            o["postprocessors"] = [{"key": "FFmpegMetadata", "add_chapters": True, "add_metadata": True},
                                   {"key": "FFmpegThumbnailsConvertor", "format": "jpg", "when": "before_dl"},
                                   {"key": "EmbedThumbnail", "already_have_thumbnail": False}]
        raw_flags = task.get("raw_flags", "")
        if raw_flags:
            try:
                import yt_dlp.options as _ytopt
                _parser = _ytopt.create_parser()
                _parsed, _ = _parser.parse_known_args(shlex.split(raw_flags))
                for k, v in vars(_parsed).items():
                    if v is not None and v is not False: o[k] = v
            except Exception as e: print(f"[yt raw_flags] {e}")
        if task.get("time"):
            try:
                s_s = hms2s(task["time"].split("-")[0]); e_s = hms2s(task["time"].split("-")[1])
                o["download_ranges"] = (lambda info, ydl, _s=s_s, _e=e_s: [{"start_time": _s, "end_time": _e}])
                o["force_keyframes_at_cuts"] = True
            except: pass
        return o
    def _do_download(opts):
        with YoutubeDL(opts) as ydl:
            with jobs_lock:
                if task_id in ACTIVE_JOBS: ACTIVE_JOBS[task_id]["obj"] = ydl
            ydl.download([url])
    def _has_output():
        return any(os.path.getsize(os.path.join(r, f)) > 0 for r, _, fs in os.walk(task_dir)
                   for f in fs if not f.endswith((".part", ".ytdl", ".mhtml", ".json")))
    try:
        asyncio.run_coroutine_threadsafe(
            sedit(chat_id, msg_id,
                f"🎬 <b>Starting download…</b>\nFormat: <code>{fmt_spec[:80]}</code>\nDest: <code>{job_prefs.get('destination','drive')}</code>",
                cbtn(task_id)), loop)
        last_err = None
        try: _do_download(_make_opts(fmt_spec, merge_ext))
        except SystemExit: sd.wait_done(); return
        except Exception as e: last_err = str(e)
        if last_err and any(x in last_err.lower() for x in ["not available", "requested format", "format is not available"]):
            for strategy in ["web", "android", "ios", "tv"]:
                if "not available" in last_err.lower() or "requested format" in last_err.lower() or "format is not available" in last_err.lower():
                    asyncio.run_coroutine_threadsafe(
                        sedit(chat_id, msg_id, f"⚠️ Retrying with <b>{strategy}</b> client…\n<code>{last_err[:120]}</code>", cbtn(task_id)), loop)
                    try:
                        _do_download(_make_opts(fmt_spec, merge_ext, client_override=strategy))
                        last_err = None; break
                    except SystemExit: sd.wait_done(); return
                    except Exception as e: last_err = str(e); continue
                else: break
        if last_err:
            if _has_output():
                asyncio.run_coroutine_threadsafe(
                    sedit(chat_id, msg_id, f"⚠️ <b>Partial download</b> — format error:\n<code>{last_err[:200]}</code>", cbtn(task_id)), loop)
            else:
                asyncio.run_coroutine_threadsafe(
                    sedit(chat_id, msg_id, f"❌ <b>Download failed</b>\n<code>{last_err[:300]}</code>"), loop)
                with jobs_lock: ACTIVE_JOBS.pop(task_id, None)
                shutil.rmtree(task_dir, ignore_errors=True); return
        for r, _, fs in os.walk(task_dir):
            for fname in fs:
                if fname.endswith((".part", ".ytdl", ".mhtml", ".json", ".jpg")): continue
                fp = os.path.join(r, fname)
                if os.path.getsize(fp) > 0: _maybe_dispatch(fp)
        sd.wait_done()
        if task_id in ACTIVE_JOBS: sd.finalize()
    except Exception as e:
        if task_id in ACTIVE_JOBS:
            asyncio.run_coroutine_threadsafe(sedit(chat_id, msg_id, f"❌ <code>{e}</code>"), loop)
        try: sd.wait_done()
        except: pass
    finally:
        with jobs_lock: ACTIVE_JOBS.pop(task_id, None)
        shutil.rmtree(task_dir, ignore_errors=True)
