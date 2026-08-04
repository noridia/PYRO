import os, threading, time, subprocess, shutil
from pyrogram import filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from . import config
from .core import (app, ACTIVE_JOBS, PENDING_TASKS, jobs_lock, task_queue,
                   disk_free_mb, disk_total_mb, _allowed_users, save_users,
                   purge_stale, send_msg, edit_msg, safe_answer, LPO, get_loop)
from .utils import (sedit, ssend, parse_cmd, get_thread_id, fmt_time, cbtn)

# ── Stats ──────────────────────────────────────────────────────────────
@app.on_message(filters.command("stats"))
async def cmd_stats(client, message):
    from .auth import is_auth
    if not is_auth(message): return
    with jobs_lock:
        job_lines = [f"  🔴 <code>{k}</code> — <code>{v.get('type','?')}</code> ({fmt_time(int(time.time()-v.get('start_time',time.time())))})"
                     for k, v in ACTIVE_JOBS.items()]
    text = (f"📊 <b>Stats</b>\n\n💾 Disk: <code>{disk_free_mb()} MB free / {disk_total_mb()} MB</code>\n"
            f"⚙️ Active: <code>{len(job_lines)}</code> | Queued: <code>{task_queue.qsize()}</code>\n\n"
            f"<b>Active Jobs:</b>\n" + ("\n".join(job_lines) if job_lines else "  <i>none</i>"))
    await message.reply(text)

# ── Clean ──────────────────────────────────────────────────────────────
@app.on_message(filters.command("clean"))
async def cmd_clean(client, message):
    from .auth import is_auth
    if not is_auth(message): return
    before = disk_free_mb(); purge_stale()
    await message.reply(f"🧹 Freed <code>{disk_free_mb()-before} MB</code>. Now <code>{disk_free_mb()} MB</code> free.")

# ── Shell ──────────────────────────────────────────────────────────────
@app.on_message(filters.command(["shell", "sh"]))
async def cmd_shell(client, message):
    if not message.from_user or str(message.from_user.id) != config.ADMIN_ID:
        return await message.reply("⛔ Admin only.")
    cmd_text = message.text.split(None, 1)
    if len(cmd_text) < 2:
        return await message.reply("⚠️ `/sh <command>`", parse_mode="Markdown")
    shell_cmd = cmd_text[1]
    msg = await message.reply(f"⚙️ <b>Running…</b>\n<code>{shell_cmd[:100]}</code>")
    import asyncio
    def _run():
        try:
            result = subprocess.run(shell_cmd, shell=True, capture_output=True,
                                    text=True, timeout=60)
            out = ((result.stdout or "") + (result.stderr or "")).strip() or "(no output)"
            if len(out) > 3800: out = out[-3800:]
            return out
        except subprocess.TimeoutExpired:
            return "❌ Timed out."
        except Exception as e:
            return f"❌ {e}"
    out = await asyncio.to_thread(_run)
    await client.edit_message_text(message.chat.id, msg.id,
        f"<pre>{out}</pre>")

# ── Cancel / Cancel All ────────────────────────────────────────────────
@app.on_message(filters.command("cancel"))
async def cmd_cancel(client, message):
    from .auth import is_auth
    if not is_auth(message): return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        with jobs_lock:
            ids = list(ACTIVE_JOBS.keys())
        if not ids:
            return await message.reply("ℹ️ No active jobs.")
        ids_str = " | ".join(ids)
        lines = [f"Usage: <code>/cancel {ids_str}</code>", "", "<b>Active IDs:</b>"]
        for tid in sorted(ids):
            j = ACTIVE_JOBS.get(tid, {})
            lines.append(f"  <code>{tid}</code> — {j.get('type','?')} ({fmt_time(int(time.time()-j.get('start_time',time.time())))})")
        return await message.reply("\n".join(lines))
    tid = parts[1].strip()
    if kill_job(tid):
        await message.reply(f"🛑 Cancelled <code>{tid}</code>.")
    else:
        await message.reply(f"⚠️ No active job <code>{tid}</code>.")

def kill_job(task_id):
    with jobs_lock:
        if task_id not in ACTIVE_JOBS: return False
        job = ACTIVE_JOBS[task_id]; job["cancelled"] = True
        try:
            if job["type"] in ("subp", "gdown") and job.get("obj"):
                try:
                    job["obj"].terminate()
                    try: job["obj"].wait(timeout=5)
                    except:
                        try: job["obj"].kill()
                        except: pass
                except Exception as e: print(f"[kill] terminate {job['type']}: {e}")
        except Exception as e: print(f"[kill] {e}")
        task_dir = job.get("dir")
        import asyncio
        try:
            loop = get_loop()
            asyncio.run_coroutine_threadsafe(
                edit_msg(job["chat_id"], job["msg_id"], "🛑 <b>Cancelled.</b>"), loop)
        except: pass
        del ACTIVE_JOBS[task_id]
    if task_dir and os.path.isdir(task_dir):
        threading.Thread(target=lambda: shutil.rmtree(task_dir, ignore_errors=True), daemon=True).start()
    return True

@app.on_callback_query(filters.regex(r"^cancel:"))
async def handle_cancel(client, call):
    killed = kill_job(call.data.split(":", 1)[1])
    await safe_answer(call.id, "🛑 Cancelling…" if killed else "Nothing to cancel.")

@app.on_callback_query(filters.regex(r"^delup$"))
async def handle_delete_upload(client, call):
    try:
        await client.delete_messages(call.message.chat.id, call.message.id)
        await safe_answer(call.id, "🗑 Deleted")
    except Exception as e:
        msg = str(e)
        if "too old" in msg.lower() or "48" in msg:
            await safe_answer(call.id, "⚠️ Message too old to delete")
        elif "not enough rights" in msg.lower() or "can't be deleted" in msg.lower():
            await safe_answer(call.id, "⚠️ No permission to delete")
        else:
            await safe_answer(call.id, f"⚠️ {msg[:60]}")

@app.on_message(filters.command(["ca", "cancelall"]))
async def cmd_cancel_all(client, message):
    if not message.from_user or str(message.from_user.id) != config.ADMIN_ID: return
    with jobs_lock: ids = list(ACTIVE_JOBS.keys())
    count = sum(1 for k in ids if kill_job(k))
    await message.reply(f"💥 <b>{count} job(s) killed.</b>")

# ── Help ───────────────────────────────────────────────────────────────
@app.on_message(filters.command(["start", "help"]))
async def cmd_help(client, message):
    from .auth import is_auth
    if not is_auth(message): return await message.reply("⛔ Unauthorized.")
    await render_help(message.chat.id)

async def render_help(chat_id, msg_id=None):
    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    m = InlineKeyboardMarkup(row_width=2)
    m.add(InlineKeyboardButton("⬇️ Downloads", callback_data="help:dl"),
          InlineKeyboardButton("🎥 YouTube", callback_data="help:yt"))
    m.add(InlineKeyboardButton("📷 Instagram", callback_data="help:ig"),
          InlineKeyboardButton("📂 Drive", callback_data="help:drive"))
    m.add(InlineKeyboardButton("📋 IG Tracker", callback_data="help:igindex"),
          InlineKeyboardButton("🍪 Cookies", callback_data="help:cookies"))
    m.add(InlineKeyboardButton("🔧 Admin", callback_data="help:admin"))
    text = ("📖 <b>AzuDL Help</b> — pick a section\n\n"
            "<a href='https://github.com/yt-dlp/yt-dlp/blob/master/supportedsites.md'>yt-dlp sites</a> | "
            "<a href='https://github.com/mikf/gallery-dl/blob/master/docs/supportedsites.md'>gallery-dl sites</a>")
    if msg_id: await edit_msg(chat_id, msg_id, text, m)
    else: await send_msg(chat_id, text, reply_markup=m)

HELP = {
    "dl": ("⬇️ <b>Downloads</b>\n\n"
           "<code>/m &lt;url&gt;</code> — Smart mirror (auto-detects type)\n"
           "<code>/l &lt;url&gt;</code> — Leech → Telegram\n"
           "<code>/zm &lt;url&gt;</code> — Zip then mirror\n"
           "<code>/zl &lt;url&gt;</code> — Zip then leech\n"
           "<code>/torrent &lt;magnet&gt;</code> — Torrent\n"
           "<code>/gallery &lt;url&gt;</code> — gallery-dl\n"
           "<code>/clone &lt;url&gt;</code> — Mirror website\n"
           "<code>/unzipl &lt;url|file&gt;</code> — Extract → Telegram\n"
           "<code>/unzipm &lt;url|file&gt;</code> — Extract → Drive\n\n"
           "<b>Options:</b> <code>url | CustomName #FolderName</code>\n"
           "<b>Flags:</b> append <code>-- &lt;yt-dlp flags&gt;</code>"),
    "yt": ("🎥 <b>YouTube / Video</b>\n\n"
           "<code>/yt &lt;url&gt;</code> — Quality picker\n"
           "<code>/ytl &lt;url&gt;</code> — YouTube → Telegram\n"
           "<code>/ytzm &lt;url&gt;</code> — YouTube → zip → dest\n"
           "<code>/ytzl &lt;url&gt;</code> — YouTube → zip → Telegram\n\n"
           "<b>Clip:</b> <code>url 0:30-1:45</code>\n"
           "<b>Name:</b> <code>url | MyName</code>\n"
           "<b>Folder:</b> <code>url #FolderName</code>\n"
           "<b>Flags:</b> <code>url -- --write-subs --sub-lang en</code>"),
    "ig": ("📷 <b>Instagram</b>\n\n"
           "<code>/ig &lt;post_url&gt;</code> — Single post\n"
           "<code>/ig &lt;profile_url&gt;</code> — Full archive picker\n\n"
           "<b>Archive types:</b> Posts, Reels, Stories, Highlights, Tagged\n"
           "Stories & Highlights require cookies.\n\n"
           "Tracker: <code>/settings</code> → 📋 IG Tracker"),
    "drive": ("📂 <b>Drive Browser</b>\n\n"
              "<code>/drive</code> — Browse your Drive folder\n"
              "<code>/drivesearch &lt;query&gt;</code> — Search files\n\n"
              "<b>In browser:</b>\n📁 Tap folder → enter\n🔗 Get public link\n"
              "🗑 Delete file/folder\n🔄 Refresh listing"),
    "igindex": ("📋 <b>IG Archive Tracker</b>\n\n"
                "Open from <code>/settings</code> → 📋 IG Tracker.\n\n"
                "The tracker stores seen post IDs in Google Drive.\n"
                "No data is kept on the server.\n"
                "Deleting an index causes all posts to re-download next run."),
    "cookies": ("🍪 <b>Cookie Profiles</b>\n\n"
                "<code>/cookie</code> — Set global cookie\n"
                "<code>/cookie #name</code> — Named profile\n"
                "Reply to a <code>.txt</code> file with <code>/cookie</code>\n\n"
                "<code>/settings</code> → Cookie Manager to switch/delete\n"
                "<i>Your cookie selection syncs across all your chats</i>\n\n"
                "Export cookies with browser extension:\n"
                "<i>Get cookies.txt LOCALLY</i> (Chrome/Firefox)"),
    "admin": ("🔧 <b>Admin Commands</b>\n\n"
              "<code>/stats</code> — Disk, active jobs\n"
              "<code>/clean</code> — Purge stale temp files\n"
              "<code>/sh &lt;cmd&gt;</code> — Run shell command\n"
              "<code>/cancelall</code> — Kill all active jobs\n"
              "<code>/allow &lt;user_id&gt;</code> — Authorize user\n"
              "<code>/ban &lt;user_id&gt;</code> — Revoke access\n"
              "<code>/settings</code> — Bot preferences"),
}

@app.on_callback_query(filters.regex(r"^help:"))
async def help_cb(client, call):
    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    s = call.data.split(":", 1)[1]; await safe_answer(call.id)
    if s == "back": return await render_help(call.message.chat.id, call.message.id)
    m = InlineKeyboardMarkup().add(InlineKeyboardButton("⬅️ Back", callback_data="help:back"))
    await sedit(call.message.chat.id, call.message.id, HELP.get(s, "?"), m)
