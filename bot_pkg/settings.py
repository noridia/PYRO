import os, re, threading, time, asyncio
from pyrogram import filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from . import config
from .core import (app, get_prefs, save_prefs, get_chat_prefs, save_settings,
                   list_cookies, cookie_path, send_msg, edit_msg, safe_answer, LPO)
from .utils import get_thread_id, sedit

@app.on_message(filters.command("settings") & filters.private)
async def cmd_settings(client, message):
    from .auth import is_auth
    if not is_auth(message):
        return await message.reply(f"⛔ Not authorized. Your ID: <code>{message.from_user.id}</code>")
    user_id   = message.from_user.id
    thread_id = get_thread_id(message)
    try:
        await render_settings(message.chat.id, user_id=user_id, thread_id=thread_id)
    except Exception as e:
        print(f"[settings] {e!r}")
        try: await message.reply(f"⚠️ Settings error: <code>{e}</code>")
        except: pass

async def render_settings(chat_id, msg_id=None, user_id=None, thread_id=None):
    if user_id is None: user_id = chat_id
    p         = get_prefs(user_id)
    cp        = get_chat_prefs(chat_id)
    dest      = cp.get("destination", "drive")
    active_ck = p.get("active_cookie", "global")
    cookies   = list_cookies()
    tid_str   = str(thread_id) if thread_id else "0"
    chat_type = "group" if str(chat_id) != str(user_id) else "PV"
    m = InlineKeyboardMarkup(row_width=1)
    m.add(InlineKeyboardButton(
        f"📬 Dest ({chat_type}): {config.DEST_LABEL[dest]} ↻",
        callback_data=f"set:dest:{user_id}:{tid_str}"))
    m.add(InlineKeyboardButton(
        f"🍪 Cookie: {active_ck} — manage ↗",
        callback_data=f"set:cookies:{user_id}:{tid_str}"))
    m.add(InlineKeyboardButton(
        "📋 IG Tracker ↗",
        callback_data=f"set:igindex:{user_id}:{tid_str}"))
    cookies_html = (", ".join(f"<code>{c}</code>" for c in cookies) or "<i>none</i>")
    text = (f"⚙️ <b>Your Settings</b>\n"
            f"<i>Destination is per-chat • Cookie is per-user</i>\n\n"
            f"📬 <b>Destination ({chat_type}):</b> "
            f"<code>{config.DEST_LABEL[dest]}</code>\n"
            f"<i>Cycles: Drive → Telegram → Both</i>\n\n"
            f"🍪 <b>Active cookie:</b> <code>{active_ck}</code>\n"
            f"<i>Saved:</i> {cookies_html}")
    if msg_id:
        await sedit(chat_id, msg_id, text, m)
    else:
        await send_msg(chat_id, text, reply_markup=m, message_thread_id=thread_id)

@app.on_callback_query(filters.regex(r"^set:"))
async def handle_settings_cb(client, call):
    from .core import save_settings as _save_all
    parts   = call.data.split(":")
    action  = parts[1]
    user_id = (int(parts[2])
               if len(parts) > 2 and parts[2].lstrip('-').isdigit()
               else call.from_user.id)
    tid_raw   = parts[3] if len(parts) > 3 else "0"
    thread_id = (int(tid_raw) if tid_raw.isdigit() and tid_raw != "0" else None)
    tid_str = str(thread_id) if thread_id else "0"
    chat_id = call.message.chat.id
    msg_id  = call.message.id
    p       = get_prefs(user_id)
    if call.from_user.id != user_id:
        return await safe_answer(call.id, "⛔ These are not your settings.", show_alert=True)
    if action == "dest":
        cp  = get_chat_prefs(chat_id)
        cur = cp.get("destination", "drive")
        cp["destination"] = (config.DEST_CYCLE[(config.DEST_CYCLE.index(cur)+1) % len(config.DEST_CYCLE)]
                             if cur in config.DEST_CYCLE else "drive")
        await safe_answer(call.id)
    elif action == "cookies":
        await safe_answer(call.id)
        return await render_cookie_manager(chat_id, msg_id, user_id, thread_id)
    elif action == "igindex":
        await safe_answer(call.id)
        from .instagram import render_igindex
        asyncio.get_event_loop().call_soon_threadsafe(
            lambda: asyncio.ensure_future(render_igindex(chat_id, msg_id, 0)))
        return
    elif action == "setcookie":
        name = parts[4] if len(parts) > 4 else "global"
        p["active_cookie"] = name
        await safe_answer(call.id, f"✅ Active: {name}")
        save_prefs(user_id)
        return await render_cookie_manager(chat_id, msg_id, user_id, thread_id)
    elif action == "delcookie":
        name = parts[4] if len(parts) > 4 else ""
        cp = cookie_path(name)
        if os.path.exists(cp): os.remove(cp)
        if p.get("active_cookie") == name: p["active_cookie"] = "global"
        await safe_answer(call.id, f"🗑 Deleted {name}")
        save_prefs(user_id)
        return await render_cookie_manager(chat_id, msg_id, user_id, thread_id)
    elif action in ("noop", "back"):
        await safe_answer(call.id)
    else:
        await safe_answer(call.id)
    save_prefs(user_id)
    _save_all()
    await render_settings(chat_id, msg_id, user_id=user_id, thread_id=thread_id)

async def render_cookie_manager(chat_id, msg_id, user_id, thread_id=None):
    cookies  = list_cookies()
    p        = get_prefs(user_id)
    active   = p.get("active_cookie", "global")
    tid_str  = str(thread_id) if thread_id else "0"
    m = InlineKeyboardMarkup(row_width=2)
    for name in cookies:
        tick = "✅ " if name == active else "○ "
        m.row(
            InlineKeyboardButton(
                f"{tick}🍪 {name}",
                callback_data=f"set:setcookie:{user_id}:{tid_str}:{name}"),
            InlineKeyboardButton(
                "🗑",
                callback_data=f"set:delcookie:{user_id}:{tid_str}:{name}"))
    if not cookies:
        m.add(InlineKeyboardButton(
            "(no cookies saved)", callback_data=f"set:noop:{user_id}:{tid_str}"))
    m.add(InlineKeyboardButton(
        "⬅️ Back", callback_data=f"set:back:{user_id}:{tid_str}"))
    await sedit(chat_id, msg_id,
        f"🍪 <b>Cookie Manager</b>\n"
        f"<i>Your selection syncs across all your chats</i>\n\n"
        f"Tap name → set active • 🗑 → delete\n\n"
        f"<b>Add:</b>\n• <code>/cookie</code> — global\n"
        f"• <code>/cookie #name</code> — named\n"
        f"• Reply to <code>.txt</code> file with <code>/cookie</code>\n\n"
        f"<b>Active:</b> <code>{active}</code>",
        m)

@app.on_message(filters.command("cookie") & filters.private)
async def handle_cookie(client, message):
    from .auth import is_auth
    if not is_auth(message): return
    user_id = message.from_user.id
    raw = (message.text or message.caption or "").replace("/cookie", "", 1).strip()
    name_m = re.search(r"#(\w+)", raw)
    name = name_m.group(1) if name_m else "global"
    cp = cookie_path(name)
    try:
        f_id = None
        if message.reply_to_message and message.reply_to_message.document:
            f_id = message.reply_to_message.document.file_id
        elif message.document:
            f_id = message.document.file_id
        if f_id:
            fpath = await client.download_media(f_id)
            import shutil
            shutil.move(fpath, cp)
            return await message.reply(
                f"✅ Cookie <code>{name}</code> saved!\n<code>/settings</code> → Cookie Manager.")
        text = re.sub(r"#\w+", "", raw).strip()
        if text:
            with open(cp, "w", encoding="utf-8") as f: f.write(text)
            return await message.reply(f"✅ Cookie <code>{name}</code> saved!")
        await message.reply(
            "⚠️ Attach a cookie <code>.txt</code> file or paste cookie text.")
    except Exception as e:
        await message.reply(f"❌ <code>{e}</code>")
