from pyrogram import filters

from . import config
from .core import app, _allowed_users, save_users
from .utils import get_thread_id, LPO

def is_auth(message):
    if message.from_user is None: return False
    uid = str(message.from_user.id)
    if not config.ADMIN_ID: return True
    return uid == config.ADMIN_ID or uid in _allowed_users

@app.on_message(filters.command(["allow", "ban"]) & filters.user(config.ADMIN_ID))
async def cmd_access(client, message):
    parts = message.text.split()
    if len(parts) < 2:
        return await message.reply("⚠️ `/allow <user_id>`", parse_mode="Markdown")
    cmd_p, target = parts[0], parts[1]
    if cmd_p == "/allow":
        _allowed_users.add(target); save_users()
        await message.reply(f"✅ `{target}` authorized.", parse_mode="Markdown")
    else:
        _allowed_users.discard(target); save_users()
        await message.reply(f"🚫 `{target}` banned.", parse_mode="Markdown")
