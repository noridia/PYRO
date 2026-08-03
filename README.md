# PYRO — Telegram Mirror/Leech Bot (Pyrogram)

A full-featured Telegram mirror and leech bot, **rewritten from scratch using Pyrogram** (MTProto) instead of pyTelegramBotAPI (Bot API).

## Why Pyrogram?

| | pyTelegramBotAPI | Pyrogram (this) |
|---|---|---|
| Protocol | Bot HTTP API | MTProto (native) |
| Max file (bot→server) | 50 MB | **2 GB** |
| Max file (server→chat) | 50 MB | **2 GB** |
| Speed | Slower (HTTP) | Faster (persistent connection) |
| Rate limiting | Manual | Built-in FloodWait handling |

## Features

- **Mirror** — download from URL → upload to Google Drive (`/m`)
- **Leech** — download from URL → send to Telegram chat (`/l`)
- **YouTube** — quality picker with yt-dlp, audio extraction, clips (`/yt`)
- **Instagram** — single post or full profile archive with tracking (`/ig`)
- **Torrent** — magnet/torrent via aria2c (`/torrent`)
- **Gallery** — gallery-dl for 20+ sites (`/gallery`)
- **Website clone** — wget mirror (`/clone`)
- **Unzip** — extract archives before sending (`/unzip`, `/unzipl`, `/unzipm`)
- **Drive browser** — browse, search, delete files (`/drive`, `/drivesearch`)
- **Cookie manager** — named profiles, per-user selection (`/cookie`)
- **Settings** — destination cycling (Drive/Telegram/Both), cookie picker (`/settings`)
- **Admin** — shell, cancel, stats, user management (`/sh`, `/stats`, `/cancelall`)

## Commands

| Command | Description |
|---|---|
| `/m <url>` | Smart mirror (auto-detects) |
| `/l <url>` | Leech → Telegram |
| `/zm <url>` | Zip then mirror |
| `/zl <url>` | Zip then leech |
| `/yt <url>` | YouTube quality picker |
| `/ytl <url>` | YouTube → Telegram |
| `/ig <url>` | Instagram post/archive |
| `/torrent <magnet>` | Torrent |
| `/gallery <url>` | gallery-dl |
| `/clone <url>` | Mirror website |
| `/unzipl <url>` | Extract → Telegram |
| `/unzipm <url>` | Extract → Drive |
| `/drive` | Browse Drive |
| `/cookie` | Cookie manager |
| `/settings` | Bot preferences |
| `/stats` | Status |
| `/sh <cmd>` | Shell (admin) |
| `/cancel <id>` | Cancel job |
| `/cancelall` | Kill all (admin) |

## Setup

### Environment Variables

```bash
cp .env.sample .env
# Edit .env with your credentials

# Required:
TELEGRAM_TOKEN=...
TELEGRAM_API=...      # Get from https://my.telegram.org
TELEGRAM_HASH=...     # Get from https://my.telegram.org

# Optional (for Drive features):
GCP_CLIENT_ID=...
GCP_CLIENT_SECRET=...
GCP_REFRESH_TOKEN=...
DRIVE_FOLDER_ID=...

# Optional:
ADMIN_ID=...          # Your Telegram user ID
```

### Run locally

```bash
pip install -r requirements.txt
python bot.py
```

### Docker

```bash
docker build -t pyro-bot .
docker run --env-file .env pyro-bot
```

## System Dependencies

- `ffmpeg` — video/audio merging
- `aria2c` — fast multi-connection downloads
- `p7zip-full` — 7z extraction
- `wget` — website cloning
- `unrar` — RAR extraction

## File Structure

```
bot.py                  Entry point
bot_pkg/
  config.py             Environment variables and constants
  core.py               Pyrogram Client, state, worker pool, telegram helpers
  utils.py              UI helpers, progress bar, file operations
  auth.py               Authorization, user management
  admin.py              Admin commands, help, cancel, stats, shell
  settings.py           Settings UI, cookie manager
  drive.py              Google Drive upload/download/browser, StreamingDispatcher
  download.py           Mirror/leech, TG file download, subprocess downloaders
  youtube.py            yt-dlp quality picker, download with format selection
  instagram.py          Instagram archive with Drive-backed index tracker
```

## License

MIT
