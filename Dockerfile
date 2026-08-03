FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    aria2 \
    p7zip-full \
    wget \
    unrar-free \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .

RUN pip install --no-cache-dir \
    pyrogram tgcrypto yt-dlp requests python-dotenv \
    google-api-python-client google-auth gdown && \
    pip install --no-cache-dir gallery-dl instaloader || echo "[warn] gallery-dl/instaloader optional"

COPY . .

ENV PORT=8080
EXPOSE $PORT

CMD ["python", "bot.py"]
