FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    aria2 \
    p7zip-full \
    wget \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .

# Upgrade pip first (old pip in slim images can mis-resolve deps)
RUN python -m pip install --upgrade pip setuptools wheel

# Install dependencies — build FAILS here if any package can't install
RUN pip install -r requirements.txt

# FAIL THE BUILD LOUDLY if core deps are missing (no silent runtime crashes)
RUN python -c "import pyrogram, tgcrypto; print('pyrogram', pyrogram.__version__, 'OK')"

COPY . .

ENV PORT=8080
EXPOSE $PORT

CMD ["python", "bot.py"]
