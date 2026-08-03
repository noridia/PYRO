import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Fail with a clear diagnostic instead of a bare ImportError
try:
    import pyrogram, tgcrypto
except ImportError as e:
    print("=" * 60)
    print("MISSING DEPENDENCY:", e)
    print("=" * 60)
    print("Run:  pip install -r requirements.txt")
    print("If you're in Docker, rebuild the image from scratch:")
    print("  docker build --no-cache -t pyro-bot .")
    sys.exit(1)

from bot_pkg.core import start
start()
