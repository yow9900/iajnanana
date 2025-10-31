import os
import tempfile
import shutil
import mimetypes
import telebot
from yt_dlp import YoutubeDL
from flask import Flask
import threading

BOT_TOKEN = "8409832972:AAGLcBs7q6PwtxZDGpB-3SCNgTwzfPKPUVw"
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)

app = Flask(__name__)

YTDLP_OPTS = {
    "format": "best",
    "outtmpl": "%(id)s.%(ext)s",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "retries": 3,
    "cachedir": False,
    "ignoreerrors": True,
    "http_headers": {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    }
}

MAX_TELEGRAM_UPLOAD = 50 * 1024 * 1024

def download_instagram(url, workdir):
    opts = YTDLP_OPTS.copy()
    opts["outtmpl"] = os.path.join(workdir, "%(id)s.%(ext)s")
    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
        if info is None:
            return None
        if info.get("entries"):
            info = info["entries"][0]
        result = ydl.extract_info(url, download=True)
        if result is None:
            return None
        if result.get("entries"):
            result = result["entries"][0]
        filename = ydl.prepare_filename(result)
        return filename, result

@bot.message_handler(commands=["start", "help"])
def send_welcome(message):
    bot.reply_to(message, "Salaan! Ii soo diri link Instagram ah si aan video-ga kuu soo dejiyo.")

@bot.message_handler(func=lambda m: True, content_types=["text"])
def handle_text(message):
    url = message.text.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        bot.reply_to(message, "Fadlan soo dir URL sax ah oo Instagram ah.")
        return
    msg = bot.reply_to(message, "Waxaan bilaabayaa soo dejinta, fadlan sug...")
    tmpdir = tempfile.mkdtemp(prefix="igdl_")
    try:
        res = download_instagram(url, tmpdir)
        if not res:
            bot.edit_message_text("Ma aanan awoodin inaan soo dejiyo content-ka. Hubi URL-ka ama isku day video kale.", chat_id=message.chat.id, message_id=msg.message_id)
            return
        filepath, info = res
        if not os.path.exists(filepath):
            bot.edit_message_text("Fayl lama helin ka dib soo dejin.", chat_id=message.chat.id, message_id=msg.message_id)
            return
        size = os.path.getsize(filepath)
        ctype, _ = mimetypes.guess_type(filepath)
        caption = []
        title = info.get("title") or info.get("id") or ""
        uploader = info.get("uploader") or info.get("uploader_id") or ""
        duration = info.get("duration")
        if title:
            caption.append(f"Title: {title}")
        if uploader:
            caption.append(f"By: {uploader}")
        if duration:
            caption.append(f"Duration: {duration} sec")
        caption_text = "\n".join(caption) if caption else None
        if size <= MAX_TELEGRAM_UPLOAD and (ctype and ctype.startswith("video")):
            with open(filepath, "rb") as f:
                bot.send_video(message.chat.id, f, caption=caption_text, reply_to_message_id=message.message_id)
        else:
            with open(filepath, "rb") as f:
                bot.send_document(message.chat.id, f, caption=caption_text, reply_to_message_id=message.message_id)
        bot.delete_message(chat_id=message.chat.id, message_id=msg.message_id)
    except Exception as e:
        try:
            bot.edit_message_text("Khalad ayaa dhacay inta lagu guda jiray soo dejinta: {}".format(str(e)), chat_id=message.chat.id, message_id=msg.message_id)
        except:
            pass
    finally:
        try:
            shutil.rmtree(tmpdir)
        except:
            pass

@app.route("/")
def home():
    return "Bot waa nool yahay!", 200

def run_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

if __name__ == "__main__":
    threading.Thread(target=run_flask).start()
    bot.infinity_polling()
