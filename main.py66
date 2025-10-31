import os
import asyncio
import aiohttp
import aiofiles
import json
from datetime import datetime, timedelta
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ChatAction
import yt_dlp
from flask import Flask
from threading import Thread
from pymongo import MongoClient

# Database setup
DB_USER = "lakicalinuur"
DB_PASSWORD = "DjReFoWZGbwjry8K"
DB_APPNAME = "SpeechBot"
MONGO_URI = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@cluster0.n4hdlxk.mongodb.net/?retryWrites=true&w=majority&appName={DB_APPNAME}"
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_APPNAME]
users_collection = db["users"]

COOKIES_TXT_PATH = "cookies.txt"
if not os.path.exists(COOKIES_TXT_PATH):
    print(f"ERROR: Faylka '{COOKIES_TXT_PATH}' lama helin. Fadlan hubi inuu jiro.")

API_ID = 29169428
API_HASH = "55742b16a85aac494c7944568b5507e5"
BOT_TOKEN = "8442831099:AAEfiOGQUWGy0TaOfSupB9nbq3f_5qTJBq0"

DOWNLOAD_PATH = "downloads"
os.makedirs(DOWNLOAD_PATH, exist_ok=True)

YDL_OPTS_PIN = {
    "format": "bestvideo+bestaudio/best",
    "outtmpl": os.path.join(DOWNLOAD_PATH, "%(title)s.%(ext)s"),
    "noplaylist": True,
    "quiet": True,
    "cookiefile": COOKIES_TXT_PATH
}

YDL_OPTS_DEFAULT = {
    "format": "best",
    "outtmpl": os.path.join(DOWNLOAD_PATH, "%(title)s.%(ext)s"),
    "noplaylist": True,
    "quiet": True,
    "cookiefile": COOKIES_TXT_PATH
}

SUPPORTED_DOMAINS = [
    "youtube.com", "youtu.be", "facebook.com", "fb.watch", "pin.it",
    "x.com", "tiktok.com", "instagram.com"
]

app = Client("video_downloader_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
flask_app = Flask(__name__)

active_downloads = 0
queue = asyncio.Queue()
MAX_CONCURRENT_DOWNLOADS = 2
lock = asyncio.Lock()

ADMIN_ID = 6964068910
admin_pending = {}

# Database‑based user helpers
def user_record(user_id):
    u = users_collection.find_one({"user_id": user_id})
    if not u:
        u = {"user_id": user_id, "downloads": 0, "premium_until": None, "blocked": False}
        users_collection.insert_one(u)
    return u

def is_premium(user_id):
    u = users_collection.find_one({"user_id": user_id})
    if not u or u.get("blocked"):
        return False
    pu = u.get("premium_until")
    if not pu:
        return False
    try:
        dt = datetime.fromisoformat(pu)
    except:
        return False
    if dt > datetime.utcnow():
        return True
    else:
        users_collection.update_one({"user_id": user_id}, {"$set": {"premium_until": None}})
        return False

def add_download(user_id):
    u = user_record(user_id)
    new_count = u.get("downloads", 0) + 1
    users_collection.update_one({"user_id": user_id}, {"$set": {"downloads": new_count}})
    return new_count

def reset_downloads(user_id):
    users_collection.update_one({"user_id": user_id}, {"$set": {"downloads": 0}})

def set_premium_days(user_id, days):
    expiry = datetime.utcnow() + timedelta(days=days)
    users_collection.update_one({"user_id": user_id}, {"$set": {"premium_until": expiry.isoformat()}})

def block_user(user_id):
    users_collection.update_one({"user_id": user_id}, {"$set": {"blocked": True}})

def unblock_user(user_id):
    users_collection.update_one({"user_id": user_id}, {"$set": {"blocked": False}})

async def download_thumbnail(url, target_path):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    f = await aiofiles.open(target_path, mode='wb')
                    await f.write(await resp.read())
                    await f.close()
                    if os.path.exists(target_path):
                        return target_path
    except:
        pass
    return None

def extract_metadata_from_info(info):
    width = info.get("width")
    height = info.get("height")
    duration = info.get("duration")
    if not width or not height:
        formats = info.get("formats") or []
        best = None
        for f in formats:
            if f.get("width") and f.get("height"):
                best = f
                break
        if best:
            if not width:
                width = best.get("width")
            if not height:
                height = best.get("height")
            if not duration:
                dms = best.get("duration_ms")
                duration = info.get("duration") or (dms / 1000 if dms else None)
    return width, height, duration

async def download_video(url: str):
    loop = asyncio.get_running_loop()
    try:
        is_pin = "pin.it" in url.lower()
        ydl_opts = YDL_OPTS_PIN.copy() if is_pin else YDL_OPTS_DEFAULT.copy()
        def extract_info_sync():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=False)
        info = await loop.run_in_executor(None, extract_info_sync)
        width, height, duration = extract_metadata_from_info(info)
        if duration and duration > 1800:
            return None
        def download_sync():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dl = ydl.extract_info(url, download=True)
                filename = ydl.prepare_filename(info_dl)
                return info_dl, filename
        info, filename = await loop.run_in_executor(None, download_sync)
        title = info.get("title") or ""
        desc = info.get("description") or ""
        is_youtube = "youtube.com" in url.lower() or "youtu.be" in url.lower()
        if is_youtube:
            caption = title or "@SooDajiye_Bot"
            if len(caption) > 1024:
                caption = caption[:1024]
        else:
            caption = desc.strip() or "@SooDajiye_Bot"
            if len(caption) > 1024:
                caption = caption[:1021] + "..."
        thumb = None
        thumb_url = info.get("thumbnail")
        if thumb_url:
            thumb_path = os.path.splitext(filename)[0] + ".jpg"
            thumb = await download_thumbnail(thumb_url, thumb_path)
        return caption, filename, width, height, duration, thumb
    except Exception as e:
        print(f"[ERROR] Failed to download {url}: {e}")
        return "ERROR"

async def download_audio_only(url: str):
    loop = asyncio.get_running_loop()
    try:
        ydl_opts_audio = {
            "format": "bestaudio[ext=m4a]/bestaudio/best",
            "outtmpl": os.path.join(DOWNLOAD_PATH, "%(title)s.m4a"),
            "noplaylist": True,
            "quiet": True,
            "cookiefile": COOKIES_TXT_PATH
        }
        def download_sync():
            with yt_dlp.YoutubeDL(ydl_opts_audio) as ydl:
                info = ydl.extract_info(url, download=True)
                filename = ydl.prepare_filename(info)
                return info, filename
        info, filename = await loop.run_in_executor(None, download_sync)
        title = info.get("title") or "Audio"
        caption = f"🎧 Audio (M4A)\n\n{title}"
        return caption, filename
    except Exception as e:
        print(f"[ERROR] Audio download failed for {url}: {e}")
        return None

async def process_download(client, message, url):
    global active_downloads
    async with lock:
        active_downloads += 1
    try:
        await client.send_chat_action(message.chat.id, ChatAction.TYPING)
        result = await download_video(url)
        if result is None:
            await message.reply("Masoo dajin kari video ka dheer 30 minute 👍")
        elif result == "ERROR":
            await message.reply("masoo dajin kari muuqaal kan waxaa laga yaa baa inuu Public aheen isku day markale 😉")
        else:
            caption, file_path, width, height, duration, thumb = result
            await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_VIDEO)
            kwargs = {"video": file_path, "caption": caption, "supports_streaming": True}
            if width:
                kwargs["width"] = int(width)
            if height:
                kwargs["height"] = int(height)
            if duration:
                kwargs["duration"] = int(float(duration))
            if thumb and os.path.exists(thumb):
                kwargs["thumb"] = thumb
            await client.send_video(message.chat.id, **kwargs)
            audio_result = await download_audio_only(url)
            if audio_result:
                audio_caption, audio_path = audio_result
                try:
                    await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_AUDIO)
                except:
                    try:
                        await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_DOCUMENT)
                    except:
                        pass
                try:
                    await client.send_audio(
                        message.chat.id,
                        audio=audio_path,
                        caption=audio_caption,
                        title=os.path.splitext(os.path.basename(audio_path))[0],
                        performer="Powered by Zack3d on TikTok.m4a"
                    )
                except Exception as e:
                    print(f"[ERROR] Sending audio failed: {e}")
                if audio_path and os.path.exists(audio_path):
                    try:
                        os.remove(audio_path)
                    except:
                        pass
            for f in [file_path, thumb]:
                if f and os.path.exists(f):
                    try:
                        os.remove(f)
                    except:
                        pass
    finally:
        async with lock:
            active_downloads -= 1
        await start_next_download()

async def start_next_download():
    async with lock:
        while not queue.empty() and active_downloads < MAX_CONCURRENT_DOWNLOADS:
            client_, message_, url_ = await queue.get()
            asyncio.create_task(process_download(client_, message_, url_))

@app.on_message(filters.private & filters.command("start"))
async def start(client, message: Message):
    await message.reply(
        "👋 Welcome!\n"
        "Send me a supported video link, and I’ll download it for you.\n\n"
        "Supported sites:\n"
        "• YouTube\n"
        "• Facebook\n"
        "• Pinterest\n"
        "• X (Twitter)\n"
        "• TikTok\n"
        "• Instagram"
    )

@app.on_message(filters.private & filters.command("admin"))
async def admin_panel(client, message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("❌ Ma tihid admin!")
        return
    args = message.text.split(maxsplit=1)
    if len(args) == 1:
        await message.reply("Fadlan u soo dir /admin followed by @username or user_id")
        return
    target = args[1].strip()
    if target.startswith("@"):
        target = target.lstrip("@")
    admin_pending[message.from_user.id] = {"target": target}
    await message.reply(f"Maxaad rabtaa inaan user-ka {target} ku sameeyo? Qor off ama tirada maalmood ee rukumo (tusaale 1 ama 30)")

@app.on_message(filters.private & filters.user(ADMIN_ID) & filters.text)
async def admin_action_handler(client, message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    session = admin_pending.get(message.from_user.id)
    if not session:
        await message.reply("Fadlan marka hore u dir /admin <@username_or_id>")
        return
    cmd = message.text.strip().lower()
    target = session.get("target")
    target_id = None
    if target.isdigit():
        target_id = int(target)
    else:
        try:
            u = client.get_users(target)
            target_id = u.id
        except:
            await message.reply("Ma aanan helin username-kaas. Haddii ay tahay user id ku qor lambarka userka.")
            admin_pending.pop(message.from_user.id, None)
            return
    if cmd == "off":
        block_user(target_id)
        await message.reply(f"✅ User {target} waa la xannibay.")
        try:
            await client.send_message(target_id, "🚫 Adeegga waa lagaa joojiyay. La xiriir @lakigithub si aad u hesho taageero.")
        except:
            pass
        admin_pending.pop(message.from_user.id, None)
        return
    if cmd.isdigit():
        days = int(cmd)
        set_premium_days(target_id, days)
        reset_downloads(target_id)
        expiry = datetime.utcnow() + timedelta(days=days)
        await message.reply(f"✅ User {target} waxaa loo siiyay rukumo {days} maalmood ah ilaa {expiry.isoformat()} UTC")
        try:
            await client.send_message(target_id, f"🎉 Waad heshay unlimited downloads muddo {days} maalmood ah. Subscription ka ayaa dhici doona: {expiry.isoformat()} UTC")
        except:
            pass
        admin_pending.pop(message.from_user.id, None)
        return
    await message.reply("Amarka ma aqoonsana. Fadlan qor off ama tirada maalmood.")

@app.on_message(filters.private & filters.text)
async def handle_link(client, message: Message):
    if not message.from_user:
        await message.reply("User information lama helin.")
        return
    user_id = message.from_user.id
    text = message.text.strip()
    if text.startswith("/"):
        return
    if text.startswith("@") and message.from_user.id == ADMIN_ID:
        await message.reply("Si aad u maamusho user dir /admin @username_or_id")
        return
    if any(d in text.lower() for d in ["http://", "https://"]):
        if not any(domain in text.lower() for domain in SUPPORTED_DOMAINS):
            await message.reply("Hubi inuu link ga sax yahay 😉")
            return
        u = user_record(user_id)
        if u.get("blocked"):
            await message.reply("🚫 Adeegga waa lagaa joojiyay, la xiriir @lakigithub")
            return
        if is_premium(user_id):
            pass
        else:
            downloads = u.get("downloads", 0)
            if downloads >= 5:
                await message.reply("😎 Waxaad gaartay xadka free downloads ka ee luugu tala galay. Haddii aad rabto unlimited downloads + high speed, la xiriir @lakigithub")
                return
        add_download(user_id)
        async with lock:
            if active_downloads < MAX_CONCURRENT_DOWNLOADS:
                asyncio.create_task(process_download(client, message, text))
            else:
                await queue.put((client, message, text))
    else:
        await message.reply("Fadlan soo dir link muuqaal 😤")

@flask_app.route("/", methods=["GET", "POST", "HEAD"])
def keep_alive():
    return "Bot is alive ✅", 200

def run_flask():
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

def run_bot():
    app.run()

Thread(target=run_flask).start()
run_bot()
