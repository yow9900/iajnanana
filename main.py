import os
import logging
import requests
import json
import threading
import time
import io
import re
from datetime import datetime
from collections import Counter, deque
from pymongo import MongoClient
from flask import Flask, request, abort, jsonify
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import asyncio

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

env = os.environ

TELEGRAM_MAX_BYTES = int(env.get("TELEGRAM_MAX_BYTES", str(50 * 1024 * 1024)))
REQUEST_TIMEOUT_TELEGRAM = int(env.get("REQUEST_TIMEOUT_TELEGRAM", "300"))
REQUEST_TIMEOUT_GEMINI = int(env.get("REQUEST_TIMEOUT_GEMINI", "300"))
MAX_CONCURRENT_TRANSCRIPTS = int(env.get("MAX_CONCURRENT_TRANSCRIPTS", "2"))
MAX_PENDING_QUEUE = int(env.get("MAX_PENDING_QUEUE", "2"))
GEMINI_API_KEYS = [t.strip() for t in env.get("GEMINI_API_KEYS", env.get("GEMINI_API_KEY", "")).split(",") if t.strip()]
WEBHOOK_URL = env.get("WEBHOOK_BASE", "").rstrip("/")
SECRET_KEY = env.get("SECRET_KEY", "testkey123")
MONGO_URI = env.get("MONGO_URI", "")
DB_NAME = env.get("DB_NAME", "telegram_bot_db")
REQUIRED_CHANNEL = env.get("REQUIRED_CHANNEL", "")
BOT_TOKEN = (env.get("BOT_TOKENS") or env.get("BOT_TOKEN") or "")
API_ID = int(env.get("API_ID", "0")) if env.get("API_ID") else None
API_HASH = env.get("API_HASH", None)
ASSEMBLYAI_API_KEYS = [t.strip() for t in env.get("ASSEMBLYAI_API_KEYS", env.get("ASSEMBLYAI_API_KEY", "")).split(",") if t.strip()]
ASSEMBLYAI_BASE_URL = "https://api.assemblyai.com/v2"
FFMPEG_BINARY = env.get("FFMPEG_BINARY", "")

if not BOT_TOKEN:
    logging.error("BOT_TOKEN is not set. Bot will not function.")

client = MongoClient(MONGO_URI) if MONGO_URI else MongoClient()
db = client[DB_NAME]
users_collection = db["users"]
groups_collection = db["groups"]

flask_app = Flask(__name__)

_LANG_RAW = "🇬🇧 English:en,🇸🇦 العربية:ar,🇪🇸 Español:es,🇫🇷 Français:fr,🇷🇺 Русский:ru,🇩🇪 Deutsch:de,🇮🇳 हिन्दी:hi,🇮🇷 فارسی:fa,🇮🇩 Indonesia:id,🇺🇦 Українська:uk,🇦🇿 Azərbaycan:az,🇮🇹 Italiano:it,🇹🇷 Türkçe:tr,🇧🇬 Български:bg,🇷🇸 Srpski:sr,🇵🇰 اردو:ur,🇹🇭 ไทย:th,🇻🇳 Tiếng Việt:vi,🇯🇵 日本語:ja,🇰🇷 한국어:ko,🇨🇳 中文:zh,🇳🇱 Nederlands:nl,🇸🇪 Svenska:sv,🇳🇴 Norsk:no,🇮🇱 עברית:he,🇩🇰 Dansk:da,🇪🇹 አማርኛ:am,🇫🇮 Suomi:fi,🇧🇩 বাংলা:bn,🇰🇪 Kiswahili:sw,🇪🇹 Oromoo:om,🇳🇵 नेपाली:ne,🇵🇱 Polski:pl,🇬🇷 Ελληνικά:el,🇨🇿 Čeština:cs,🇮🇸 Íslenska:is,🇱🇹 Lietuvių:lt,🇱🇻 Latviešu:lv,🇭🇷 Hrvatski:hr,🇷🇸 Bosanski:bs,🇭🇺 Magyar:hu,🇷🇴 Română:ro,🇸🇴 Somali:so,🇲🇾 Melayu:ms,🇺🇿 O'zbekcha:uz,🇵🇭 Tagalog:tl,🇵🇹 Português:pt"
LANG_OPTIONS = [(p.split(":", 1)[0].strip(), p.split(":", 1)[1].strip()) for p in _LANG_RAW.split(",")]
CODE_TO_LABEL = {code: label for label, code in LANG_OPTIONS}
LABEL_TO_CODE = {label: code for label, code in LANG_OPTIONS}

user_transcriptions = {}
in_memory_data = {"pending_media": {}}
action_usage = {}
memory_lock = threading.Lock()
ALLOWED_EXTENSIONS = set(["mp3", "wav", "m4a", "ogg", "webm", "flac", "mp4", "mkv", "avi", "mov", "hevc", "aac", "aiff", "amr", "wma", "opus", "m4v", "ts", "flv", "3gp"])
transcript_semaphore = threading.Semaphore(MAX_CONCURRENT_TRANSCRIPTS)
PENDING_QUEUE = deque()

def norm_user_id(uid):
    try:
        return str(int(uid))
    except:
        return str(uid)

def check_subscription(user_id,bot_client):
    if not REQUIRED_CHANNEL:
        return True
    try:
        member = bot_client.get_chat_member(REQUIRED_CHANNEL, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except:
        return False

def send_subscription_message(chat_id,bot_client):
    if not REQUIRED_CHANNEL:
        return
    try:
        chat = bot_client.get_chat(chat_id)
        if chat.type != 'private':
            return
    except:
        return
    try:
        m = InlineKeyboardMarkup([[InlineKeyboardButton("Click here to join the Group", url=f"https://t.me/{REQUIRED_CHANNEL.lstrip('@')}")]])
        bot_client.send_message(chat_id, "🔒 Access Locked You cannot use this bot until you join the Group.", reply_markup=m)
    except:
        pass

def update_user_activity(user_id):
    uid = norm_user_id(user_id)
    now = datetime.now()
    users_collection.update_one({"user_id": uid}, {"$set": {"last_active": now}, "$setOnInsert": {"first_seen": now, "stt_conversion_count": 0}}, upsert=True)

def increment_processing_count(user_id,service_type):
    users_collection.update_one({"user_id": norm_user_id(user_id)}, {"$inc": {f"{service_type}_conversion_count": 1}})

def get_stt_user_lang(user_id):
    ud = users_collection.find_one({"user_id": norm_user_id(user_id)})
    return ud.get("stt_language", "en") if ud else "en"

def set_stt_user_lang(user_id,lang_code):
    users_collection.update_one({"user_id": norm_user_id(user_id)}, {"$set": {"stt_language": lang_code}}, upsert=True)

def get_user_send_mode(user_id):
    ud = users_collection.find_one({"user_id": norm_user_id(user_id)})
    return ud.get("stt_send_mode", "file") if ud else "file"

def set_user_send_mode(user_id,mode):
    if mode not in ("file", "split"):
        mode = "file"
    users_collection.update_one({"user_id": norm_user_id(user_id)}, {"$set": {"stt_send_mode": mode}}, upsert=True)

def save_pending_media(user_id,media_type,data):
    with memory_lock:
        in_memory_data["pending_media"][user_id] = {"media_type": media_type, "data": data, "saved_at": datetime.now()}

def pop_pending_media(user_id):
    with memory_lock:
        return in_memory_data["pending_media"].pop(user_id, None)

def delete_transcription_later(user_id,message_id):
    time.sleep(86400)
    with memory_lock:
        if user_id in user_transcriptions and message_id in user_transcriptions[user_id]:
            del user_transcriptions[user_id][message_id]

def is_transcoding_like_error(msg):
    if not msg:
        return False
    m = msg.lower()
    checks = ["transcoding failed", "file does not appear to contain audio", "text/html", "html document", "unsupported media type", "could not decode"]
    return any(ch in m for ch in checks)

def build_lang_keyboard(callback_prefix, row_width=3, message_id=None):
    buttons = []
    for label, code in LANG_OPTIONS:
        cb = f"{callback_prefix}|{code}|{message_id}" if message_id else f"{callback_prefix}|{code}"
        buttons.append(InlineKeyboardButton(label, callback_data=cb))
    rows = []
    for i in range(0, len(buttons), row_width):
        rows.append(buttons[i:i+row_width])
    return InlineKeyboardMarkup(rows)

def build_result_mode_keyboard(prefix="result_mode"):
    rows = [[InlineKeyboardButton("📄 .txt file", callback_data=f"{prefix}|file"), InlineKeyboardButton("💬 Split messages", callback_data=f"{prefix}|split")]]
    return InlineKeyboardMarkup(rows)

def animate_processing_message(bot_client, chat_id, message_id, stop_event):
    frames = ["🔄 Processing", "🔄 Processing.", "🔄 Processing..", "🔄 Processing..."]
    idx = 0
    while not stop_event():
        try:
            future = asyncio.run_coroutine_threadsafe(bot_client.edit_message_text(chat_id, message_id, frames[idx % len(frames)]), bot_client.loop)
            future.result(timeout=5)
        except:
            pass
        idx = (idx + 1) % len(frames)
        time.sleep(0.6)

def normalize_text_offline(text):
    return re.sub(r'\s+', ' ', text).strip() if text else text

def extract_key_points_offline(text, max_points=6):
    if not text:
        return ""
    sentences = [s.strip() for s in re.split(r'(?<=[\.\!\?])\s+', text) if s.strip()]
    if not sentences:
        return ""
    words = [w for w in re.findall(r'\w+', text.lower()) if len(w) > 3]
    if not words:
        return "\n".join(f"- {s}" for s in sentences[:max_points])
    freq = Counter(words)
    sentence_scores = [(sum(freq.get(w, 0) for w in re.findall(r'\w+', s.lower())), s) for s in sentences]
    sentence_scores.sort(key=lambda x: x[0], reverse=True)
    top_sentences = sorted(sentence_scores[:max_points], key=lambda x: sentences.index(x[1]))
    return "\n".join(f"- {s}" for _, s in top_sentences)

def safe_extension_from_filename(filename):
    return filename.rsplit(".", 1)[-1].lower() if filename and "." in filename else ""

def telegram_file_info_and_url(bot_token, file_id):
    url = f"https://api.telegram.org/bot{bot_token}/getFile?file_id={file_id}"
    resp = requests.get(url, timeout=REQUEST_TIMEOUT_TELEGRAM)
    resp.raise_for_status()
    file_path = resp.json().get("result", {}).get("file_path")
    return type('T', (), {'file_path': file_path})(), f"https://api.telegram.org/file/bot{bot_token}/{file_path}"

def transcribe_file_with_assemblyai(audio_source, language_code):
    if not ASSEMBLYAI_API_KEYS:
        raise RuntimeError("ASSEMBLYAI_API_KEYS not set")
    last_exception = None
    for api_key in ASSEMBLYAI_API_KEYS:
        try:
            headers = {"Authorization": api_key, "Content-Type": "application/json"}
            config = {"audio_url": audio_source}
            if language_code != "en":
                config["language_code"] = language_code
            submit_url = f"{ASSEMBLYAI_BASE_URL}/transcript"
            submit_resp = requests.post(submit_url, headers=headers, json=config, timeout=REQUEST_TIMEOUT_GEMINI)
            submit_resp.raise_for_status()
            transcript_id = submit_resp.json().get("id")
            if not transcript_id:
                raise RuntimeError("AssemblyAI submission failed: No transcript ID received")
            poll_url = f"{ASSEMBLYAI_BASE_URL}/transcript/{transcript_id}"
            while True:
                poll_resp = requests.get(poll_url, headers={"Authorization": api_key}, timeout=REQUEST_TIMEOUT_GEMINI)
                poll_resp.raise_for_status()
                status = poll_resp.json().get("status")
                if status == "completed":
                    return poll_resp.json().get("text", "")
                elif status in ["failed", "error"]:
                    raise RuntimeError(f"AssemblyAI transcription failed. Details: {poll_resp.json()}")
                elif status == "processing":
                    time.sleep(5)
                else:
                    time.sleep(3)
        except Exception as e:
            logging.warning(f"AssemblyAI key failed: {str(e)}. Trying next key if available.")
            last_exception = e
            continue
    raise RuntimeError(f"All AssemblyAI keys failed. Last error: {str(last_exception) if last_exception else 'No keys were available.'}")

def transcribe_via_selected_service(input_source, lang_code):
    try:
        text = transcribe_file_with_assemblyai(input_source, lang_code)
        service = "assemblyai"
        if text is None:
            raise RuntimeError("AssemblyAI returned no text")
        return text, service
    except Exception as e:
        logging.exception("AssemblyAI failed")
        raise RuntimeError("AssemblyAI failed: " + str(e))

def split_text_into_chunks(text, limit=4096):
    if not text:
        return []
    chunks = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + limit, n)
        if end < n:
            last_space = text.rfind(" ", start, end)
            if last_space > start:
                end = last_space
        chunk = text[start:end].strip()
        if not chunk:
            end = start + limit
            chunk = text[start:end].strip()
        chunks.append(chunk)
        start = end
    return chunks

def attach_action_buttons(bot_client, chat_id, message_id, text):
    try:
        include_summarize = len(text) > 1000 if text else False
        rows = [[InlineKeyboardButton("⭐️Clean transcript", callback_data=f"clean_up|{chat_id}|{message_id}")]]
        if include_summarize:
            rows[0].append(InlineKeyboardButton("Get Summarize", callback_data=f"get_key_points|{chat_id}|{message_id}"))
        markup = InlineKeyboardMarkup(rows)
        try:
            asyncio.run_coroutine_threadsafe(bot_client.edit_message_reply_markup(chat_id, message_id, reply_markup=markup), bot_client.loop)
        except:
            pass
    except:
        pass
    try:
        action_usage[f"{chat_id}|{message_id}|clean_up"] = 0
        action_usage[f"{chat_id}|{message_id}|get_key_points"] = 0
    except:
        pass

def process_media_file(message, bot_client, bot_token, file_id, file_size, filename):
    uid = str(message.from_user.id)
    chatid = str(message.chat.id)
    lang = get_stt_user_lang(uid)
    future_processing = asyncio.run_coroutine_threadsafe(bot_client.send_message(message.chat.id, "🔄 Processing..."), bot_client.loop)
    try:
        processing_msg = future_processing.result(timeout=10)
        processing_msg_id = processing_msg.message_id
    except:
        processing_msg = None
        processing_msg_id = None
    stop = {"stop": False}
    animation_thread = threading.Thread(target=animate_processing_message, args=(bot_client, message.chat.id, processing_msg_id, lambda: stop["stop"]), daemon=True)
    animation_thread.start()
    try:
        tf, file_url = telegram_file_info_and_url(bot_token, file_id)
        try:
            text, used_service = transcribe_via_selected_service(file_url, lang)
        except Exception as e:
            error_msg = str(e)
            logging.exception("Error during transcription")
            if "ffmpeg" in error_msg.lower() and not FFMPEG_BINARY:
                asyncio.run_coroutine_threadsafe(bot_client.send_message(message.chat.id, "⚠️ Server error: ffmpeg not found. Notify the admin.", reply_to_message_id=message.message_id), bot_client.loop)
            elif is_transcoding_like_error(error_msg):
                asyncio.run_coroutine_threadsafe(bot_client.send_message(message.chat.id, "⚠️ Transcription error: file is not audible. Please send a different file.", reply_to_message_id=message.message_id), bot_client.loop)
            else:
                asyncio.run_coroutine_threadsafe(bot_client.send_message(message.chat.id, f"Error during transcription: {error_msg}", reply_to_message_id=message.message_id), bot_client.loop)
            return
        corrected_text = normalize_text_offline(text)
        uid_key = str(message.chat.id)
        user_mode = get_user_send_mode(uid_key)
        if len(corrected_text) > 4000:
            if user_mode == "file":
                f = io.BytesIO(corrected_text.encode("utf-8"))
                f.name = "Transcript.txt"
                try:
                    future = asyncio.run_coroutine_threadsafe(bot_client.send_document(message.chat.id, f), bot_client.loop)
                    sent = future.result(timeout=60)
                    try:
                        attach_action_buttons(bot_client, message.chat.id, sent.message_id, corrected_text)
                    except:
                        pass
                    try:
                        user_transcriptions.setdefault(uid_key, {})[sent.message_id] = corrected_text
                        threading.Thread(target=delete_transcription_later, args=(uid_key, sent.message_id), daemon=True).start()
                    except:
                        pass
                except:
                    pass
            else:
                chunks = split_text_into_chunks(corrected_text, limit=4096)
                last_sent = None
                for idx, chunk in enumerate(chunks):
                    try:
                        if idx == 0:
                            future = asyncio.run_coroutine_threadsafe(bot_client.send_message(message.chat.id, chunk, reply_to_message_id=message.message_id), bot_client.loop)
                        else:
                            future = asyncio.run_coroutine_threadsafe(bot_client.send_message(message.chat.id, chunk), bot_client.loop)
                        last_sent = future.result(timeout=60)
                    except:
                        last_sent = None
                try:
                    attach_action_buttons(bot_client, message.chat.id, last_sent.message_id, corrected_text)
                except:
                    pass
                try:
                    user_transcriptions.setdefault(uid_key, {})[last_sent.message_id] = corrected_text
                    threading.Thread(target=delete_transcription_later, args=(uid_key, last_sent.message_id), daemon=True).start()
                except:
                    pass
        else:
            try:
                future = asyncio.run_coroutine_threadsafe(bot_client.send_message(message.chat.id, corrected_text or "⚠️ Warning Make sure the voice is clear or speaking in the language you Choosed.", reply_to_message_id=message.message_id), bot_client.loop)
                sent_msg = future.result(timeout=60)
                try:
                    attach_action_buttons(bot_client, message.chat.id, sent_msg.message_id, corrected_text)
                except:
                    pass
                try:
                    user_transcriptions.setdefault(uid_key, {})[sent_msg.message_id] = corrected_text
                    threading.Thread(target=delete_transcription_later, args=(uid_key, sent_msg.message_id), daemon=True).start()
                except:
                    pass
            except:
                pass
        increment_processing_count(uid, "stt")
    finally:
        stop["stop"] = True
        animation_thread.join(timeout=2)
        try:
            if processing_msg_id:
                asyncio.run_coroutine_threadsafe(bot_client.delete_messages(message.chat.id, processing_msg_id), bot_client.loop)
        except:
            pass

def worker_thread():
    while True:
        try:
            transcript_semaphore.acquire()
            item = None
            with memory_lock:
                if PENDING_QUEUE:
                    item = PENDING_QUEUE.popleft()
            if item:
                message, bot_client, bot_token, file_id, file_size, filename = item
                logging.info(f"Starting processing for user {message.from_user.id} (Chat {message.chat.id}) from queue. Current queue size: {len(PENDING_QUEUE)}")
                process_media_file(message, bot_client, bot_token, file_id, file_size, filename)
            else:
                transcript_semaphore.release()
        except:
            logging.exception("Error in worker thread")
        finally:
            if item:
                transcript_semaphore.release()
            time.sleep(0.5)

def start_worker_threads():
    for i in range(MAX_CONCURRENT_TRANSCRIPTS):
        t = threading.Thread(target=worker_thread, daemon=True)
        t.start()

start_worker_threads()

def handle_media_common(message, bot_client):
    if not bot_client:
        return
    update_user_activity(message.from_user.id)
    if message.chat.type == 'private' and not check_subscription(message.from_user.id, bot_client):
        send_subscription_message(message.chat.id, bot_client)
        return
    file_id = file_size = filename = None
    if message.voice:
        file_id = message.voice.file_id
        file_size = message.voice.file_size
        filename = "voice.ogg"
    elif message.audio:
        file_id = message.audio.file_id
        file_size = message.audio.file_size
        filename = getattr(message.audio, "file_name", "audio")
    elif message.video:
        file_id = message.video.file_id
        file_size = message.video.file_size
        filename = getattr(message.video, "file_name", "video.mp4")
    elif message.document:
        mime = getattr(message.document, "mime_type", None)
        filename = getattr(message.document, "file_name", None) or "file"
        ext = safe_extension_from_filename(filename)
        if (mime and ("audio" in mime or "video" in mime)) or ext in ALLOWED_EXTENSIONS:
            file_id = message.document.file_id
            file_size = message.document.file_size
        else:
            asyncio.run_coroutine_threadsafe(bot_client.send_message(message.chat.id, "Sorry, I can only transcribe audio or video files."), bot_client.loop)
            return
    if file_size and file_size > TELEGRAM_MAX_BYTES:
        max_display_mb = TELEGRAM_MAX_BYTES // (1024 * 1024)
        asyncio.run_coroutine_threadsafe(bot_client.send_message(message.chat.id, f"Just Send me a file less than {max_display_mb}MB 😎", reply_to_message_id=message.message_id), bot_client.loop)
        return
    with memory_lock:
        if len(PENDING_QUEUE) >= MAX_PENDING_QUEUE:
            asyncio.run_coroutine_threadsafe(bot_client.send_message(message.chat.id, "⚠️ Server busy. Try again later.", reply_to_message_id=message.message_id), bot_client.loop)
            return
        PENDING_QUEUE.append((message, bot_client, BOT_TOKEN, file_id, file_size, filename))

def ask_gemini(text, instruction, timeout=REQUEST_TIMEOUT_GEMINI):
    if not GEMINI_API_KEYS:
        raise RuntimeError("GEMINI_API_KEYS not set")
    last_exception = None
    for api_key in GEMINI_API_KEYS:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"
            payload = {"contents": [{"parts": [{"text": instruction}, {"text": text}]}]}
            headers = {"Content-Type": "application/json"}
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
            resp.raise_for_status()
            result = resp.json()
            if "candidates" in result and isinstance(result["candidates"], list) and len(result["candidates"]) > 0:
                try:
                    return result['candidates'][0]['content']['parts'][0]['text']
                except:
                    return json.dumps(result['candidates'][0])
            raise RuntimeError(f"Gemini response lacks candidates: {json.dumps(result)}")
        except Exception as e:
            logging.warning(f"Gemini API key failed: {str(e)}. Trying next key if available.")
            last_exception = e
            continue
    raise RuntimeError(f"All Gemini API keys failed. Last error: {str(last_exception) if last_exception else 'No keys were available.'}")

def register_handlers(bot_client):
    @bot_client.on_message(filters.command("start"))
    async def start_handler(client_obj, message):
        try:
            update_user_activity(message.from_user.id)
            if message.chat.type == 'private' and not check_subscription(message.from_user.id, bot_client):
                send_subscription_message(message.chat.id, bot_client)
                return
            kb = build_lang_keyboard("start_select_lang")
            await client_obj.send_message(message.chat.id, "Choose your file language for transcription using the below buttons:", reply_markup=kb)
        except:
            logging.exception("Error in start_handler")

    @bot_client.on_callback_query(filters.create(lambda _, __, query: query.data and query.data.startswith("start_select_lang|")))
    async def start_select_lang_callback(client_obj, callback_query):
        try:
            uid = str(callback_query.from_user.id)
            _, lang_code = callback_query.data.split("|", 1)
            lang_label = CODE_TO_LABEL.get(lang_code, lang_code)
            set_stt_user_lang(uid, lang_code)
            try:
                await callback_query.message.delete()
            except:
                pass
            welcome_text = "👋 Salaam!    \n• Send me\n• voice message\n• audio file\n• video\n• to transcribe for free"
            await client_obj.send_message(callback_query.message.chat.id, welcome_text)
            await callback_query.answer(f"✅ Language set to {lang_label}")
        except:
            logging.exception("Error in start_select_lang_callback")
            try:
                await callback_query.answer("❌ Error setting language, try again.", show_alert=True)
            except:
                pass

    @bot_client.on_message(filters.command("help"))
    async def handle_help(client_obj, message):
        try:
            update_user_activity(message.from_user.id)
            if message.chat.type == 'private' and not check_subscription(message.from_user.id, bot_client):
                send_subscription_message(message.chat.id, bot_client)
                return
            text = "Commands supported:\n/start - Show welcome message\n/lang  - Change language\n/mode  - Change result delivery mode\n/help  - This help message\n\nSend a voice/audio/video (up to 20MB) and I will transcribe it Need help? Contact: @lakigithub"
            await client_obj.send_message(message.chat.id, text)
        except:
            logging.exception("Error in handle_help")

    @bot_client.on_message(filters.command("lang"))
    async def handle_lang(client_obj, message):
        try:
            if message.chat.type == 'private' and not check_subscription(message.from_user.id, bot_client):
                send_subscription_message(message.chat.id, bot_client)
                return
            kb = build_lang_keyboard("stt_lang")
            await client_obj.send_message(message.chat.id, "Choose your file language for transcription using the below buttons:", reply_markup=kb)
        except:
            logging.exception("Error in handle_lang")

    @bot_client.on_message(filters.command("mode"))
    async def handle_mode(client_obj, message):
        try:
            if message.chat.type == 'private' and not check_subscription(message.from_user.id, bot_client):
                send_subscription_message(message.chat.id, bot_client)
                return
            current_mode = get_user_send_mode(str(message.from_user.id))
            mode_text = "📄 .txt file" if current_mode == "file" else "💬 Split messages"
            await client_obj.send_message(message.chat.id, f"Result delivery mode: {mode_text}. Change it below:", reply_markup=build_result_mode_keyboard())
        except:
            logging.exception("Error in handle_mode")

    @bot_client.on_callback_query(filters.create(lambda _, __, query: query.data and query.data.startswith("stt_lang|")))
    async def on_stt_language_select(client_obj, callback_query):
        try:
            uid = str(callback_query.from_user.id)
            _, lang_code = callback_query.data.split("|", 1)
            lang_label = CODE_TO_LABEL.get(lang_code, lang_code)
            set_stt_user_lang(uid, lang_code)
            await callback_query.answer(f"✅ Language set: {lang_label}")
            try:
                await callback_query.message.delete()
            except:
                pass
        except:
            logging.exception("Error in on_stt_language_select")
            try:
                await callback_query.answer("❌ Error setting language, try again.", show_alert=True)
            except:
                pass

    @bot_client.on_callback_query(filters.create(lambda _, __, query: query.data and query.data.startswith("result_mode|")))
    async def on_result_mode_select(client_obj, callback_query):
        try:
            uid = str(callback_query.from_user.id)
            _, mode = callback_query.data.split("|", 1)
            set_user_send_mode(uid, mode)
            mode_text = "📄 .txt file" if mode == "file" else "💬 Split messages"
            try:
                await callback_query.message.delete()
            except:
                pass
            await callback_query.answer(f"✅ Result mode set: {mode_text}")
        except:
            logging.exception("Error in on_result_mode_select")
            try:
                await callback_query.answer("❌ Error setting result mode, try again.", show_alert=True)
            except:
                pass

    @bot_client.on_message(filters.new_chat_members)
    async def handle_new_chat_members(client_obj, message):
        try:
            if message.new_chat_members and message.new_chat_members[0].id == (await client_obj.get_me()).id:
                group_data = {'_id': str(message.chat.id), 'title': message.chat.title, 'type': message.chat.type, 'added_date': datetime.now()}
                groups_collection.update_one({'_id': group_data['_1'] if '_1' in group_data else group_data['_id']}, {'$set': group_data}, upsert=True)
                await client_obj.send_message(message.chat.id, "Thanks for adding me! I'm ready to transcribe your media files.")
        except:
            logging.exception("Error in handle_new_chat_members")

    @bot_client.on_message(filters.left_chat_member)
    async def handle_left_chat_member(client_obj, message):
        try:
            if message.left_chat_member and message.left_chat_member.id == (await client_obj.get_me()).id:
                groups_collection.delete_one({'_id': str(message.chat.id)})
        except:
            logging.exception("Error in handle_left_chat_member")

    @bot_client.on_message(filters.media)
    async def handle_media_types(client_obj, message):
        try:
            handle_media_common(message, bot_client)
        except:
            logging.exception("Error in handle_media_types")

    @bot_client.on_message(filters.text)
    async def handle_text_messages(client_obj, message):
        try:
            if message.chat.type == 'private' and not check_subscription(message.from_user.id, bot_client):
                send_subscription_message(message.chat.id, bot_client)
                return
            await client_obj.send_message(message.chat.id, "For Text to Audio Use: @TextToSpeechBBot")
        except:
            logging.exception("Error in handle_text_messages")

    @bot_client.on_callback_query(filters.create(lambda _, __, query: query.data and query.data.startswith("get_key_points|")))
    async def get_key_points_callback(client_obj, callback_query):
        try:
            parts = callback_query.data.split("|")
            if len(parts) == 3:
                _, chat_id_part, msg_id_part = parts
            elif len(parts) == 2:
                _, msg_id_part = parts
                chat_id_part = str(callback_query.message.chat.id)
            else:
                await callback_query.answer("Invalid request", show_alert=True)
                return
            try:
                chat_id_val = int(chat_id_part)
                msg_id = int(msg_id_part)
            except:
                await callback_query.answer("Invalid message id", show_alert=True)
                return
            usage_key = f"{chat_id_val}|{msg_id}|get_key_points"
            usage = action_usage.get(usage_key, 0)
            if usage >= 1:
                await callback_query.answer("Get Summarize unavailable (maybe expired)", show_alert=True)
                return
            action_usage[usage_key] = usage + 1
            uid_key = str(chat_id_val)
            stored = user_transcriptions.get(uid_key, {}).get(msg_id)
            if not stored:
                await callback_query.answer("Get Summarize unavailable (maybe expired)", show_alert=True)
                return
            await callback_query.answer("Generating...")
            status_msg = await client_obj.send_message(callback_query.message.chat.id, "🔄 Processing...", reply_to_message_id=callback_query.message.message_id)
            stop = {"stop": False}
            animation_thread = threading.Thread(target=animate_processing_message, args=(bot_client, callback_query.message.chat.id, status_msg.message_id, lambda: stop["stop"]), daemon=True)
            animation_thread.start()
            try:
                lang = get_stt_user_lang(str(chat_id_val)) or "en"
                instruction = f"What is this report and what is it about? Please summarize them for me into (lang={lang}) without adding any introductions, notes, or extra phrases."
                try:
                    summary = ask_gemini(stored, instruction)
                except:
                    summary = extract_key_points_offline(stored, max_points=6)
            except:
                summary = ""
            stop["stop"] = True
            animation_thread.join(timeout=2)
            if not summary:
                try:
                    await client_obj.edit_message_text(callback_query.message.chat.id, status_msg.message_id, "No Summary returned.")
                except:
                    pass
            else:
                try:
                    await client_obj.edit_message_text(callback_query.message.chat.id, status_msg.message_id, summary)
                except:
                    pass
        except:
            logging.exception("Error in get_key_points_callback")

    @bot_client.on_callback_query(filters.create(lambda _, __, query: query.data and query.data.startswith("clean_up|")))
    async def clean_up_callback(client_obj, callback_query):
        try:
            parts = callback_query.data.split("|")
            if len(parts) == 3:
                _, chat_id_part, msg_id_part = parts
            elif len(parts) == 2:
                _, msg_id_part = parts
                chat_id_part = str(callback_query.message.chat.id)
            else:
                await callback_query.answer("Invalid request", show_alert=True)
                return
            try:
                chat_id_val = int(chat_id_part)
                msg_id = int(msg_id_part)
            except:
                await callback_query.answer("Invalid message id", show_alert=True)
                return
            usage_key = f"{chat_id_val}|{msg_id}|clean_up"
            usage = action_usage.get(usage_key, 0)
            if usage >= 1:
                await callback_query.answer("Clean up unavailable (maybe expired)", show_alert=True)
                return
            action_usage[usage_key] = usage + 1
            uid_key = str(chat_id_val)
            stored = user_transcriptions.get(uid_key, {}).get(msg_id)
            if not stored:
                await callback_query.answer("Clean up unavailable (maybe expired)", show_alert=True)
                return
            await callback_query.answer("Cleaning up...")
            status_msg = await client_obj.send_message(callback_query.message.chat.id, "🔄 Processing...", reply_to_message_id=callback_query.message.message_id)
            stop = {"stop": False}
            animation_thread = threading.Thread(target=animate_processing_message, args=(bot_client, callback_query.message.chat.id, status_msg.message_id, lambda: stop["stop"]), daemon=True)
            animation_thread.start()
            try:
                lang = get_stt_user_lang(str(chat_id_val)) or "en"
                instruction = f"Clean and normalize this transcription (lang={lang}). Remove ASR artifacts like [inaudible], repeated words, filler noises, timestamps, and incorrect punctuation. Produce a clean, well-punctuated, readable text in the same language. Do not add introductions or explanations."
                try:
                    cleaned = ask_gemini(stored, instruction)
                except:
                    cleaned = normalize_text_offline(stored)
            except:
                cleaned = ""
            stop["stop"] = True
            animation_thread.join(timeout=2)
            if not cleaned:
                try:
                    await client_obj.edit_message_text(callback_query.message.chat.id, status_msg.message_id, "No cleaned text returned.")
                except:
                    pass
                return
            uid_key = str(chat_id_val)
            user_mode = get_user_send_mode(uid_key)
            if len(cleaned) > 4000:
                if user_mode == "file":
                    f = io.BytesIO(cleaned.encode("utf-8"))
                    f.name = "cleaned.txt"
                    try:
                        await client_obj.delete_messages(callback_query.message.chat.id, status_msg.message_id)
                    except:
                        pass
                    sent = await client_obj.send_document(callback_query.message.chat.id, f, reply_to_message_id=callback_query.message.message_id)
                    try:
                        user_transcriptions.setdefault(uid_key, {})[sent.message_id] = cleaned
                        threading.Thread(target=delete_transcription_later, args=(uid_key, sent.message_id), daemon=True).start()
                    except:
                        pass
                    try:
                        action_usage[f"{callback_query.message.chat.id}|{sent.message_id}|clean_up"] = 0
                        action_usage[f"{callback_query.message.chat.id}|{sent.message_id}|get_key_points"] = 0
                    except:
                        pass
                else:
                    try:
                        await client_obj.delete_messages(callback_query.message.chat.id, status_msg.message_id)
                    except:
                        pass
                    chunks = split_text_into_chunks(cleaned, limit=4096)
                    last_sent = None
                    for idx, chunk in enumerate(chunks):
                        if idx == 0:
                            last_sent = await client_obj.send_message(callback_query.message.chat.id, chunk, reply_to_message_id=callback_query.message.message_id)
                        else:
                            last_sent = await client_obj.send_message(callback_query.message.chat.id, chunk)
                    try:
                        user_transcriptions.setdefault(uid_key, {})[last_sent.message_id] = cleaned
                        threading.Thread(target=delete_transcription_later, args=(uid_key, last_sent.message_id), daemon=True).start()
                    except:
                        pass
                    try:
                        action_usage[f"{callback_query.message.chat.id}|{last_sent.message_id}|clean_up"] = 0
                        action_usage[f"{callback_query.message.chat.id}|{last_sent.message_id}|get_key_points"] = 0
                    except:
                        pass
            else:
                try:
                    await client_obj.edit_message_text(callback_query.message.chat.id, status_msg.message_id, cleaned)
                    uid_key = str(chat_id_val)
                    user_transcriptions.setdefault(uid_key, {})[status_msg.message_id] = cleaned
                    threading.Thread(target=delete_transcription_later, args=(uid_key, status_msg.message_id), daemon=True).start()
                    action_usage[f"{callback_query.message.chat.id}|{status_msg.message_id}|clean_up"] = 0
                    action_usage[f"{callback_query.message.chat.id}|{status_msg.message_id}|get_key_points"] = 0
                except:
                    pass
        except:
            logging.exception("Error in clean_up_callback")

bot_client = Client("pyro_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

register_handlers(bot_client)

@flask_app.route("/", methods=["GET", "POST", "HEAD"])
def keep_alive():
    return "Bot is alive ✅", 200

def run_flask():
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

if __name__ == "__main__":
    Thread = threading.Thread
    Thread(target=run_flask, daemon=True).start()
    try:
        loop = asyncio.get_event_loop()
    except:
        loop = None
    try:
        try:
            client.admin.command('ping')
            logging.info("Successfully connected to MongoDB!")
        except Exception as e:
            logging.error("Could not connect to MongoDB: %s", e)
    except:
        pass
    bot_client.run()
