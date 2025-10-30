import os
import logging
import requests
import json
import threading
import time
import io
import re
from datetime import datetime
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor
from collections import Counter, deque
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.enums import ChatAction, ChatType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

env = os.environ

TELEGRAM_MAX_BYTES = int(env.get("TELEGRAM_MAX_BYTES", str(50 * 1024 * 1024)))
REQUEST_TIMEOUT_TELEGRAM = int(env.get("REQUEST_TIMEOUT_TELEGRAM", "300"))
REQUEST_TIMEOUT_GEMINI = int(env.get("REQUEST_TIMEOUT_GEMINI", "300"))
MAX_CONCURRENT_TRANSCRIPTS = int(env.get("MAX_CONCURRENT_TRANSCRIPTS", "2"))
MAX_PENDING_QUEUE = int(env.get("MAX_PENDING_QUEUE", "2"))
GEMINI_API_KEYS = [t.strip() for t in env.get("GEMINI_API_KEYS", env.get("GEMINI_API_KEY", "")).split(",") if t.strip()]
MONGO_URI = env.get("MONGO_URI", "")
DB_NAME = env.get("DB_NAME", "telegram_bot_db")
REQUIRED_CHANNEL = env.get("REQUIRED_CHANNEL", "")
BOT_TOKEN = ([t.strip() for t in env.get("BOT_TOKENS", "").split(",") if t.strip()] + [""])[0]
ASSEMBLYAI_API_KEYS = [t.strip() for t in env.get("ASSEMBLYAI_API_KEYS", env.get("ASSEMBLYAI_API_KEY", "")).split(",") if t.strip()]
ASSEMBLYAI_BASE_URL = "https://api.assemblyai.com/v2"

if not BOT_TOKEN:
    logging.error("BOT_TOKEN is not set. Bot will not function.")

from flask import Flask
flask_app = Flask(__name__)

client = MongoClient(MONGO_URI) if MONGO_URI else MongoClient()
db = client[DB_NAME]
users_collection = db["users"]
groups_collection = db["groups"]

app = None
if BOT_TOKEN:
    try:
        app = Client("stt_bot", bot_token=BOT_TOKEN, api_id=29169428, api_hash="55742b16a85aac494c7944568b5507e5", sleep_threshold=30)
    except Exception as e:
        logging.error(f"Error creating Pyrogram client: {e}")

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

def check_subscription(user_id, client_obj):
    if not REQUIRED_CHANNEL:
        return True
    try:
        member = client_obj.get_chat_member(REQUIRED_CHANNEL, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception:
        return False

async def send_subscription_message(chat_id, client_obj):
    if not REQUIRED_CHANNEL:
        return
    try:
        chat = await client_obj.get_chat(chat_id)
        if chat.type != ChatType.PRIVATE:
            return
    except Exception:
        return
    try:
        m = InlineKeyboardMarkup([[InlineKeyboardButton("Click here to join the Group ", url=f"https://t.me/{REQUIRED_CHANNEL.lstrip('@')}")]])
        await client_obj.send_message(chat_id, "🔒 Access Locked You cannot use this bot until you join the Group.", reply_markup=m)
    except Exception:
        pass

def update_user_activity(user_id):
    uid = norm_user_id(user_id)
    now = datetime.now()
    users_collection.update_one({"user_id": uid}, {"$set": {"last_active": now}, "$setOnInsert": {"first_seen": now, "stt_conversion_count": 0}}, upsert=True)

def increment_processing_count(user_id, service_type):
    users_collection.update_one({"user_id": norm_user_id(user_id)}, {"$inc": {f"{service_type}_conversion_count": 1}})

def get_stt_user_lang(user_id):
    ud = users_collection.find_one({"user_id": norm_user_id(user_id)})
    return ud.get("stt_language", "en") if ud else "en"

def set_stt_user_lang(user_id, lang_code):
    users_collection.update_one({"user_id": norm_user_id(user_id)}, {"$set": {"stt_language": lang_code}}, upsert=True)

def get_user_send_mode(user_id):
    ud = users_collection.find_one({"user_id": norm_user_id(user_id)})
    return ud.get("stt_send_mode", "file") if ud else "file"

def set_user_send_mode(user_id, mode):
    if mode not in ("file", "split"):
        mode = "file"
    users_collection.update_one({"user_id": norm_user_id(user_id)}, {"$set": {"stt_send_mode": mode}}, upsert=True)

def delete_transcription_later(user_id, message_id):
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
    buttons = [InlineKeyboardButton(label, callback_data=f"{callback_prefix}|{code}|{message_id}" if message_id else f"{callback_prefix}|{code}") for label, code in LANG_OPTIONS]
    keyboard = []
    for i in range(0, len(buttons), row_width):
        keyboard.append(buttons[i:i + row_width])
    return InlineKeyboardMarkup(keyboard)

def build_result_mode_keyboard(prefix="result_mode"):
    return InlineKeyboardMarkup([[InlineKeyboardButton("📄 .txt file", callback_data=f"{prefix}|file"), InlineKeyboardButton("💬 Split messages", callback_data=f"{prefix}|split")]])

async def animate_processing_message(client_obj, chat_id, message_id, stop_event):
    frames = ["🔄 Processing", "🔄 Processing.", "🔄 Processing..", "🔄 Processing..."]
    idx = 0
    while not stop_event():
        try:
            await client_obj.edit_message_text(chat_id, message_id, frames[idx % len(frames)])
        except Exception:
            pass
        idx = (idx + 1) % len(frames)
        await asyncio.sleep(0.6)

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

def telegram_file_url(bot_token, file_path):
    return f"https://api.telegram.org/file/bot{bot_token}/{file_path}"

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
        if text is None:
            raise RuntimeError("AssemblyAI returned no text")
        return text, "assemblyai"
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

async def attach_action_buttons(client_obj, chat_id, message_id, text):
    try:
        include_summarize = len(text) > 1000 if text else False
        m = InlineKeyboardMarkup([[InlineKeyboardButton("⭐️Clean transcript", callback_data=f"clean_up|{chat_id}|{message_id}")]])
        if include_summarize:
            m.inline_keyboard.append([InlineKeyboardButton("Get Summarize", callback_data=f"get_key_points|{chat_id}|{message_id}")]])
        try:
            await client_obj.edit_message_reply_markup(chat_id, message_id, reply_markup=m)
        except Exception:
            pass
    except Exception:
        pass
    try:
        action_usage[f"{chat_id}|{message_id}|clean_up"] = 0
        action_usage[f"{chat_id}|{message_id}|get_key_points"] = 0
    except Exception:
        pass

async def process_media_file_pyrogram(client_obj: Client, message: Message, file_id, file_ref, file_size, filename):
    uid = str(message.from_user.id)
    chatid = str(message.chat.id)
    lang = get_stt_user_lang(uid)
    
    await client_obj.send_chat_action(message.chat.id, ChatAction.TYPING)
    processing_msg = await message.reply_text("🔄 Processing...")
    processing_msg_id = processing_msg.id
    
    stop = {"stop": False}
    animation_task = client_obj.loop.create_task(animate_processing_message(client_obj, message.chat.id, processing_msg_id, lambda: stop["stop"]))

    try:
        file_path = (await client_obj.get_file(file_id, file_ref)).file_path
        file_url = telegram_file_url(client_obj.bot_token, file_path)
        
        try:
            text, used_service = await client_obj.loop.run_in_executor(
                None,
                lambda: transcribe_via_selected_service(file_url, lang)
            )
        except Exception as e:
            error_msg = str(e)
            logging.exception("Error during transcription")
            if is_transcoding_like_error(error_msg):
                await message.reply_text("⚠️ Transcription error: file is not audible. Please send a different file.")
            else:
                await message.reply_text(f"Error during transcription: {error_msg}")
            return
            
        corrected_text = normalize_text_offline(text)
        uid_key = str(message.chat.id)
        user_mode = get_user_send_mode(uid_key)

        if len(corrected_text) > 4000:
            if user_mode == "file":
                f = io.BytesIO(corrected_text.encode("utf-8"))
                f.name = "Transcript.txt"
                sent = await client_obj.send_document(message.chat.id, f, reply_to_message_id=message.id)
                try:
                    await attach_action_buttons(client_obj, message.chat.id, sent.id, corrected_text)
                except Exception:
                    pass
                try:
                    user_transcriptions.setdefault(uid_key, {})[sent.id] = corrected_text
                    threading.Thread(target=delete_transcription_later, args=(uid_key, sent.id), daemon=True).start()
                except Exception:
                    pass
            else:
                chunks = split_text_into_chunks(corrected_text, limit=4096)
                last_sent = None
                for idx, chunk in enumerate(chunks):
                    if idx == 0:
                        last_sent = await client_obj.send_message(message.chat.id, chunk, reply_to_message_id=message.id)
                    else:
                        last_sent = await client_obj.send_message(message.chat.id, chunk)
                if last_sent:
                    try:
                        await attach_action_buttons(client_obj, message.chat.id, last_sent.id, corrected_text)
                    except Exception:
                        pass
                    try:
                        user_transcriptions.setdefault(uid_key, {})[last_sent.id] = corrected_text
                        threading.Thread(target=delete_transcription_later, args=(uid_key, last_sent.id), daemon=True).start()
                    except Exception:
                        pass
        else:
            sent_msg = await message.reply_text(corrected_text or "⚠️ Warning Make sure the voice is clear or speaking in the language you Choosed.")
            try:
                await attach_action_buttons(client_obj, message.chat.id, sent_msg.id, corrected_text)
            except Exception:
                pass
            try:
                user_transcriptions.setdefault(uid_key, {})[sent_msg.id] = corrected_text
                threading.Thread(target=delete_transcription_later, args=(uid_key, sent_msg.id), daemon=True).start()
            except Exception:
                pass
        increment_processing_count(uid, "stt")
    finally:
        stop["stop"] = True
        animation_task.cancel()
        try:
            await client_obj.delete_messages(message.chat.id, processing_msg_id)
        except Exception:
            pass

async def worker_pyrogram():
    import asyncio
    while True:
        try:
            await client_obj.loop.run_in_executor(None, transcript_semaphore.acquire)
            item = None
            with memory_lock:
                if PENDING_QUEUE:
                    item = PENDING_QUEUE.popleft()
            if item:
                client_obj, message, file_id, file_ref, file_size, filename = item
                logging.info(f"Starting processing for user {message.from_user.id} (Chat {message.chat.id}) from queue. Current queue size: {len(PENDING_QUEUE)}")
                await process_media_file_pyrogram(client_obj, message, file_id, file_ref, file_size, filename)
            else:
                client_obj.loop.run_in_executor(None, transcript_semaphore.release)
        except Exception:
            logging.exception("Error in worker thread")
        finally:
            if item:
                client_obj.loop.run_in_executor(None, transcript_semaphore.release)
            await asyncio.sleep(0.5)

def start_worker_threads_pyrogram(client_obj):
    import asyncio
    for _ in range(MAX_CONCURRENT_TRANSCRIPTS):
        client_obj.loop.create_task(worker_pyrogram())

async def ask_gemini_pyrogram(text, instruction, timeout=REQUEST_TIMEOUT_GEMINI):
    return await app.loop.run_in_executor(None, lambda: ask_gemini(text, instruction, timeout))

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
                except Exception:
                    return json.dumps(result['candidates'][0])
            raise RuntimeError(f"Gemini response lacks candidates: {json.dumps(result)}")
        except Exception as e:
            logging.warning(f"Gemini API key failed: {str(e)}. Trying next key if available.")
            last_exception = e
            continue
    raise RuntimeError(f"All Gemini API keys failed. Last error: {str(last_exception) if last_exception else 'No keys were available.'}")

async def handle_media_common_pyrogram(client_obj: Client, message: Message):
    if not client_obj:
        return
    update_user_activity(message.from_user.id)
    if message.chat.type == ChatType.PRIVATE and not check_subscription(message.from_user.id, client_obj):
        await send_subscription_message(message.chat.id, client_obj)
        return
    
    file_id = file_size = filename = file_ref = None
    
    if message.voice:
        file_id = message.voice.file_id
        file_size = message.voice.file_size
        filename = "voice.ogg"
        file_ref = message.voice.file_ref
    elif message.audio:
        file_id = message.audio.file_id
        file_size = message.audio.file_size
        filename = getattr(message.audio, "file_name", "audio")
        file_ref = message.audio.file_ref
    elif message.video:
        file_id = message.video.file_id
        file_size = message.video.file_size
        filename = getattr(message.video, "file_name", "video.mp4")
        file_ref = message.video.file_ref
    elif message.document:
        mime = getattr(message.document, "mime_type", None)
        filename = getattr(message.document, "file_name", None) or "file"
        ext = safe_extension_from_filename(filename)
        if (mime and ("audio" in mime or "video" in mime)) or ext in ALLOWED_EXTENSIONS:
            file_id = message.document.file_id
            file_size = message.document.file_size
            file_ref = message.document.file_ref
        else:
            await message.reply_text("Sorry, I can only transcribe audio or video files.")
            return

    if file_size and file_size > TELEGRAM_MAX_BYTES:
        max_display_mb = TELEGRAM_MAX_BYTES // (1024 * 1024)
        await message.reply_text(f"Just Send me a file less than {max_display_mb}MB 😎", reply_to_message_id=message.id)
        return
        
    with memory_lock:
        if len(PENDING_QUEUE) >= MAX_PENDING_QUEUE:
            await message.reply_text("⚠️ Server busy. Try again later.", reply_to_message_id=message.id)
            return
        PENDING_QUEUE.append((client_obj, message, file_id, file_ref, file_size, filename))


@flask_app.route("/", methods=["GET", "POST", "HEAD"])
def keep_alive():
    return "Bot is alive ✅", 200

def run_flask():
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

if app:
    
    @app.on_message(filters.private & filters.command("start"))
    async def start_handler(client_obj, message: Message):
        try:
            update_user_activity(message.from_user.id)
            if message.chat.type == ChatType.PRIVATE and not check_subscription(message.from_user.id, client_obj):
                await send_subscription_message(message.chat.id, client_obj)
                return
            await message.reply_text("Choose your file language for transcription using the below buttons:", reply_markup=build_lang_keyboard("start_select_lang"))
        except Exception:
            logging.exception("Error in start_handler")

    @app.on_callback_query(filters.regex("^start_select_lang\\|"))
    async def start_select_lang_callback(client_obj, call):
        try:
            uid = str(call.from_user.id)
            _, lang_code = call.data.split("|", 1)
            lang_label = CODE_TO_LABEL.get(lang_code, lang_code)
            set_stt_user_lang(uid, lang_code)
            try:
                await call.message.delete()
            except Exception:
                pass
            welcome_text = "👋 Salaam!    \n• Send me\n• voice message\n• audio file\n• video\n• to transcribe for free"
            await client_obj.send_message(call.message.chat.id, welcome_text)
            await call.answer(f"✅ Language set to {lang_label}")
        except Exception:
            logging.exception("Error in start_select_lang_callback")
            try:
                await call.answer("❌ Error setting language, try again.", show_alert=True)
            except Exception:
                pass

    @app.on_message(filters.private & filters.command("help"))
    async def handle_help(client_obj, message: Message):
        try:
            update_user_activity(message.from_user.id)
            if message.chat.type == ChatType.PRIVATE and not check_subscription(message.from_user.id, client_obj):
                await send_subscription_message(message.chat.id, client_obj)
                return
            text = "Commands supported:\n/start - Show welcome message\n/lang  - Change language\n/mode  - Change result delivery mode\n/help  - This help message\n\nSend a voice/audio/video (up to 20MB) and I will transcribe it Need help? Contact: @lakigithub"
            await message.reply_text(text)
        except Exception:
            logging.exception("Error in handle_help")

    @app.on_message(filters.private & filters.command("lang"))
    async def handle_lang(client_obj, message: Message):
        try:
            if message.chat.type == ChatType.PRIVATE and not check_subscription(message.from_user.id, client_obj):
                await send_subscription_message(message.chat.id, client_obj)
                return
            kb = build_lang_keyboard("stt_lang")
            await message.reply_text("Choose your file language for transcription using the below buttons:", reply_markup=kb)
        except Exception:
            logging.exception("Error in handle_lang")

    @app.on_message(filters.private & filters.command("mode"))
    async def handle_mode(client_obj, message: Message):
        try:
            if message.chat.type == ChatType.PRIVATE and not check_subscription(message.from_user.id, client_obj):
                await send_subscription_message(message.chat.id, client_obj)
                return
            current_mode = get_user_send_mode(str(message.from_user.id))
            mode_text = "📄 .txt file" if current_mode == "file" else "💬 Split messages"
            await message.reply_text(f"Result delivery mode: {mode_text}. Change it below:", reply_markup=build_result_mode_keyboard())
        except Exception:
            logging.exception("Error in handle_mode")

    @app.on_callback_query(filters.regex("^stt_lang\\|"))
    async def on_stt_language_select(client_obj, call):
        try:
            uid = str(call.from_user.id)
            _, lang_code = call.data.split("|", 1)
            lang_label = CODE_TO_LABEL.get(lang_code, lang_code)
            set_stt_user_lang(uid, lang_code)
            await call.answer(f"✅ Language set: {lang_label}")
            try:
                await call.message.delete()
            except Exception:
                pass
        except Exception:
            logging.exception("Error in on_stt_language_select")
            try:
                await call.answer("❌ Error setting language, try again.", show_alert=True)
            except Exception:
                pass

    @app.on_callback_query(filters.regex("^result_mode\\|"))
    async def on_result_mode_select(client_obj, call):
        try:
            uid = str(call.from_user.id)
            _, mode = call.data.split("|", 1)
            set_user_send_mode(uid, mode)
            mode_text = "📄 .txt file" if mode == "file" else "💬 Split messages"
            try:
                await call.message.delete()
            except Exception:
                pass
            await call.answer(f"✅ Result mode set: {mode_text}")
        except Exception:
            logging.exception("Error in on_result_mode_select")
            try:
                await call.answer("❌ Error setting result mode, try again.", show_alert=True)
            except Exception:
                pass

    @app.on_message(filters.new_chat_members)
    async def handle_new_chat_members(client_obj, message: Message):
        try:
            me = await client_obj.get_me()
            if message.new_chat_members and message.new_chat_members[0].id == me.id:
                group_data = {'_id': str(message.chat.id), 'title': message.chat.title, 'type': str(message.chat.type), 'added_date': datetime.now()}
                groups_collection.update_one({'_id': group_data['_id']}, {'$set': group_data}, upsert=True)
                await message.reply_text("Thanks for adding me! I'm ready to transcribe your media files.")
        except Exception:
            logging.exception("Error in handle_new_chat_members")

    @app.on_message(filters.left_chat_member)
    async def handle_left_chat_member(client_obj, message: Message):
        try:
            me = await client_obj.get_me()
            if message.left_chat_member and message.left_chat_member.id == me.id:
                groups_collection.delete_one({'_id': str(message.chat.id)})
        except Exception:
            logging.exception("Error in handle_left_chat_member")

    @app.on_message(filters.media & ~filters.document & (filters.voice | filters.audio | filters.video) | filters.document)
    async def handle_media_types(client_obj, message: Message):
        try:
            await handle_media_common_pyrogram(client_obj, message)
        except Exception:
            logging.exception("Error in handle_media_types")

    @app.on_message(filters.text & filters.private & ~filters.command(["start", "help", "lang", "mode"]))
    async def handle_text_messages(client_obj, message: Message):
        try:
            if message.chat.type == ChatType.PRIVATE and not check_subscription(message.from_user.id, client_obj):
                await send_subscription_message(message.chat.id, client_obj)
                return
            await message.reply_text("For Text to Audio Use: @TextToSpeechBBot")
        except Exception:
            logging.exception("Error in handle_text_messages")

    @app.on_callback_query(filters.regex("^get_key_points\\|"))
    async def get_key_points_callback(client_obj, call):
        try:
            parts = call.data.split("|")
            if len(parts) == 3:
                _, chat_id_part, msg_id_part = parts
            elif len(parts) == 2:
                _, msg_id_part = parts
                chat_id_part = str(call.message.chat.id)
            else:
                await call.answer("Invalid request", show_alert=True)
                return
            
            try:
                chat_id_val = int(chat_id_part)
                msg_id = int(msg_id_part)
            except ValueError:
                await call.answer("Invalid message id", show_alert=True)
                return
            
            usage_key = f"{chat_id_val}|{msg_id}|get_key_points"
            usage = action_usage.get(usage_key, 0)
            
            if usage >= 1:
                await call.answer("Get Summarize unavailable (maybe expired)", show_alert=True)
                return
            
            action_usage[usage_key] = usage + 1
            uid_key = str(chat_id_val)
            stored = user_transcriptions.get(uid_key, {}).get(msg_id)
            
            if not stored:
                await call.answer("Get Summarize unavailable (maybe expired)", show_alert=True)
                return
            
            await call.answer("Generating...")
            await client_obj.send_chat_action(call.message.chat.id, ChatAction.TYPING)
            status_msg = await client_obj.send_message(call.message.chat.id, "🔄 Processing...", reply_to_message_id=call.message.id)
            
            stop = {"stop": False}
            animation_task = client_obj.loop.create_task(animate_processing_message(client_obj, call.message.chat.id, status_msg.id, lambda: stop["stop"]))
            
            try:
                lang = get_stt_user_lang(str(chat_id_val)) or "en"
                instruction = f"What is this report and what is it about? Please summarize them for me into (lang={lang}) without adding any introductions, notes, or extra phrases."
                try:
                    summary = await ask_gemini_pyrogram(stored, instruction)
                except Exception:
                    summary = extract_key_points_offline(stored, max_points=6)
            except Exception:
                summary = ""
                
            stop["stop"] = True
            animation_task.cancel()
            
            if not summary:
                try:
                    await client_obj.edit_message_text(call.message.chat.id, status_msg.id, "No Summary returned.")
                except Exception:
                    pass
            else:
                try:
                    await client_obj.edit_message_text(call.message.chat.id, status_msg.id, f"{summary}")
                except Exception:
                    pass
        except Exception:
            logging.exception("Error in get_key_points_callback")

    @app.on_callback_query(filters.regex("^clean_up\\|"))
    async def clean_up_callback(client_obj, call):
        try:
            parts = call.data.split("|")
            if len(parts) == 3:
                _, chat_id_part, msg_id_part = parts
            elif len(parts) == 2:
                _, msg_id_part = parts
                chat_id_part = str(call.message.chat.id)
            else:
                await call.answer("Invalid request", show_alert=True)
                return
            
            try:
                chat_id_val = int(chat_id_part)
                msg_id = int(msg_id_part)
            except ValueError:
                await call.answer("Invalid message id", show_alert=True)
                return
            
            usage_key = f"{chat_id_val}|{msg_id}|clean_up"
            usage = action_usage.get(usage_key, 0)
            
            if usage >= 1:
                await call.answer("Clean up unavailable (maybe expired)", show_alert=True)
                return
            
            action_usage[usage_key] = usage + 1
            uid_key = str(chat_id_val)
            stored = user_transcriptions.get(uid_key, {}).get(msg_id)
            
            if not stored:
                await call.answer("Clean up unavailable (maybe expired)", show_alert=True)
                return
            
            await call.answer("Cleaning up...")
            await client_obj.send_chat_action(call.message.chat.id, ChatAction.TYPING)
            status_msg = await client_obj.send_message(call.message.chat.id, "🔄 Processing...", reply_to_message_id=call.message.id)
            
            stop = {"stop": False}
            animation_task = client_obj.loop.create_task(animate_processing_message(client_obj, call.message.chat.id, status_msg.id, lambda: stop["stop"]))
            
            try:
                lang = get_stt_user_lang(str(chat_id_val)) or "en"
                instruction = f"Clean and normalize this transcription (lang={lang}). Remove ASR artifacts like [inaudible], repeated words, filler noises, timestamps, and incorrect punctuation. Produce a clean, well-punctuated, readable text in the same language. Do not add introductions or explanations."
                try:
                    cleaned = await ask_gemini_pyrogram(stored, instruction)
                except Exception:
                    cleaned = normalize_text_offline(stored)
            except Exception:
                cleaned = ""
                
            stop["stop"] = True
            animation_task.cancel()
            
            if not cleaned:
                try:
                    await client_obj.edit_message_text(call.message.chat.id, status_msg.id, "No cleaned text returned.")
                except Exception:
                    pass
                return
                
            uid_key = str(chat_id_val)
            user_mode = get_user_send_mode(uid_key)

            if len(cleaned) > 4000:
                if user_mode == "file":
                    f = io.BytesIO(cleaned.encode("utf-8"))
                    f.name = "cleaned.txt"
                    try:
                        await client_obj.delete_messages(call.message.chat.id, status_msg.id)
                    except Exception:
                        pass
                    sent = await client_obj.send_document(call.message.chat.id, f, reply_to_message_id=call.message.id)
                    try:
                        user_transcriptions.setdefault(uid_key, {})[sent.id] = cleaned
                        threading.Thread(target=delete_transcription_later, args=(uid_key, sent.id), daemon=True).start()
                    except Exception:
                        pass
                    try:
                        action_usage[f"{call.message.chat.id}|{sent.id}|clean_up"] = 0
                        action_usage[f"{call.message.chat.id}|{sent.id}|get_key_points"] = 0
                    except Exception:
                        pass
                else:
                    try:
                        await client_obj.delete_messages(call.message.chat.id, status_msg.id)
                    except Exception:
                        pass
                    chunks = split_text_into_chunks(cleaned, limit=4096)
                    last_sent = None
                    for idx, chunk in enumerate(chunks):
                        if idx == 0:
                            last_sent = await client_obj.send_message(call.message.chat.id, chunk, reply_to_message_id=call.message.id)
                        else:
                            last_sent = await client_obj.send_message(call.message.chat.id, chunk)
                    if last_sent:
                        try:
                            user_transcriptions.setdefault(uid_key, {})[last_sent.id] = cleaned
                            threading.Thread(target=delete_transcription_later, args=(uid_key, last_sent.id), daemon=True).start()
                        except Exception:
                            pass
                        try:
                            action_usage[f"{call.message.chat.id}|{last_sent.id}|clean_up"] = 0
                            action_usage[f"{call.message.chat.id}|{last_sent.id}|get_key_points"] = 0
                        except Exception:
                            pass
            else:
                try:
                    await client_obj.edit_message_text(call.message.chat.id, status_msg.id, f"{cleaned}")
                    uid_key = str(chat_id_val)
                    user_transcriptions.setdefault(uid_key, {})[status_msg.id] = cleaned
                    threading.Thread(target=delete_transcription_later, args=(uid_key, status_msg.id), daemon=True).start()
                    action_usage[f"{call.message.chat.id}|{status_msg.id}|clean_up"] = 0
                    action_usage[f"{call.message.chat.id}|{status_msg.id}|get_key_points"] = 0
                except Exception:
                    pass
        except Exception:
            logging.exception("Error in clean_up_callback")


if __name__ == "__main__":
    if not BOT_TOKEN:
        logging.error("Startup failed: BOT_TOKEN is not configured.")
    else:
        try:
            threading.Thread(target=run_flask).start()
            if app:
                try:
                    client.admin.command('ping')
                    logging.info("Successfully connected to MongoDB!")
                except Exception as e:
                    logging.error("Could not connect to MongoDB: %s", e)
                
                # Start the Pyrogram worker tasks
                start_worker_threads_pyrogram(app)
                
                logging.info("Starting Pyrogram bot...")
                app.run()
        except Exception:
            logging.exception("Failed during startup")
