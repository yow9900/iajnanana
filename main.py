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
from collections import Counter, deque
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ChatAction

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_ID = 29169428
API_HASH = "55742b16a85aac494c7944568b5507e5"
env = os.environ
BOT_TOKEN = env.get("BOT_TOKEN", "8303813448:AAEy5txrGzcK8o_0AhX-40YudvdEa0hpgNY")
TELEGRAM_MAX_BYTES = int(env.get("TELEGRAM_MAX_BYTES", str(50 * 1024 * 1024)))
REQUEST_TIMEOUT_TELEGRAM = int(env.get("REQUEST_TIMEOUT_TELEGRAM", "300"))
REQUEST_TIMEOUT_GEMINI = int(env.get("REQUEST_TIMEOUT_GEMINI", "300"))
MAX_CONCURRENT_TRANSCRIPTS = int(env.get("MAX_CONCURRENT_TRANSCRIPTS", "2"))
MAX_PENDING_QUEUE = int(env.get("MAX_PENDING_QUEUE", "2"))
GEMINI_API_KEYS = [t.strip() for t in env.get("GEMINI_API_KEYS", env.get("GEMINI_API_KEY", "")).split(",") if t.strip()]
MONGO_URI = env.get("MONGO_URI", "")
DB_NAME = env.get("DB_NAME", "telegram_bot_db")
REQUIRED_CHANNEL = env.get("REQUIRED_CHANNEL", "")
ASSEMBLYAI_API_KEYS = [t.strip() for t in env.get("ASSEMBLYAI_API_KEYS", env.get("ASSEMBLYAI_API_KEY", "")).split(",") if t.strip()]
ASSEMBLYAI_BASE_URL = "https://api.assemblyai.com/v2"

client_db = MongoClient(MONGO_URI) if MONGO_URI else MongoClient()
db = client_db[DB_NAME]
users_collection = db["users"]
groups_collection = db["groups"]

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

def check_subscription(user_id):
    if not REQUIRED_CHANNEL:
        return True
    try:
        member = app.get_chat_member(REQUIRED_CHANNEL, user_id)
        return member.status in ["member", "administrator", "creator"]
    except:
        return False

def send_subscription_message(chat_id):
    if not REQUIRED_CHANNEL:
        return
    try:
        chat = app.get_chat(chat_id)
        if chat.type != "private":
            return
    except:
        return
    try:
        m = InlineKeyboardMarkup([[InlineKeyboardButton("Click here to join the Group", url=f"https://t.me/{REQUIRED_CHANNEL.lstrip('@')}")]])
        app.send_message(chat_id, "🔒 Access Locked You cannot use this bot until you join the Group.", reply_markup=m)
    except:
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

def save_pending_media(user_id, media_type, data):
    with memory_lock:
        in_memory_data["pending_media"][user_id] = {"media_type": media_type, "data": data, "saved_at": datetime.now()}

def pop_pending_media(user_id):
    with memory_lock:
        return in_memory_data["pending_media"].pop(user_id, None)

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
    buttons = []
    for label, code in LANG_OPTIONS:
        data = f"{callback_prefix}|{code}"
        if message_id:
            data = f"{callback_prefix}|{code}|{message_id}"
        buttons.append(InlineKeyboardButton(label, callback_data=data))
    rows = [buttons[i:i+row_width] for i in range(0, len(buttons), row_width)]
    return InlineKeyboardMarkup(rows)

def build_result_mode_keyboard(prefix="result_mode"):
    return InlineKeyboardMarkup([[InlineKeyboardButton("📄 .txt file", callback_data=f"{prefix}|file"), InlineKeyboardButton("💬 Split messages", callback_data=f"{prefix}|split")]])

def animate_processing_message(chat_id, message_id, stop_event):
    frames = ["🔄 Processing", "🔄 Processing.", "🔄 Processing..", "🔄 Processing..."]
    idx = 0
    while not stop_event():
        try:
            app.edit_message_text(chat_id, message_id, frames[idx % len(frames)])
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
    return type("T", (), {"file_path": file_path})(), f"https://api.telegram.org/file/bot{bot_token}/{file_path}"

def upload_file_to_assemblyai(path):
    if not ASSEMBLYAI_API_KEYS:
        raise RuntimeError("ASSEMBLYAI_API_KEYS not set")
    last_exception = None
    for api_key in ASSEMBLYAI_API_KEYS:
        try:
            headers = {"authorization": api_key}
            with open(path, "rb") as f:
                resp = requests.post(f"{ASSEMBLYAI_BASE_URL}/upload", headers=headers, data=f, timeout=REQUEST_TIMEOUT_GEMINI)
            resp.raise_for_status()
            upload_url = resp.json().get("upload_url")
            if not upload_url:
                raise RuntimeError("Upload failed: no upload_url returned")
            return upload_url, api_key
        except Exception as e:
            last_exception = e
            continue
    raise RuntimeError(f"All AssemblyAI upload attempts failed. Last error: {last_exception}")

def transcribe_file_with_assemblyai_local(path, language_code):
    upload_url, api_key = upload_file_to_assemblyai(path)
    headers = {"Authorization": api_key, "Content-Type": "application/json"}
    config = {"audio_url": upload_url}
    if language_code and language_code != "en":
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

def transcribe_via_selected_service(input_path, lang_code):
    try:
        text = transcribe_file_with_assemblyai_local(input_path, lang_code)
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

def attach_action_buttons(chat_id, message_id, text):
    try:
        include_summarize = len(text) > 1000 if text else False
        buttons = [[InlineKeyboardButton("⭐️Clean transcript", callback_data=f"clean_up|{chat_id}|{message_id}")]]
        if include_summarize:
            buttons.append([InlineKeyboardButton("Get Summarize", callback_data=f"get_key_points|{chat_id}|{message_id}")])
        app.edit_message_reply_markup(chat_id, message_id, reply_markup=InlineKeyboardMarkup(buttons))
    except:
        pass
    try:
        action_usage[f"{chat_id}|{message_id}|clean_up"] = 0
        action_usage[f"{chat_id}|{message_id}|get_key_points"] = 0
    except:
        pass

def process_media_file(message, path, filename):
    uid = str(message.from_user.id)
    chatid = message.chat.id
    lang = get_stt_user_lang(uid)
    processing_msg = app.send_message(chatid, "🔄 Processing...", reply_to_message_id=message.message_id)
    processing_msg_id = processing_msg.message_id
    stop = {"stop": False}
    animation_thread = threading.Thread(target=animate_processing_message, args=(chatid, processing_msg_id, lambda: stop["stop"]))
    animation_thread.start()
    try:
        try:
            text, used_service = transcribe_via_selected_service(path, lang)
        except Exception as e:
            error_msg = str(e)
            logging.exception("Error during transcription")
            if is_transcoding_like_error(error_msg):
                app.send_message(chatid, "⚠️ Transcription error: file is not audible. Please send a different file.", reply_to_message_id=message.message_id)
            else:
                app.send_message(chatid, f"Error during transcription: {error_msg}", reply_to_message_id=message.message_id)
            return
        corrected_text = normalize_text_offline(text)
        uid_key = str(chatid)
        user_mode = get_user_send_mode(uid_key)
        if len(corrected_text) > 4000:
            if user_mode == "file":
                f = io.BytesIO(corrected_text.encode("utf-8"))
                f.name = "Transcript.txt"
                sent = app.send_document(chatid, f, reply_to_message_id=message.message_id)
                try:
                    attach_action_buttons(chatid, sent.message_id, corrected_text)
                except:
                    pass
                try:
                    user_transcriptions.setdefault(uid_key, {})[sent.message_id] = corrected_text
                    threading.Thread(target=delete_transcription_later, args=(uid_key, sent.message_id), daemon=True).start()
                except:
                    pass
            else:
                chunks = split_text_into_chunks(corrected_text, limit=4096)
                last_sent = None
                for idx, chunk in enumerate(chunks):
                    if idx == 0:
                        last_sent = app.send_message(chatid, chunk, reply_to_message_id=message.message_id)
                    else:
                        last_sent = app.send_message(chatid, chunk)
                try:
                    attach_action_buttons(chatid, last_sent.message_id, corrected_text)
                except:
                    pass
                try:
                    user_transcriptions.setdefault(uid_key, {})[last_sent.message_id] = corrected_text
                    threading.Thread(target=delete_transcription_later, args=(uid_key, last_sent.message_id), daemon=True).start()
                except:
                    pass
        else:
            sent_msg = app.send_message(chatid, corrected_text or "⚠️ Warning Make sure the voice is clear or speaking in the language you Choosed.", reply_to_message_id=message.message_id)
            try:
                attach_action_buttons(chatid, sent_msg.message_id, corrected_text)
            except:
                pass
            try:
                user_transcriptions.setdefault(uid_key, {})[sent_msg.message_id] = corrected_text
                threading.Thread(target=delete_transcription_later, args=(uid_key, sent_msg.message_id), daemon=True).start()
            except:
                pass
        increment_processing_count(uid, "stt")
    finally:
        stop["stop"] = True
        animation_thread.join()
        try:
            app.delete_messages(chatid, processing_msg_id)
        except:
            pass
        try:
            os.remove(path)
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
                message, path, filename = item
                logging.info(f"Starting processing for user {message.from_user.id} (Chat {message.chat.id}) from queue. Current queue size: {len(PENDING_QUEUE)}")
                process_media_file(message, path, filename)
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

def handle_media_common(message):
    update_user_activity(message.from_user.id)
    if message.chat.type == "private" and not check_subscription(message.from_user.id):
        send_subscription_message(message.chat.id)
        return
    file_size = None
    filename = None
    if message.voice:
        file_size = message.voice.file_size
        filename = "voice.ogg"
    elif message.audio:
        file_size = message.audio.file_size
        filename = getattr(message.audio, "file_name", "audio")
    elif message.video:
        file_size = message.video.file_size
        filename = getattr(message.video, "file_name", "video.mp4")
    elif message.document:
        mime = getattr(message.document, "mime_type", None)
        filename = getattr(message.document, "file_name", None) or "file"
        ext = safe_extension_from_filename(filename)
        if (mime and ("audio" in mime or "video" in mime)) or ext in ALLOWED_EXTENSIONS:
            file_size = message.document.file_size
        else:
            app.send_message(message.chat.id, "Sorry, I can only transcribe audio or video files.")
            return
    if file_size and file_size > TELEGRAM_MAX_BYTES:
        max_display_mb = TELEGRAM_MAX_BYTES // (1024 * 1024)
        app.send_message(message.chat.id, f"Just Send me a file less than {max_display_mb}MB 😎", reply_to_message_id=message.message_id)
        return
    with memory_lock:
        if len(PENDING_QUEUE) >= MAX_PENDING_QUEUE:
            app.send_message(message.chat.id, "⚠️ Server busy. Try again later.", reply_to_message_id=message.message_id)
            return
    try:
        path = message.download()
    except Exception as e:
        app.send_message(message.chat.id, f"Download failed: {str(e)}", reply_to_message_id=message.message_id)
        return
    with memory_lock:
        PENDING_QUEUE.append((message, path, filename))

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
                    return result["candidates"][0]["content"]["parts"][0]["text"]
                except:
                    return json.dumps(result["candidates"][0])
            raise RuntimeError(f"Gemini response lacks candidates: {json.dumps(result)}")
        except Exception as e:
            last_exception = e
            continue
    raise RuntimeError(f"All Gemini API keys failed. Last error: {str(last_exception) if last_exception else 'No keys were available.'}")

app = Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

@app.on_message(filters.command("start") & filters.private)
def start_handler(client, message):
    try:
        update_user_activity(message.from_user.id)
        if message.chat.type == "private" and not check_subscription(message.from_user.id):
            send_subscription_message(message.chat.id)
            return
        message.reply_text("Choose your file language for transcription using the below buttons:", reply_markup=build_lang_keyboard("start_select_lang"))
    except:
        logging.exception("Error in start_handler")

@app.on_callback_query(filters.create(lambda _, __, q: q.data and q.data.startswith("start_select_lang|")))
def start_select_lang_callback(client, callback_query):
    try:
        uid = str(callback_query.from_user.id)
        _, lang_code = callback_query.data.split("|", 1)
        lang_label = CODE_TO_LABEL.get(lang_code, lang_code)
        set_stt_user_lang(uid, lang_code)
        try:
            app.delete_messages(callback_query.message.chat.id, callback_query.message.message_id)
        except:
            pass
        welcome_text = "👋 Salaam!    \n• Send me\n• voice message\n• audio file\n• video\n• to transcribe for free"
        app.send_message(callback_query.message.chat.id, welcome_text)
        callback_query.answer(f"✅ Language set to {lang_label}")
    except:
        logging.exception("Error in start_select_lang_callback")
        try:
            callback_query.answer("❌ Error setting language, try again.", show_alert=True)
        except:
            pass

@app.on_message(filters.command("help"))
def handle_help(client, message):
    try:
        update_user_activity(message.from_user.id)
        if message.chat.type == "private" and not check_subscription(message.from_user.id):
            send_subscription_message(message.chat.id)
            return
        text = "Commands supported:\n/start - Show welcome message\n/lang  - Change language\n/mode  - Change result delivery mode\n/help  - This help message\n\nSend a voice/audio/video and I will transcribe it Need help? Contact: @lakigithub"
        app.send_message(message.chat.id, text)
    except:
        logging.exception("Error in handle_help")

@app.on_message(filters.command("lang"))
def handle_lang(client, message):
    try:
        if message.chat.type == "private" and not check_subscription(message.from_user.id):
            send_subscription_message(message.chat.id)
            return
        kb = build_lang_keyboard("stt_lang")
        app.send_message(message.chat.id, "Choose your file language for transcription using the below buttons:", reply_markup=kb)
    except:
        logging.exception("Error in handle_lang")

@app.on_message(filters.command("mode"))
def handle_mode(client, message):
    try:
        if message.chat.type == "private" and not check_subscription(message.from_user.id):
            send_subscription_message(message.chat.id)
            return
        current_mode = get_user_send_mode(str(message.from_user.id))
        mode_text = "📄 .txt file" if current_mode == "file" else "💬 Split messages"
        app.send_message(message.chat.id, f"Result delivery mode: {mode_text}. Change it below:", reply_markup=build_result_mode_keyboard())
    except:
        logging.exception("Error in handle_mode")

@app.on_callback_query(filters.create(lambda _, __, q: q.data and q.data.startswith("stt_lang|")))
def on_stt_language_select(client, callback_query):
    try:
        uid = str(callback_query.from_user.id)
        _, lang_code = callback_query.data.split("|", 1)
        lang_label = CODE_TO_LABEL.get(lang_code, lang_code)
        set_stt_user_lang(uid, lang_code)
        callback_query.answer(f"✅ Language set: {lang_label}")
        try:
            app.delete_messages(callback_query.message.chat.id, callback_query.message.message_id)
        except:
            pass
    except:
        logging.exception("Error in on_stt_language_select")
        try:
            callback_query.answer("❌ Error setting language, try again.", show_alert=True)
        except:
            pass

@app.on_callback_query(filters.create(lambda _, __, q: q.data and q.data.startswith("result_mode|")))
def on_result_mode_select(client, callback_query):
    try:
        uid = str(callback_query.from_user.id)
        _, mode = callback_query.data.split("|", 1)
        set_user_send_mode(uid, mode)
        mode_text = "📄 .txt file" if mode == "file" else "💬 Split messages"
        try:
            app.delete_messages(callback_query.message.chat.id, callback_query.message.message_id)
        except:
            pass
        callback_query.answer(f"✅ Result mode set: {mode_text}")
    except:
        logging.exception("Error in on_result_mode_select")
        try:
            callback_query.answer("❌ Error setting result mode, try again.", show_alert=True)
        except:
            pass

@app.on_message(filters.new_chat_members)
def handle_new_chat_members(client, message):
    try:
        if message.new_chat_members[0].id == app.get_me().id:
            group_data = {"_id": str(message.chat.id), "title": message.chat.title, "type": message.chat.type, "added_date": datetime.now()}
            groups_collection.update_one({"_id": group_data["_id"]}, {"$set": group_data}, upsert=True)
            app.send_message(message.chat.id, "Thanks for adding me! I'm ready to transcribe your media files.")
    except:
        logging.exception("Error in handle_new_chat_members")

@app.on_message(filters.left_chat_member)
def handle_left_chat_member(client, message):
    try:
        if message.left_chat_member.id == app.get_me().id:
            groups_collection.delete_one({"_id": str(message.chat.id)})
    except:
        logging.exception("Error in handle_left_chat_member")

@app.on_message(filters.voice | filters.audio | filters.video | filters.document)
def handle_media_types(client, message):
    try:
        handle_media_common(message)
    except:
        logging.exception("Error in handle_media_types")

@app.on_message(filters.text)
def handle_text_messages(client, message):
    try:
        if message.chat.type == "private" and not check_subscription(message.from_user.id):
            send_subscription_message(message.chat.id)
            return
        app.send_message(message.chat.id, "For Text to Audio Use: @TextToSpeechBBot")
    except:
        logging.exception("Error in handle_text_messages")

@app.on_callback_query(filters.create(lambda _, __, q: q.data and q.data.startswith("get_key_points|")))
def get_key_points_callback(client, callback_query):
    try:
        parts = callback_query.data.split("|")
        if len(parts) == 3:
            _, chat_id_part, msg_id_part = parts
        elif len(parts) == 2:
            _, msg_id_part = parts
            chat_id_part = str(callback_query.message.chat.id)
        else:
            callback_query.answer("Invalid request", show_alert=True)
            return
        try:
            chat_id_val = int(chat_id_part)
            msg_id = int(msg_id_part)
        except:
            callback_query.answer("Invalid message id", show_alert=True)
            return
        usage_key = f"{chat_id_val}|{msg_id}|get_key_points"
        usage = action_usage.get(usage_key, 0)
        if usage >= 1:
            callback_query.answer("Get Summarize unavailable (maybe expired)", show_alert=True)
            return
        action_usage[usage_key] = usage + 1
        uid_key = str(chat_id_val)
        stored = user_transcriptions.get(uid_key, {}).get(msg_id)
        if not stored:
            callback_query.answer("Get Summarize unavailable (maybe expired)", show_alert=True)
            return
        callback_query.answer("Generating...")
        status_msg = app.send_message(callback_query.message.chat.id, "🔄 Processing...", reply_to_message_id=callback_query.message.message_id)
        stop = {"stop": False}
        animation_thread = threading.Thread(target=animate_processing_message, args=(callback_query.message.chat.id, status_msg.message_id, lambda: stop["stop"]))
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
        animation_thread.join()
        if not summary:
            try:
                app.edit_message_text(callback_query.message.chat.id, status_msg.message_id, "No Summary returned.")
            except:
                pass
        else:
            try:
                app.edit_message_text(callback_query.message.chat.id, status_msg.message_id, f"{summary}")
            except:
                pass
    except:
        logging.exception("Error in get_key_points_callback")

@app.on_callback_query(filters.create(lambda _, __, q: q.data and q.data.startswith("clean_up|")))
def clean_up_callback(client, callback_query):
    try:
        parts = callback_query.data.split("|")
        if len(parts) == 3:
            _, chat_id_part, msg_id_part = parts
        elif len(parts) == 2:
            _, msg_id_part = parts
            chat_id_part = str(callback_query.message.chat.id)
        else:
            callback_query.answer("Invalid request", show_alert=True)
            return
        try:
            chat_id_val = int(chat_id_part)
            msg_id = int(msg_id_part)
        except:
            callback_query.answer("Invalid message id", show_alert=True)
            return
        usage_key = f"{chat_id_val}|{msg_id}|clean_up"
        usage = action_usage.get(usage_key, 0)
        if usage >= 1:
            callback_query.answer("Clean up unavailable (maybe expired)", show_alert=True)
            return
        action_usage[usage_key] = usage + 1
        uid_key = str(chat_id_val)
        stored = user_transcriptions.get(uid_key, {}).get(msg_id)
        if not stored:
            callback_query.answer("Clean up unavailable (maybe expired)", show_alert=True)
            return
        callback_query.answer("Cleaning up...")
        status_msg = app.send_message(callback_query.message.chat.id, "🔄 Processing...", reply_to_message_id=callback_query.message.message_id)
        stop = {"stop": False}
        animation_thread = threading.Thread(target=animate_processing_message, args=(callback_query.message.chat.id, status_msg.message_id, lambda: stop["stop"]))
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
        animation_thread.join()
        if not cleaned:
            try:
                app.edit_message_text(callback_query.message.chat.id, status_msg.message_id, "No cleaned text returned.")
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
                    app.delete_messages(callback_query.message.chat.id, status_msg.message_id)
                except:
                    pass
                sent = app.send_document(callback_query.message.chat.id, f, reply_to_message_id=callback_query.message.message_id)
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
                    app.delete_messages(callback_query.message.chat.id, status_msg.message_id)
                except:
                    pass
                chunks = split_text_into_chunks(cleaned, limit=4096)
                last_sent = None
                for idx, chunk in enumerate(chunks):
                    if idx == 0:
                        last_sent = app.send_message(callback_query.message.chat.id, chunk, reply_to_message_id=callback_query.message.message_id)
                    else:
                        last_sent = app.send_message(callback_query.message.chat.id, chunk)
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
                app.edit_message_text(callback_query.message.chat.id, status_msg.message_id, f"{cleaned}")
                uid_key = str(chat_id_val)
                user_transcriptions.setdefault(uid_key, {})[status_msg.message_id] = cleaned
                threading.Thread(target=delete_transcription_later, args=(uid_key, status_msg.message_id), daemon=True).start()
                action_usage[f"{callback_query.message.chat.id}|{status_msg.message_id}|clean_up"] = 0
                action_usage[f"{callback_query.message.chat.id}|{status_msg.message_id}|get_key_points"] = 0
            except:
                pass

if __name__ == "__main__":
    try:
        client_db.admin.command("ping")
        logging.info("Successfully connected to MongoDB!")
    except Exception as e:
        logging.error("Could not connect to MongoDB: %s", e)
    app.run()
