"""
═══════════════════════════════════════════════════════════════════════════
🤖 TELEGRAM MINI APP CHAT BOT — TO'LIQ ISHLAYDI
═══════════════════════════════════════════════════════════════════════════
Bitta faylda: Bot + FastAPI + WebSocket + Frontend + Ma'lumotlar bazasi
Render.com'da bepul deploy qilish uchun tayyor.
═══════════════════════════════════════════════════════════════════════════
"""

import os
import json
import logging
import asyncio
import hashlib
import hmac
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import parse_qsl
from contextlib import asynccontextmanager

# ============ AIOGRAM ============
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    WebAppInfo,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    MenuButtonWebApp,
)

# ============ FASTAPI ============
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Depends, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ============ SQLALCHEMY ============
from sqlalchemy import (
    String, Text, DateTime, BigInteger, Integer, Boolean,
    select, desc, func, or_, and_
)
from sqlalchemy.ext.asyncio import (
    create_async_engine, AsyncSession, async_sessionmaker
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# ============ LOGGING ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("chat-miniapp")

# ============ CONFIG ============
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "").rstrip("/")
WEBHOOK_PATH = "/webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "chat_miniapp_secret_2024")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./chat.db")
ADMIN_IDS = [
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
]
PORT = int(os.getenv("PORT", "8000"))

if not BOT_TOKEN:
    logger.warning("⚠️  BOT_TOKEN o'rnatilmagan! Bot ishlamaydi.")
if not WEBAPP_URL:
    logger.warning("⚠️  WEBAPP_URL o'rnatilmagan! Webhook o'rnatilmaydi.")

# ============ DATABASE ============
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    username: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    photo_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    bio: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    is_online: Mapped[bool] = mapped_column(Boolean, default=False)
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    is_banned: Mapped[bool] = mapped_column(Boolean, default=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sender_id: Mapped[int] = mapped_column(BigInteger, index=True)
    sender_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    text: Mapped[str] = mapped_column(Text)
    reply_to_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_edited: Mapped[bool] = mapped_column(Boolean, default=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    is_pinned: Mapped[bool] = mapped_column(Boolean, default=False)
    reactions: Mapped[Optional[str]] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


async def init_db():
    """Jadvallarni yaratish"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("✅ Ma'lumotlar bazasi tayyor")


async def get_session():
    """Session generator"""
    async with async_session() as session:
        yield session


# ============ WEBSOCKET MANAGER ============
class ConnectionManager:
    """WebSocket ulanishlarini boshqarish"""

    def __init__(self):
        self.active: dict[int, set[WebSocket]] = {}  # user_id -> websockets

    async def connect(self, user_id: int, ws: WebSocket):
        await ws.accept()
        if user_id not in self.active:
            self.active[user_id] = set()
        self.active[user_id].add(ws)
        logger.info(f"🔌 WebSocket ulandi: user={user_id}, jami={len(self.active[user_id])}")

    def disconnect(self, user_id: int, ws: WebSocket):
        if user_id in self.active:
            self.active[user_id].discard(ws)
            if not self.active[user_id]:
                del self.active[user_id]
        logger.info(f"🔌 WebSocket uzildi: user={user_id}")

    async def broadcast(self, message: dict):
        """Barcha aktiv foydalanuvchilarga yuborish"""
        data = json.dumps(message, default=str)
        dead = []
        for user_id, sockets in self.active.items():
            for ws in list(sockets):
                try:
                    await ws.send_text(data)
                except Exception:
                    dead.append((user_id, ws))
        for user_id, ws in dead:
            self.disconnect(user_id, ws)

    async def send_to(self, user_id: int, message: dict):
        """Bitta foydalanuvchiga yuborish"""
        if user_id not in self.active:
            return
        data = json.dumps(message, default=str)
        for ws in list(self.active[user_id]):
            try:
                await ws.send_text(data)
            except Exception:
                self.disconnect(user_id, ws)


manager = ConnectionManager()

# ============ BOT SETUP ============
bot: Optional[Bot] = None
dp = Dispatcher() if BOT_TOKEN else None


if dp is not None:
    @dp.message(CommandStart())
    async def cmd_start(message: types.Message):
        """Start buyrug'i"""
        if not WEBAPP_URL:
            await message.answer("⚠️ WebApp URL o'rnatilmagan. Administratorga murojaat qiling.")
            return

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="💬 Chatni ochish", web_app=WebAppInfo(url=WEBAPP_URL))]
            ]
        )

        await message.answer(
            f"👋 Salom, <b>{message.from_user.first_name}</b>!\n\n"
            "🚀 <b>Chat Mini App</b>ga xush kelibsiz!\n\n"
            "Quyidagi tugmani bosib chatni oching va boshqa foydalanuvchilar bilan yozishingiz mumkin.\n\n"
            "📌 <b>Buyruqlar:</b>\n"
            "/start — Botni boshlash\n"
            "/help — Yordam\n"
            "/stats — Statistika",
            reply_markup=keyboard,
            parse_mode="HTML",
        )

    @dp.message(Command("help"))
    async def cmd_help(message: types.Message):
        await message.answer(
            "📖 <b>Yordam</b>\n\n"
            "Bu bot Telegram Mini App orqali ishlaydigan chat tizimi.\n\n"
            "/start — Boshlash\n"
            "/help — Yordam\n"
            "/stats — Statistika",
            parse_mode="HTML",
        )

    @dp.message(Command("stats"))
    async def cmd_stats(message: types.Message):
        async for session in get_session():
            users_count = await session.scalar(select(func.count(User.id)))
            messages_count = await session.scalar(select(func.count(Message.id)))
            await message.answer(
                f"📊 <b>Statistika</b>\n\n"
                f"👥 Foydalanuvchilar: <b>{users_count or 0}</b>\n"
                f"💬 Xabarlar: <b>{messages_count or 0}</b>",
                parse_mode="HTML",
            )

    @dp.message(F.web_app_data)
    async def webapp_data(message: types.Message):
        await message.answer(f"📥 Ma'lumot qabul qilindi: {message.web_app_data.data[:100]}")


# ============ FASTAPI APP ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup va shutdown"""
    global bot

    await init_db()
    logger.info("🚀 Ilova ishga tushmoqda...")

    if BOT_TOKEN and WEBAPP_URL:
        bot = Bot(token=BOT_TOKEN)
        try:
            await bot.set_webhook(
                url=f"{WEBAPP_URL}{WEBHOOK_PATH}",
                secret_token=WEBHOOK_SECRET,
                drop_pending_updates=True,
                allowed_updates=dp.resolve_used_update_types() if dp else None,
            )
            logger.info(f"✅ Webhook: {WEBAPP_URL}{WEBHOOK_PATH}")

            await bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(
                    text="💬 Chat",
                    web_app=WebAppInfo(url=WEBAPP_URL),
                )
            )
            logger.info("✅ Menu button o'rnatildi")
        except Exception as e:
            logger.error(f"❌ Bot setup xatosi: {e}")

    yield

    logger.info("🛑 Ilova to'xtatilmoqda...")
    if bot:
        try:
            await bot.delete_webhook()
        except Exception:
            pass
        await bot.session.close()


app = FastAPI(lifespan=lifespan, title="Telegram Mini App Chat")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============ UTILS ============
def verify_init_data(init_data: str) -> Optional[dict]:
    """Telegram WebApp initData'ni tekshirish"""
    if not init_data or not BOT_TOKEN:
        return None

    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop("hash", None)
        if not received_hash:
            return None

        data_check_string = "\n".join(
            f"{k}={v}" for k, v in sorted(parsed.items())
        )
        secret_key = hmac.new(
            b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256
        ).digest()
        calculated_hash = hmac.new(
            secret_key, data_check_string.encode(), hashlib.sha256
        ).hexdigest()

        if calculated_hash != received_hash:
            return None

        # auth_date tekshirish (24 soatdan oshmasin)
        auth_date = int(parsed.get("auth_date", "0"))
        if auth_date and (datetime.utcnow().timestamp() - auth_date) > 86400:
            return None

        user_json = parsed.get("user")
        if user_json:
            return json.loads(user_json)
        return None
    except Exception as e:
        logger.error(f"initData xatosi: {e}")
        return None


async def get_or_create_user(session: AsyncSession, tg_user: dict) -> User:
    """Foydalanuvchini topish yoki yaratish"""
    result = await session.execute(
        select(User).where(User.telegram_id == tg_user["id"])
    )
    user = result.scalar_one_or_none()

    if not user:
        is_admin = tg_user["id"] in ADMIN_IDS
        user = User(
            telegram_id=tg_user["id"],
            username=tg_user.get("username"),
            first_name=tg_user.get("first_name"),
            last_name=tg_user.get("last_name"),
            photo_url=tg_user.get("photo_url"),
            is_admin=is_admin,
        )
        session.add(user)
    else:
        user.username = tg_user.get("username")
        user.first_name = tg_user.get("first_name")
        user.last_name = tg_user.get("last_name")
        user.photo_url = tg_user.get("photo_url")
        user.is_online = True
        user.last_seen = datetime.utcnow()

    await session.commit()
    await session.refresh(user)
    return user


# ============ PYDANTIC MODELS ============
class InitData(BaseModel):
    initData: str


class SendMessageReq(BaseModel):
    initData: str
    text: str
    reply_to_id: Optional[int] = None


class ReactReq(BaseModel):
    initData: str
    emoji: str


# ============ WEBHOOK ============
@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    """Telegram webhook"""
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return JSONResponse({"ok": False}, status_code=403)

    if not bot or not dp:
        return JSONResponse({"ok": False, "error": "Bot not configured"}, status_code=503)

    try:
        update = types.Update.model_validate(await request.json())
        await dp.feed_update(bot=bot, update=update)
    except Exception as e:
        logger.error(f"Webhook xatosi: {e}")

    return JSONResponse({"ok": True})


# ============ HTML FRONTEND ============
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="uz">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no">
<title>💬 Chat Mini App</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
:root{
  --bg:var(--tg-theme-bg-color,#fff);
  --bg2:var(--tg-theme-secondary-bg-color,#f4f4f5);
  --text:var(--tg-theme-text-color,#1a1a1a);
  --hint:var(--tg-theme-hint-color,#8e8e93);
  --btn:var(--tg-theme-button-color,#007aff);
  --btn-text:var(--tg-theme-button-text-color,#fff);
}
body{
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  background:var(--bg);color:var(--text);height:100vh;overflow:hidden;font-size:15px;
}
.app{display:flex;flex-direction:column;height:100vh;max-width:720px;margin:0 auto}
.header{
  display:flex;justify-content:space-between;align-items:center;
  padding:12px 16px;background:var(--bg2);
  border-bottom:1px solid rgba(0,0,0,.06);flex-shrink:0;
}
.header-left{display:flex;align-items:center;gap:8px}
.dot{width:8px;height:8px;border-radius:50%;background:#31c48d;box-shadow:0 0 0 3px rgba(49,196,141,.2);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.header h1{font-size:16px;font-weight:600}
.user-name{font-size:12px;color:var(--hint);background:rgba(0,0,0,.05);padding:4px 10px;border-radius:10px;max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.messages{flex:1;overflow-y:auto;padding:12px 16px;display:flex;flex-direction:column;gap:8px;scroll-behavior:smooth}
.loading{text-align:center;color:var(--hint);font-size:14px;padding:20px}
.msg{max-width:78%;padding:8px 12px;border-radius:16px;font-size:14px;line-height:1.4;word-wrap:break-word;word-break:break-word;animation:slideIn .2s ease-out;position:relative}
@keyframes slideIn{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:translateY(0)}}
.msg.own{align-self:flex-end;background:var(--btn);color:var(--btn-text);border-bottom-right-radius:4px}
.msg.other{align-self:flex-start;background:var(--bg2);color:var(--text);border-bottom-left-radius:4px}
.msg .sender{font-size:11px;font-weight:600;opacity:.75;margin-bottom:3px;color:var(--btn)}
.msg.own .sender{color:rgba(255,255,255,.9)}
.msg .time{font-size:10px;opacity:.6;margin-top:4px;text-align:right;display:flex;justify-content:flex-end;gap:4px;align-items:center}
.msg .reply-preview{font-size:11px;border-left:3px solid var(--btn);padding-left:6px;margin-bottom:4px;opacity:.75;font-style:italic}
.msg .reactions{display:flex;gap:4px;margin-top:5px;flex-wrap:wrap}
.reaction{background:rgba(0,0,0,.08);padding:2px 6px;border-radius:10px;font-size:11px;cursor:pointer;user-select:none}
.msg.own .reaction{background:rgba(255,255,255,.2)}
.reply-bar{background:var(--bg2);border-left:3px solid var(--btn);padding:6px 12px;margin:0 12px;border-radius:6px;font-size:12px;display:none;justify-content:space-between;align-items:center}
.reply-bar.active{display:flex}
.reply-bar button{background:none;border:none;color:var(--hint);font-size:16px;cursor:pointer}
.input-area{display:flex;gap:8px;padding:10px 12px;background:var(--bg2);border-top:1px solid rgba(0,0,0,.06);flex-shrink:0;padding-bottom:max(10px,env(safe-area-inset-bottom))}
.input-area input{flex:1;padding:11px 16px;border:none;border-radius:22px;background:var(--bg);color:var(--text);font-size:15px;outline:none;transition:box-shadow .2s}
.input-area input:focus{box-shadow:0 0 0 2px var(--btn)}
.input-area button{width:44px;height:44px;border:none;border-radius:50%;background:var(--btn);color:var(--btn-text);font-size:18px;cursor:pointer;display:flex;align-items:center;justify-content:center;transition:transform .1s,opacity .2s;flex-shrink:0}
.input-area button:active{transform:scale(.92);opacity:.85}
.msg-actions{position:absolute;top:-30px;right:0;background:var(--bg);border-radius:8px;box-shadow:0 2px 10px rgba(0,0,0,.15);display:none;gap:4px;padding:4px;z-index:10}
.msg-actions.active{display:flex}
.msg-actions button{background:none;border:none;font-size:14px;padding:4px 6px;cursor:pointer;border-radius:4px}
.msg-actions button:hover{background:var(--bg2)}
.messages::-webkit-scrollbar{width:4px}
.messages::-webkit-scrollbar-thumb{background:rgba(0,0,0,.15);border-radius:2px}
@media(max-width:500px){.msg{max-width:85%}}
</style>
</head>
<body>
<div class="app">
  <header class="header">
    <div class="header-left">
      <span class="dot"></span>
      <h1>💬 Umumiy Chat</h1>
    </div>
    <span id="userName" class="user-name">...</span>
  </header>
  <div id="messages" class="messages">
    <div class="loading">Xabarlar yuklanmoqda...</div>
  </div>
  <div id="replyBar" class="reply-bar">
    <span id="replyText">Javob...</span>
    <button onclick="cancelReply()">✕</button>
  </div>
  <div class="input-area">
    <input type="text" id="messageInput" placeholder="Xabar yozing..." autocomplete="off" maxlength="4000">
    <button id="sendBtn" aria-label="Yuborish">➤</button>
  </div>
</div>

<script>
const tg = window.Telegram?.WebApp;
if (tg) { tg.ready(); tg.expand(); }

let currentUserId = null;
let lastMessageId = 0;
let isSending = false;
let replyToId = null;
let ws = null;
let reconnectTimer = null;
const renderedIds = new Set();

async function api(path, opts = {}) {
  const res = await fetch(path, {
    ...opts,
    headers: { "Content-Type": "application/json", ...(opts.headers || {}) }
  });
  return res.json();
}

async function init() {
  if (!tg || !tg.initData) {
    showError("Iltimos, botni Telegram orqali oching!");
    return;
  }
  try {
    const data = await api("/api/init", {
      method: "POST",
      body: JSON.stringify({ initData: tg.initData })
    });
    if (!data.ok) { showError("Avtorizatsiya xatosi: " + (data.error || "")); return; }

    currentUserId = data.user.id;
    document.getElementById("userName").textContent =
      data.user.username ? "@" + data.user.username : (data.user.first_name || "User");

    await loadMessages(true);
    connectWebSocket();

    setInterval(() => loadMessages(false), 5000);
    setTimeout(() => document.getElementById("messageInput").focus(), 300);

    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) loadMessages(false);
    });
  } catch (e) {
    console.error(e);
    showError("Serverga ulanib bo'lmadi");
  }
}

function connectWebSocket() {
  if (!tg || !tg.initData) return;
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const url = `${proto}//${location.host}/ws?token=${encodeURIComponent(tg.initData)}`;

  try {
    ws = new WebSocket(url);
    ws.onopen = () => console.log("✅ WS ulandi");
    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (msg.type === "new_message") handleNewMessage(msg.data);
        else if (msg.type === "delete_message") removeMessage(msg.data.id);
        else if (msg.type === "edit_message") updateMessage(msg.data);
        else if (msg.type === "reaction") updateReaction(msg.data);
        else if (msg.type === "typing" && msg.data.user_id !== currentUserId) showTyping(msg.data);
      } catch (e) { console.error("WS parse xato:", e); }
    };
    ws.onclose = () => {
      console.log("❌ WS uzildi, qayta ulanmoqda...");
      clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(connectWebSocket, 3000);
    };
    ws.onerror = (e) => console.error("WS xato:", e);
  } catch (e) {
    console.error("WS ulanish xatosi:", e);
    reconnectTimer = setTimeout(connectWebSocket, 3000);
  }
}

function handleNewMessage(msg) {
  if (renderedIds.has(msg.id)) return;
  if (msg.id > lastMessageId) lastMessageId = msg.id;
  appendMessage(msg, true);
}

async function loadMessages(initial) {
  try {
    const url = initial ? "/api/messages" : `/api/messages?after_id=${lastMessageId}`;
    const data = await api(url);
    if (!data.ok) return;

    const container = document.getElementById("messages");
    if (initial) {
      container.innerHTML = "";
      renderedIds.clear();
      if (!data.messages.length) {
        container.innerHTML = '<div class="loading">Hozircha xabarlar yo\'q. Birinchi bo\'lib yozing! ✍️</div>';
        return;
      }
    }

    const wasBottom = initial ||
      (container.scrollTop + container.clientHeight >= container.scrollHeight - 100);

    const loading = container.querySelector(".loading");
    if (loading) loading.remove();

    for (const m of data.messages) {
      if (renderedIds.has(m.id)) continue;
      if (m.id > lastMessageId) lastMessageId = m.id;
      appendMessage(m, false);
    }

    if (wasBottom) container.scrollTop = container.scrollHeight;
  } catch (e) { console.error("Load xato:", e); }
}

function appendMessage(msg, scroll) {
  const c = document.getElementById("messages");
  const loading = c.querySelector(".loading");
  if (loading) loading.remove();

  const div = document.createElement("div");
  const isOwn = msg.sender_id === currentUserId;
  div.className = "msg " + (isOwn ? "own" : "other");
  div.dataset.id = msg.id;

  let html = "";
  if (msg.reply_preview) {
    html += `<div class="reply-preview">↩ ${escapeHtml(msg.reply_preview)}</div>`;
  }
  if (!isOwn) {
    html += `<div class="sender">${escapeHtml(msg.sender_name || "Anonim")}</div>`;
  }
  html += `<div class="text">${escapeHtml(msg.text)}</div>`;
  html += `<div class="time">${formatTime(msg.created_at)}${isOwn ? " ✓✓" : ""}</div>`;

  let reactions = {};
  try { reactions = JSON.parse(msg.reactions || "{}"); } catch (e) {}
  if (Object.keys(reactions).length > 0) {
    html += '<div class="reactions">';
    for (const [emoji, users] of Object.entries(reactions)) {
      html += `<span class="reaction" onclick="react(${msg.id}, '${emoji}')">${emoji} ${users.length}</span>`;
    }
    html += "</div>";
  }

  div.innerHTML = html;

  // Long press → reply/reaksiya/copy
  let pressTimer;
  div.addEventListener("touchstart", () => {
    pressTimer = setTimeout(() => showActions(div, msg), 500);
  }, { passive: true });
  div.addEventListener("touchend", () => clearTimeout(pressTimer));
  div.addEventListener("mousedown", () => {
    pressTimer = setTimeout(() => showActions(div, msg), 500);
  });
  div.addEventListener("mouseup", () => clearTimeout(pressTimer));

  c.appendChild(div);
  renderedIds.add(msg.id);

  if (scroll) c.scrollTop = c.scrollHeight;
}

function showActions(div, msg) {
  if (navigator.vibrate) navigator.vibrate(20);
  const choice = prompt(
    "Amalni tanlang:\n1 - Javob berish\n2 - Reaksiya\n3 - Nusxalash\n4 - O'chirish",
    "1"
  );
  if (choice === "1") setReply(msg);
  else if (choice === "2") {
    const emoji = prompt("Emoji kiriting:", "👍");
    if (emoji) react(msg.id, emoji);
  }
  else if (choice === "3") {
    navigator.clipboard.writeText(msg.text).then(() => alert("Nusxalandi!"));
  }
  else if (choice === "4") {
    if (msg.sender_id === currentUserId) deleteMessage(msg.id);
    else alert("Faqat o'z xabarlaringizni o'chirishingiz mumkin");
  }
}

function setReply(msg) {
  replyToId = msg.id;
  const bar = document.getElementById("replyBar");
  document.getElementById("replyText").textContent =
    (msg.sender_name || "User") + ": " + msg.text.slice(0, 40);
  bar.classList.add("active");
  document.getElementById("messageInput").focus();
}

function cancelReply() {
  replyToId = null;
  document.getElementById("replyBar").classList.remove("active");
}

async function sendMessage() {
  if (isSending) return;
  const input = document.getElementById("messageInput");
  const text = input.value.trim();
  if (!text || !currentUserId) return;

  isSending = true;
  input.value = "";
  const replyId = replyToId;
  cancelReply();

  try {
    const data = await api("/api/messages", {
      method: "POST",
      body: JSON.stringify({ initData: tg.initData, text, reply_to_id: replyId })
    });
    if (data.ok && data.message) {
      if (!renderedIds.has(data.message.id)) {
        if (data.message.id > lastMessageId) lastMessageId = data.message.id;
        appendMessage(data.message, true);
      }
      if (tg.HapticFeedback) tg.HapticFeedback.impactOccurred("light");
    } else {
      alert("Xatolik: " + (data.error || "Noma'lum"));
      input.value = text;
    }
  } catch (e) {
    console.error(e);
    alert("Server xatosi");
    input.value = text;
  } finally {
    isSending = false;
    input.focus();
  }
}

async function react(messageId, emoji) {
  try {
    await api(`/api/messages/${messageId}/react`, {
      method: "POST",
      body: JSON.stringify({ initData: tg.initData, emoji })
    });
  } catch (e) { console.error(e); }
}

async function deleteMessage(id) {
  try {
    const r = await api(`/api/messages/${id}`, {
      method: "DELETE",
      body: JSON.stringify({ initData: tg.initData })
    });
    if (!r.ok) alert(r.error || "Xatolik");
  } catch (e) { console.error(e); }
}

function removeMessage(id) {
  const el = document.querySelector(`.msg[data-id="${id}"]`);
  if (el) el.remove();
  renderedIds.delete(id);
}

function updateMessage(msg) {
  const el = document.querySelector(`.msg[data-id="${msg.id}"]`);
  if (el) {
    const t = el.querySelector(".text");
    if (t) t.textContent = msg.text + " (tahrirlangan)";
  }
}

function updateReaction(data) {
  const el = document.querySelector(`.msg[data-id="${data.id}"]`);
  if (!el) return;
  let reactions = {};
  try { reactions = JSON.parse(data.reactions || "{}"); } catch (e) {}
  let html = "";
  if (Object.keys(reactions).length > 0) {
    html = '<div class="reactions">';
    for (const [emoji, users] of Object.entries(reactions)) {
      html += `<span class="reaction" onclick="react(${data.id}, '${emoji}')">${emoji} ${users.length}</span>`;
    }
    html += "</div>";
  }
  const old = el.querySelector(".reactions");
  if (old) old.remove();
  if (html) el.insertAdjacentHTML("beforeend", html);
}

function showTyping(data) {}

function escapeHtml(text) {
  const d = document.createElement("div");
  d.textContent = text || "";
  return d.innerHTML;
}

function formatTime(iso) {
  try {
    const d = new Date(iso);
    const now = new Date();
    const isToday = d.toDateString() === now.toDateString();
    const t = d.toLocaleTimeString("uz-UZ", { hour: "2-digit", minute: "2-digit" });
    return isToday ? t : d.toLocaleDateString("uz-UZ", { day: "2-digit", month: "2-digit" }) + " " + t;
  } catch (e) { return ""; }
}

function showError(msg) {
  document.getElementById("messages").innerHTML =
    `<div class="loading" style="color:#e74c3c">❌ ${escapeHtml(msg)}</div>`;
}

document.getElementById("sendBtn").addEventListener("click", sendMessage);
document.getElementById("messageInput").addEventListener("keypress", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    sendMessage();
  }
});

window.addEventListener("beforeunload", () => {
  if (ws) ws.close();
});

init();
</script>
</body>
</html>
"""


# ============ FRONTEND ROUTE ============
@app.get("/", response_class=HTMLResponse)
async def root():
    """Mini App sahifasi"""
    return HTMLResponse(HTML_PAGE)


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.get("/ping")
async def ping():
    """UptimeRobot uchun"""
    return "pong"


# ============ API ENDPOINTS ============
@app.post("/api/init")
async def api_init(req: InitData):
    """Foydalanuvchini tekshirish va ro'yxatdan o'tkazish"""
    tg_user = verify_init_data(req.initData)
    if not tg_user:
        return JSONResponse({"ok": False, "error": "initData yaroqsiz"}, status_code=401)

    async for session in get_session():
        user = await get_or_create_user(session, tg_user)
        if user.is_banned:
            return JSONResponse({"ok": False, "error": "Siz banlangansiz"}, status_code=403)
        return {
            "ok": True,
            "user": {
                "id": user.telegram_id,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "username": user.username,
                "photo_url": user.photo_url,
                "is_admin": user.is_admin,
            },
        }


@app.get("/api/messages")
async def api_get_messages(after_id: int = 0):
    """Xabarlarni olish"""
    async for session in get_session():
        if after_id > 0:
            result = await session.execute(
                select(Message)
                .where(and_(Message.id > after_id, Message.is_deleted == False))
                .order_by(Message.id.asc())
                .limit(200)
            )
        else:
            result = await session.execute(
                select(Message)
                .where(Message.is_deleted == False)
                .order_by(desc(Message.id))
                .limit(100)
            )

        messages = list(result.scalars().all())
        if after_id == 0:
            messages.reverse()

        # Reply previewlar uchun
        reply_ids = [m.reply_to_id for m in messages if m.reply_to_id]
        replies_map = {}
        if reply_ids:
            r = await session.execute(select(Message).where(Message.id.in_(reply_ids)))
            replies_map = {mm.id: mm for mm in r.scalars().all()}

        return {
            "ok": True,
            "messages": [
                {
                    "id": m.id,
                    "sender_id": m.sender_id,
                    "sender_name": m.sender_name or "Anonim",
                    "text": m.text,
                    "reply_to_id": m.reply_to_id,
                    "reply_preview": (
                        (replies_map[m.reply_to_id].text[:60] + "...")
                        if m.reply_to_id and m.reply_to_id in replies_map
                        and len(replies_map[m.reply_to_id].text) > 60
                        else (replies_map[m.reply_to_id].text
                              if m.reply_to_id in replies_map else None)
                    ),
                    "reactions": m.reactions or "{}",
                    "is_edited": m.is_edited,
                    "created_at": m.created_at.isoformat(),
                }
                for m in messages
            ],
        }


@app.post("/api/messages")
async def api_send_message(req: SendMessageReq):
    """Yangi xabar yuborish"""
    tg_user = verify_init_data(req.initData)
    if not tg_user:
        return JSONResponse({"ok": False, "error": "Avtorizatsiya xatosi"}, status_code=401)

    text = (req.text or "").strip()
    if not text:
        return JSONResponse({"ok": False, "error": "Bo'sh xabar"}, status_code=400)
    if len(text) > 4000:
        return JSONResponse({"ok": False, "error": "Xabar juda uzun"}, status_code=400)

    async for session in get_session():
        user = await get_or_create_user(session, tg_user)
        if user.is_banned:
            return JSONResponse({"ok": False, "error": "Siz banlangansiz"}, status_code=403)

        sender_name = user.first_name or user.username or "Anonim"

        reply_preview = None
        if req.reply_to_id:
            r = await session.execute(
                select(Message).where(Message.id == req.reply_to_id)
            )
            parent = r.scalar_one_or_none()
            if parent:
                reply_preview = parent.text[:60] + ("..." if len(parent.text) > 60 else "")

        msg = Message(
            sender_id=user.telegram_id,
            sender_name=sender_name,
            text=text,
            reply_to_id=req.reply_to_id,
        )
        session.add(msg)
        await session.commit()
        await session.refresh(msg)

        payload = {
            "id": msg.id,
            "sender_id": msg.sender_id,
            "sender_name": msg.sender_name,
            "text": msg.text,
            "reply_to_id": msg.reply_to_id,
            "reply_preview": reply_preview,
            "reactions": "{}",
            "is_edited": False,
            "created_at": msg.created_at.isoformat(),
        }

        await manager.broadcast({"type": "new_message", "data": payload})
        return {"ok": True, "message": payload}


@app.delete("/api/messages/{message_id}")
async def api_delete_message(message_id: int, req: InitData):
    """Xabarni o'chirish (faqat o'ziniki)"""
    tg_user = verify_init_data(req.initData)
    if not tg_user:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)

    async for session in get_session():
        r = await session.execute(select(Message).where(Message.id == message_id))
        msg = r.scalar_one_or_none()
        if not msg:
            return JSONResponse({"ok": False, "error": "Xabar topilmadi"}, status_code=404)
        if msg.sender_id != tg_user["id"]:
            return JSONResponse({"ok": False, "error": "Ruxsat yo'q"}, status_code=403)

        msg.is_deleted = True
        await session.commit()
        await manager.broadcast({"type": "delete_message", "data": {"id": message_id}})
        return {"ok": True}


@app.post("/api/messages/{message_id}/react")
async def api_react(message_id: int, req: ReactReq):
    """Reaksiya qo'shish/o'chirish"""
    tg_user = verify_init_data(req.initData)
    if not tg_user:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)

    emoji = req.emoji.strip()[:8]
    if not emoji:
        return JSONResponse({"ok": False, "error": "Emoji kerak"}, status_code=400)

    async for session in get_session():
        r = await session.execute(select(Message).where(Message.id == message_id))
        msg = r.scalar_one_or_none()
        if not msg:
            return JSONResponse({"ok": False, "error": "Topilmadi"}, status_code=404)

        try:
            reactions = json.loads(msg.reactions or "{}")
        except Exception:
            reactions = {}

        users = reactions.get(emoji, [])
        uid = tg_user["id"]
        if uid in users:
            users.remove(uid)
            if not users:
                del reactions[emoji]
        else:
            users.append(uid)
            reactions[emoji] = users

        msg.reactions = json.dumps(reactions)
        await session.commit()

        await manager.broadcast({
            "type": "reaction",
            "data": {"id": message_id, "reactions": msg.reactions}
        })
        return {"ok": True, "reactions": reactions}


@app.get("/api/users/online")
async def api_online():
    """Online foydalanuvchilar"""
    return {"ok": True, "count": len(manager.active), "users": list(manager.active.keys())}


@app.get("/api/stats")
async def api_stats():
    """Statistika"""
    async for session in get_session():
        u = await session.scalar(select(func.count(User.id)))
        m = await session.scalar(select(func.count(Message.id)))
        return {"ok": True, "users": u or 0, "messages": m or 0, "online": len(manager.active)}


# ============ WEBSOCKET ============
@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket, token: str = ""):
    """WebSocket ulanish"""
    tg_user = verify_init_data(token)
    if not tg_user:
        await websocket.close(code=4001)
        return

    user_id = tg_user["id"]

    async for session in get_session():
        r = await session.execute(select(User).where(User.telegram_id == user_id))
        user = r.scalar_one_or_none()
        if not user or user.is_banned:
            await websocket.close(code=4003)
            return
        user.is_online = True
        user.last_seen = datetime.utcnow()
        await session.commit()

    await manager.connect(user_id, websocket)
    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                try:
                    payload = json.loads(data)
                    if payload.get("type") == "ping":
                        await websocket.send_text(json.dumps({"type": "pong"}))
                except Exception:
                    pass
            except asyncio.TimeoutError:
                # Heartbeat
                try:
                    await websocket.send_text(json.dumps({"type": "ping"}))
                except Exception:
                    break
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WS xatosi: {e}")
    finally:
        manager.disconnect(user_id, websocket)
        async for session in get_session():
            r = await session.execute(select(User).where(User.telegram_id == user_id))
            u = r.scalar_one_or_none()
            if u:
                u.is_online = False
                u.last_seen = datetime.utcnow()
                await session.commit()


# ============ STARTUP ============
if __name__ == "__main__":
    import uvicorn
    logger.info(f"🚀 Uvicorn ishga tushmoqda: http://0.0.0.0:{PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
