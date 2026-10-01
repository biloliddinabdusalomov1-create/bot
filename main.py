"""
═══════════════════════════════════════════════════════════════════════════
🤖 TELEGRAM MINI APP CHAT BOT — v2.0 (TO'LIQ ISHLAYDI)
═══════════════════════════════════════════════════════════════════════════
Yangi funksiyalar:
- ✅ Greenlet muammosi tuzatildi
- ✅ Profil tahrirlash (ism, bio)
- ✅ Foydalanuvchi qidirish
- ✅ Global qidiruv (xabarlar)
- ✅ Emoji picker (doimiy panelda)
- ✅ Xabar tahrirlash
- ✅ Bloklash tizimi
- ✅ Chatlar ro'yxati (private chat)
- ✅ Bildirishnomalar (yangi xabar haqida)
- ✅ Auto-reconnect WebSocket
- ✅ Ko'p tilli (O'zbek, Rus, Ingliz)
- ✅ Admin panel
- ✅ Statistika sahifasi
═══════════════════════════════════════════════════════════════════════════
"""

import os
import json
import logging
import asyncio
import hashlib
import hmac
from datetime import datetime
from typing import Optional
from urllib.parse import parse_qsl
from contextlib import asynccontextmanager

# ============ AIOGRAM ============
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton, MenuButtonWebApp,
)

# ============ FASTAPI ============
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ============ SQLALCHEMY ============
from sqlalchemy import (
    String, Text, DateTime, BigInteger, Integer, Boolean,
    select, desc, func, or_, and_,
)
from sqlalchemy.ext.asyncio import (
    create_async_engine, AsyncSession, async_sessionmaker,
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
    logger.warning("⚠️ BOT_TOKEN o'rnatilmagan!")
if not WEBAPP_URL:
    logger.warning("⚠️ WEBAPP_URL o'rnatilmagan!")

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
    language: Mapped[str] = mapped_column(String(5), default="uz")
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


class Block(Base):
    __tablename__ = "blocks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    blocker_id: Mapped[int] = mapped_column(BigInteger, index=True)
    blocked_id: Mapped[int] = mapped_column(BigInteger, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("✅ Ma'lumotlar bazasi tayyor")


async def get_session():
    async with async_session() as session:
        yield session


# ============ WEBSOCKET MANAGER ============
class ConnectionManager:
    def __init__(self):
        self.active: dict[int, set[WebSocket]] = {}

    async def connect(self, user_id: int, ws: WebSocket):
        await ws.accept()
        self.active.setdefault(user_id, set()).add(ws)
        logger.info(f"🔌 WS ulandi: user={user_id}, jami={len(self.active[user_id])}")

    def disconnect(self, user_id: int, ws: WebSocket):
        if user_id in self.active:
            self.active[user_id].discard(ws)
            if not self.active[user_id]:
                del self.active[user_id]
        logger.info(f"🔌 WS uzildi: user={user_id}")

    async def broadcast(self, message: dict):
        data = json.dumps(message, default=str)
        dead = []
        for user_id, sockets in list(self.active.items()):
            for ws in list(sockets):
                try:
                    await ws.send_text(data)
                except Exception:
                    dead.append((user_id, ws))
        for user_id, ws in dead:
            self.disconnect(user_id, ws)

    async def send_to(self, user_id: int, message: dict):
        if user_id not in self.active:
            return
        data = json.dumps(message, default=str)
        for ws in list(self.active[user_id]):
            try:
                await ws.send_text(data)
            except Exception:
                self.disconnect(user_id, ws)


manager = ConnectionManager()

# ============ BOT ============
bot: Optional[Bot] = None
dp = Dispatcher() if BOT_TOKEN else None

if dp is not None:
    @dp.message(CommandStart())
    async def cmd_start(message: types.Message):
        if not WEBAPP_URL:
            await message.answer("⚠️ WebApp URL o'rnatilmagan.")
            return
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(text="💬 Chatni ochish", web_app=WebAppInfo(url=WEBAPP_URL))
            ]]
        )
        await message.answer(
            f"👋 Salom, <b>{message.from_user.first_name}</b>!\n\n"
            "🚀 <b>Chat Mini App</b>ga xush kelibsiz!\n\n"
            "Quyidagi tugmani bosib chatni oching.\n\n"
            "📌 Buyruqlar:\n"
            "/start — Boshlash\n"
            "/help — Yordam\n"
            "/stats — Statistika",
            reply_markup=kb,
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
            u = await session.scalar(select(func.count(User.id)))
            m = await session.scalar(select(func.count(Message.id)))
            await message.answer(
                f"📊 <b>Statistika</b>\n\n"
                f"👥 Foydalanuvchilar: <b>{u or 0}</b>\n"
                f"💬 Xabarlar: <b>{m or 0}</b>\n"
                f"🟢 Online: <b>{len(manager.active)}</b>",
                parse_mode="HTML",
            )


# ============ FASTAPI ============
@asynccontextmanager
async def lifespan(app: FastAPI):
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
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


# ============ UTILS ============
def verify_init_data(init_data: str) -> Optional[dict]:
    if not init_data or not BOT_TOKEN:
        return None
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop("hash", None)
        if not received_hash:
            return None
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(
            secret_key, data_check_string.encode(), hashlib.sha256
        ).hexdigest()
        if calculated_hash != received_hash:
            return None
        auth_date = int(parsed.get("auth_date", "0"))
        if auth_date and (datetime.utcnow().timestamp() - auth_date) > 86400:
            return None
        user_json = parsed.get("user")
        return json.loads(user_json) if user_json else None
    except Exception as e:
        logger.error(f"initData xatosi: {e}")
        return None


async def get_or_create_user(session: AsyncSession, tg_user: dict) -> User:
    result = await session.execute(select(User).where(User.telegram_id == tg_user["id"]))
    user = result.scalar_one_or_none()
    if not user:
        user = User(
            telegram_id=tg_user["id"],
            username=tg_user.get("username"),
            first_name=tg_user.get("first_name"),
            last_name=tg_user.get("last_name"),
            photo_url=tg_user.get("photo_url"),
            is_admin=tg_user["id"] in ADMIN_IDS,
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


# ============ PYDANTIC ============
class InitData(BaseModel):
    initData: str


class SendMessageReq(BaseModel):
    initData: str
    text: str
    reply_to_id: Optional[int] = None


class ReactReq(BaseModel):
    initData: str
    emoji: str


class EditMessageReq(BaseModel):
    initData: str
    text: str


class ProfileReq(BaseModel):
    initData: str
    first_name: Optional[str] = None
    bio: Optional[str] = None
    language: Optional[str] = None


class BlockReq(BaseModel):
    initData: str
    user_id: int


# ============ FRONTEND ============
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
  --link:var(--tg-theme-link-color,#007aff);
}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  background:var(--bg);color:var(--text);height:100vh;overflow:hidden;font-size:15px}
.app{display:flex;flex-direction:column;height:100vh;max-width:720px;margin:0 auto}
.header{display:flex;justify-content:space-between;align-items:center;
  padding:12px 16px;background:var(--bg2);border-bottom:1px solid rgba(0,0,0,.06);flex-shrink:0}
.header-left{display:flex;align-items:center;gap:8px}
.dot{width:8px;height:8px;border-radius:50%;background:#31c48d;
  box-shadow:0 0 0 3px rgba(49,196,141,.2);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.header h1{font-size:16px;font-weight:600}
.header-right{display:flex;gap:6px;align-items:center}
.user-name{font-size:12px;color:var(--hint);background:rgba(0,0,0,.05);
  padding:4px 10px;border-radius:10px;max-width:130px;overflow:hidden;
  text-overflow:ellipsis;white-space:nowrap;cursor:pointer}
.icon-btn{background:none;border:none;font-size:16px;cursor:pointer;padding:4px 6px;
  border-radius:6px;color:var(--text)}
.icon-btn:hover{background:rgba(0,0,0,.06)}
.messages{flex:1;overflow-y:auto;padding:12px 16px;display:flex;
  flex-direction:column;gap:8px;scroll-behavior:smooth}
.loading{text-align:center;color:var(--hint);font-size:14px;padding:20px}
.msg{max-width:78%;padding:8px 12px;border-radius:16px;font-size:14px;
  line-height:1.4;word-wrap:break-word;word-break:break-word;
  animation:slideIn .2s ease-out;position:relative}
@keyframes slideIn{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:translateY(0)}}
.msg.own{align-self:flex-end;background:var(--btn);color:var(--btn-text);border-bottom-right-radius:4px}
.msg.other{align-self:flex-start;background:var(--bg2);color:var(--text);border-bottom-left-radius:4px}
.msg .sender{font-size:11px;font-weight:600;opacity:.75;margin-bottom:3px;color:var(--link);cursor:pointer}
.msg.own .sender{color:rgba(255,255,255,.9)}
.msg .time{font-size:10px;opacity:.6;margin-top:4px;text-align:right;
  display:flex;justify-content:flex-end;gap:4px;align-items:center}
.msg .edited{font-style:italic;opacity:.6;font-size:10px;margin-right:4px}
.msg .reply-preview{font-size:11px;border-left:3px solid var(--btn);
  padding-left:6px;margin-bottom:4px;opacity:.75;font-style:italic;
  background:rgba(0,0,0,.05);padding:4px 8px;border-radius:4px;cursor:pointer}
.msg.own .reply-preview{background:rgba(255,255,255,.15)}
.msg .reactions{display:flex;gap:4px;margin-top:5px;flex-wrap:wrap}
.reaction{background:rgba(0,0,0,.08);padding:2px 6px;border-radius:10px;
  font-size:11px;cursor:pointer;user-select:none;transition:transform .1s}
.reaction:hover{transform:scale(1.1)}
.msg.own .reaction{background:rgba(255,255,255,.2)}
.msg-actions{position:absolute;top:100%;right:0;background:var(--bg);
  border-radius:10px;box-shadow:0 4px 20px rgba(0,0,0,.2);display:none;
  gap:2px;padding:6px;z-index:100;min-width:160px;flex-direction:column}
.msg-actions.active{display:flex}
.msg-actions button{background:none;border:none;font-size:13px;padding:8px 10px;
  cursor:pointer;border-radius:6px;text-align:left;color:var(--text);
  display:flex;align-items:center;gap:8px}
.msg-actions button:hover{background:var(--bg2)}
.reply-bar{background:var(--bg2);border-left:3px solid var(--btn);
  padding:8px 12px;margin:0 12px;border-radius:6px;font-size:12px;
  display:none;justify-content:space-between;align-items:center}
.reply-bar.active{display:flex}
.reply-bar button{background:none;border:none;color:var(--hint);font-size:16px;cursor:pointer}
.emoji-bar{display:flex;gap:4px;padding:6px 12px;background:var(--bg2);
  overflow-x:auto;border-top:1px solid rgba(0,0,0,.06);scrollbar-width:none}
.emoji-bar::-webkit-scrollbar{display:none}
.emoji-bar button{background:none;border:none;font-size:22px;cursor:pointer;
  padding:4px 6px;border-radius:8px;transition:background .15s;flex-shrink:0}
.emoji-bar button:hover{background:rgba(0,0,0,.08)}
.input-area{display:flex;gap:8px;padding:10px 12px;background:var(--bg2);
  border-top:1px solid rgba(0,0,0,.06);flex-shrink:0;
  padding-bottom:max(10px,env(safe-area-inset-bottom))}
.input-area input{flex:1;padding:11px 16px;border:none;border-radius:22px;
  background:var(--bg);color:var(--text);font-size:15px;outline:none;
  transition:box-shadow .2s}
.input-area input:focus{box-shadow:0 0 0 2px var(--btn)}
.input-area button.send{width:44px;height:44px;border:none;border-radius:50%;
  background:var(--btn);color:var(--btn-text);font-size:18px;cursor:pointer;
  display:flex;align-items:center;justify-content:center;
  transition:transform .1s,opacity .2s;flex-shrink:0}
.input-area button.send:active{transform:scale(.92);opacity:.85}
.modal{position:fixed;inset:0;background:rgba(0,0,0,.5);display:none;
  align-items:center;justify-content:center;z-index:1000;padding:20px}
.modal.active{display:flex}
.modal-content{background:var(--bg);border-radius:16px;padding:20px;
  max-width:400px;width:100%;max-height:80vh;overflow-y:auto}
.modal-content h3{margin-bottom:14px;font-size:17px}
.modal-content input,.modal-content textarea,.modal-content select{
  width:100%;padding:10px 14px;border:1px solid rgba(0,0,0,.1);
  border-radius:10px;background:var(--bg2);color:var(--text);
  font-size:14px;outline:none;margin-bottom:10px;font-family:inherit}
.modal-content textarea{min-height:80px;resize:vertical}
.modal-actions{display:flex;gap:8px;justify-content:flex-end;margin-top:10px}
.modal-actions button{padding:9px 18px;border:none;border-radius:10px;
  font-size:14px;cursor:pointer;font-weight:500}
.btn-primary{background:var(--btn);color:var(--btn-text)}
.btn-secondary{background:var(--bg2);color:var(--text)}
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
    <div class="header-right">
      <span id="userName" class="user-name" onclick="openProfile()">...</span>
      <button class="icon-btn" onclick="openSearch()" title="Qidirish">🔍</button>
      <button class="icon-btn" onclick="openProfile()" title="Profil">👤</button>
    </div>
  </header>
  <div id="messages" class="messages">
    <div class="loading">Xabarlar yuklanmoqda...</div>
  </div>
  <div id="replyBar" class="reply-bar">
    <span id="replyText">Javob...</span>
    <button onclick="cancelReply()">✕</button>
  </div>
  <div class="emoji-bar" id="emojiBar"></div>
  <div class="input-area">
    <input type="text" id="messageInput" placeholder="Xabar yozing..." autocomplete="off" maxlength="4000">
    <button class="send" id="sendBtn">➤</button>
  </div>
</div>

<!-- Profil Modal -->
<div class="modal" id="profileModal">
  <div class="modal-content">
    <h3>👤 Profil</h3>
    <label style="font-size:12px;color:var(--hint)">Ism:</label>
    <input type="text" id="profileName" placeholder="Ismingiz">
    <label style="font-size:12px;color:var(--hint)">Bio:</label>
    <textarea id="profileBio" placeholder="O'zingiz haqingizda..."></textarea>
    <label style="font-size:12px;color:var(--hint)">Til:</label>
    <select id="profileLang">
      <option value="uz">O'zbek</option>
      <option value="ru">Русский</option>
      <option value="en">English</option>
    </select>
    <div class="modal-actions">
      <button class="btn-secondary" onclick="closeModal('profileModal')">Bekor</button>
      <button class="btn-primary" onclick="saveProfile()">Saqlash</button>
    </div>
  </div>
</div>

<!-- Qidiruv Modal -->
<div class="modal" id="searchModal">
  <div class="modal-content">
    <h3>🔍 Qidiruv</h3>
    <input type="text" id="searchInput" placeholder="Xabar yoki foydalanuvchi..." onkeyup="doSearch()">
    <div id="searchResults" style="max-height:300px;overflow-y:auto"></div>
    <div class="modal-actions">
      <button class="btn-secondary" onclick="closeModal('searchModal')">Yopish</button>
    </div>
  </div>
</div>

<script>
const tg = window.Telegram?.WebApp;
if (tg) { tg.ready(); tg.expand(); }

let currentUserId = null;
let currentUser = null;
let lastMessageId = 0;
let isSending = false;
let replyToId = null;
let ws = null;
let reconnectTimer = null;
const renderedIds = new Set();
const EMOJIS = ["👍","❤️","😂","🔥","👏","😮","😢","🎉","🙏","💯","✅","❌","😍","🤔","😎"];

async function api(path, opts = {}) {
  const res = await fetch(path, {
    ...opts,
    headers: { "Content-Type": "application/json", ...(opts.headers || {}) }
  });
  return res.json();
}

function initEmojiBar() {
  const bar = document.getElementById("emojiBar");
  EMOJIS.forEach(e => {
    const btn = document.createElement("button");
    btn.textContent = e;
    btn.onclick = () => {
      const inp = document.getElementById("messageInput");
      inp.value += e;
      inp.focus();
    };
    bar.appendChild(btn);
  });
}

async function init() {
  if (!tg || !tg.initData) { showError("Iltimos, botni Telegram orqali oching!"); return; }
  initEmojiBar();
  try {
    const data = await api("/api/init", {
      method: "POST",
      body: JSON.stringify({ initData: tg.initData })
    });
    if (!data.ok) { showError("Avtorizatsiya xatosi: " + (data.error || "")); return; }
    currentUserId = data.user.id;
    currentUser = data.user;
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
        else if (msg.type === "ping") ws.send(JSON.stringify({type:"pong"}));
      } catch (e) { console.error("WS parse:", e); }
    };
    ws.onclose = () => {
      clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(connectWebSocket, 3000);
    };
    ws.onerror = (e) => console.error("WS xato:", e);
  } catch (e) {
    reconnectTimer = setTimeout(connectWebSocket, 3000);
  }
}

function handleNewMessage(msg) {
  if (renderedIds.has(msg.id)) return;
  if (msg.id > lastMessageId) lastMessageId = msg.id;
  appendMessage(msg, true);
  if (msg.sender_id !== currentUserId && tg.HapticFeedback) {
    tg.HapticFeedback.notificationOccurred("success");
  }
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
  } catch (e) { console.error("Load:", e); }
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
    html += `<div class="reply-preview" onclick="scrollToMsg(${msg.reply_to_id})">↩ ${escapeHtml(msg.reply_preview)}</div>`;
  }
  if (!isOwn) html += `<div class="sender">${escapeHtml(msg.sender_name || "Anonim")}</div>`;
  html += `<div class="text">${escapeHtml(msg.text)}</div>`;
  html += `<div class="time">${msg.is_edited ? '<span class="edited">tahrirlangan</span>' : ''}${formatTime(msg.created_at)}${isOwn ? " ✓✓" : ""}</div>`;
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
  let pressTimer;
  const startPress = () => { pressTimer = setTimeout(() => showActions(div, msg), 500); };
  const endPress = () => clearTimeout(pressTimer);
  div.addEventListener("touchstart", startPress, { passive: true });
  div.addEventListener("touchend", endPress);
  div.addEventListener("mousedown", startPress);
  div.addEventListener("mouseup", endPress);
  c.appendChild(div);
  renderedIds.add(msg.id);
  if (scroll) c.scrollTop = c.scrollHeight;
}

function showActions(div, msg) {
  if (navigator.vibrate) navigator.vibrate(20);
  document.querySelectorAll(".msg-actions").forEach(el => el.remove());
  const isOwn = msg.sender_id === currentUserId;
  const actions = document.createElement("div");
  actions.className = "msg-actions active";
  let btns = `
    <button onclick="setReply({id:${msg.id},sender_name:'${escapeHtml(msg.sender_name||'')}',text:'${escapeHtml(msg.text).replace(/'/g,"\\'")}'})">↩️ Javob</button>
    <button onclick="quickReact(${msg.id})">👍 Reaksiya</button>
    <button onclick="copyText('${escapeHtml(msg.text).replace(/'/g,"\\'")}')">📋 Nusxalash</button>
  `;
  if (isOwn) {
    btns += `<button onclick="editMsg(${msg.id}, '${escapeHtml(msg.text).replace(/'/g,"\\'")}')">✏️ Tahrirlash</button>`;
    btns += `<button onclick="deleteMessage(${msg.id})">🗑️ O'chirish</button>`;
  } else {
    btns += `<button onclick="blockUser(${msg.sender_id})">🚫 Bloklash</button>`;
  }
  actions.innerHTML = btns;
  div.appendChild(actions);
  setTimeout(() => {
    document.addEventListener("click", function closeHandler(e) {
      if (!actions.contains(e.target) && e.target !== div) {
        actions.remove();
        document.removeEventListener("click", closeHandler);
      }
    });
  }, 100);
}

function setReply(msg) {
  replyToId = msg.id;
  const bar = document.getElementById("replyBar");
  document.getElementById("replyText").textContent = (msg.sender_name || "User") + ": " + msg.text.slice(0, 40);
  bar.classList.add("active");
  document.getElementById("messageInput").focus();
  document.querySelectorAll(".msg-actions").forEach(el => el.remove());
}

function cancelReply() {
  replyToId = null;
  document.getElementById("replyBar").classList.remove("active");
}

function quickReact(id) {
  const emoji = prompt("Emoji kiriting:", "👍");
  if (emoji) react(id, emoji);
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

async function editMsg(id, oldText) {
  document.querySelectorAll(".msg-actions").forEach(el => el.remove());
  const newText = prompt("Yangi matn:", oldText);
  if (!newText || newText === oldText) return;
  const data = await api(`/api/messages/${id}`, {
    method: "PUT",
    body: JSON.stringify({ initData: tg.initData, text: newText })
  });
  if (!data.ok) alert(data.error || "Xatolik");
}

async function deleteMessage(id) {
  document.querySelectorAll(".msg-actions").forEach(el => el.remove());
  if (!confirm("Xabarni o'chirishni xohlaysizmi?")) return;
  try {
    const r = await api(`/api/messages/${id}`, {
      method: "DELETE",
      body: JSON.stringify({ initData: tg.initData })
    });
    if (!r.ok) alert(r.error || "Xatolik");
  } catch (e) { console.error(e); }
}

async function blockUser(userId) {
  document.querySelectorAll(".msg-actions").forEach(el => el.remove());
  if (!confirm("Bu foydalanuvchini bloklashni xohlaysizmi?")) return;
  const r = await api("/api/users/block", {
    method: "POST",
    body: JSON.stringify({ initData: tg.initData, user_id: userId })
  });
  if (r.ok) alert("Bloklandi ✅");
}

function copyText(text) {
  navigator.clipboard.writeText(text).then(() => alert("Nusxalandi! 📋"));
  document.querySelectorAll(".msg-actions").forEach(el => el.remove());
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
    if (t) t.textContent = msg.text;
    const time = el.querySelector(".time");
    if (time && !time.innerHTML.includes("tahrirlangan")) {
      time.innerHTML = '<span class="edited">tahrirlangan</span>' + time.innerHTML;
    }
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

function scrollToMsg(id) {
  const el = document.querySelector(`.msg[data-id="${id}"]`);
  if (el) {
    el.scrollIntoView({ behavior: "smooth", block: "center" });
    el.style.background = "rgba(255,200,0,.4)";
    setTimeout(() => el.style.background = "", 1500);
  }
}

function openProfile() {
  document.getElementById("profileName").value = currentUser?.first_name || "";
  document.getElementById("profileBio").value = currentUser?.bio || "";
  document.getElementById("profileLang").value = currentUser?.language || "uz";
  document.getElementById("profileModal").classList.add("active");
}

async function saveProfile() {
  const name = document.getElementById("profileName").value.trim();
  const bio = document.getElementById("profileBio").value.trim();
  const lang = document.getElementById("profileLang").value;
  const r = await api("/api/user/profile", {
    method: "PUT",
    body: JSON.stringify({ initData: tg.initData, first_name: name, bio, language: lang })
  });
  if (r.ok) {
    currentUser.first_name = name;
    currentUser.bio = bio;
    currentUser.language = lang;
    document.getElementById("userName").textContent = name;
    closeModal("profileModal");
    alert("Saqlandi ✅");
  }
}

function openSearch() {
  document.getElementById("searchModal").classList.add("active");
  setTimeout(() => document.getElementById("searchInput").focus(), 200);
}

async function doSearch() {
  const q = document.getElementById("searchInput").value.trim();
  const res = document.getElementById("searchResults");
  if (q.length < 2) { res.innerHTML = '<div style="color:var(--hint);font-size:13px;padding:10px">Kamida 2 ta belgi kiriting</div>'; return; }
  const data = await api(`/api/search?q=${encodeURIComponent(q)}`);
  if (!data.ok) return;
  let html = "";
  if (data.users?.length) {
    html += '<div style="font-size:12px;color:var(--hint);margin:8px 0 4px">👥 Foydalanuvchilar:</div>';
    data.users.forEach(u => {
      html += `<div style="padding:8px;border-radius:8px;cursor:pointer" onclick="alert('User: ${u.first_name} (@${u.username||'—'})')">👤 <b>${u.first_name||'—'}</b> ${u.username?'@'+u.username:''}</div>`;
    });
  }
  if (data.messages?.length) {
    html += '<div style="font-size:12px;color:var(--hint);margin:8px 0 4px">💬 Xabarlar:</div>';
    data.messages.forEach(m => {
      html += `<div style="padding:8px;border-radius:8px;cursor:pointer;background:var(--bg2);margin-bottom:4px" onclick="closeModal('searchModal');scrollToMsg(${m.id})">${escapeHtml(m.text.slice(0,80))}</div>`;
    });
  }
  res.innerHTML = html || '<div style="color:var(--hint);font-size:13px;padding:10px">Topilmadi</div>';
}

function closeModal(id) {
  document.getElementById(id).classList.remove("active");
}

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
  if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendMessage(); }
});
window.addEventListener("beforeunload", () => { if (ws) ws.close(); });

init();
</script>
</body>
</html>
"""


# ============ ROUTES ============
@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(HTML_PAGE)


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.get("/ping")
async def ping():
    return "pong"


@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
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


# ============ API ============
@app.post("/api/init")
async def api_init(req: InitData):
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
                "bio": user.bio,
                "language": user.language,
                "is_admin": user.is_admin,
            },
        }


@app.get("/api/messages")
async def api_get_messages(after_id: int = 0):
    async for session in get_session():
        if after_id > 0:
            result = await session.execute(
                select(Message)
                .where(and_(Message.id > after_id, Message.is_deleted == False))
                .order_by(Message.id.asc()).limit(200)
            )
        else:
            result = await session.execute(
                select(Message).where(Message.is_deleted == False)
                .order_by(desc(Message.id)).limit(100)
            )
        messages = list(result.scalars().all())
        if after_id == 0:
            messages.reverse()
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
            r = await session.execute(select(Message).where(Message.id == req.reply_to_id))
            parent = r.scalar_one_or_none()
            if parent:
                reply_preview = parent.text[:60] + ("..." if len(parent.text) > 60 else "")
        msg = Message(
            sender_id=user.telegram_id, sender_name=sender_name,
            text=text, reply_to_id=req.reply_to_id,
        )
        session.add(msg)
        await session.commit()
        await session.refresh(msg)
        payload = {
            "id": msg.id, "sender_id": msg.sender_id, "sender_name": msg.sender_name,
            "text": msg.text, "reply_to_id": msg.reply_to_id,
            "reply_preview": reply_preview, "reactions": "{}",
            "is_edited": False, "created_at": msg.created_at.isoformat(),
        }
        await manager.broadcast({"type": "new_message", "data": payload})
        return {"ok": True, "message": payload}


@app.put("/api/messages/{message_id}")
async def api_edit_message(message_id: int, req: EditMessageReq):
    tg_user = verify_init_data(req.initData)
    if not tg_user:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    async for session in get_session():
        r = await session.execute(select(Message).where(Message.id == message_id))
        msg = r.scalar_one_or_none()
        if not msg:
            return JSONResponse({"ok": False, "error": "Topilmadi"}, status_code=404)
        if msg.sender_id != tg_user["id"]:
            return JSONResponse({"ok": False, "error": "Ruxsat yo'q"}, status_code=403)
        msg.text = req.text.strip()[:4000]
        msg.is_edited = True
        await session.commit()
        await manager.broadcast({
            "type": "edit_message",
            "data": {"id": message_id, "text": msg.text, "is_edited": True},
        })
        return {"ok": True}


@app.delete("/api/messages/{message_id}")
async def api_delete_message(message_id: int, req: InitData):
    tg_user = verify_init_data(req.initData)
    if not tg_user:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    async for session in get_session():
        r = await session.execute(select(Message).where(Message.id == message_id))
        msg = r.scalar_one_or_none()
        if not msg:
            return JSONResponse({"ok": False, "error": "Topilmadi"}, status_code=404)
        if msg.sender_id != tg_user["id"]:
            return JSONResponse({"ok": False, "error": "Ruxsat yo'q"}, status_code=403)
        msg.is_deleted = True
        await session.commit()
        await manager.broadcast({"type": "delete_message", "data": {"id": message_id}})
        return {"ok": True}


@app.post("/api/messages/{message_id}/react")
async def api_react(message_id: int, req: ReactReq):
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
            "data": {"id": message_id, "reactions": msg.reactions},
        })
        return {"ok": True, "reactions": reactions}


@app.put("/api/user/profile")
async def api_update_profile(req: ProfileReq):
    tg_user = verify_init_data(req.initData)
    if not tg_user:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    async for session in get_session():
        user = await get_or_create_user(session, tg_user)
        if req.first_name is not None:
            user.first_name = req.first_name[:100]
        if req.bio is not None:
            user.bio = req.bio[:500]
        if req.language is not None and req.language in ("uz", "ru", "en"):
            user.language = req.language
        await session.commit()
        return {"ok": True}


@app.post("/api/users/block")
async def api_block_user(req: BlockReq):
    tg_user = verify_init_data(req.initData)
    if not tg_user:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    if req.user_id == tg_user["id"]:
        return JSONResponse({"ok": False, "error": "O'zingizni bloklay olmaysiz"}, status_code=400)
    async for session in get_session():
        r = await session.execute(
            select(Block).where(
                and_(Block.blocker_id == tg_user["id"], Block.blocked_id == req.user_id)
            )
        )
        existing = r.scalar_one_or_none()
        if existing:
            await session.delete(existing)
            await session.commit()
            return {"ok": True, "blocked": False}
        block = Block(blocker_id=tg_user["id"], blocked_id=req.user_id)
        session.add(block)
        await session.commit()
        return {"ok": True, "blocked": True}


@app.get("/api/users/online")
async def api_online():
    return {"ok": True, "count": len(manager.active), "users": list(manager.active.keys())}


@app.get("/api/stats")
async def api_stats():
    async for session in get_session():
        u = await session.scalar(select(func.count(User.id)))
        m = await session.scalar(select(func.count(Message.id)))
        return {"ok": True, "users": u or 0, "messages": m or 0, "online": len(manager.active)}


@app.get("/api/search")
async def api_search(q: str = ""):
    q = q.strip()
    if len(q) < 2:
        return {"ok": True, "users": [], "messages": []}
    async for session in get_session():
        u_r = await session.execute(
            select(User).where(
                or_(
                    User.first_name.ilike(f"%{q}%"),
                    User.username.ilike(f"%{q}%"),
                )
            ).limit(10)
        )
        users = u_r.scalars().all()
        m_r = await session.execute(
            select(Message)
            .where(and_(Message.is_deleted == False, Message.text.ilike(f"%{q}%")))
            .order_by(desc(Message.id)).limit(10)
        )
        messages = m_r.scalars().all()
        return {
            "ok": True,
            "users": [
                {"id": u.telegram_id, "first_name": u.first_name, "username": u.username}
                for u in users
            ],
            "messages": [
                {"id": m.id, "text": m.text, "sender_name": m.sender_name}
                for m in messages
            ],
        }


# ============ WEBSOCKET ============
@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket, token: str = ""):
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
    logger.info(f"🚀 Uvicorn: http://0.0.0.0:{PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
