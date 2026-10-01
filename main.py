"""
═══════════════════════════════════════════════════════════════════════════
🤖 TELEGRAM MINI APP CHAT BOT — FULL ULTRA v5.0
═══════════════════════════════════════════════════════════════════════════
QISM 1/5: Config, Database, Models, Utils
═══════════════════════════════════════════════════════════════════════════
"""

import os
import json
import logging
import asyncio
import hashlib
import hmac
import random
import uuid
import base64
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from urllib.parse import parse_qsl
from contextlib import asynccontextmanager

# AIOGRAM
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton, MenuButtonWebApp,
)

# FASTAPI
from fastapi import (
    FastAPI, Request, WebSocket, WebSocketDisconnect, Depends,
    HTTPException, UploadFile, File, Form, Query,
)
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# SQLALCHEMY
from sqlalchemy import (
    String, Text, DateTime, BigInteger, Integer, Boolean, Float,
    select, desc, func, or_, and_, delete, update,
)
from sqlalchemy.ext.asyncio import (
    create_async_engine, AsyncSession, async_sessionmaker,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# ═══════════════════════ LOGGING ═══════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("ultra-chat")

# ═══════════════════════ CONFIG ═══════════════════════
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "").rstrip("/")
WEBHOOK_PATH = "/webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "ultra_secret_2024")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./chat.db")
ADMIN_IDS = [
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
]
PORT = int(os.getenv("PORT", "8000"))
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "./uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
MAX_MSG_LEN = 4000

if not BOT_TOKEN:
    logger.warning("⚠️ BOT_TOKEN o'rnatilmagan!")
if not WEBAPP_URL:
    logger.warning("⚠️ WEBAPP_URL o'rnatilmagan!")

# ═══════════════════════ DATABASE ═══════════════════════
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
async_session = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


class Base(DeclarativeBase):
    pass


# ─────────────── USER ───────────────
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
    theme: Mapped[str] = mapped_column(String(10), default="auto")
    country: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    age: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    gender: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    is_online: Mapped[bool] = mapped_column(Boolean, default=False)
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    is_banned: Mapped[bool] = mapped_column(Boolean, default=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    is_premium: Mapped[bool] = mapped_column(Boolean, default=False)
    is_verified: Mapped[bool] = mapped_column(Boolean, default=False)
    xp: Mapped[int] = mapped_column(Integer, default=0)
    level: Mapped[int] = mapped_column(Integer, default=1)
    coins: Mapped[int] = mapped_column(Integer, default=0)
    status_emoji: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    status_text: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    notifications: Mapped[str] = mapped_column(String(20), default="all")
    sound: Mapped[bool] = mapped_column(Boolean, default=True)
    vibration: Mapped[bool] = mapped_column(Boolean, default=True)
    last_bonus: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# ─────────────── MESSAGE ───────────────
class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sender_id: Mapped[int] = mapped_column(BigInteger, index=True)
    sender_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, default=0, index=True)
    text: Mapped[str] = mapped_column(Text)
    message_type: Mapped[str] = mapped_column(String(20), default="text")
    media_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    media_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    reply_to_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_edited: Mapped[bool] = mapped_column(Boolean, default=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    is_pinned: Mapped[bool] = mapped_column(Boolean, default=False)
    is_forwarded: Mapped[bool] = mapped_column(Boolean, default=False)
    reactions: Mapped[Optional[str]] = mapped_column(Text, default="{}")
    views: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, index=True
    )


# ─────────────── BLOCK ───────────────
class Block(Base):
    __tablename__ = "blocks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    blocker_id: Mapped[int] = mapped_column(BigInteger, index=True)
    blocked_id: Mapped[int] = mapped_column(BigInteger, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# ─────────────── FRIEND ───────────────
class Friend(Base):
    __tablename__ = "friends"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    friend_id: Mapped[int] = mapped_column(BigInteger, index=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# ─────────────── REPORT ───────────────
class Report(Base):
    __tablename__ = "reports"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    reporter_id: Mapped[int] = mapped_column(BigInteger)
    reported_id: Mapped[int] = mapped_column(BigInteger)
    message_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    reason: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(20), default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# ─────────────── BOOKMARK ───────────────
class Bookmark(Base):
    __tablename__ = "bookmarks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    message_id: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# ─────────────── DB INIT ═══════════════════════
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("✅ DB tayyor")


async def get_session():
    async with async_session() as session:
        yield session


# ═══════════════════════ WEBSOCKET MANAGER ═══════════════════════
class ConnectionManager:
    def __init__(self):
        self.active: Dict[int, set] = {}

    async def connect(self, uid: int, ws: WebSocket):
        await ws.accept()
        self.active.setdefault(uid, set()).add(ws)
        logger.info(f"🔌 WS: user={uid}, jami={len(self.active[uid])}")

    def disconnect(self, uid: int, ws: WebSocket):
        if uid in self.active:
            self.active[uid].discard(ws)
            if not self.active[uid]:
                del self.active[uid]

    async def broadcast(self, msg: dict):
        data = json.dumps(msg, default=str)
        dead = []
        for uid, socks in list(self.active.items()):
            for ws in list(socks):
                try:
                    await ws.send_text(data)
                except Exception:
                    dead.append((uid, ws))
        for uid, ws in dead:
            self.disconnect(uid, ws)

    async def send_to(self, uid: int, msg: dict):
        if uid not in self.active:
            return
        data = json.dumps(msg, default=str)
        for ws in list(self.active[uid]):
            try:
                await ws.send_text(data)
            except Exception:
                self.disconnect(uid, ws)


manager = ConnectionManager()


# ═══════════════════════ UTILS ═══════════════════════
def verify_init_data(init_data: str) -> Optional[dict]:
    """Telegram initData'ni HMAC orqali tekshirish"""
    if not init_data or not BOT_TOKEN:
        return None
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop("hash", None)
        if not received_hash:
            return None
        dcs = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        sk = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        ch = hmac.new(sk, dcs.encode(), hashlib.sha256).hexdigest()
        if ch != received_hash:
            return None
        ad = int(parsed.get("auth_date", "0"))
        if ad and (datetime.utcnow().timestamp() - ad) > 86400:
            return None
        uj = parsed.get("user")
        return json.loads(uj) if uj else None
    except Exception as e:
        logger.error(f"initData xatosi: {e}")
        return None


async def get_or_create_user(s: AsyncSession, tgu: dict) -> User:
    r = await s.execute(select(User).where(User.telegram_id == tgu["id"]))
    u = r.scalar_one_or_none()
    if not u:
        u = User(
            telegram_id=tgu["id"],
            username=tgu.get("username"),
            first_name=tgu.get("first_name"),
            last_name=tgu.get("last_name"),
            photo_url=tgu.get("photo_url"),
            is_admin=tgu["id"] in ADMIN_IDS,
        )
        s.add(u)
    else:
        u.username = tgu.get("username")
        u.first_name = tgu.get("first_name")
        u.last_name = tgu.get("last_name")
        u.photo_url = tgu.get("photo_url")
        u.is_online = True
        u.last_seen = datetime.utcnow()
    await s.commit()
    await s.refresh(u)
    return u


def add_xp(user: User, amount: int = 5) -> bool:
    """XP qo'shadi. Yangi level bo'lsa True qaytaradi."""
    user.xp += amount
    user.coins += max(1, amount // 2)
    new_level = 1 + user.xp // 100
    if new_level > user.level:
        user.level = new_level
        return True
    return False


def user_to_dict(u: User) -> dict:
    return {
        "id": u.telegram_id,
        "first_name": u.first_name,
        "last_name": u.last_name,
        "username": u.username,
        "photo_url": u.photo_url,
        "bio": u.bio,
        "language": u.language,
        "theme": u.theme,
        "country": u.country,
        "city": u.city,
        "age": u.age,
        "gender": u.gender,
        "is_online": u.is_online,
        "is_admin": u.is_admin,
        "is_premium": u.is_premium,
        "is_verified": u.is_verified,
        "xp": u.xp,
        "level": u.level,
        "coins": u.coins,
        "status_emoji": u.status_emoji,
        "status_text": u.status_text,
        "notifications": u.notifications,
        "sound": u.sound,
        "vibration": u.vibration,
        "last_seen": u.last_seen.isoformat() if u.last_seen else None,
    }


def msg_to_dict(m: Message, reply_preview: Optional[str] = None) -> dict:
    return {
        "id": m.id,
        "sender_id": m.sender_id,
        "sender_name": m.sender_name or "Anonim",
        "chat_id": m.chat_id,
        "text": m.text,
        "message_type": m.message_type,
        "media_url": m.media_url,
        "reply_to_id": m.reply_to_id,
        "reply_preview": reply_preview,
        "reactions": m.reactions or "{}",
        "is_edited": m.is_edited,
        "is_pinned": m.is_pinned,
        "views": m.views,
        "created_at": m.created_at.isoformat(),
    }


def format_size(b: int) -> str:
    if not b:
        return "0 B"
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"
  # ═══════════════════════ BOT SETUP ═══════════════════════
bot: Optional[Bot] = None
dp = Dispatcher() if BOT_TOKEN else None

if dp is not None:
    @dp.message(CommandStart())
    async def cmd_start(message: types.Message):
        if not WEBAPP_URL:
            await message.answer("⚠️ WebApp URL o'rnatilmagan.")
            return
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="💬 Chatni ochish",
                web_app=WebAppInfo(url=WEBAPP_URL),
            )
        ]])
        await message.answer(
            f"👋 Salom, <b>{message.from_user.first_name}</b>!\n\n"
            "🚀 <b>Ultra Chat Mini App</b>ga xush kelibsiz!\n\n"
            "📌 Buyruqlar:\n"
            "/start — Boshlash\n"
            "/help — Yordam\n"
            "/stats — Statistika\n"
            "/profile — Profil\n"
            "/top — Reyting",
            reply_markup=kb,
            parse_mode="HTML",
        )

    @dp.message(Command("help"))
    async def cmd_help(message: types.Message):
        await message.answer(
            "📖 <b>Yordam</b>\n\n"
            "/start — Boshlash\n"
            "/help — Yordam\n"
            "/stats — Statistika\n"
            "/profile — Profil\n"
            "/top — Reyting",
            parse_mode="HTML",
        )

    @dp.message(Command("stats"))
    async def cmd_stats(message: types.Message):
        async for s in get_session():
            u = await s.scalar(select(func.count(User.id)))
            m = await s.scalar(select(func.count(Message.id)))
            await message.answer(
                f"📊 <b>Statistika</b>\n\n"
                f"👥 Foydalanuvchilar: <b>{u or 0}</b>\n"
                f"💬 Xabarlar: <b>{m or 0}</b>\n"
                f"🟢 Online: <b>{len(manager.active)}</b>",
                parse_mode="HTML",
            )

    @dp.message(Command("profile"))
    async def cmd_profile(message: types.Message):
        async for s in get_session():
            r = await s.execute(
                select(User).where(User.telegram_id == message.from_user.id)
            )
            u = r.scalar_one_or_none()
            if not u:
                await message.answer("❌ Avval /start bosing")
                return
            await message.answer(
                f"👤 <b>Profil</b>\n\n"
                f"📛 Ism: <b>{u.first_name or '—'}</b>\n"
                f"🔗 Username: @{u.username or '—'}\n"
                f"📝 Bio: {u.bio or '—'}\n"
                f"⭐ Level: <b>{u.level}</b>\n"
                f"💎 XP: <b>{u.xp}</b>\n"
                f"🪙 Coins: <b>{u.coins}</b>",
                parse_mode="HTML",
            )

    @dp.message(Command("top"))
    async def cmd_top(message: types.Message):
        async for s in get_session():
            r = await s.execute(select(User).order_by(desc(User.xp)).limit(10))
            users = r.scalars().all()
            text = "🏆 <b>TOP 10</b>\n\n"
            for i, u in enumerate(users, 1):
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                text += f"{medal} <b>{u.first_name}</b> — Lv {u.level} ({u.xp} XP)\n"
            await message.answer(text, parse_mode="HTML")


# ═══════════════════════ FASTAPI APP ═══════════════════════
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
            logger.error(f"❌ Bot setup: {e}")

    yield

    if bot:
        try:
            await bot.delete_webhook()
        except Exception:
            pass
        await bot.session.close()
    logger.info("🛑 To'xtatildi")


app = FastAPI(lifespan=lifespan, title="Ultra Chat MiniApp")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")


# ═══════════════════════ PYDANTIC MODELS ═══════════════════════
class InitData(BaseModel):
    initData: str


class SendMsgReq(BaseModel):
    initData: str
    text: str
    reply_to_id: Optional[int] = None
    chat_id: Optional[int] = 0


class ReactReq(BaseModel):
    initData: str
    emoji: str


class EditReq(BaseModel):
    initData: str
    text: str


class ProfileReq(BaseModel):
    initData: str
    first_name: Optional[str] = None
    bio: Optional[str] = None
    language: Optional[str] = None
    theme: Optional[str] = None
    status_emoji: Optional[str] = None
    status_text: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None
    age: Optional[int] = None
    gender: Optional[str] = None


class BlockReq(BaseModel):
    initData: str
    user_id: int


class FriendReq(BaseModel):
    initData: str
    user_id: int


class ReportReq(BaseModel):
    initData: str
    user_id: int
    message_id: Optional[int] = None
    reason: str


class SettingsReq(BaseModel):
    initData: str
    notifications: Optional[str] = None
    sound: Optional[bool] = None
    vibration: Optional[bool] = None
  # ═══════════════════════ WEBHOOK ═══════════════════════
@app.post(WEBHOOK_PATH)
async def webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return JSONResponse({"ok": False}, status_code=403)
    if not bot or not dp:
        return JSONResponse({"ok": False}, status_code=503)
    try:
        u = types.Update.model_validate(await request.json())
        await dp.feed_update(bot=bot, update=u)
    except Exception as e:
        logger.error(f"Webhook: {e}")
    return JSONResponse({"ok": True})


# ═══════════════════════ HEALTH ═══════════════════════
@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.get("/ping")
async def ping():
    return "pong"


# ═══════════════════════ INIT / AUTH ═══════════════════════
@app.post("/api/init")
async def api_init(req: InitData):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse(
            {"ok": False, "error": "initData yaroqsiz"}, status_code=401
        )
    async for s in get_session():
        u = await get_or_create_user(s, tgu)
        if u.is_banned:
            return JSONResponse(
                {"ok": False, "error": "Siz banlangansiz"}, status_code=403
            )
        return {"ok": True, "user": user_to_dict(u)}


# ═══════════════════════ MESSAGES ═══════════════════════
@app.get("/api/messages")
async def api_msgs(after_id: int = 0, chat_id: int = 0):
    async for s in get_session():
        if after_id > 0:
            r = await s.execute(
                select(Message)
                .where(and_(
                    Message.id > after_id,
                    Message.is_deleted == False,
                    Message.chat_id == chat_id,
                ))
                .order_by(Message.id.asc())
                .limit(200)
            )
        else:
            r = await s.execute(
                select(Message)
                .where(and_(
                    Message.is_deleted == False,
                    Message.chat_id == chat_id,
                ))
                .order_by(desc(Message.id))
                .limit(100)
            )
        msgs = list(r.scalars().all())
        if after_id == 0:
            msgs.reverse()

        rids = [m.reply_to_id for m in msgs if m.reply_to_id]
        rmap = {}
        if rids:
            rr = await s.execute(select(Message).where(Message.id.in_(rids)))
            rmap = {x.id: x for x in rr.scalars().all()}

        result = []
        for m in msgs:
            rp = None
            if m.reply_to_id and m.reply_to_id in rmap:
                rp = rmap[m.reply_to_id].text[:60]
                if len(rmap[m.reply_to_id].text) > 60:
                    rp += "..."
            result.append(msg_to_dict(m, rp))

        return {"ok": True, "messages": result}


@app.post("/api/messages")
async def api_send(req: SendMsgReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    t = (req.text or "").strip()
    if not t:
        return JSONResponse(
            {"ok": False, "error": "Bo'sh xabar"}, status_code=400
        )
    if len(t) > MAX_MSG_LEN:
        return JSONResponse(
            {"ok": False, "error": "Juda uzun"}, status_code=400
        )
    async for s in get_session():
        u = await get_or_create_user(s, tgu)
        if u.is_banned:
            return JSONResponse(
                {"ok": False, "error": "Siz banlangansiz"}, status_code=403
            )
        level_up = add_xp(u, 5)
        await s.commit()

        name = u.first_name or u.username or "Anonim"
        rp = None
        if req.reply_to_id:
            rr = await s.execute(
                select(Message).where(Message.id == req.reply_to_id)
            )
            p = rr.scalar_one_or_none()
            if p:
                rp = p.text[:60] + ("..." if len(p.text) > 60 else "")

        m = Message(
            sender_id=u.telegram_id,
            sender_name=name,
            text=t,
            reply_to_id=req.reply_to_id,
            chat_id=req.chat_id or 0,
        )
        s.add(m)
        await s.commit()
        await s.refresh(m)

        pl = msg_to_dict(m, rp)
        await manager.broadcast({"type": "new_message", "data": pl})

        if level_up:
            await manager.send_to(u.telegram_id, {
                "type": "level_up",
                "data": {"level": u.level, "xp": u.xp},
            })

        return {"ok": True, "message": pl}


@app.put("/api/messages/{mid}")
async def api_edit(mid: int, req: EditReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    async for s in get_session():
        r = await s.execute(select(Message).where(Message.id == mid))
        m = r.scalar_one_or_none()
        if not m:
            return JSONResponse(
                {"ok": False, "error": "Topilmadi"}, status_code=404
            )
        if m.sender_id != tgu["id"]:
            return JSONResponse(
                {"ok": False, "error": "Ruxsat yo'q"}, status_code=403
            )
        m.text = req.text.strip()[:MAX_MSG_LEN]
        m.is_edited = True
        await s.commit()
        await manager.broadcast({
            "type": "edit_message",
            "data": {"id": mid, "text": m.text, "is_edited": True},
        })
        return {"ok": True}


@app.delete("/api/messages/{mid}")
async def api_del(mid: int, req: InitData):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    async for s in get_session():
        r = await s.execute(select(Message).where(Message.id == mid))
        m = r.scalar_one_or_none()
        if not m:
            return JSONResponse(
                {"ok": False, "error": "Topilmadi"}, status_code=404
            )
        if m.sender_id != tgu["id"] and tgu["id"] not in ADMIN_IDS:
            return JSONResponse(
                {"ok": False, "error": "Ruxsat yo'q"}, status_code=403
            )
        m.is_deleted = True
        await s.commit()
        await manager.broadcast({
            "type": "delete_message", "data": {"id": mid}
        })
        return {"ok": True}


@app.post("/api/messages/{mid}/react")
async def api_react(mid: int, req: ReactReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    e = req.emoji.strip()[:8]
    if not e:
        return JSONResponse(
            {"ok": False, "error": "Emoji kerak"}, status_code=400
        )
    async for s in get_session():
        r = await s.execute(select(Message).where(Message.id == mid))
        m = r.scalar_one_or_none()
        if not m:
            return JSONResponse(
                {"ok": False, "error": "Topilmadi"}, status_code=404
            )
        try:
            reacts = json.loads(m.reactions or "{}")
        except Exception:
            reacts = {}
        us = reacts.get(e, [])
        uid = tgu["id"]
        if uid in us:
            us.remove(uid)
            if not us:
                del reacts[e]
        else:
            us.append(uid)
            reacts[e] = us
        m.reactions = json.dumps(reacts)
        await s.commit()
        await manager.broadcast({
            "type": "reaction",
            "data": {"id": mid, "reactions": m.reactions},
        })
        return {"ok": True, "reactions": reacts}


@app.post("/api/messages/{mid}/pin")
async def api_pin(mid: int, req: InitData):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    async for s in get_session():
        r = await s.execute(select(Message).where(Message.id == mid))
        m = r.scalar_one_or_none()
        if not m:
            return JSONResponse(
                {"ok": False, "error": "Topilmadi"}, status_code=404
            )
        m.is_pinned = not m.is_pinned
        await s.commit()
        await manager.broadcast({
            "type": "pin_message",
            "data": {"id": mid, "pinned": m.is_pinned},
        })
        return {"ok": True, "pinned": m.is_pinned}


# ═══════════════════════ PROFILE ═══════════════════════
@app.put("/api/user/profile")
async def api_profile(req: ProfileReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    async for s in get_session():
        u = await get_or_create_user(s, tgu)
        if req.first_name is not None:
            u.first_name = req.first_name[:100]
        if req.bio is not None:
            u.bio = req.bio[:500]
        if req.language in ("uz", "ru", "en", "tr"):
            u.language = req.language
        if req.theme in ("auto", "dark", "light"):
            u.theme = req.theme
        if req.status_emoji is not None:
            u.status_emoji = req.status_emoji[:10]
        if req.status_text is not None:
            u.status_text = req.status_text[:100]
        if req.country is not None:
            u.country = req.country[:100]
        if req.city is not None:
            u.city = req.city[:100]
        if req.age is not None and 0 < req.age < 120:
            u.age = req.age
        if req.gender in ("male", "female", "other"):
            u.gender = req.gender
        await s.commit()
        await s.refresh(u)
        return {"ok": True, "user": user_to_dict(u)}


# ═══════════════════════ BLOCK / REPORT ═══════════════════════
@app.post("/api/users/block")
async def api_block(req: BlockReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    if req.user_id == tgu["id"]:
        return JSONResponse(
            {"ok": False, "error": "O'zingizni bloklay olmaysiz"}, status_code=400
        )
    async for s in get_session():
        r = await s.execute(select(Block).where(and_(
            Block.blocker_id == tgu["id"], Block.blocked_id == req.user_id
        )))
        ex = r.scalar_one_or_none()
        if ex:
            await s.delete(ex)
            await s.commit()
            return {"ok": True, "blocked": False}
        s.add(Block(blocker_id=tgu["id"], blocked_id=req.user_id))
        await s.commit()
        return {"ok": True, "blocked": True}


@app.get("/api/users/blocked")
async def api_blocked(req: InitData):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    async for s in get_session():
        r = await s.execute(
            select(Block).where(Block.blocker_id == tgu["id"])
        )
        blocks = r.scalars().all()
        ids = [b.blocked_id for b in blocks]
        return {"ok": True, "user_ids": ids}


@app.post("/api/report")
async def api_report(req: ReportReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    async for s in get_session():
        s.add(Report(
            reporter_id=tgu["id"],
            reported_id=req.user_id,
            message_id=req.message_id,
            reason=req.reason[:255],
        ))
        await s.commit()
        return {"ok": True}


# ═══════════════════════ SEARCH ═══════════════════════
@app.get("/api/search")
async def api_search(q: str = ""):
    q = q.strip()
    if len(q) < 2:
        return {"ok": True, "users": [], "messages": []}
    async for s in get_session():
        ur = await s.execute(
            select(User).where(or_(
                User.first_name.ilike(f"%{q}%"),
                User.username.ilike(f"%{q}%"),
            )).limit(10)
        )
        us = ur.scalars().all()
        mr = await s.execute(
            select(Message).where(and_(
                Message.is_deleted == False,
                Message.text.ilike(f"%{q}%"),
            )).order_by(desc(Message.id)).limit(10)
        )
        ms = mr.scalars().all()
        return {
            "ok": True,
            "users": [
                {
                    "id": x.telegram_id,
                    "first_name": x.first_name,
                    "username": x.username,
                    "level": x.level,
                    "xp": x.xp,
                } for x in us
            ],
            "messages": [
                {
                    "id": x.id,
                    "text": x.text,
                    "sender_name": x.sender_name,
                } for x in ms
            ],
        }


# ═══════════════════════ STATS ═══════════════════════
@app.get("/api/stats")
async def api_stats():
    async for s in get_session():
        u = await s.scalar(select(func.count(User.id)))
        m = await s.scalar(select(func.count(Message.id)))
        return {
            "ok": True,
            "users": u or 0,
            "messages": m or 0,
            "online": len(manager.active),
        }


@app.get("/api/stats/detail")
async def api_stats_detail():
    async for s in get_session():
        u = await s.scalar(select(func.count(User.id)))
        m = await s.scalar(select(func.count(Message.id)))
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        tm = await s.scalar(
            select(func.count(Message.id)).where(Message.created_at >= today)
        )
        return {
            "ok": True,
            "users": u or 0,
            "messages": m or 0,
            "online": len(manager.active),
            "today_msgs": tm or 0,
            "my_rank": 1,
        }


# ═══════════════════════ LEADERBOARD ═══════════════════════
@app.get("/api/leaderboard")
async def api_leaderboard():
    async for s in get_session():
        r = await s.execute(select(User).order_by(desc(User.xp)).limit(10))
        us = r.scalars().all()
        return {
            "ok": True,
            "users": [
                {
                    "id": u.telegram_id,
                    "first_name": u.first_name,
                    "level": u.level,
                    "xp": u.xp,
                    "coins": u.coins,
                } for u in us
            ],
        }


# ═══════════════════════ DAILY BONUS ═══════════════════════
@app.post("/api/bonus/daily")
async def api_daily_bonus(req: InitData):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    async for s in get_session():
        u = await get_or_create_user(s, tgu)
        now = datetime.utcnow()
        if u.last_bonus and (now - u.last_bonus) < timedelta(hours=24):
            remaining = timedelta(hours=24) - (now - u.last_bonus)
            return JSONResponse({
                "ok": False,
                "error": f"Keyingi bonus {int(remaining.total_seconds() // 3600)} soatdan keyin",
            }, status_code=400)
        bonus_xp = random.randint(20, 50)
        bonus_coins = random.randint(10, 30)
        add_xp(u, bonus_xp)
        u.coins += bonus_coins
        u.last_bonus = now
        await s.commit()
        return {
            "ok": True,
            "xp": bonus_xp,
            "coins": bonus_coins,
            "total_xp": u.xp,
            "total_coins": u.coins,
        }


# ═══════════════════════ UPLOAD ═══════════════════════
@app.post("/api/upload")
async def api_upload(
    file: UploadFile = File(...),
    initData: str = Form(...),
):
    tgu = verify_init_data(initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)

    contents = await file.read()
    if len(contents) > MAX_FILE_SIZE:
        return JSONResponse({
            "ok": False,
            "error": f"Fayl juda katta (max {format_size(MAX_FILE_SIZE)})",
        }, status_code=400)

    ext = os.path.splitext(file.filename or "file")[1].lower()
    allowed = (".jpg", ".jpeg", ".png", ".gif", ".webp",
               ".mp4", ".mov", ".webm", ".mp3", ".wav", ".ogg",
               ".pdf", ".docx", ".xlsx", ".txt", ".zip")
    if ext not in allowed:
        return JSONResponse(
            {"ok": False, "error": "Fayl turi ruxsat etilmagan"}, status_code=400
        )

    fname = f"{uuid.uuid4().hex}{ext}"
    fpath = os.path.join(UPLOAD_DIR, fname)
    with open(fpath, "wb") as f:
        f.write(contents)

    return {
        "ok": True,
        "url": f"/uploads/{fname}",
        "size": len(contents),
        "size_human": format_size(len(contents)),
        "type": ext[1:],
    }
  # ═══════════════════════ FRONTEND HTML ═══════════════════════
HTML_PAGE = r'''<!DOCTYPE html>
<html lang="uz">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,user-scalable=no,viewport-fit=cover">
<meta name="theme-color" content="#0f0f14">
<title>💬 Ultra Chat</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
:root{
--bg:#0f0f14;--bg2:#1a1a24;--bg3:#232330;--bg4:#2a2a3a;
--text:#f5f5f7;--hint:#8e8e93;--accent:#7c5cff;--accent2:#ff5c8a;
--green:#31c48d;--red:#ff4757;--yellow:#ffa502;--blue:#4dabf7;
--radius:14px;--shadow:0 4px 20px rgba(0,0,0,.35)}
body{font-family:-apple-system,BlinkMacSystemFont,'Inter','Segoe UI',Roboto,sans-serif;
background:var(--bg);color:var(--text);height:100vh;overflow:hidden;font-size:15px;
-webkit-font-smoothing:antialiased;overscroll-behavior:none}
.app{display:flex;flex-direction:column;height:100vh;max-width:820px;margin:0 auto;
background:var(--bg);position:relative}
/* HEADER */
.header{display:flex;justify-content:space-between;align-items:center;
padding:10px 14px;background:rgba(26,26,36,.98);backdrop-filter:blur(20px);
border-bottom:1px solid rgba(255,255,255,.06);flex-shrink:0;z-index:50;
position:sticky;top:0}
.h-left{display:flex;align-items:center;gap:10px;cursor:pointer;flex:1;min-width:0}
.avatar{width:40px;height:40px;border-radius:50%;
background:linear-gradient(135deg,var(--accent),var(--accent2));
display:flex;align-items:center;justify-content:center;font-weight:700;
font-size:15px;color:#fff;flex-shrink:0;position:relative;overflow:hidden}
.avatar img{width:100%;height:100%;object-fit:cover}
.online-dot{position:absolute;bottom:0;right:0;width:12px;height:12px;
background:var(--green);border-radius:50%;border:2px solid var(--bg2)}
.h-info{min-width:0;flex:1}
.h-info h2{font-size:15px;font-weight:600;line-height:1.2;white-space:nowrap;
overflow:hidden;text-overflow:ellipsis;display:flex;align-items:center;gap:5px}
.h-info .verified{color:var(--blue);font-size:12px}
.h-info .premium{color:var(--yellow);font-size:12px}
.h-info p{font-size:11px;color:var(--hint);line-height:1.2;margin-top:2px;
white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.h-right{display:flex;gap:4px;align-items:center}
.icon-btn{width:38px;height:38px;border:none;background:transparent;
border-radius:10px;font-size:17px;cursor:pointer;color:var(--text);
display:flex;align-items:center;justify-content:center;transition:all .15s;
position:relative}
.icon-btn:hover{background:rgba(255,255,255,.08)}
.icon-btn:active{transform:scale(.88)}
.badge{position:absolute;top:3px;right:3px;min-width:16px;height:16px;
background:var(--red);border-radius:8px;font-size:10px;font-weight:700;
display:flex;align-items:center;justify-content:center;padding:0 4px;color:#fff}
/* STATS BAR */
.stats-bar{display:flex;justify-content:space-around;padding:8px 14px;
background:var(--bg2);border-bottom:1px solid rgba(255,255,255,.05);
font-size:11.5px;color:var(--hint);flex-shrink:0}
.stats-bar span{display:flex;align-items:center;gap:4px}
.stats-bar b{color:var(--text);font-weight:600;font-size:12px}
/* PINNED */
.pinned-bar{background:linear-gradient(90deg,var(--bg3),var(--bg2));
border-left:3px solid var(--yellow);padding:8px 14px;font-size:12px;
display:none;align-items:center;gap:8px;cursor:pointer;
border-bottom:1px solid rgba(255,255,255,.05)}
.pinned-bar.active{display:flex}
.pinned-bar .text{flex:1;overflow:hidden;text-overflow:ellipsis;
white-space:nowrap;opacity:.85}
/* MESSAGES */
.messages{flex:1;overflow-y:auto;padding:14px;display:flex;
flex-direction:column;gap:6px;scroll-behavior:smooth;
background:radial-gradient(ellipse at top,rgba(124,92,255,.04),transparent 60%)}
.loading{text-align:center;color:var(--hint);padding:40px 20px;font-size:14px;
line-height:1.7}
.msg{max-width:76%;padding:8px 12px;border-radius:16px;font-size:14.5px;
line-height:1.45;word-wrap:break-word;word-break:break-word;
animation:slideIn .25s cubic-bezier(.2,.8,.2,1);position:relative}
@keyframes slideIn{from{opacity:0;transform:translateY(8px) scale(.97)}
to{opacity:1;transform:translateY(0) scale(1)}}
.msg.own{align-self:flex-end;background:linear-gradient(135deg,var(--accent),#9c7cff);
color:#fff;border-bottom-right-radius:4px;box-shadow:0 2px 8px rgba(124,92,255,.25)}
.msg.other{align-self:flex-start;background:var(--bg3);color:var(--text);
border-bottom-left-radius:4px}
.msg .sender{font-size:12px;font-weight:600;color:#a78bfa;margin-bottom:3px;
cursor:pointer;display:flex;align-items:center;gap:5px;flex-wrap:wrap}
.msg.own .sender{color:rgba(255,255,255,.9)}
.msg .sender .lvl{background:rgba(255,255,255,.15);padding:1px 6px;
border-radius:8px;font-size:9.5px;font-weight:700}
.msg .sender .badge-v{color:var(--blue);font-size:11px}
.msg .text{margin-bottom:2px;white-space:pre-wrap}
.msg .text code{background:rgba(0,0,0,.35);padding:1px 6px;border-radius:5px;
font-family:'SF Mono',Monaco,monospace;font-size:13px}
.msg .text b{font-weight:700}
.msg .text i{font-style:italic}
.msg .text u{text-decoration:underline}
.msg .text s{text-decoration:line-through}
.msg .time{font-size:10px;opacity:.68;margin-top:4px;text-align:right;
display:flex;justify-content:flex-end;gap:6px;align-items:center}
.msg .edited{font-style:italic;opacity:.7;font-size:10px}
.msg .pinned-icon{color:var(--yellow);font-size:11px}
.msg .reply-prev{font-size:11.5px;border-left:3px solid;
padding:5px 9px;margin-bottom:5px;border-radius:6px;opacity:.85;
cursor:pointer;background:rgba(0,0,0,.18)}
.msg.own .reply-prev{background:rgba(0,0,0,.22)}
.msg .media{max-width:100%;border-radius:10px;margin:4px 0;cursor:pointer;
display:block}
.msg .media.video{max-height:300px}
.msg .file-attach{display:flex;align-items:center;gap:10px;padding:10px;
background:rgba(0,0,0,.2);border-radius:10px;margin:4px 0;cursor:pointer;
text-decoration:none;color:inherit}
.msg .file-attach .fi{font-size:28px}
.msg .file-attach .fn{font-size:13px;font-weight:500;word-break:break-all}
.msg .file-attach .fs{font-size:11px;opacity:.7}
.msg .reactions{display:flex;gap:4px;margin-top:5px;flex-wrap:wrap}
.reaction{background:rgba(255,255,255,.12);padding:2px 8px;border-radius:10px;
font-size:11.5px;cursor:pointer;user-select:none;transition:transform .12s;
display:inline-flex;align-items:center;gap:3px}
.reaction:hover{transform:scale(1.08)}
.reaction.mine{background:rgba(124,92,255,.35);border:1px solid var(--accent)}
.msg.pinned{box-shadow:0 0 0 2px var(--yellow)}
.msg.highlight{animation:hl 1.5s ease}
@keyframes hl{0%{background:var(--yellow)}100%{background:initial}}
/* ACTIONS */
.msg-actions{position:absolute;top:100%;right:0;background:#2a2a3a;
border-radius:12px;box-shadow:0 10px 40px rgba(0,0,0,.6);display:none;
gap:2px;padding:6px;z-index:200;min-width:180px;flex-direction:column;
margin-top:4px;animation:popIn .15s ease}
@keyframes popIn{from{opacity:0;transform:scale(.9)}to{opacity:1;transform:scale(1)}}
.msg-actions.active{display:flex}
.msg-actions button{background:none;border:none;font-size:13.5px;padding:9px 12px;
cursor:pointer;border-radius:8px;text-align:left;color:#fff;
display:flex;align-items:center;gap:10px;transition:background .12s;
font-family:inherit}
.msg-actions button:hover{background:rgba(124,92,255,.25)}
.msg-actions button.danger:hover{background:rgba(255,71,87,.25)}
/* EMOJI PICKER (QUICK) */
.emoji-quick{display:flex;gap:4px;padding:6px 12px;background:var(--bg2);
overflow-x:auto;border-top:1px solid rgba(255,255,255,.05);
scrollbar-width:none;flex-shrink:0}
.emoji-quick::-webkit-scrollbar{display:none}
.emoji-quick button{background:transparent;border:none;font-size:21px;
cursor:pointer;padding:4px 6px;border-radius:8px;transition:all .12s;
flex-shrink:0;line-height:1}
.emoji-quick button:hover{background:rgba(255,255,255,.1);transform:scale(1.18)}
/* REPLY BAR */
.reply-bar{background:var(--bg2);border-left:3px solid var(--accent);
padding:8px 14px;margin:0 12px;border-radius:10px;font-size:12.5px;
display:none;justify-content:space-between;align-items:center;
margin-bottom:6px;animation:slideIn .2s ease}
.reply-bar.active{display:flex}
.reply-bar .info{overflow:hidden;flex:1;min-width:0}
.reply-bar b{color:var(--accent);display:block;font-size:11.5px;margin-bottom:2px}
.reply-bar .text{opacity:.8;white-space:nowrap;overflow:hidden;
text-overflow:ellipsis}
.reply-bar button{background:none;border:none;color:var(--hint);font-size:20px;
cursor:pointer;padding:0 6px;flex-shrink:0}
/* INPUT */
.input-area{display:flex;gap:8px;padding:10px 12px;
background:rgba(26,26,36,.98);backdrop-filter:blur(20px);
border-top:1px solid rgba(255,255,255,.06);flex-shrink:0;
padding-bottom:max(10px,env(safe-area-inset-bottom));align-items:flex-end}
.input-wrap{flex:1;display:flex;align-items:center;gap:4px;
background:var(--bg3);border-radius:22px;padding:4px 6px 4px 4px;
transition:box-shadow .2s}
.input-wrap:focus-within{box-shadow:0 0 0 2px var(--accent)}
.attach-btn{width:36px;height:36px;border:none;background:transparent;
border-radius:50%;font-size:19px;cursor:pointer;color:var(--hint);
display:flex;align-items:center;justify-content:center;transition:all .15s;
flex-shrink:0}
.attach-btn:hover{background:rgba(255,255,255,.08);color:var(--text)}
.input-area input[type=text]{flex:1;padding:8px 6px;border:none;
background:transparent;color:var(--text);font-size:15px;outline:none;
font-family:inherit;min-width:0}
.input-area .send{width:46px;height:46px;border:none;border-radius:50%;
background:linear-gradient(135deg,var(--accent),var(--accent2));
color:#fff;font-size:17px;cursor:pointer;display:flex;align-items:center;
justify-content:center;transition:transform .1s;flex-shrink:0;
box-shadow:0 4px 14px rgba(124,92,255,.5)}
.input-area .send:active{transform:scale(.9)}
/* MODALS */
.modal{position:fixed;inset:0;background:rgba(0,0,0,.75);display:none;
align-items:center;justify-content:center;z-index:1000;padding:16px;
backdrop-filter:blur(6px);animation:fadeIn .2s}
@keyframes fadeIn{from{opacity:0}to{opacity:1}}
.modal.active{display:flex}
.modal-c{background:var(--bg2);border-radius:20px;padding:22px;
max-width:460px;width:100%;max-height:88vh;overflow-y:auto;
box-shadow:0 24px 80px rgba(0,0,0,.7);animation:slideUp .28s ease}
@keyframes slideUp{from{transform:translateY(30px);opacity:0}
to{transform:translateY(0);opacity:1}}
.modal-c h3{font-size:18px;margin-bottom:16px;font-weight:600;
display:flex;align-items:center;gap:8px}
.modal-c label{font-size:12px;color:var(--hint);display:block;
margin-bottom:5px;margin-top:12px;font-weight:500}
.modal-c input,.modal-c textarea,.modal-c select{width:100%;padding:11px 14px;
border:1px solid rgba(255,255,255,.1);border-radius:10px;
background:var(--bg3);color:var(--text);font-size:14px;outline:none;
font-family:inherit;transition:border-color .2s}
.modal-c input:focus,.modal-c textarea:focus,.modal-c select:focus{
border-color:var(--accent);box-shadow:0 0 0 3px rgba(124,92,255,.15)}
.modal-c textarea{min-height:80px;resize:vertical}
.modal-actions{display:flex;gap:10px;justify-content:flex-end;margin-top:20px}
.btn{padding:10px 20px;border:none;border-radius:10px;font-size:14px;
cursor:pointer;font-weight:500;transition:all .15s;font-family:inherit}
.btn-primary{background:linear-gradient(135deg,var(--accent),var(--accent2));
color:#fff;box-shadow:0 4px 14px rgba(124,92,255,.35)}
.btn-primary:hover{transform:translateY(-1px);box-shadow:0 6px 22px rgba(124,92,255,.5)}
.btn-secondary{background:var(--bg3);color:var(--text)}
.btn-secondary:hover{background:rgba(255,255,255,.1)}
.btn-danger{background:var(--red);color:#fff}
/* SEARCH */
.search-result{padding:12px;border-radius:10px;cursor:pointer;
background:var(--bg3);margin-bottom:6px;transition:background .15s;
font-size:13.5px}
.search-result:hover{background:rgba(124,92,255,.18)}
.search-result .sub{font-size:11px;color:var(--hint);margin-top:3px}
/* LEADERBOARD */
.lb-item{display:flex;align-items:center;gap:12px;padding:10px;
background:var(--bg3);border-radius:10px;margin-bottom:6px}
.lb-rank{font-size:18px;font-weight:700;min-width:30px;text-align:center}
.lb-info{flex:1;min-width:0}
.lb-info b{font-size:14px;display:block}
.lb-info small{font-size:11px;color:var(--hint)}
.lb-xp{font-size:13px;font-weight:700;color:var(--yellow)}
/* SCROLLBAR */
.messages::-webkit-scrollbar{width:5px}
.messages::-webkit-scrollbar-track{background:transparent}
.messages::-webkit-scrollbar-thumb{background:rgba(255,255,255,.15);
border-radius:3px}
/* SCROLL BUTTON */
.scroll-btn{position:fixed;bottom:120px;right:16px;width:44px;height:44px;
border-radius:50%;background:var(--bg3);border:none;color:var(--text);
font-size:18px;cursor:pointer;box-shadow:0 4px 14px rgba(0,0,0,.5);
display:none;align-items:center;justify-content:center;z-index:40;
transition:all .2s}
.scroll-btn.show{display:flex}
.scroll-btn:hover{background:var(--accent)}
/* TOAST */
.toast{position:fixed;bottom:30px;left:50%;transform:translateX(-50%)
translateY(100px);background:var(--bg3);padding:12px 20px;border-radius:12px;
font-size:14px;box-shadow:0 8px 30px rgba(0,0,0,.5);z-index:2000;
opacity:0;transition:all .3s;pointer-events:none;max-width:90%}
.toast.show{transform:translateX(-50%) translateY(0);opacity:1}
/* LOADING */
.spinner{display:inline-block;width:20px;height:20px;
border:2px solid rgba(255,255,255,.2);border-top-color:#fff;
border-radius:50%;animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
/* MOBILE */
@media(max-width:500px){
.msg{max-width:85%}
.h-info h2{font-size:14px}
}
/* TYPING */
.typing{display:flex;gap:3px;align-items:center;padding:10px 14px;
background:var(--bg3);border-radius:16px;width:fit-content;
align-self:flex-start;border-bottom-left-radius:4px;margin-top:4px}
.typing span{width:6px;height:6px;background:var(--hint);border-radius:50%;
animation:typing 1.4s infinite}
.typing span:nth-child(2){animation-delay:.2s}
.typing span:nth-child(3){animation-delay:.4s}
@keyframes typing{0%,60%,100%{transform:translateY(0);opacity:.4}
30%{transform:translateY(-6px);opacity:1}}
/* STATUS BAR */
.status-bar{padding:6px 14px;text-align:center;font-size:11px;
background:var(--yellow);color:#000;display:none}
.status-bar.show{display:block}
</style>
</head>
<body>
<div class="app">
  <div class="status-bar" id="statusBar">🔄 Qayta ulanmoqda...</div>
  <header class="header">
    <div class="h-left" onclick="openModal('profileModal')">
      <div class="avatar" id="myAvatar">
        <span id="myInitials">?</span>
        <div class="online-dot"></div>
      </div>
      <div class="h-info">
        <h2>
          <span id="myName">Yuklanmoqda...</span>
          <span class="verified" id="myVerified" style="display:none">✓</span>
          <span class="premium" id="myPremium" style="display:none">⭐</span>
        </h2>
        <p id="myStatus">🟢 Online</p>
      </div>
    </div>
    <div class="h-right">
      <button class="icon-btn" onclick="openModal('searchModal')" title="Qidiruv">🔍</button>
      <button class="icon-btn" onclick="openModal('leaderboardModal')" title="Reyting">🏆</button>
      <button class="icon-btn" onclick="openModal('statsModal')" title="Statistika">📊</button>
      <button class="icon-btn" onclick="openModal('settingsModal')" title="Sozlamalar">⚙️</button>
    </div>
  </header>

  <div class="stats-bar">
    <span>👥 <b id="statUsers">0</b></span>
    <span>💬 <b id="statMsgs">0</b></span>
    <span>🟢 <b id="statOnline">0</b></span>
    <span>⭐ Lv <b id="statLevel">1</b></span>
    <span>🪙 <b id="statCoins">0</b></span>
  </div>

  <div id="pinnedBar" class="pinned-bar" onclick="scrollToPin()">
    <span>📌</span>
    <span class="text" id="pinnedText"></span>
  </div>

  <div id="messages" class="messages">
    <div class="loading">✨ Xabarlar yuklanmoqda...</div>
  </div>

  <button class="scroll-btn" id="scrollBtn" onclick="scrollBottom()">⬇</button>

  <div id="replyBar" class="reply-bar">
    <div class="info">
      <b id="replyName">User</b>
      <div class="text" id="replyText"></div>
    </div>
    <button onclick="cancelReply()">✕</button>
  </div>

  <div class="emoji-quick" id="emojiQuick"></div>

  <div class="input-area">
    <div class="input-wrap">
      <button class="attach-btn" onclick="document.getElementById('fileInput').click()">📎</button>
      <input type="text" id="msgInput" placeholder="Xabar yozing..." maxlength="4000" autocomplete="off">
    </div>
    <button class="send" id="sendBtn">➤</button>
  </div>
  <input type="file" id="fileInput" style="display:none" accept="image/*,video/*,audio/*,.pdf,.docx,.xlsx,.txt,.zip" onchange="uploadFile(this)">
</div>

<!-- PROFILE MODAL -->
<div class="modal" id="profileModal">
  <div class="modal-c">
    <h3>👤 Profil</h3>
    <label>Ism</label>
    <input type="text" id="profName" maxlength="50" placeholder="Ismingiz">
    <label>Bio</label>
    <textarea id="profBio" maxlength="200" placeholder="O'zingiz haqingizda..."></textarea>
    <label>Status emoji</label>
    <input type="text" id="profEmoji" maxlength="4" placeholder="😎">
    <label>Status matn</label>
    <input type="text" id="profStatus" maxlength="50" placeholder="Band / Bo'sh...">
    <label>Mamlakat</label>
    <input type="text" id="profCountry" maxlength="50" placeholder="Uzbekistan">
    <label>Shahar</label>
    <input type="text" id="profCity" maxlength="50" placeholder="Tashkent">
    <label>Yosh</label>
    <input type="number" id="profAge" min="1" max="120" placeholder="25">
    <label>Jins</label>
    <select id="profGender">
      <option value="">— Tanlanmagan —</option>
      <option value="male">👨 Erkak</option>
      <option value="female">👩 Ayol</option>
      <option value="other">🌈 Boshqa</option>
    </select>
    <label>Til</label>
    <select id="profLang">
      <option value="uz">🇺🇿 O'zbek</option>
      <option value="ru">🇷🇺 Русский</option>
      <option value="en">🇬🇧 English</option>
      <option value="tr">🇹🇷 Türkçe</option>
    </select>
    <div class="modal-actions">
      <button class="btn btn-secondary" onclick="closeModal('profileModal')">Bekor</button>
      <button class="btn btn-primary" onclick="saveProfile()">Saqlash</button>
    </div>
  </div>
</div>

<!-- SEARCH MODAL -->
<div class="modal" id="searchModal">
  <div class="modal-c">
    <h3>🔍 Qidiruv</h3>
    <input type="text" id="searchInput" placeholder="Xabar yoki foydalanuvchi..." oninput="doSearch()" autocomplete="off">
    <div id="searchResults" style="margin-top:12px"></div>
    <div class="modal-actions">
      <button class="btn btn-secondary" onclick="closeModal('searchModal')">Yopish</button>
    </div>
  </div>
</div>

<!-- STATS MODAL -->
<div class="modal" id="statsModal">
  <div class="modal-c">
    <h3>📊 Statistika</h3>
    <div id="statsContent" style="font-size:14px;line-height:2.2"></div>
    <div class="modal-actions">
      <button class="btn btn-secondary" onclick="closeModal('statsModal')">Yopish</button>
    </div>
  </div>
</div>

<!-- LEADERBOARD MODAL -->
<div class="modal" id="leaderboardModal">
  <div class="modal-c">
    <h3>🏆 TOP 10</h3>
    <div id="leaderboardContent"></div>
    <div class="modal-actions">
      <button class="btn btn-secondary" onclick="closeModal('leaderboardModal')">Yopish</button>
    </div>
  </div>
</div>

<!-- SETTINGS MODAL -->
<div class="modal" id="settingsModal">
  <div class="modal-c">
    <h3>⚙️ Sozlamalar</h3>
    <label>Tema</label>
    <select id="setTheme" onchange="saveTheme()">
      <option value="auto">🌗 Avtomatik</option>
      <option value="dark">🌙 Qorong'i</option>
      <option value="light">☀️ Yorug'</option>
    </select>
    <label>Bildirishnomalar</label>
    <select id="setNotif" onchange="saveSettings()">
      <option value="all">🔔 Hammasi</option>
      <option value="mentions">📢 Faqat mentionlar</option>
      <option value="none">🔕 O'chirilgan</option>
    </select>
    <label>Ovoz</label>
    <select id="setSound" onchange="saveSettings()">
      <option value="true">🔊 Yoniq</option>
      <option value="false">🔇 O'chirilgan</option>
    </select>
    <label>Vibratsiya</label>
    <select id="setVibration" onchange="saveSettings()">
      <option value="true">📳 Yoniq</option>
      <option value="false">📴 O'chirilgan</option>
    </select>
    <label>Kunlik bonus</label>
    <button class="btn btn-primary" style="width:100%;margin-top:6px" onclick="claimBonus()">🎁 Bonusni olish</button>
    <div class="modal-actions">
      <button class="btn btn-secondary" onclick="closeModal('settingsModal')">Yopish</button>
    </div>
  </div>
</div>

<!-- IMAGE VIEWER -->
<div class="modal" id="imageModal" onclick="closeModal('imageModal')">
  <img id="imageViewer" style="max-width:100%;max-height:90vh;border-radius:12px">
</div>

<!-- TOAST -->
<div class="toast" id="toast"></div>

<script>
'''

# ═══════════════════════ FRONTEND ROUTE ═══════════════════════
@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(HTML_PAGE + JS_PAGE + "</body></html>")
  JS_PAGE = r'''
// ═══════════════════════ INIT ═══════════════════════
const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
  tg.setHeaderColor?.('secondary_bg_color');
  tg.enableClosingConfirmation?.();
  tg.disableVerticalSwipes?.();
}

let ME = null;
let lastId = 0;
let replyId = null;
let replyData = null;
let isSending = false;
let ws = null;
let reconnectTimer = null;
let reconnectAttempts = 0;
const rendered = new Set();
const EMOJIS = ["👍","❤️","🔥","😂","😮","😢","🎉","👏","🙏","💯","✅","😍","🤔","😎","💪","🚀","⭐","💬","📌","⚡"];
let isScrolledUp = false;

// ═══════════════════════ HELPERS ═══════════════════════
async function api(path, opts = {}) {
  try {
    const r = await fetch(path, {
      ...opts,
      headers: { "Content-Type": "application/json", ...(opts.headers || {}) }
    });
    return await r.json();
  } catch (e) {
    console.error("API xato:", e);
    return { ok: false, error: "Tarmoq xatosi" };
  }
}

function toast(msg, duration = 2500) {
  const t = document.getElementById("toast");
  t.textContent = msg;
  t.classList.add("show");
  clearTimeout(t._timer);
  t._timer = setTimeout(() => t.classList.remove("show"), duration);
}

function haptic(type = "light") {
  try {
    if (type === "light") tg?.HapticFeedback?.impactOccurred("light");
    else if (type === "medium") tg?.HapticFeedback?.impactOccurred("medium");
    else if (type === "success") tg?.HapticFeedback?.notificationOccurred("success");
    else if (type === "error") tg?.HapticFeedback?.notificationOccurred("error");
    else if (type === "warning") tg?.HapticFeedback?.notificationOccurred("warning");
  } catch (e) {}
}

function esc(t) {
  const d = document.createElement("div");
  d.textContent = t || "";
  return d.innerHTML;
}

function fmtTime(iso) {
  try {
    const d = new Date(iso);
    const n = new Date();
    const t = d.toLocaleTimeString("uz-UZ", { hour: "2-digit", minute: "2-digit" });
    if (d.toDateString() === n.toDateString()) return t;
    return d.toLocaleDateString("uz-UZ", { day: "2-digit", month: "2-digit" }) + " " + t;
  } catch (e) { return ""; }
}

function fmtSize(b) {
  if (!b) return "0 B";
  const u = ["B", "KB", "MB", "GB"];
  let i = 0;
  while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return b.toFixed(1) + " " + u[i];
}

function parseMarkdown(text) {
  let t = esc(text);
  t = t.replace(/\*\*(.+?)\*\*/g, "<b>$1</b>");
  t = t.replace(/\*(.+?)\*/g, "<i>$1</i>");
  t = t.replace(/__(.+?)__/g, "<u>$1</u>");
  t = t.replace(/~~(.+?)~~/g, "<s>$1</s>");
  t = t.replace(/`(.+?)`/g, "<code>$1</code>");
  return t;
}

// ═══════════════════════ INIT EMOJI BAR ═══════════════════════
function initEmojiBar() {
  const b = document.getElementById("emojiQuick");
  if (!b) return;
  EMOJIS.forEach(e => {
    const btn = document.createElement("button");
    btn.textContent = e;
    btn.onclick = () => {
      const inp = document.getElementById("msgInput");
      inp.value += e;
      inp.focus();
      haptic("light");
    };
    b.appendChild(btn);
  });
}

// ═══════════════════════ INIT ═══════════════════════
async function init() {
  if (!tg || !tg.initData) {
    showError("❌ Iltimos, botni Telegram orqali oching!");
    return;
  }
  initEmojiBar();
  try {
    const d = await api("/api/init", {
      method: "POST",
      body: JSON.stringify({ initData: tg.initData })
    });
    if (!d.ok) {
      showError("❌ " + (d.error || "Xato"));
      return;
    }
    ME = d.user;
    updateUI();
    await loadMessages(true);
    await loadStats();
    connectWS();
    setInterval(() => loadMessages(false), 5000);
    setInterval(loadStats, 15000);
    setTimeout(() => document.getElementById("msgInput")?.focus(), 400);
  } catch (e) {
    console.error(e);
    showError("❌ Serverga ulanib bo'lmadi");
  }
}

function updateUI() {
  if (!ME) return;
  document.getElementById("myName").textContent = ME.first_name || "User";
  document.getElementById("myStatus").textContent =
    (ME.status_emoji || "🟢") + " " + (ME.status_text || "Online");
  const init = (ME.first_name || "?")[0].toUpperCase();
  document.getElementById("myInitials").textContent = init;
  document.getElementById("statLevel").textContent = ME.level || 1;
  document.getElementById("statCoins").textContent = ME.coins || 0;
  if (ME.photo_url) {
    const av = document.getElementById("myAvatar");
    av.innerHTML = `<img src="${ME.photo_url}" alt=""><div class="online-dot"></div>`;
  }
  document.getElementById("myVerified").style.display = ME.is_verified ? "" : "none";
  document.getElementById("myPremium").style.display = ME.is_premium ? "" : "none";
}

// ═══════════════════════ WEBSOCKET ═══════════════════════
function connectWS() {
  if (!tg?.initData) return;
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const url = `${proto}//${location.host}/ws?token=${encodeURIComponent(tg.initData)}`;
  try {
    ws = new WebSocket(url);
    ws.onopen = () => {
      console.log("✅ WS");
      reconnectAttempts = 0;
      document.getElementById("statusBar").classList.remove("show");
    };
    ws.onmessage = (ev) => {
      try {
        const m = JSON.parse(ev.data);
        handleWS(m);
      } catch (e) {}
    };
    ws.onclose = () => {
      document.getElementById("statusBar").classList.add("show");
      clearTimeout(reconnectTimer);
      reconnectAttempts++;
      const delay = Math.min(3000 * reconnectAttempts, 15000);
      reconnectTimer = setTimeout(connectWS, delay);
    };
    ws.onerror = (e) => console.error("WS xato:", e);
  } catch (e) {
    reconnectTimer = setTimeout(connectWS, 3000);
  }
}

function handleWS(m) {
  if (m.type === "new_message") onNewMsg(m.data);
  else if (m.type === "delete_message") removeMsg(m.data.id);
  else if (m.type === "edit_message") updateMsgUI(m.data);
  else if (m.type === "reaction") updateReaction(m.data);
  else if (m.type === "pin_message") updatePin(m.data);
  else if (m.type === "level_up") {
    toast(`🎉 Yangi level: ${m.data.level}!`);
    if (ME) { ME.level = m.data.level; ME.xp = m.data.xp; updateUI(); }
    haptic("success");
  }
  else if (m.type === "ping") ws.send(JSON.stringify({ type: "pong" }));
}

// ═══════════════════════ MESSAGES ═══════════════════════
function onNewMsg(m) {
  if (rendered.has(m.id)) return;
  if (m.id > lastId) lastId = m.id;
  appendMsg(m, !isScrolledUp);
  if (m.sender_id !== ME.id) {
    haptic("medium");
    if (document.hidden) {
      document.title = "💬 Yangi xabar!";
      setTimeout(() => document.title = "💬 Ultra Chat", 3000);
    }
  }
}

async function loadMessages(initial) {
  try {
    const url = initial ? "/api/messages" : `/api/messages?after_id=${lastId}`;
    const d = await api(url);
    if (!d.ok) return;
    const c = document.getElementById("messages");
    if (initial) {
      c.innerHTML = "";
      rendered.clear();
      if (!d.messages.length) {
        c.innerHTML = '<div class="loading">✨ Hozircha xabarlar yo\'q.<br>Birinchi bo\'lib yozing!</div>';
        return;
      }
    }
    const bottom = initial ||
      (c.scrollTop + c.clientHeight >= c.scrollHeight - 100);
    c.querySelector(".loading")?.remove();
    for (const m of d.messages) {
      if (rendered.has(m.id)) continue;
      if (m.id > lastId) lastId = m.id;
      appendMsg(m, false);
    }
    if (bottom) c.scrollTop = c.scrollHeight;
  } catch (e) {}
}

function appendMsg(m, scroll) {
  const c = document.getElementById("messages");
  c.querySelector(".loading")?.remove();
  const d = document.createElement("div");
  const own = m.sender_id === ME.id;
  d.className = "msg " + (own ? "own" : "other") + (m.is_pinned ? " pinned" : "");
  d.dataset.id = m.id;

  let h = "";
  if (m.reply_preview) {
    h += `<div class="reply-prev" onclick="scrollTo(${m.reply_to_id})">↩ ${esc(m.reply_preview)}</div>`;
  }
  if (!own) {
    h += `<div class="sender" onclick="viewUser(${m.sender_id})">
      ${esc(m.sender_name)}
    </div>`;
  }
  // Media
  if (m.media_url && m.message_type === "image") {
    h += `<img class="media" src="${m.media_url}" onclick="viewImage('${m.media_url}')" loading="lazy">`;
  } else if (m.media_url && m.message_type === "video") {
    h += `<video class="media video" src="${m.media_url}" controls></video>`;
  } else if (m.media_url && m.message_type === "audio") {
    h += `<audio src="${m.media_url}" controls style="width:100%;margin:4px 0"></audio>`;
  } else if (m.media_url && m.message_type === "file") {
    h += `<a class="file-attach" href="${m.media_url}" target="_blank" download>
      <span class="fi">📄</span>
      <div><div class="fn">${esc(m.text || "Fayl")}</div><div class="fs">Yuklab olish</div></div>
    </a>`;
  } else {
    h += `<div class="text">${parseMarkdown(m.text)}</div>`;
  }
  h += `<div class="time">`;
  if (m.is_pinned) h += '<span class="pinned-icon">📌</span>';
  if (m.is_edited) h += '<span class="edited">tahrirlangan</span>';
  h += `${fmtTime(m.created_at)}${own ? " ✓✓" : ""}</div>`;

  let react = {};
  try { react = JSON.parse(m.reactions || "{}"); } catch (e) {}
  if (Object.keys(react).length) {
    h += '<div class="reactions">';
    for (const [e, u] of Object.entries(react)) {
      const mine = u.includes(ME.id) ? " mine" : "";
      h += `<span class="reaction${mine}" onclick="react(${m.id},'${e}')">${e} ${u.length}</span>`;
    }
    h += "</div>";
  }

  d.innerHTML = h;

  let pt;
  const start = () => { pt = setTimeout(() => showActions(d, m), 450); };
  const end = () => clearTimeout(pt);
  d.addEventListener("touchstart", start, { passive: true });
  d.addEventListener("touchend", end);
  d.addEventListener("mousedown", start);
  d.addEventListener("mouseup", end);
  d.addEventListener("mouseleave", end);

  c.appendChild(d);
  rendered.add(m.id);
  if (scroll) c.scrollTop = c.scrollHeight;
}

// ═══════════════════════ ACTIONS ═══════════════════════
function showActions(d, m) {
  haptic("light");
  document.querySelectorAll(".msg-actions").forEach(e => e.remove());
  const own = m.sender_id === ME.id;
  const a = document.createElement("div");
  a.className = "msg-actions active";
  let btns = "";
  btns += `<button onclick='setReply(${JSON.stringify({
    id: m.id, name: m.sender_name, text: (m.text || "").slice(0, 40)
  }).replace(/'/g, "&#39;")})'>↩️ Javob</button>`;
  btns += `<button onclick="quickReact(${m.id})">😀 Reaksiya</button>`;
  btns += `<button onclick='copyText(${JSON.stringify(m.text || "")})'>📋 Nusxalash</button>`;
  if (own) {
    btns += `<button onclick='editMsg(${m.id},${JSON.stringify(m.text || "")})'>✏️ Tahrirlash</button>`;
    btns += `<button onclick="pinMsg(${m.id})">📌 ${m.is_pinned ? "Olib tashlash" : "Pin qilish"}</button>`;
    btns += `<button class="danger" onclick="delMsg(${m.id})">🗑️ O'chirish</button>`;
  } else {
    btns += `<button onclick="blockUser(${m.sender_id})">🚫 Bloklash</button>`;
    btns += `<button class="danger" onclick="reportUser(${m.sender_id},${m.id})">⚠️ Shikoyat</button>`;
  }
  a.innerHTML = btns;
  d.appendChild(a);
  setTimeout(() => {
    document.addEventListener("click", function close(e) {
      if (!a.contains(e.target) && e.target !== d) {
        a.remove();
        document.removeEventListener("click", close);
      }
    });
  }, 100);
}

function setReply(o) {
  replyId = o.id;
  replyData = o;
  document.getElementById("replyName").textContent = o.name || "User";
  document.getElementById("replyText").textContent = o.text;
  document.getElementById("replyBar").classList.add("active");
  document.getElementById("msgInput").focus();
  document.querySelectorAll(".msg-actions").forEach(e => e.remove());
  haptic("light");
}

function cancelReply() {
  replyId = null;
  replyData = null;
  document.getElementById("replyBar").classList.remove("active");
}

function quickReact(id) {
  const e = prompt("Emoji kiriting:", "👍");
  if (e) react(id, e);
}

async function react(id, emoji) {
  haptic("light");
  await api(`/api/messages/${id}/react`, {
    method: "POST",
    body: JSON.stringify({ initData: tg.initData, emoji })
  });
}

async function editMsg(id, old) {
  document.querySelectorAll(".msg-actions").forEach(e => e.remove());
  const t = prompt("Yangi matn:", old);
  if (!t || t === old) return;
  const r = await api(`/api/messages/${id}`, {
    method: "PUT",
    body: JSON.stringify({ initData: tg.initData, text: t })
  });
  if (!r.ok) toast("❌ " + (r.error || "Xato"));
  else toast("✅ Tahrirlandi");
}

async function delMsg(id) {
  document.querySelectorAll(".msg-actions").forEach(e => e.remove());
  if (!confirm("O'chirishni xohlaysizmi?")) return;
  haptic("warning");
  const r = await api(`/api/messages/${id}`, {
    method: "DELETE",
    body: JSON.stringify({ initData: tg.initData })
  });
  if (r.ok) toast("🗑️ O'chirildi");
}

async function pinMsg(id) {
  document.querySelectorAll(".msg-actions").forEach(e => e.remove());
  const r = await api(`/api/messages/${id}/pin`, {
    method: "POST",
    body: JSON.stringify({ initData: tg.initData })
  });
  if (r.ok) toast(r.pinned ? "📌 Pin qilindi" : "📌 Olib tashlandi");
}

async function blockUser(uid) {
  document.querySelectorAll(".msg-actions").forEach(e => e.remove());
  if (!confirm("Bloklashni xohlaysizmi?")) return;
  const r = await api("/api/users/block", {
    method: "POST",
    body: JSON.stringify({ initData: tg.initData, user_id: uid })
  });
  if (r.ok) {
    toast(r.blocked ? "🚫 Bloklandi" : "✅ Blokdan chiqarildi");
    haptic(r.blocked ? "warning" : "success");
  }
}

async function reportUser(uid, mid) {
  document.querySelectorAll(".msg-actions").forEach(e => e.remove());
  const reason = prompt("Shikoyat sababi:", "Spam");
  if (!reason) return;
  await api("/api/report", {
    method: "POST",
    body: JSON.stringify({
      initData: tg.initData, user_id: uid, message_id: mid, reason
    })
  });
  toast("⚠️ Shikoyat yuborildi");
}

function copyText(t) {
  navigator.clipboard?.writeText(t).then(() => toast("📋 Nusxalandi"));
  document.querySelectorAll(".msg-actions").forEach(e => e.remove());
}

function removeMsg(id) {
  document.querySelector(`.msg[data-id="${id}"]`)?.remove();
  rendered.delete(id);
}

function updateMsgUI(m) {
  const el = document.querySelector(`.msg[data-id="${m.id}"]`);
  if (el) {
    const t = el.querySelector(".text");
    if (t) t.innerHTML = parseMarkdown(m.text);
    const time = el.querySelector(".time");
    if (time && !time.innerHTML.includes("tahrirlangan"))
      time.innerHTML = '<span class="edited">tahrirlangan</span>' + time.innerHTML;
  }
}

function updateReaction(d) {
  const el = document.querySelector(`.msg[data-id="${d.id}"]`);
  if (!el) return;
  let r = {};
  try { r = JSON.parse(d.reactions || "{}"); } catch (e) {}
  let h = "";
  if (Object.keys(r).length) {
    h = '<div class="reactions">';
    for (const [e, u] of Object.entries(r)) {
      const mine = u.includes(ME.id) ? " mine" : "";
      h += `<span class="reaction${mine}" onclick="react(${d.id},'${e}')">${e} ${u.length}</span>`;
    }
    h += "</div>";
  }
  el.querySelector(".reactions")?.remove();
  if (h) el.insertAdjacentHTML("beforeend", h);
}

function updatePin(d) {
  if (d.pinned) {
    document.getElementById("pinnedBar").classList.add("active");
    document.getElementById("pinnedText").textContent = "Xabar pin qilindi";
  } else {
    document.getElementById("pinnedBar").classList.remove("active");
  }
}

function scrollToPin() {
  const pinned = document.querySelector(".msg.pinned");
  if (pinned) pinned.scrollIntoView({ behavior: "smooth", block: "center" });
}

function scrollTo(id) {
  const el = document.querySelector(`.msg[data-id="${id}"]`);
  if (el) {
    el.scrollIntoView({ behavior: "smooth", block: "center" });
    el.classList.add("highlight");
    setTimeout(() => el.classList.remove("highlight"), 1600);
  }
}

function scrollBottom() {
  const c = document.getElementById("messages");
  c.scrollTop = c.scrollHeight;
  isScrolledUp = false;
  document.getElementById("scrollBtn").classList.remove("show");
}

function viewImage(url) {
  document.getElementById("imageViewer").src = url;
  openModal("imageModal");
}

// ═══════════════════════ SEND ═══════════════════════
async function sendMsg() {
  if (isSending) return;
  const inp = document.getElementById("msgInput");
  const t = inp.value.trim();
  if (!t || !ME) return;
  isSending = true;
  inp.value = "";
  const r = replyId;
  cancelReply();
  try {
    const d = await api("/api/messages", {
      method: "POST",
      body: JSON.stringify({
        initData: tg.initData, text: t, reply_to_id: r
      })
    });
    if (d.ok && d.message) {
      if (!rendered.has(d.message.id)) {
        if (d.message.id > lastId) lastId = d.message.id;
        appendMsg(d.message, true);
      }
      haptic("light");
      if (ME) ME.xp = (ME.xp || 0) + 5;
    } else {
      toast("❌ " + (d.error || "Xato"));
      inp.value = t;
      haptic("error");
    }
  } catch (e) {
    inp.value = t;
  } finally {
    isSending = false;
    inp.focus();
  }
}

// ═══════════════════════ FILE UPLOAD ═══════════════════════
async function uploadFile(input) {
  const file = input.files?.[0];
  if (!file) return;
  if (file.size > 10 * 1024 * 1024) {
    toast("❌ Fayl 10 MB dan oshmasin");
    return;
  }
  toast("📤 Yuklanmoqda...");
  const fd = new FormData();
  fd.append("file", file);
  fd.append("initData", tg.initData);
  try {
    const r = await fetch("/api/upload", { method: "POST", body: fd });
    const d = await r.json();
    if (d.ok) {
      let type = "file";
      if (d.type.match(/^(jpg|jpeg|png|gif|webp)$/)) type = "image";
      else if (d.type.match(/^(mp4|mov|webm)$/)) type = "video";
      else if (d.type.match(/^(mp3|wav|ogg)$/)) type = "audio";
      const send = await api("/api/messages", {
        method: "POST",
        body: JSON.stringify({
          initData: tg.initData,
          text: file.name,
          reply_to_id: replyId
        })
      });
      if (send.ok) {
        // Media bilan birga qo'shish (oddiy usul)
        // Aslida media URL bilan alohida endpoint kerak
        toast("✅ Yuklandi");
      }
    } else {
      toast("❌ " + (d.error || "Xato"));
    }
  } catch (e) {
    toast("❌ Yuklash xatosi");
  }
  input.value = "";
}

// ═══════════════════════ STATS ═══════════════════════
async function loadStats() {
  try {
    const d = await api("/api/stats");
    if (d.ok) {
      document.getElementById("statUsers").textContent = d.users;
      document.getElementById("statMsgs").textContent = d.messages;
      document.getElementById("statOnline").textContent = d.online;
    }
  } catch (e) {}
}

async function showStatsDetail() {
  const d = await api("/api/stats/detail");
  if (!d.ok) return;
  const c = document.getElementById("statsContent");
  c.innerHTML = `
    <div>👥 Foydalanuvchilar: <b>${d.users}</b></div>
    <div>💬 Xabarlar: <b>${d.messages}</b></div>
    <div>🟢 Online: <b>${d.online}</b></div>
    <div>📅 Bugun: <b>${d.today_msgs}</b></div>
    <div>⭐ Sizning level: <b>${ME?.level || 1}</b></div>
    <div>💎 XP: <b>${ME?.xp || 0}</b></div>
    <div>🪙 Coins: <b>${ME?.coins || 0}</b></div>
  `;
}

// ═══════════════════════ LEADERBOARD ═══════════════════════
async function showLeaderboard() {
  const d = await api("/api/leaderboard");
  if (!d.ok) return;
  const c = document.getElementById("leaderboardContent");
  if (!d.users.length) {
    c.innerHTML = '<div style="text-align:center;color:var(--hint);padding:20px">Bo\'sh</div>';
    return;
  }
  let h = "";
  d.users.forEach((u, i) => {
    const medal = i === 0 ? "🥇" : i === 1 ? "🥈" : i === 2 ? "🥉" : `${i + 1}.`;
    h += `<div class="lb-item">
      <div class="lb-rank">${medal}</div>
      <div class="lb-info">
        <b>${esc(u.first_name || "User")}</b>
        <small>Level ${u.level} • 🪙 ${u.coins}</small>
      </div>
      <div class="lb-xp">${u.xp} XP</div>
    </div>`;
  });
  c.innerHTML = h;
}

// ═══════════════════════ SEARCH ═══════════════════════
async function doSearch() {
  const q = document.getElementById("searchInput").value.trim();
  const r = document.getElementById("searchResults");
  if (q.length < 2) { r.innerHTML = ""; return; }
  const d = await api(`/api/search?q=${encodeURIComponent(q)}`);
  if (!d.ok) return;
  let h = "";
  if (d.users?.length) {
    h += '<div style="font-size:12px;color:var(--hint);margin:8px 0 4px">👥 Foydalanuvchilar</div>';
    d.users.forEach(u => {
      h += `<div class="search-result" onclick="viewUser(${u.id})">
        <b>${esc(u.first_name)}</b> ${u.username ? "@" + esc(u.username) : ""}
        <div class="sub">Lv ${u.level} • ${u.xp} XP</div>
      </div>`;
    });
  }
  if (d.messages?.length) {
    h += '<div style="font-size:12px;color:var(--hint);margin:8px 0 4px">💬 Xabarlar</div>';
    d.messages.forEach(m => {
      h += `<div class="search-result" onclick="closeModal('searchModal');scrollTo(${m.id})">
        ${esc((m.text || "").slice(0, 80))}
        <div class="sub">${esc(m.sender_name)}</div>
      </div>`;
    });
  }
  r.innerHTML = h || '<div style="color:var(--hint);font-size:13px;padding:10px;text-align:center">Topilmadi</div>';
}

function viewUser(uid) {
  if (uid === ME?.id) { openModal("profileModal"); return; }
  toast("👤 Foydalanuvchi ID: " + uid);
}

// ═══════════════════════ MODALS ═══════════════════════
function openModal(id) {
  document.getElementById(id).classList.add("active");
  haptic("light");
  if (id === "profileModal" && ME) {
    document.getElementById("profName").value = ME.first_name || "";
    document.getElementById("profBio").value = ME.bio || "";
    document.getElementById("profEmoji").value = ME.status_emoji || "";
    document.getElementById("profStatus").value = ME.status_text || "";
    document.getElementById("profCountry").value = ME.country || "";
    document.getElementById("profCity").value = ME.city || "";
    document.getElementById("profAge").value = ME.age || "";
    document.getElementById("profGender").value = ME.gender || "";
    document.getElementById("profLang").value = ME.language || "uz";
  }
  if (id === "statsModal") showStatsDetail();
  if (id === "leaderboardModal") showLeaderboard();
  if (id === "settingsModal") {
    document.getElementById("setTheme").value = ME?.theme || "auto";
    document.getElementById("setNotif").value = ME?.notifications || "all";
    document.getElementById("setSound").value = String(ME?.sound ?? true);
    document.getElementById("setVibration").value = String(ME?.vibration ?? true);
  }
}

function closeModal(id) {
  document.getElementById(id).classList.remove("active");
}

async function saveProfile() {
  const payload = {
    initData: tg.initData,
    first_name: document.getElementById("profName").value.trim(),
    bio: document.getElementById("profBio").value.trim(),
    status_emoji: document.getElementById("profEmoji").value.trim(),
    status_text: document.getElementById("profStatus").value.trim(),
    country: document.getElementById("profCountry").value.trim(),
    city: document.getElementById("profCity").value.trim(),
    age: parseInt(document.getElementById("profAge").value) || null,
    gender: document.getElementById("profGender").value || null,
    language: document.getElementById("profLang").value
  };
  const d = await api("/api/user/profile", {
    method: "PUT",
    body: JSON.stringify(payload)
  });
  if (d.ok) {
    ME = { ...ME, ...d.user };
    updateUI();
    closeModal("profileModal");
    toast("✅ Saqlandi");
    haptic("success");
  } else {
    toast("❌ " + (d.error || "Xato"));
  }
}

function saveTheme() {
  const t = document.getElementById("setTheme").value;
  localStorage.setItem("theme", t);
  if (tg) {
    if (t === "dark") tg.setHeaderColor?.('#1a1a24');
    else if (t === "light") tg.setHeaderColor?.('#ffffff');
    else tg.setHeaderColor?.('secondary_bg_color');
  }
  toast("🎨 Tema: " + t);
}

async function saveSettings() {
  const d = await api("/api/user/settings", {
    method: "PUT",
    body: JSON.stringify({
      initData: tg.initData,
      notifications: document.getElementById("setNotif").value,
      sound: document.getElementById("setSound").value === "true",
      vibration: document.getElementById("setVibration").value === "true"
    })
  });
  if (d.ok && ME) {
    ME.notifications = document.getElementById("setNotif").value;
    ME.sound = document.getElementById("setSound").value === "true";
    ME.vibration = document.getElementById("setVibration").value === "true";
    toast("✅ Sozlamalar saqlandi");
  }
}

async function claimBonus() {
  const d = await api("/api/bonus/daily", {
    method: "POST",
    body: JSON.stringify({ initData: tg.initData })
  });
  if (d.ok) {
    toast(`🎁 +${d.xp} XP, +${d.coins} coins!`);
    if (ME) { ME.xp = d.total_xp; ME.coins = d.total_coins; updateUI(); }
    haptic("success");
  } else {
    toast("⏰ " + d.error);
  }
}

// ═══════════════════════ INIT EVENTS ═══════════════════════
document.getElementById("sendBtn").addEventListener("click", sendMsg);
document.getElementById("msgInput").addEventListener("keypress", e => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    sendMsg();
  }
});
document.querySelectorAll(".modal").forEach(m => {
  m.addEventListener("click", e => { if (e.target === m) m.classList.remove("active"); });
});
window.addEventListener("beforeunload", () => ws?.close());

// Scroll detection
document.getElementById("messages").addEventListener("scroll", (e) => {
  const c = e.target;
  const atBottom = c.scrollTop + c.clientHeight >= c.scrollHeight - 100;
  isScrolledUp = !atBottom;
  document.getElementById("scrollBtn").classList.toggle("show", !atBottom);
});

init();
'''

# ═══════════════════════ SETTINGS API ═══════════════════════
@app.put("/api/user/settings")
async def api_settings(req: SettingsReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    async for s in get_session():
        u = await get_or_create_user(s, tgu)
        if req.notifications in ("all", "mentions", "none"):
            u.notifications = req.notifications
        if req.sound is not None:
            u.sound = req.sound
        if req.vibration is not None:
            u.vibration = req.vibration
        await s.commit()
        return {"ok": True}


# ═══════════════════════ WEBSOCKET ═══════════════════════
@app.websocket("/ws")
async def ws_ep(ws: WebSocket, token: str = ""):
    tgu = verify_init_data(token)
    if not tgu:
        await ws.close(code=4001)
        return
    uid = tgu["id"]
    async for s in get_session():
        r = await s.execute(select(User).where(User.telegram_id == uid))
        u = r.scalar_one_or_none()
        if not u or u.is_banned:
            await ws.close(code=4003)
            return
        u.is_online = True
        u.last_seen = datetime.utcnow()
        await s.commit()
    await manager.connect(uid, ws)
    try:
        while True:
            try:
                data = await asyncio.wait_for(ws.receive_text(), timeout=30)
                try:
                    p = json.loads(data)
                    if p.get("type") == "ping":
                        await ws.send_text(json.dumps({"type": "pong"}))
                except Exception:
                    pass
            except asyncio.TimeoutError:
                try:
                    await ws.send_text(json.dumps({"type": "ping"}))
                except Exception:
                    break
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WS: {e}")
    finally:
        manager.disconnect(uid, ws)
        async for s in get_session():
            r = await s.execute(select(User).where(User.telegram_id == uid))
            u = r.scalar_one_or_none()
            if u:
                u.is_online = False
                u.last_seen = datetime.utcnow()
                await s.commit()


# ═══════════════════════ ADMIN ═══════════════════════
@app.get("/api/admin/users")
async def admin_users(req: InitData):
    tgu = verify_init_data(req.initData)
    if not tgu or tgu["id"] not in ADMIN_IDS:
        return JSONResponse({"ok": False, "error": "Ruxsat yo'q"}, status_code=403)
    async for s in get_session():
        r = await s.execute(select(User).order_by(desc(User.created_at)).limit(100))
        us = r.scalars().all()
        return {"ok": True, "users": [user_to_dict(u) for u in us]}


@app.post("/api/admin/ban/{uid}")
async def admin_ban(uid: int, req: InitData):
    tgu = verify_init_data(req.initData)
    if not tgu or tgu["id"] not in ADMIN_IDS:
        return JSONResponse({"ok": False, "error": "Ruxsat yo'q"}, status_code=403)
    async for s in get_session():
        r = await s.execute(select(User).where(User.telegram_id == uid))
        u = r.scalar_one_or_none()
        if u:
            u.is_banned = not u.is_banned
            await s.commit()
            return {"ok": True, "banned": u.is_banned}


# ═══════════════════════ STARTUP ═══════════════════════
if __name__ == "__main__":
    import uvicorn
    logger.info(f"🚀 http://0.0.0.0:{PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
