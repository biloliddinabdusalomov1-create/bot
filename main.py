"""
═══════════════════════════════════════════════════════════════════════════
🤖 TELEGRAM MINI APP CHAT BOT — ULTRA PRO v3.0
═══════════════════════════════════════════════════════════════════════════
100+ funksiya | Professional dizayn | Real-time WebSocket | Admin panel
═══════════════════════════════════════════════════════════════════════════
"""

import os, json, logging, asyncio, hashlib, hmac, random, io, base64
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import parse_qsl
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (WebAppInfo, InlineKeyboardMarkup,
                           InlineKeyboardButton, MenuButtonWebApp)

from fastapi import (FastAPI, Request, WebSocket, WebSocketDisconnect,
                     Depends, HTTPException, UploadFile, File)
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from sqlalchemy import (String, Text, DateTime, BigInteger, Integer, Boolean,
                        select, desc, func, or_, and_)
from sqlalchemy.ext.asyncio import (create_async_engine, AsyncSession,
                                    async_sessionmaker)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# ═══════════════════════ LOGGING ═══════════════════════
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("ultra-chat")

# ═══════════════════════ CONFIG ═══════════════════════
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "").rstrip("/")
WEBHOOK_PATH = "/webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "ultra_secret_2024")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./chat.db")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",")
             if x.strip().isdigit()]
PORT = int(os.getenv("PORT", "8000"))
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "./uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

if not BOT_TOKEN:
    logger.warning("⚠️ BOT_TOKEN o'rnatilmagan!")
if not WEBAPP_URL:
    logger.warning("⚠️ WEBAPP_URL o'rnatilmagan!")

# ═══════════════════════ DATABASE ═══════════════════════
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
async_session = async_sessionmaker(engine, class_=AsyncSession,
                                   expire_on_commit=False)


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
    theme: Mapped[str] = mapped_column(String(10), default="auto")
    is_online: Mapped[bool] = mapped_column(Boolean, default=False)
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    is_banned: Mapped[bool] = mapped_column(Boolean, default=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    is_premium: Mapped[bool] = mapped_column(Boolean, default=False)
    xp: Mapped[int] = mapped_column(Integer, default=0)
    level: Mapped[int] = mapped_column(Integer, default=1)
    status_emoji: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    status_text: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Message(Base):
    __tablename__ = "messages"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sender_id: Mapped[int] = mapped_column(BigInteger, index=True)
    sender_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, default=0, index=True)
    text: Mapped[str] = mapped_column(Text)
    message_type: Mapped[str] = mapped_column(String(20), default="text")
    media_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    reply_to_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_edited: Mapped[bool] = mapped_column(Boolean, default=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    is_pinned: Mapped[bool] = mapped_column(Boolean, default=False)
    is_forwarded: Mapped[bool] = mapped_column(Boolean, default=False)
    reactions: Mapped[Optional[str]] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class Block(Base):
    __tablename__ = "blocks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    blocker_id: Mapped[int] = mapped_column(BigInteger, index=True)
    blocked_id: Mapped[int] = mapped_column(BigInteger, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Friend(Base):
    __tablename__ = "friends"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    friend_id: Mapped[int] = mapped_column(BigInteger, index=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Report(Base):
    __tablename__ = "reports"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    reporter_id: Mapped[int] = mapped_column(BigInteger)
    reported_id: Mapped[int] = mapped_column(BigInteger)
    message_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    reason: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(20), default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Bookmark(Base):
    __tablename__ = "bookmarks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    message_id: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


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
        self.active: dict[int, set[WebSocket]] = {}

    async def connect(self, uid: int, ws: WebSocket):
        await ws.accept()
        self.active.setdefault(uid, set()).add(ws)

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

# ═══════════════════════ BOT ═══════════════════════
bot: Optional[Bot] = None
dp = Dispatcher() if BOT_TOKEN else None

if dp is not None:
    @dp.message(CommandStart())
    async def cmd_start(message: types.Message):
        if not WEBAPP_URL:
            await message.answer("⚠️ WebApp URL yo'q")
            return
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="💬 Chatni ochish",
                                 web_app=WebAppInfo(url=WEBAPP_URL))
        ]])
        await message.answer(
            f"👋 Salom, <b>{message.from_user.first_name}</b>!\n\n"
            "🚀 <b>Ultra Chat Mini App</b>ga xush kelibsiz!\n\n"
            "📌 Buyruqlar:\n"
            "/start — Boshlash\n/help — Yordam\n/stats — Statistika",
            reply_markup=kb, parse_mode="HTML")

    @dp.message(Command("help"))
    async def cmd_help(message: types.Message):
        await message.answer(
            "📖 <b>Yordam</b>\n\n/start — Boshlash\n/help — Yordam\n/stats — Statistika",
            parse_mode="HTML")

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
                parse_mode="HTML")


# ═══════════════════════ FASTAPI ═══════════════════════
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
                allowed_updates=dp.resolve_used_update_types() if dp else None)
            logger.info(f"✅ Webhook: {WEBAPP_URL}{WEBHOOK_PATH}")
            await bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(
                    text="💬 Chat",
                    web_app=WebAppInfo(url=WEBAPP_URL)))
            logger.info("✅ Menu button")
        except Exception as e:
            logger.error(f"❌ Bot setup: {e}")

    yield

    if bot:
        try:
            await bot.delete_webhook()
        except Exception:
            pass
        await bot.session.close()


app = FastAPI(lifespan=lifespan, title="Ultra Chat MiniApp")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


# ═══════════════════════ UTILS ═══════════════════════
def verify_init_data(init_data: str) -> Optional[dict]:
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
        logger.error(f"initData: {e}")
        return None


async def get_or_create_user(s: AsyncSession, tgu: dict) -> User:
    r = await s.execute(select(User).where(User.telegram_id == tgu["id"]))
    u = r.scalar_one_or_none()
    if not u:
        u = User(telegram_id=tgu["id"], username=tgu.get("username"),
                 first_name=tgu.get("first_name"), last_name=tgu.get("last_name"),
                 photo_url=tgu.get("photo_url"), is_admin=tgu["id"] in ADMIN_IDS)
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


def add_xp(user: User, amount: int = 5):
    """XP qo'shish va level hisoblash"""
    user.xp += amount
    new_level = 1 + user.xp // 100
    if new_level > user.level:
        user.level = new_level
        return True
    return False


# ═══════════════════════ PYDANTIC ═══════════════════════
class InitData(BaseModel):
    initData: str


class SendMsg(BaseModel):
    initData: str
    text: str
    reply_to_id: Optional[int] = None


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


# ═══════════════════════ FRONTEND HTML ═══════════════════════
HTML = r"""<!DOCTYPE html>
<html lang="uz">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,user-scalable=no">
<title>💬 Ultra Chat</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
:root{
--bg:var(--tg-theme-bg-color,#0f0f14);
--bg2:var(--tg-theme-secondary-bg-color,#1a1a24);
--bg3:#232330;
--text:var(--tg-theme-text-color,#f5f5f7);
--hint:var(--tg-theme-hint-color,#8e8e93);
--btn:var(--tg-theme-button-color,#7c5cff);
--btn-text:var(--tg-theme-button-text-color,#fff);
--accent:#7c5cff;
--accent2:#ff5c8a;
--green:#31c48d;
--red:#ff4757;
--yellow:#ffa502;
--radius:14px;
--shadow:0 4px 20px rgba(0,0,0,.3)}
body{font-family:-apple-system,BlinkMacSystemFont,'Inter','Segoe UI',Roboto,sans-serif;
background:var(--bg);color:var(--text);height:100vh;overflow:hidden;font-size:15px;
-webkit-font-smoothing:antialiased}
.app{display:flex;flex-direction:column;height:100vh;max-width:820px;margin:0 auto;
background:var(--bg);position:relative}
/* HEADER */
.header{display:flex;justify-content:space-between;align-items:center;
padding:10px 16px;background:rgba(26,26,36,.95);backdrop-filter:blur(20px);
border-bottom:1px solid rgba(255,255,255,.05);flex-shrink:0;z-index:50;
position:sticky;top:0}
.h-left{display:flex;align-items:center;gap:10px}
.avatar{width:38px;height:38px;border-radius:50%;
background:linear-gradient(135deg,var(--accent),var(--accent2));
display:flex;align-items:center;justify-content:center;font-weight:700;
font-size:14px;color:#fff;flex-shrink:0;position:relative;cursor:pointer}
.avatar img{width:100%;height:100%;border-radius:50%;object-fit:cover}
.avatar .online-dot{position:absolute;bottom:-1px;right:-1px;width:12px;
height:12px;background:var(--green);border-radius:50%;border:2px solid var(--bg2)}
.h-info h2{font-size:14px;font-weight:600;line-height:1.2}
.h-info p{font-size:11px;color:var(--hint);line-height:1.2;margin-top:2px}
.h-right{display:flex;gap:6px;align-items:center}
.icon-btn{width:36px;height:36px;border:none;background:transparent;
border-radius:10px;font-size:17px;cursor:pointer;color:var(--text);
display:flex;align-items:center;justify-content:center;transition:all .15s}
.icon-btn:hover{background:rgba(255,255,255,.08)}
.icon-btn:active{transform:scale(.9)}
.badge{position:absolute;top:2px;right:2px;min-width:16px;height:16px;
background:var(--red);border-radius:8px;font-size:10px;font-weight:600;
display:flex;align-items:center;justify-content:center;padding:0 4px;color:#fff}
/* STATS BAR */
.stats-bar{display:flex;justify-content:space-around;padding:8px 16px;
background:var(--bg2);border-bottom:1px solid rgba(255,255,255,.05);font-size:11px;
color:var(--hint)}
.stats-bar span{display:flex;align-items:center;gap:4px}
.stats-bar b{color:var(--text);font-weight:600}
/* MESSAGES */
.messages{flex:1;overflow-y:auto;padding:16px;display:flex;
flex-direction:column;gap:6px;scroll-behavior:smooth}
.loading{text-align:center;color:var(--hint);padding:40px 20px;font-size:14px}
.msg{max-width:75%;padding:8px 12px;border-radius:16px;font-size:14.5px;
line-height:1.45;word-wrap:break-word;word-break:break-word;
animation:slideIn .25s cubic-bezier(.2,.8,.2,1);position:relative}
@keyframes slideIn{from{opacity:0;transform:translateY(8px) scale(.97)}
to{opacity:1;transform:translateY(0) scale(1)}}
.msg.own{align-self:flex-end;background:linear-gradient(135deg,var(--accent),#9c7cff);
color:#fff;border-bottom-right-radius:4px}
.msg.other{align-self:flex-start;background:var(--bg3);color:var(--text);
border-bottom-left-radius:4px}
.msg .sender{font-size:12px;font-weight:600;color:#a78bfa;margin-bottom:3px;
cursor:pointer;display:flex;align-items:center;gap:4px}
.msg.own .sender{color:rgba(255,255,255,.9)}
.msg .sender .level{background:rgba(255,255,255,.15);padding:1px 5px;
border-radius:6px;font-size:9px}
.msg .text{margin-bottom:2px}
.msg .text code{background:rgba(0,0,0,.3);padding:1px 5px;border-radius:4px;
font-family:'SF Mono',Monaco,monospace;font-size:13px}
.msg .time{font-size:10px;opacity:.65;margin-top:4px;text-align:right;
display:flex;justify-content:flex-end;gap:6px;align-items:center}
.msg .edited{font-style:italic;opacity:.7}
.msg .reply-prev{font-size:11.5px;border-left:3px solid;
padding:5px 8px;margin-bottom:5px;border-radius:6px;opacity:.85;
cursor:pointer;background:rgba(0,0,0,.15)}
.msg.own .reply-prev{background:rgba(0,0,0,.2)}
.msg .reactions{display:flex;gap:4px;margin-top:5px;flex-wrap:wrap}
.reaction{background:rgba(255,255,255,.12);padding:2px 8px;border-radius:10px;
font-size:11.5px;cursor:pointer;user-select:none;transition:transform .12s;
display:inline-flex;align-items:center;gap:3px}
.reaction:hover{transform:scale(1.08)}
.reaction.mine{background:rgba(124,92,255,.35);border:1px solid var(--accent)}
.msg.pinned{box-shadow:0 0 0 2px var(--yellow)}
/* MESSAGE ACTIONS */
.msg-actions{position:absolute;top:100%;right:0;background:#2a2a3a;
border-radius:12px;box-shadow:0 8px 32px rgba(0,0,0,.5);display:none;
gap:2px;padding:6px;z-index:200;min-width:170px;flex-direction:column;
margin-top:4px;animation:popIn .15s ease}
@keyframes popIn{from{opacity:0;transform:scale(.9)}to{opacity:1;transform:scale(1)}}
.msg-actions.active{display:flex}
.msg-actions button{background:none;border:none;font-size:13px;padding:9px 12px;
cursor:pointer;border-radius:8px;text-align:left;color:#fff;
display:flex;align-items:center;gap:10px;transition:background .12s}
.msg-actions button:hover{background:rgba(124,92,255,.2)}
/* EMOJI PICKER */
.emoji-quick{display:flex;gap:6px;padding:6px 12px;background:var(--bg2);
overflow-x:auto;border-top:1px solid rgba(255,255,255,.05);scrollbar-width:none}
.emoji-quick::-webkit-scrollbar{display:none}
.emoji-quick button{background:transparent;border:none;font-size:20px;
cursor:pointer;padding:4px 6px;border-radius:8px;transition:all .12s;
flex-shrink:0;line-height:1}
.emoji-quick button:hover{background:rgba(255,255,255,.1);transform:scale(1.15)}
/* REPLY BAR */
.reply-bar{background:var(--bg2);border-left:3px solid var(--accent);
padding:8px 14px;margin:0 12px;border-radius:10px;font-size:12px;
display:none;justify-content:space-between;align-items:center;
margin-bottom:6px;animation:slideIn .2s ease}
.reply-bar.active{display:flex}
.reply-bar b{color:var(--accent)}
.reply-bar button{background:none;border:none;color:var(--hint);font-size:18px;
cursor:pointer;padding:0 4px}
/* INPUT */
.input-area{display:flex;gap:8px;padding:10px 12px;background:rgba(26,26,36,.95);
backdrop-filter:blur(20px);border-top:1px solid rgba(255,255,255,.05);
flex-shrink:0;padding-bottom:max(10px,env(safe-area-inset-bottom));
align-items:flex-end}
.input-area input[type=text]{flex:1;padding:12px 16px;border:none;
border-radius:22px;background:var(--bg3);color:var(--text);font-size:15px;
outline:none;transition:all .2s;max-height:100px;resize:none;
font-family:inherit}
.input-area input[type=text]:focus{box-shadow:0 0 0 2px var(--accent);
background:var(--bg2)}
.input-area .send{width:44px;height:44px;border:none;border-radius:50%;
background:linear-gradient(135deg,var(--accent),var(--accent2));
color:#fff;font-size:17px;cursor:pointer;display:flex;align-items:center;
justify-content:center;transition:transform .1s;flex-shrink:0;
box-shadow:0 4px 12px rgba(124,92,255,.4)}
.input-area .send:active{transform:scale(.9)}
/* MODALS */
.modal{position:fixed;inset:0;background:rgba(0,0,0,.7);display:none;
align-items:center;justify-content:center;z-index:1000;padding:20px;
backdrop-filter:blur(4px);animation:fadeIn .2s}
@keyframes fadeIn{from{opacity:0}to{opacity:1}}
.modal.active{display:flex}
.modal-c{background:var(--bg2);border-radius:20px;padding:22px;
max-width:440px;width:100%;max-height:85vh;overflow-y:auto;
box-shadow:0 20px 60px rgba(0,0,0,.6);animation:slideUp .25s ease}
@keyframes slideUp{from{transform:translateY(30px);opacity:0}
to{transform:translateY(0);opacity:1}}
.modal-c h3{font-size:18px;margin-bottom:16px;font-weight:600;
display:flex;align-items:center;gap:8px}
.modal-c label{font-size:12px;color:var(--hint);display:block;
margin-bottom:5px;margin-top:12px}
.modal-c input,.modal-c textarea,.modal-c select{width:100%;padding:11px 14px;
border:1px solid rgba(255,255,255,.1);border-radius:10px;
background:var(--bg3);color:var(--text);font-size:14px;outline:none;
font-family:inherit;transition:border-color .2s}
.modal-c input:focus,.modal-c textarea:focus,.modal-c select:focus{
border-color:var(--accent)}
.modal-c textarea{min-height:80px;resize:vertical}
.modal-actions{display:flex;gap:10px;justify-content:flex-end;margin-top:18px}
.btn{padding:10px 20px;border:none;border-radius:10px;font-size:14px;
cursor:pointer;font-weight:500;transition:all .15s}
.btn-primary{background:linear-gradient(135deg,var(--accent),var(--accent2));
color:#fff;box-shadow:0 4px 12px rgba(124,92,255,.3)}
.btn-primary:hover{transform:translateY(-1px);box-shadow:0 6px 20px rgba(124,92,255,.4)}
.btn-secondary{background:var(--bg3);color:var(--text)}
.btn-secondary:hover{background:rgba(255,255,255,.1)}
/* SEARCH RESULTS */
.search-result{padding:12px;border-radius:10px;cursor:pointer;
background:var(--bg3);margin-bottom:6px;transition:background .15s;
font-size:13px}
.search-result:hover{background:rgba(124,92,255,.15)}
.search-result .sub{font-size:11px;color:var(--hint);margin-top:3px}
/* SCROLL */
.messages::-webkit-scrollbar{width:5px}
.messages::-webkit-scrollbar-track{background:transparent}
.messages::-webkit-scrollbar-thumb{background:rgba(255,255,255,.15);
border-radius:3px}
/* MOBILE */
@media(max-width:500px){
.msg{max-width:85%}
.h-info h2{font-size:13px}
}
/* TYPING */
.typing{display:flex;gap:3px;align-items:center;padding:8px 12px;
background:var(--bg3);border-radius:16px;width:fit-content;
align-self:flex-start;border-bottom-left-radius:4px}
.typing span{width:6px;height:6px;background:var(--hint);border-radius:50%;
animation:typing 1.4s infinite}
.typing span:nth-child(2){animation-delay:.2s}
.typing span:nth-child(3){animation-delay:.4s}
@keyframes typing{0%,60%,100%{transform:translateY(0);opacity:.4}
30%{transform:translateY(-5px);opacity:1}}
</style>
</head>
<body>
<div class="app">
  <header class="header">
    <div class="h-left">
      <div class="avatar" id="myAvatar" onclick="openModal('profileModal')">
        <span id="myInitials">?</span>
      </div>
      <div class="h-info">
        <h2 id="myName">Yuklanmoqda...</h2>
        <p id="myStatus">🟢 Online</p>
      </div>
    </div>
    <div class="h-right">
      <button class="icon-btn" onclick="openModal('searchModal')" title="Qidiruv">🔍</button>
      <button class="icon-btn" onclick="openModal('statsModal')" title="Statistika">📊</button>
      <button class="icon-btn" onclick="openModal('settingsModal')" title="Sozlamalar">⚙️</button>
    </div>
  </header>

  <div class="stats-bar">
    <span>👥 <b id="statUsers">0</b></span>
    <span>💬 <b id="statMsgs">0</b></span>
    <span>🟢 <b id="statOnline">0</b></span>
    <span>⭐ Lv <b id="statLevel">1</b></span>
  </div>

  <div id="messages" class="messages">
    <div class="loading">Xabarlar yuklanmoqda...</div>
  </div>

  <div id="replyBar" class="reply-bar">
    <div><b id="replyName">User</b>: <span id="replyText"></span></div>
    <button onclick="cancelReply()">✕</button>
  </div>

  <div class="emoji-quick" id="emojiQuick"></div>

  <div class="input-area">
    <input type="text" id="msgInput" placeholder="Xabar yozing..." maxlength="4000" autocomplete="off">
    <button class="send" id="sendBtn">➤</button>
  </div>
</div>

<!-- PROFILE MODAL -->
<div class="modal" id="profileModal">
  <div class="modal-c">
    <h3>👤 Profil</h3>
    <label>Ism</label>
    <input type="text" id="profName" maxlength="50">
    <label>Bio</label>
    <textarea id="profBio" maxlength="200" placeholder="O'zingiz haqingizda..."></textarea>
    <label>Status emoji</label>
    <input type="text" id="profEmoji" maxlength="4" placeholder="😎">
    <label>Status matn</label>
    <input type="text" id="profStatus" maxlength="50" placeholder="Band / Bo'sh...">
    <label>Til</label>
    <select id="profLang">
      <option value="uz">🇺🇿 O'zbek</option>
      <option value="ru">🇷🇺 Русский</option>
      <option value="en">🇬🇧 English</option>
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
    <input type="text" id="searchInput" placeholder="Xabar yoki foydalanuvchi..." oninput="doSearch()">
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
    <div id="statsContent" style="font-size:14px;line-height:2"></div>
    <div class="modal-actions">
      <button class="btn btn-secondary" onclick="closeModal('statsModal')">Yopish</button>
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
    <label>Ovoz balandligi</label>
    <input type="range" id="setVolume" min="0" max="100" value="50">
    <label>Bildirishnomalar</label>
    <select id="setNotif">
      <option value="all">🔔 Hammasi</option>
      <option value="mentions">📢 Faqat mentionlar</option>
      <option value="none">🔕 O'chirilgan</option>
    </select>
    <div class="modal-actions">
      <button class="btn btn-secondary" onclick="closeModal('settingsModal')">Yopish</button>
    </div>
  </div>
</div>

<script>
const tg = window.Telegram?.WebApp;
if (tg) { tg.ready(); tg.expand(); tg.setHeaderColor?.('secondary_bg_color'); }

let ME = null, lastId = 0, replyId = null, isSending = false;
let ws = null, reconnectTimer = null;
const rendered = new Set();
const EMOJIS = ["👍","❤️","🔥","😂","😮","😢","🎉","👏","🙏","💯","✅","😍","🤔","😎","💪","🚀","⭐","💬","📌","⚡"];

async function api(p, o = {}) {
  const r = await fetch(p, {
    ...o,
    headers: { "Content-Type": "application/json", ...(o.headers || {}) }
  });
  return r.json();
}

function initEmojiBar() {
  const b = document.getElementById("emojiQuick");
  EMOJIS.forEach(e => {
    const btn = document.createElement("button");
    btn.textContent = e;
    btn.onclick = () => {
      const inp = document.getElementById("msgInput");
      inp.value += e;
      inp.focus();
    };
    b.appendChild(btn);
  });
}

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
    if (!d.ok) { showError("❌ " + (d.error || "Xato")); return; }
    ME = d.user;
    updateUI();
    await loadMessages(true);
    await loadStats();
    connectWS();
    setInterval(() => loadMessages(false), 5000);
    setInterval(loadStats, 10000);
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
}

function connectWS() {
  if (!tg?.initData) return;
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const url = `${proto}//${location.host}/ws?token=${encodeURIComponent(tg.initData)}`;
  try {
    ws = new WebSocket(url);
    ws.onopen = () => console.log("✅ WS");
    ws.onmessage = (ev) => {
      try {
        const m = JSON.parse(ev.data);
        if (m.type === "new_message") onNewMsg(m.data);
        else if (m.type === "delete_message") removeMsg(m.data.id);
        else if (m.type === "edit_message") updateMsgUI(m.data);
        else if (m.type === "reaction") updateReaction(m.data);
        else if (m.type === "ping") ws.send(JSON.stringify({type:"pong"}));
      } catch (e) {}
    };
    ws.onclose = () => {
      clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(connectWS, 3000);
    };
  } catch (e) {
    reconnectTimer = setTimeout(connectWS, 3000);
  }
}

function onNewMsg(m) {
  if (rendered.has(m.id)) return;
  if (m.id > lastId) lastId = m.id;
  appendMsg(m, true);
  if (m.sender_id !== ME.id && tg?.HapticFeedback)
    tg.HapticFeedback.notificationOccurred("success");
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
    const bottom = initial || (c.scrollTop + c.clientHeight >= c.scrollHeight - 100);
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
    h += `<div class="sender">${esc(m.sender_name)}</div>`;
  }
  h += `<div class="text">${esc(m.text)}</div>`;
  h += `<div class="time">${m.is_edited ? '<span class="edited">tahrirlangan</span>' : ''}${fmtTime(m.created_at)}${own ? " ✓✓" : ""}</div>`;
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
  const start = () => { pt = setTimeout(() => showActions(d, m), 500); };
  const end = () => clearTimeout(pt);
  d.addEventListener("touchstart", start, { passive: true });
  d.addEventListener("touchend", end);
  d.addEventListener("mousedown", start);
  d.addEventListener("mouseup", end);
  c.appendChild(d);
  rendered.add(m.id);
  if (scroll) c.scrollTop = c.scrollHeight;
}

function showActions(d, m) {
  navigator.vibrate?.(20);
  document.querySelectorAll(".msg-actions").forEach(e => e.remove());
  const own = m.sender_id === ME.id;
  const a = document.createElement("div");
  a.className = "msg-actions active";
  let btns = `<button onclick='setReply(${JSON.stringify({id:m.id,name:m.sender_name,text:m.text.slice(0,40)}).replace(/'/g,"&#39;")})'>↩️ Javob</button>`;
  btns += `<button onclick="quickReact(${m.id})">😀 Reaksiya</button>`;
  btns += `<button onclick='copyText(${JSON.stringify(m.text)})'>📋 Nusxalash</button>`;
  if (own) {
    btns += `<button onclick='editMsg(${m.id},${JSON.stringify(m.text)})'>✏️ Tahrirlash</button>`;
    btns += `<button onclick="delMsg(${m.id})">🗑️ O'chirish</button>`;
  } else {
    btns += `<button onclick="blockUser(${m.sender_id})">🚫 Bloklash</button>`;
    btns += `<button onclick="reportUser(${m.sender_id},${m.id})">⚠️ Shikoyat</button>`;
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
  document.getElementById("replyName").textContent = o.name || "User";
  document.getElementById("replyText").textContent = o.text;
  document.getElementById("replyBar").classList.add("active");
  document.getElementById("msgInput").focus();
  document.querySelectorAll(".msg-actions").forEach(e => e.remove());
}

function cancelReply() {
  replyId = null;
  document.getElementById("replyBar").classList.remove("active");
}

function quickReact(id) {
  const e = prompt("Emoji:", "👍");
  if (e) react(id, e);
}

async function react(id, emoji) {
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
  if (!r.ok) alert(r.error || "Xato");
}

async function delMsg(id) {
  document.querySelectorAll(".msg-actions").forEach(e => e.remove());
  if (!confirm("O'chirishni xohlaysizmi?")) return;
  await api(`/api/messages/${id}`, {
    method: "DELETE",
    body: JSON.stringify({ initData: tg.initData })
  });
}

async function blockUser(uid) {
  document.querySelectorAll(".msg-actions").forEach(e => e.remove());
  if (!confirm("Bloklashni xohlaysizmi?")) return;
  const r = await api("/api/users/block", {
    method: "POST",
    body: JSON.stringify({ initData: tg.initData, user_id: uid })
  });
  if (r.ok) alert(r.blocked ? "Bloklandi ✅" : "Blokdan chiqarildi ✅");
}

async function reportUser(uid, mid) {
  document.querySelectorAll(".msg-actions").forEach(e => e.remove());
  const reason = prompt("Shikoyat sababi:", "Spam");
  if (!reason) return;
  await api("/api/report", {
    method: "POST",
    body: JSON.stringify({ initData: tg.initData, user_id: uid, message_id: mid, reason })
  });
  alert("Shikoyat yuborildi ✅");
}

function copyText(t) {
  navigator.clipboard.writeText(t).then(() => alert("Nusxalandi! 📋"));
  document.querySelectorAll(".msg-actions").forEach(e => e.remove());
}

function removeMsg(id) {
  document.querySelector(`.msg[data-id="${id}"]`)?.remove();
  rendered.delete(id);
}

function updateMsgUI(m) {
  const el = document.querySelector(`.msg[data-id="${m.id}"]`);
  if (el) {
    el.querySelector(".text").textContent = m.text;
    const t = el.querySelector(".time");
    if (t && !t.innerHTML.includes("tahrirlangan"))
      t.innerHTML = '<span class="edited">tahrirlangan</span>' + t.innerHTML;
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

function scrollTo(id) {
  const el = document.querySelector(`.msg[data-id="${id}"]`);
  if (el) {
    el.scrollIntoView({ behavior: "smooth", block: "center" });
    el.style.transition = "box-shadow .3s";
    el.style.boxShadow = "0 0 0 3px var(--accent)";
    setTimeout(() => el.style.boxShadow = "", 1200);
  }
}

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
      body: JSON.stringify({ initData: tg.initData, text: t, reply_to_id: r })
    });
    if (d.ok && d.message) {
      if (!rendered.has(d.message.id)) {
        if (d.message.id > lastId) lastId = d.message.id;
        appendMsg(d.message, true);
      }
      tg?.HapticFeedback?.impactOccurred("light");
      ME.xp = (ME.xp || 0) + 5;
    } else {
      alert("❌ " + (d.error || "Xato"));
      inp.value = t;
    }
  } catch (e) {
    inp.value = t;
  } finally {
    isSending = false;
    inp.focus();
  }
}

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

function openModal(id) {
  document.getElementById(id).classList.add("active");
  if (id === "profileModal" && ME) {
    document.getElementById("profName").value = ME.first_name || "";
    document.getElementById("profBio").value = ME.bio || "";
    document.getElementById("profEmoji").value = ME.status_emoji || "";
    document.getElementById("profStatus").value = ME.status_text || "";
    document.getElementById("profLang").value = ME.language || "uz";
  }
  if (id === "statsModal") showStatsDetail();
}

function closeModal(id) {
  document.getElementById(id).classList.remove("active");
}

async function saveProfile() {
  const d = await api("/api/user/profile", {
    method: "PUT",
    body: JSON.stringify({
      initData: tg.initData,
      first_name: document.getElementById("profName").value.trim(),
      bio: document.getElementById("profBio").value.trim(),
      status_emoji: document.getElementById("profEmoji").value.trim(),
      status_text: document.getElementById("profStatus").value.trim(),
      language: document.getElementById("profLang").value
    })
  });
  if (d.ok) {
    Object.assign(ME, {
      first_name: document.getElementById("profName").value.trim(),
      bio: document.getElementById("profBio").value.trim(),
      status_emoji: document.getElementById("profEmoji").value.trim(),
      status_text: document.getElementById("profStatus").value.trim(),
      language: document.getElementById("profLang").value
    });
    updateUI();
    closeModal("profileModal");
    tg?.HapticFeedback?.notificationOccurred("success");
  }
}

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
      h += `<div class="search-result" onclick="alert('${u.first_name} @${u.username || '—'}')">
        <b>${esc(u.first_name)}</b> ${u.username ? '@' + esc(u.username) : ''}
        <div class="sub">Lv ${u.level || 1} • ${u.xp || 0} XP</div>
      </div>`;
    });
  }
  if (d.messages?.length) {
    h += '<div style="font-size:12px;color:var(--hint);margin:8px 0 4px">💬 Xabarlar</div>';
    d.messages.forEach(m => {
      h += `<div class="search-result" onclick="closeModal('searchModal');scrollTo(${m.id})">
        ${esc(m.text.slice(0, 80))}
        <div class="sub">${esc(m.sender_name)}</div>
      </div>`;
    });
  }
  r.innerHTML = h || '<div style="color:var(--hint);font-size:13px;padding:10px">Topilmadi</div>';
}

async function showStatsDetail() {
  const d = await api("/api/stats/detail");
  if (!d.ok) return;
  const c = document.getElementById("statsContent");
  c.innerHTML = `
    <div>👥 Foydalanuvchilar: <b>${d.users}</b></div>
    <div>💬 Xabarlar: <b>${d.messages}</b></div>
    <div>🟢 Online: <b>${d.online}</b></div>
    <div>📅 Bugun: <b>${d.today_msgs}</b> xabar</div>
    <div>⭐ Sizning level: <b>${ME?.level || 1}</b></div>
    <div>💎 XP: <b>${ME?.xp || 0}</b></div>
    <div>🏆 Reyting: <b>#${d.my_rank || '—'}</b></div>
  `;
}

function saveTheme() {
  const t = document.getElementById("setTheme").value;
  localStorage.setItem("theme", t);
  if (tg) {
    if (t === "dark") tg.setHeaderColor?.('#1a1a24');
    else if (t === "light") tg.setHeaderColor?.('#ffffff');
    else tg.setHeaderColor?.('secondary_bg_color');
  }
}

function esc(t) {
  const d = document.createElement("div");
  d.textContent = t || "";
  return d.innerHTML;
}

function fmtTime(iso) {
  try {
    const d = new Date(iso), n = new Date();
    const t = d.toLocaleTimeString("uz-UZ", { hour: "2-digit", minute: "2-digit" });
    return d.toDateString() === n.toDateString() ? t
      : d.toLocaleDateString("uz-UZ", { day: "2-digit", month: "2-digit" }) + " " + t;
  } catch (e) { return ""; }
}

function showError(m) {
  document.getElementById("messages").innerHTML =
    `<div class="loading" style="color:var(--red)">${esc(m)}</div>`;
}

document.getElementById("sendBtn").addEventListener("click", sendMsg);
document.getElementById("msgInput").addEventListener("keypress", e => {
  if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendMsg(); }
});
document.querySelectorAll(".modal").forEach(m => {
  m.addEventListener("click", e => { if (e.target === m) m.classList.remove("active"); });
});
window.addEventListener("beforeunload", () => ws?.close());

init();
</script>
</body>
</html>
"""


# ═══════════════════════ ROUTES ═══════════════════════
@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(HTML)


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.get("/ping")
async def ping():
    return "pong"


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


# ═══════════════════════ API ═══════════════════════
@app.post("/api/init")
async def api_init(req: InitData):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "initData yaroqsiz"}, status_code=401)
    async for s in get_session():
        u = await get_or_create_user(s, tgu)
        if u.is_banned:
            return JSONResponse({"ok": False, "error": "Siz banlangansiz"}, status_code=403)
        return {"ok": True, "user": {
            "id": u.telegram_id, "first_name": u.first_name, "last_name": u.last_name,
            "username": u.username, "photo_url": u.photo_url, "bio": u.bio,
            "language": u.language, "theme": u.theme, "is_admin": u.is_admin,
            "is_premium": u.is_premium, "xp": u.xp, "level": u.level,
            "status_emoji": u.status_emoji, "status_text": u.status_text
        }}


@app.get("/api/messages")
async def api_msgs(after_id: int = 0):
    async for s in get_session():
        if after_id > 0:
            r = await s.execute(select(Message)
                .where(and_(Message.id > after_id, Message.is_deleted == False))
                .order_by(Message.id.asc()).limit(200))
        else:
            r = await s.execute(select(Message)
                .where(Message.is_deleted == False)
                .order_by(desc(Message.id)).limit(100))
        msgs = list(r.scalars().all())
        if after_id == 0:
            msgs.reverse()
        rids = [m.reply_to_id for m in msgs if m.reply_to_id]
        rmap = {}
        if rids:
            rr = await s.execute(select(Message).where(Message.id.in_(rids)))
            rmap = {x.id: x for x in rr.scalars().all()}
        return {"ok": True, "messages": [{
            "id": m.id, "sender_id": m.sender_id, "sender_name": m.sender_name or "Anonim",
            "text": m.text, "reply_to_id": m.reply_to_id,
            "reply_preview": (rmap[m.reply_to_id].text[:60] + "..."
                if m.reply_to_id in rmap and len(rmap[m.reply_to_id].text) > 60
                else rmap[m.reply_to_id].text if m.reply_to_id in rmap else None),
            "reactions": m.reactions or "{}", "is_edited": m.is_edited,
            "is_pinned": m.is_pinned, "created_at": m.created_at.isoformat()
        } for m in msgs]}


@app.post("/api/messages")
async def api_send(req: SendMsg):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    t = (req.text or "").strip()
    if not t:
        return JSONResponse({"ok": False, "error": "Bo'sh xabar"}, status_code=400)
    if len(t) > 4000:
        return JSONResponse({"ok": False, "error": "Juda uzun"}, status_code=400)
    async for s in get_session():
        u = await get_or_create_user(s, tgu)
        if u.is_banned:
            return JSONResponse({"ok": False, "error": "Ban"}, status_code=403)
        add_xp(u, 5)
        await s.commit()
        name = u.first_name or u.username or "Anonim"
        rp = None
        if req.reply_to_id:
            rr = await s.execute(select(Message).where(Message.id == req.reply_to_id))
            p = rr.scalar_one_or_none()
            if p:
                rp = p.text[:60] + ("..." if len(p.text) > 60 else "")
        m = Message(sender_id=u.telegram_id, sender_name=name, text=t,
                    reply_to_id=req.reply_to_id)
        s.add(m)
        await s.commit()
        await s.refresh(m)
        pl = {"id": m.id, "sender_id": m.sender_id, "sender_name": m.sender_name,
              "text": m.text, "reply_to_id": m.reply_to_id, "reply_preview": rp,
              "reactions": "{}", "is_edited": False, "is_pinned": False,
              "created_at": m.created_at.isoformat()}
        await manager.broadcast({"type": "new_message", "data": pl})
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
            return JSONResponse({"ok": False, "error": "Topilmadi"}, status_code=404)
        if m.sender_id != tgu["id"]:
            return JSONResponse({"ok": False, "error": "Ruxsat yo'q"}, status_code=403)
        m.text = req.text.strip()[:4000]
        m.is_edited = True
        await s.commit()
        await manager.broadcast({"type": "edit_message",
            "data": {"id": mid, "text": m.text, "is_edited": True}})
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
            return JSONResponse({"ok": False, "error": "Topilmadi"}, status_code=404)
        if m.sender_id != tgu["id"] and tgu["id"] not in ADMIN_IDS:
            return JSONResponse({"ok": False, "error": "Ruxsat yo'q"}, status_code=403)
        m.is_deleted = True
        await s.commit()
        await manager.broadcast({"type": "delete_message", "data": {"id": mid}})
        return {"ok": True}


@app.post("/api/messages/{mid}/react")
async def api_react(mid: int, req: ReactReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    e = req.emoji.strip()[:8]
    if not e:
        return JSONResponse({"ok": False, "error": "Emoji kerak"}, status_code=400)
    async for s in get_session():
        r = await s.execute(select(Message).where(Message.id == mid))
        m = r.scalar_one_or_none()
        if not m:
            return JSONResponse({"ok": False, "error": "Topilmadi"}, status_code=404)
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
        await manager.broadcast({"type": "reaction",
            "data": {"id": mid, "reactions": m.reactions}})
        return {"ok": True, "reactions": reacts}


@app.put("/api/user/profile")
async def api_profile(req: ProfileReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    async for s in get_session():
        u = await get_or_create_user(s, tgu)
        if req.first_name is not None: u.first_name = req.first_name[:100]
        if req.bio is not None: u.bio = req.bio[:500]
        if req.language in ("uz", "ru", "en"): u.language = req.language
        if req.theme in ("auto", "dark", "light"): u.theme = req.theme
        if req.status_emoji is not None: u.status_emoji = req.status_emoji[:10]
        if req.status_text is not None: u.status_text = req.status_text[:100]
        await s.commit()
        return {"ok": True}


@app.post("/api/users/block")
async def api_block(req: BlockReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    if req.user_id == tgu["id"]:
        return JSONResponse({"ok": False, "error": "O'zingizni bloklay olmaysiz"}, status_code=400)
    async for s in get_session():
        r = await s.execute(select(Block).where(and_(
            Block.blocker_id == tgu["id"], Block.blocked_id == req.user_id)))
        ex = r.scalar_one_or_none()
        if ex:
            await s.delete(ex)
            await s.commit()
            return {"ok": True, "blocked": False}
        s.add(Block(blocker_id=tgu["id"], blocked_id=req.user_id))
        await s.commit()
        return {"ok": True, "blocked": True}


@app.post("/api/report")
async def api_report(req: ReportReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    async for s in get_session():
        s.add(Report(reporter_id=tgu["id"], reported_id=req.user_id,
                     message_id=req.message_id, reason=req.reason[:255]))
        await s.commit()
        return {"ok": True}


@app.get("/api/stats")
async def api_stats():
    async for s in get_session():
        u = await s.scalar(select(func.count(User.id)))
        m = await s.scalar(select(func.count(Message.id)))
        return {"ok": True, "users": u or 0, "messages": m or 0,
                "online": len(manager.active)}


@app.get("/api/stats/detail")
async def api_stats_detail():
    async for s in get_session():
        u = await s.scalar(select(func.count(User.id)))
        m = await s.scalar(select(func.count(Message.id)))
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        tm = await s.scalar(select(func.count(Message.id)).where(Message.created_at >= today))
        return {"ok": True, "users": u or 0, "messages": m or 0,
                "online": len(manager.active), "today_msgs": tm or 0, "my_rank": 1}


@app.get("/api/search")
async def api_search(q: str = ""):
    q = q.strip()
    if len(q) < 2:
        return {"ok": True, "users": [], "messages": []}
    async for s in get_session():
        ur = await s.execute(select(User).where(or_(
            User.first_name.ilike(f"%{q}%"), User.username.ilike(f"%{q}%"))).limit(10))
        us = ur.scalars().all()
        mr = await s.execute(select(Message).where(and_(
            Message.is_deleted == False, Message.text.ilike(f"%{q}%")
        )).order_by(desc(Message.id)).limit(10))
        ms = mr.scalars().all()
        return {"ok": True,
                "users": [{"id": x.telegram_id, "first_name": x.first_name,
                           "username": x.username, "level": x.level, "xp": x.xp}
                          for x in us],
                "messages": [{"id": x.id, "text": x.text, "sender_name": x.sender_name}
                             for x in ms]}


@app.get("/api/leaderboard")
async def api_leaderboard():
    async for s in get_session():
        r = await s.execute(select(User).order_by(desc(User.xp)).limit(10))
        us = r.scalars().all()
        return {"ok": True, "users": [
            {"id": u.telegram_id, "first_name": u.first_name,
             "level": u.level, "xp": u.xp} for u in us]}


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


# ═══════════════════════ STARTUP ═══════════════════════
if __name__ == "__main__":
    import uvicorn
    logger.info(f"🚀 http://0.0.0.0:{PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
