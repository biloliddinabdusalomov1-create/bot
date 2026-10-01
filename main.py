"""Telegram-ga o'xshash shaxsiy chat Mini App"""
import os, json, logging, asyncio, hashlib, hmac, uuid
from datetime import datetime, timedelta
from typing import Optional, Dict
from urllib.parse import parse_qsl
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart, Command
from aiogram.types import WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton, MenuButtonWebApp

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from sqlalchemy import (String, Text, DateTime, BigInteger, Integer, Boolean,
                        select, desc, func, or_, and_)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("chat")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "").rstrip("/")
WEBHOOK_PATH = "/webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "secret123")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./chat.db")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
PORT = int(os.getenv("PORT", "8000"))
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "./uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    username: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    photo_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    bio: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    is_online: Mapped[bool] = mapped_column(Boolean, default=False)
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    is_banned: Mapped[bool] = mapped_column(Boolean, default=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    xp: Mapped[int] = mapped_column(Integer, default=0)
    level: Mapped[int] = mapped_column(Integer, default=1)
    coins: Mapped[int] = mapped_column(Integer, default=0)
    status_emoji: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    status_text: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Chat(Base):
    __tablename__ = "chats"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user1_id: Mapped[int] = mapped_column(BigInteger, index=True)
    user2_id: Mapped[int] = mapped_column(BigInteger, index=True)
    chat_key: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    last_message_text: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    last_message_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Message(Base):
    __tablename__ = "messages"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(Integer, index=True)
    sender_id: Mapped[int] = mapped_column(BigInteger, index=True)
    text: Mapped[str] = mapped_column(Text)
    reply_to_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_edited: Mapped[bool] = mapped_column(Boolean, default=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False)
    reactions: Mapped[Optional[str]] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_session():
    async with async_session() as session:
        yield session


class Manager:
    def __init__(self):
        self.active: Dict[int, set] = {}

    async def connect(self, uid, ws):
        await ws.accept()
        self.active.setdefault(uid, set()).add(ws)

    def disconnect(self, uid, ws):
        if uid in self.active:
            self.active[uid].discard(ws)
            if not self.active[uid]:
                del self.active[uid]

    async def send_to(self, uid, msg):
        if uid not in self.active:
            return
        data = json.dumps(msg, default=str)
        for ws in list(self.active[uid]):
            try:
                await ws.send_text(data)
            except Exception:
                self.disconnect(uid, ws)

    async def send_to_many(self, uids, msg):
        for uid in uids:
            await self.send_to(uid, msg)


manager = Manager()


def verify_init_data(init_data: str) -> Optional[dict]:
    if not init_data or not BOT_TOKEN:
        return None
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        received = parsed.pop("hash", None)
        if not received:
            return None
        dcs = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        sk = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        ch = hmac.new(sk, dcs.encode(), hashlib.sha256).hexdigest()
        if ch != received:
            return None
        ad = int(parsed.get("auth_date", "0"))
        if ad and (datetime.utcnow().timestamp() - ad) > 86400:
            return None
        uj = parsed.get("user")
        return json.loads(uj) if uj else None
    except Exception:
        return None


async def get_or_create_user(s, tgu):
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


def user_dict(u):
    return {"id": u.telegram_id, "first_name": u.first_name, "last_name": u.last_name,
            "username": u.username, "photo_url": u.photo_url, "bio": u.bio,
            "is_online": u.is_online, "is_admin": u.is_admin, "xp": u.xp,
            "level": u.level, "coins": u.coins, "status_emoji": u.status_emoji,
            "status_text": u.status_text,
            "last_seen": u.last_seen.isoformat() if u.last_seen else None}


def msg_dict(m, rp=None):
    return {"id": m.id, "chat_id": m.chat_id, "sender_id": m.sender_id,
            "text": m.text, "reply_to_id": m.reply_to_id, "reply_preview": rp,
            "reactions": m.reactions or "{}", "is_edited": m.is_edited,
            "is_read": m.is_read, "created_at": m.created_at.isoformat()}


bot = None
dp = Dispatcher() if BOT_TOKEN else None

if dp is not None:
    @dp.message(CommandStart())
    async def cmd_start(message: types.Message):
        if not WEBAPP_URL:
            await message.answer("WebApp URL yo'q")
            return
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="💬 Chatni ochish", web_app=WebAppInfo(url=WEBAPP_URL))
        ]])
        await message.answer(
            f"👋 Salom, <b>{message.from_user.first_name}</b>!\n\n"
            "🚀 Chat Mini App'ga xush kelibsiz!",
            reply_markup=kb, parse_mode="HTML")

    @dp.message(Command("stats"))
    async def cmd_stats(message: types.Message):
        async for s in get_session():
            u = await s.scalar(select(func.count(User.id)))
            m = await s.scalar(select(func.count(Message.id)))
            await message.answer(
                f"👥 {u or 0} | 💬 {m or 0} | 🟢 {len(manager.active)}",
                parse_mode="HTML")


@asynccontextmanager
async def lifespan(app):
    global bot
    await init_db()
    if BOT_TOKEN and WEBAPP_URL:
        bot = Bot(token=BOT_TOKEN)
        try:
            await bot.set_webhook(url=f"{WEBAPP_URL}{WEBHOOK_PATH}",
                                  secret_token=WEBHOOK_SECRET,
                                  drop_pending_updates=True,
                                  allowed_updates=dp.resolve_used_update_types() if dp else None)
            await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(
                text="💬 Chat", web_app=WebAppInfo(url=WEBAPP_URL)))
            logger.info(f"✅ Webhook: {WEBAPP_URL}{WEBHOOK_PATH}")
        except Exception as e:
            logger.error(f"Bot setup: {e}")
    yield
    if bot:
        try:
            await bot.delete_webhook()
        except Exception:
            pass
        await bot.session.close()


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")


class InitReq(BaseModel):
    initData: str


class ProfileReq(BaseModel):
    initData: str
    first_name: Optional[str] = None
    bio: Optional[str] = None
    status_emoji: Optional[str] = None
    status_text: Optional[str] = None


class OpenChatReq(BaseModel):
    initData: str
    user_id: int


class SendMsgReq(BaseModel):
    initData: str
    text: str
    reply_to_id: Optional[int] = None


class ReactReq(BaseModel):
    initData: str
    emoji: str


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="uz">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,user-scalable=no,viewport-fit=cover">
<meta name="theme-color" content="#0a0a0f">
<title>Chat</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
:root{
--bg:#0a0a0f;--bg1:#13131a;--bg2:#1c1c26;--bg3:#252532;
--border:rgba(255,255,255,.08);--text:#fff;--text2:#a1a1aa;--text3:#71717a;
--accent:#8b5cf6;--accent2:#ec4899;--grad:linear-gradient(135deg,#8b5cf6,#ec4899);
--green:#22c55e;--red:#ef4444;--yellow:#f59e0b;--radius:16px;
}
body{font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
background:var(--bg);color:var(--text);height:100vh;overflow:hidden;font-size:15px;
-webkit-font-smoothing:antialiased}
.app{display:flex;flex-direction:column;height:100vh;max-width:480px;
margin:0 auto;position:relative;background:var(--bg);overflow:hidden}
.screen{position:absolute;inset:0;display:flex;flex-direction:column;background:var(--bg);
transition:transform .35s cubic-bezier(.4,0,.2,1)}
.screen.hidden{transform:translateX(100%)}
.header{display:flex;align-items:center;gap:12px;padding:14px 16px;
background:rgba(19,19,26,.85);backdrop-filter:blur(20px);
border-bottom:1px solid var(--border);flex-shrink:0;min-height:64px;z-index:10}
.back-btn{background:none;border:none;color:var(--text);font-size:22px;cursor:pointer;
padding:8px;border-radius:12px;display:flex;align-items:center;justify-content:center;
width:40px;height:40px;transition:background .15s}
.back-btn:active{background:rgba(255,255,255,.1);transform:scale(.92)}
.header-title{font-size:17px;font-weight:600;flex:1;letter-spacing:-.3px}
.icon-btn{width:40px;height:40px;border:none;background:rgba(255,255,255,.06);
border-radius:12px;font-size:18px;cursor:pointer;color:var(--text);
display:flex;align-items:center;justify-content:center;transition:all .15s}
.icon-btn:active{background:rgba(255,255,255,.12);transform:scale(.92)}
.avatar{width:48px;height:48px;border-radius:50%;background:var(--grad);
display:flex;align-items:center;justify-content:center;font-weight:700;font-size:18px;
color:#fff;flex-shrink:0;position:relative;overflow:hidden;
box-shadow:0 4px 12px rgba(139,92,246,.3)}
.avatar img{width:100%;height:100%;object-fit:cover}
.avatar.sm{width:42px;height:42px;font-size:16px}
.avatar.lg{width:88px;height:88px;font-size:34px}
.online-dot{position:absolute;bottom:1px;right:1px;width:13px;height:13px;
background:var(--green);border-radius:50%;border:2.5px solid var(--bg1);
box-shadow:0 0 8px rgba(34,197,94,.5)}
.list{flex:1;overflow-y:auto;padding:8px 0}
.chat-item{display:flex;gap:14px;padding:14px 18px;cursor:pointer;
transition:background .12s;align-items:center}
.chat-item:active{background:rgba(139,92,246,.1)}
.ci-info{flex:1;min-width:0}
.ci-top{display:flex;justify-content:space-between;align-items:center;
margin-bottom:5px;gap:8px}
.ci-name{font-weight:600;font-size:15.5px;white-space:nowrap;overflow:hidden;
text-overflow:ellipsis;letter-spacing:-.2px}
.ci-time{font-size:12px;color:var(--text3);flex-shrink:0}
.ci-bottom{display:flex;justify-content:space-between;align-items:center;gap:10px}
.ci-last{font-size:13.5px;color:var(--text2);white-space:nowrap;overflow:hidden;
text-overflow:ellipsis;flex:1}
.unread{background:var(--grad);color:#fff;font-size:11.5px;font-weight:700;
min-width:22px;height:22px;border-radius:11px;display:flex;align-items:center;
justify-content:center;padding:0 7px;flex-shrink:0;
box-shadow:0 2px 8px rgba(139,92,246,.5)}
.empty{display:flex;flex-direction:column;align-items:center;justify-content:center;
min-height:100%;padding:60px 30px;text-align:center}
.empty-icon{width:120px;height:120px;border-radius:50%;
background:radial-gradient(circle,rgba(139,92,246,.2) 0%,transparent 70%);
display:flex;align-items:center;justify-content:center;font-size:56px;
margin-bottom:24px;animation:float 3s ease-in-out infinite}
@keyframes float{0%,100%{transform:translateY(0)}50%{transform:translateY(-10px)}}
.empty h2{font-size:20px;font-weight:700;color:var(--text);margin-bottom:10px;
letter-spacing:-.3px}
.empty p{font-size:14px;line-height:1.6;color:var(--text2);max-width:300px;
margin-bottom:28px}
.btn-primary{padding:14px 28px;border:none;border-radius:14px;background:var(--grad);
color:#fff;font-size:15px;font-weight:600;cursor:pointer;font-family:inherit;
box-shadow:0 8px 24px rgba(139,92,246,.4);transition:all .2s;
display:inline-flex;align-items:center;gap:8px}
.btn-primary:active{transform:scale(.96)}
.fab{position:absolute;bottom:24px;right:20px;width:60px;height:60px;
border-radius:50%;background:var(--grad);border:none;color:#fff;font-size:28px;
cursor:pointer;box-shadow:0 10px 30px rgba(139,92,246,.5);
display:flex;align-items:center;justify-content:center;z-index:20;
transition:transform .15s}
.fab:active{transform:scale(.9)}
.search-wrap{padding:14px 18px 8px;position:relative}
.search-input{width:100%;padding:14px 18px 14px 46px;border:none;border-radius:14px;
background:var(--bg1);color:var(--text);font-size:15px;outline:none;
font-family:inherit;border:1px solid var(--border);transition:all .2s}
.search-input:focus{border-color:var(--accent);background:var(--bg2);
box-shadow:0 0 0 4px rgba(139,92,246,.15)}
.search-icon{position:absolute;left:32px;top:29px;font-size:16px;
color:var(--text3);pointer-events:none}
.search-hint{padding:16px 24px;font-size:13px;color:var(--text3);
text-align:center;line-height:1.7}
.user-item{display:flex;gap:14px;padding:14px 18px;cursor:pointer;
align-items:center;transition:background .12s}
.user-item:active{background:rgba(139,92,246,.1)}
.ui-info{flex:1;min-width:0}
.ui-name{font-weight:600;font-size:15px;margin-bottom:3px;letter-spacing:-.2px}
.ui-sub{font-size:12.5px;color:var(--text2)}
.messages{flex:1;overflow-y:auto;padding:18px 16px;display:flex;
flex-direction:column;gap:4px;
background-image:radial-gradient(circle at 20% 10%,rgba(139,92,246,.06),transparent 40%),
radial-gradient(circle at 80% 90%,rgba(236,72,153,.05),transparent 40%)}
.msg{max-width:80%;padding:10px 14px;border-radius:18px;font-size:14.5px;
line-height:1.5;word-wrap:break-word;word-break:break-word;position:relative;
animation:msgIn .25s cubic-bezier(.4,0,.2,1);letter-spacing:-.1px}
@keyframes msgIn{from{opacity:0;transform:translateY(8px) scale(.97)}
to{opacity:1;transform:translateY(0) scale(1)}}
.msg.own{align-self:flex-end;background:var(--grad);color:#fff;
border-bottom-right-radius:6px;box-shadow:0 4px 12px rgba(139,92,246,.25)}
.msg.other{align-self:flex-start;background:var(--bg2);color:var(--text);
border-bottom-left-radius:6px}
.msg .text{white-space:pre-wrap;word-break:break-word}
.msg .time{font-size:11px;opacity:.75;margin-top:5px;text-align:right;
display:flex;justify-content:flex-end;align-items:center;gap:5px;font-weight:500}
.msg.other .time{color:var(--text3)}
.msg .reply-prev{font-size:12px;border-left:3px solid rgba(255,255,255,.6);
padding:6px 10px;margin-bottom:6px;border-radius:8px;background:rgba(0,0,0,.2);
cursor:pointer;opacity:.9}
.msg.other .reply-prev{border-color:var(--accent);background:rgba(139,92,246,.12)}
.rp-name{font-weight:700;font-size:11px;margin-bottom:2px;opacity:.95}
.msg .reactions{display:flex;gap:4px;margin-top:6px;flex-wrap:wrap}
.reaction{background:rgba(255,255,255,.15);padding:3px 9px;border-radius:11px;
font-size:12px;cursor:pointer;user-select:none;display:inline-flex;
align-items:center;gap:4px;transition:transform .12s;font-weight:500}
.reaction:active{transform:scale(.92)}
.reaction.mine{background:rgba(255,255,255,.3);box-shadow:0 0 0 1.5px rgba(255,255,255,.5)}
.msg.other .reaction{background:rgba(139,92,246,.2)}
.msg.other .reaction.mine{background:rgba(139,92,246,.45)}
.msg-actions{position:absolute;top:100%;right:0;background:var(--bg2);
border-radius:14px;box-shadow:0 12px 40px rgba(0,0,0,.7);display:none;
flex-direction:column;padding:6px;z-index:200;min-width:180px;margin-top:6px;
border:1px solid var(--border)}
.msg-actions.active{display:flex;animation:popIn .15s ease}
@keyframes popIn{from{opacity:0;transform:scale(.9)}to{opacity:1;transform:scale(1)}}
.msg-actions button{background:none;border:none;font-size:14px;padding:10px 14px;
cursor:pointer;border-radius:10px;text-align:left;color:var(--text);
display:flex;align-items:center;gap:12px;font-family:inherit;font-weight:500;
transition:background .12s}
.msg-actions button:active{background:rgba(139,92,246,.25)}
.msg-actions button.danger:active{background:rgba(239,68,68,.25)}
.input-area{display:flex;gap:10px;padding:12px 16px;
background:rgba(19,19,26,.95);backdrop-filter:blur(20px);
border-top:1px solid var(--border);padding-bottom:max(12px,env(safe-area-inset-bottom));
align-items:flex-end;flex-shrink:0}
.input-wrap{flex:1;display:flex;align-items:center;background:var(--bg1);
border-radius:22px;padding:4px 6px 4px 16px;border:1px solid var(--border);
transition:all .2s}
.input-wrap:focus-within{border-color:var(--accent);
box-shadow:0 0 0 4px rgba(139,92,246,.12)}
.input-wrap input{flex:1;padding:10px 6px;border:none;background:transparent;
color:var(--text);font-size:15px;outline:none;font-family:inherit;min-width:0}
.input-wrap input::placeholder{color:var(--text3)}
.send{width:48px;height:48px;border-radius:50%;background:var(--grad);
border:none;color:#fff;font-size:18px;cursor:pointer;flex-shrink:0;
display:flex;align-items:center;justify-content:center;
box-shadow:0 4px 16px rgba(139,92,246,.45);transition:transform .12s}
.send:active{transform:scale(.88)}
.emoji-bar{display:flex;gap:4px;padding:8px 12px;background:var(--bg1);
overflow-x:auto;border-top:1px solid var(--border);scrollbar-width:none;flex-shrink:0}
.emoji-bar::-webkit-scrollbar{display:none}
.emoji-bar button{background:transparent;border:none;font-size:22px;cursor:pointer;
padding:6px 8px;border-radius:10px;flex-shrink:0;transition:all .12s;line-height:1}
.emoji-bar button:active{background:rgba(139,92,246,.2);transform:scale(1.2)}
.reply-bar{display:none;background:var(--bg1);border-left:3px solid var(--accent);
padding:10px 16px;margin:0 16px 6px;border-radius:12px;font-size:13px;
justify-content:space-between;align-items:center;animation:slideIn .2s ease}
@keyframes slideIn{from{opacity:0;transform:translateY(-6px)}
to{opacity:1;transform:translateY(0)}}
.reply-bar.active{display:flex}
.reply-bar .info{flex:1;min-width:0}
.reply-bar b{color:var(--accent);display:block;font-size:12px;margin-bottom:2px}
.reply-bar .rtext{opacity:.85;white-space:nowrap;overflow:hidden;
text-overflow:ellipsis;font-size:12.5px}
.reply-bar button{background:none;border:none;color:var(--text2);font-size:22px;
cursor:pointer;padding:0 6px;line-height:1}
.profile-content{padding:24px;overflow-y:auto;flex:1}
.profile-avatar-wrap{display:flex;flex-direction:column;align-items:center;
margin-bottom:28px}
.profile-name{font-size:20px;font-weight:700;margin-top:14px;letter-spacing:-.3px}
.profile-username{font-size:13.5px;color:var(--text2);margin-top:4px}
.field{margin-bottom:16px}
.field label{font-size:12.5px;color:var(--text2);display:block;margin-bottom:7px;
font-weight:500;letter-spacing:.2px}
.field input,.field textarea{width:100%;padding:13px 16px;
border:1px solid var(--border);border-radius:12px;background:var(--bg1);
color:var(--text);font-size:14.5px;outline:none;font-family:inherit;
transition:all .2s;resize:none}
.field input:focus,.field textarea:focus{border-color:var(--accent);
background:var(--bg2);box-shadow:0 0 0 4px rgba(139,92,246,.12)}
.toast{position:fixed;bottom:30px;left:50%;transform:translateX(-50%) translateY(100px);
background:var(--bg2);padding:13px 22px;border-radius:14px;font-size:14px;
box-shadow:0 10px 40px rgba(0,0,0,.6);z-index:2000;opacity:0;
transition:all .3s cubic-bezier(.4,0,.2,1);pointer-events:none;max-width:90%;
font-weight:500;border:1px solid var(--border)}
.toast.show{transform:translateX(-50%) translateY(0);opacity:1}
.status-bar{padding:8px 16px;text-align:center;font-size:12px;
background:var(--yellow);color:#000;display:none;font-weight:600}
.status-bar.show{display:block}
.loading{text-align:center;color:var(--text3);padding:60px 20px;font-size:14px;
line-height:1.7}
.messages::-webkit-scrollbar,.list::-webkit-scrollbar{width:5px}
.messages::-webkit-scrollbar-thumb,.list::-webkit-scrollbar-thumb{
background:rgba(255,255,255,.12);border-radius:3px}
@media(max-width:500px){.msg{max-width:85%}}
</style>
</head>
<body>
<div class="app">
  <div class="status-bar" id="statusBar">🔄 Qayta ulanmoqda...</div>

  <div class="screen" id="screenChats">
    <div class="header">
      <div class="avatar" id="myAvatar" onclick="openProfile()"><span id="myInit">?</span></div>
      <div class="header-title" id="myTitle">Chatlar</div>
      <button class="icon-btn" onclick="showSearch()">🔍</button>
    </div>
    <div class="list" id="chatList"><div class="loading">Yuklanmoqda...</div></div>
    <button class="fab" onclick="showSearch()">+</button>
  </div>

  <div class="screen hidden" id="screenSearch">
    <div class="header">
      <button class="back-btn" onclick="hideSearch()">←</button>
      <div class="header-title">Yangi chat</div>
    </div>
    <div class="search-wrap">
      <span class="search-icon">🔍</span>
      <input type="text" id="searchInput" class="search-input" placeholder="@username yoki ism..." oninput="doSearch()" autocomplete="off">
    </div>
    <div class="search-hint" id="searchHint">
      @username, t.me/username yoki ism kiriting<br>
      Faqat botga /start bosgan foydalanuvchilar topiladi
    </div>
    <div class="list" id="searchResults"></div>
  </div>

  <div class="screen hidden" id="screenChat">
    <div class="header">
      <button class="back-btn" onclick="closeChat()">←</button>
      <div class="avatar sm" id="chatAvatar"><span id="chatInit">?</span></div>
      <div style="flex:1;min-width:0">
        <div id="chatName" style="font-size:15.5px;font-weight:600;margin-bottom:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">...</div>
        <div id="chatStatus" style="font-size:12px;color:var(--text2)">...</div>
      </div>
    </div>
    <div class="messages" id="chatMessages"><div class="loading">Yuklanmoqda...</div></div>
    <div id="replyBar" class="reply-bar">
      <div class="info">
        <b id="replyName">Javob</b>
        <div class="rtext" id="replyText"></div>
      </div>
      <button onclick="cancelReply()">✕</button>
    </div>
    <div class="emoji-bar" id="emojiBar"></div>
    <div class="input-area">
      <div class="input-wrap">
        <input type="text" id="msgInput" placeholder="Xabar yozing..." maxlength="4000" autocomplete="off">
      </div>
      <button class="send" id="sendBtn">➤</button>
    </div>
  </div>

  <div class="screen hidden" id="screenProfile">
    <div class="header">
      <button class="back-btn" onclick="closeProfile()">←</button>
      <div class="header-title">Profil</div>
    </div>
    <div class="profile-content">
      <div class="profile-avatar-wrap">
        <div class="avatar lg" id="pfAvatar"><span id="pfInit">?</span></div>
        <div class="profile-name" id="pfNameDisplay">...</div>
        <div class="profile-username" id="pfUsernameDisplay">@...</div>
      </div>
      <div class="field"><label>Ism</label><input id="pfName" maxlength="50" placeholder="Ismingiz"></div>
      <div class="field"><label>Bio</label><textarea id="pfBio" maxlength="200" placeholder="O'zingiz haqingizda..." rows="3"></textarea></div>
      <div class="field"><label>Status emoji</label><input id="pfEmoji" maxlength="4" placeholder="😎"></div>
      <div class="field"><label>Status matn</label><input id="pfStatus" maxlength="50" placeholder="Band / Bo'sh..."></div>
      <button class="btn-primary" style="width:100%;justify-content:center" onclick="saveProfile()">💾 Saqlash</button>
    </div>
  </div>

  <div class="toast" id="toast"></div>
</div>

<script>
var tg = window.Telegram ? window.Telegram.WebApp : null;
if (tg) { tg.ready(); tg.expand(); tg.setHeaderColor && tg.setHeaderColor('#0a0a0f'); tg.setBackgroundColor && tg.setBackgroundColor('#0a0a0f'); }

var ME = null, currentChatId = null, currentChatUser = null;
var lastMsgId = 0, replyId = null;
var ws = null, reconnectTimer = null, reconnectAttempts = 0;
var rendered = {};
var EMOJIS = ["\uD83D\uDC4D","\u2764\uFE0F","\uD83D\uDD25","\uD83D\uDE02","\uD83D\uDE2E","\uD83D\uDE22","\uD83C\uDF89","\uD83D\uDC4F","\uD83D\uDE4F","\uD83D\uDCAF","\u2705","\uD83D\uDE0D","\uD83E\uDD14","\uD83D\uDE0E","\uD83D\uDCAA","\uD83D\uDE80","\u2B50","\uD83D\uDCAC","\uD83D\uDCCC","\u26A1"];

function api(path, opts) {
  opts = opts || {};
  return fetch(path, Object.assign({}, opts, {
    headers: Object.assign({"Content-Type": "application/json"}, opts.headers || {})
  })).then(function(r){ return r.json(); }).catch(function(e){
    console.error(e); return { ok: false, error: "Tarmoq xatosi" };
  });
}

function toast(msg, duration) {
  duration = duration || 2500;
  var t = document.getElementById("toast");
  t.textContent = msg;
  t.classList.add("show");
  clearTimeout(t._timer);
  t._timer = setTimeout(function(){ t.classList.remove("show"); }, duration);
}

function haptic(type) {
  try {
    if (!tg || !tg.HapticFeedback) return;
    if (type === "success") tg.HapticFeedback.notificationOccurred("success");
    else if (type === "error") tg.HapticFeedback.notificationOccurred("error");
    else if (type === "medium") tg.HapticFeedback.impactOccurred("medium");
    else tg.HapticFeedback.impactOccurred("light");
  } catch (e) {}
}

function esc(t) {
  var d = document.createElement("div");
  d.textContent = t || "";
  return d.innerHTML;
}

function fmtTime(iso) {
  try {
    var d = new Date(iso), n = new Date();
    var t = d.toLocaleTimeString("uz-UZ", { hour: "2-digit", minute: "2-digit" });
    if (d.toDateString() === n.toDateString()) return t;
    return d.toLocaleDateString("uz-UZ", { day: "2-digit", month: "2-digit" }) + " " + t;
  } catch (e) { return ""; }
}

function getInitials(u) {
  if (!u) return "?";
  return (u.first_name || u.username || "?").charAt(0).toUpperCase();
}

function renderAvatar(u, cls) {
  cls = cls || "";
  var init = getInitials(u);
  var html = '<div class="avatar ' + cls + '">';
  if (u && u.photo_url) html += '<img src="' + u.photo_url + '">';
  else html += '<span>' + init + '</span>';
  if (u && u.is_online) html += '<div class="online-dot"></div>';
  html += '</div>';
  return html;
}

function initEmojiBar() {
  var b = document.getElementById("emojiBar");
  EMOJIS.forEach(function(e){
    var btn = document.createElement("button");
    btn.textContent = e;
    btn.onclick = function(){
      var inp = document.getElementById("msgInput");
      inp.value += e;
      inp.focus();
      haptic();
    };
    b.appendChild(btn);
  });
}

function init() {
  if (!tg || !tg.initData) {
    document.body.innerHTML = '<div style="padding:60px 24px;text-align:center;color:#a1a1aa;font-family:Inter,sans-serif"><div style="font-size:64px;margin-bottom:20px">📱</div><h2 style="color:#fff;margin-bottom:12px">Telegram orqali oching</h2><p>Bu ilova faqat Telegram Mini App sifatida ishlaydi</p></div>';
    return;
  }
  initEmojiBar();
  api("/api/init", { method: "POST", body: JSON.stringify({ initData: tg.initData }) })
    .then(function(d){
      if (!d.ok) { toast(d.error || "Xato"); return; }
      ME = d.user;
      updateHeader();
      loadChats();
      connectWS();
      setInterval(loadChats, 8000);
    });
}

function updateHeader() {
  if (!ME) return;
  document.getElementById("myInit").textContent = getInitials(ME);
  if (ME.photo_url) {
    document.getElementById("myAvatar").innerHTML = '<img src="' + ME.photo_url + '">';
  }
  document.getElementById("myTitle").textContent = ME.first_name || "Chatlar";
}

function loadChats() {
  api("/api/chats", { method: "POST", body: JSON.stringify({ initData: tg.initData }) })
    .then(function(d){
      if (!d.ok) return;
      var c = document.getElementById("chatList");
      if (!d.chats.length) {
        c.innerHTML = '<div class="empty"><div class="empty-icon">💬</div><h2>Hozircha chatlar yo\'q</h2><p>Qidiruv orqali yangi chat boshlang va boshqa foydalanuvchilar bilan yozishing</p><button class="btn-primary" onclick="showSearch()">🔍 Qidirish</button></div>';
        return;
      }
      var h = "";
      d.chats.forEach(function(ch){
        var o = ch.other_user || {};
        h += '<div class="chat-item" onclick="openChat(' + ch.chat_id + ')">';
        h += renderAvatar(o);
        h += '<div class="ci-info"><div class="ci-top"><div class="ci-name">' + esc(o.first_name || "User") + '</div><div class="ci-time">' + (ch.last_message_at ? fmtTime(ch.last_message_at) : "") + '</div></div>';
        h += '<div class="ci-bottom"><div class="ci-last">' + esc((ch.last_message_text || "Chat boshlash").substring(0, 50)) + '</div>';
        if (ch.unread > 0) h += '<div class="unread">' + ch.unread + '</div>';
        h += '</div></div></div>';
      });
      c.innerHTML = h;
    });
}

function showSearch() {
  document.getElementById("screenSearch").classList.remove("hidden");
  document.getElementById("searchResults").innerHTML = "";
  document.getElementById("searchInput").value = "";
  document.getElementById("searchHint").style.display = "block";
  setTimeout(function(){ document.getElementById("searchInput").focus(); }, 200);
  haptic();
}

function hideSearch() {
  document.getElementById("screenSearch").classList.add("hidden");
}

var searchTimer;
function doSearch() {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(function(){
    var q = document.getElementById("searchInput").value.trim();
    var r = document.getElementById("searchResults");
    if (!q) {
      r.innerHTML = "";
      document.getElementById("searchHint").style.display = "block";
      return;
    }
    document.getElementById("searchHint").style.display = "none";
    api("/api/users/search?q=" + encodeURIComponent(q)).then(function(d){
      if (!d.ok) return;
      var users = d.users.filter(function(u){ return !ME || u.id !== ME.id; });
      if (!users.length) {
        r.innerHTML = '<div class="empty"><div class="empty-icon">🔍</div><h2>Topilmadi</h2><p>"' + esc(q) + '" bo\'yicha hech kim topilmadi.<br>Faqat botga /start bosgan odamlar qidiriladi.</p></div>';
        return;
      }
      var h = "";
      users.forEach(function(u){
        h += '<div class="user-item" onclick="startChat(' + u.id + ')">';
        h += renderAvatar(u);
        h += '<div class="ui-info"><div class="ui-name">' + esc(u.first_name || "User") + '</div>';
        h += '<div class="ui-sub">' + (u.username ? "@" + esc(u.username) + " · " : "") + "Lv " + u.level + '</div></div></div>';
      });
      r.innerHTML = h;
    });
  }, 300);
}

function startChat(userId) {
  haptic();
  api("/api/chats/open", { method: "POST", body: JSON.stringify({ initData: tg.initData, user_id: userId }) })
    .then(function(d){
      if (!d.ok) { toast(d.error || "Xato"); return; }
      hideSearch();
      openChat(d.chat_id, d.other_user);
    });
}

function openChat(chatId, otherUser) {
  currentChatId = chatId;
  lastMsgId = 0;
  rendered = {};
  document.getElementById("screenChat").classList.remove("hidden");
  document.getElementById("chatMessages").innerHTML = '<div class="loading">Yuklanmoqda...</div>';
  if (otherUser) setChatHeader(otherUser);
  else {
    api("/api/chats", { method: "POST", body: JSON.stringify({ initData: tg.initData }) })
      .then(function(d){
        if (d.ok) {
          var ch = d.chats.find(function(c){ return c.chat_id === chatId; });
          if (ch && ch.other_user) setChatHeader(ch.other_user);
        }
      });
  }
  loadMessages(true);
  setTimeout(function(){ document.getElementById("msgInput").focus(); }, 200);
}

function setChatHeader(u) {
  currentChatUser = u;
  document.getElementById("chatInit").textContent = getInitials(u);
  var av = document.getElementById("chatAvatar");
  av.innerHTML = (u.photo_url ? '<img src="' + u.photo_url + '">' : '<span>' + getInitials(u) + '</span>') + (u.is_online ? '<div class="online-dot"></div>' : '');
  document.getElementById("chatName").textContent = u.first_name || "User";
  var status = u.is_online ? "🟢 Online" : (u.last_seen ? "oxirgi: " + fmtTime(u.last_seen) : "");
  if (u.status_emoji || u.status_text) status = (u.status_emoji || "") + " " + (u.status_text || status);
  document.getElementById("chatStatus").textContent = status;
}

function closeChat() {
  document.getElementById("screenChat").classList.add("hidden");
  currentChatId = null;
  currentChatUser = null;
  loadChats();
  haptic();
}

function loadMessages(initial) {
  if (!currentChatId) return;
  var url = "/api/chats/" + currentChatId + "/messages?initData=" + encodeURIComponent(tg.initData);
  if (!initial) url += "&after_id=" + lastMsgId;
  api(url).then(function(d){
    if (!d.ok) return;
    var c = document.getElementById("chatMessages");
    if (initial) {
      c.innerHTML = "";
      rendered = {};
      if (!d.messages.length) {
        c.innerHTML = '<div class="loading">Xabar yozib boshlang 👋</div>';
        return;
      }
    }
    var wasBottom = initial || (c.scrollTop + c.clientHeight >= c.scrollHeight - 100);
    var ld = c.querySelector(".loading");
    if (ld) ld.remove();
    d.messages.forEach(function(m){
      if (rendered[m.id]) return;
      if (m.id > lastMsgId) lastMsgId = m.id;
      appendMsg(m, false);
    });
    if (wasBottom) c.scrollTop = c.scrollHeight;
  });
}

function appendMsg(m, scroll) {
  var c = document.getElementById("chatMessages");
  var ld = c.querySelector(".loading");
  if (ld) ld.remove();
  var own = ME && m.sender_id === ME.id;
  var d = document.createElement("div");
  d.className = "msg " + (own ? "own" : "other");
  d.dataset.id = m.id;
  var h = "";
  if (m.reply_preview) {
    h += '<div class="reply-prev" onclick="scrollTo(' + m.reply_to_id + ')"><div class="rp-name">↩ Javob</div>' + esc(m.reply_preview) + '</div>';
  }
  h += '<div class="text">' + esc(m.text) + '</div>';
  h += '<div class="time">' + (m.is_edited ? '<span style="font-style:italic;opacity:.7">tahrir</span>' : "") + fmtTime(m.created_at) + (own ? " ✓✓" : "") + '</div>';
  var react = {};
  try { react = JSON.parse(m.reactions || "{}"); } catch (e) {}
  if (Object.keys(react).length) {
    h += '<div class="reactions">';
    Object.entries(react).forEach(function(p){
      var e = p[0], u = p[1];
      var mine = (ME && u.indexOf(ME.id) !== -1) ? " mine" : "";
      h += '<span class="reaction' + mine + '" onclick="react(' + m.id + ',\'' + e + '\')">' + e + ' ' + u.length + '</span>';
    });
    h += '</div>';
  }
  d.innerHTML = h;
  var pt;
  var start = function(){ pt = setTimeout(function(){ showActions(d, m); }, 450); };
  var end = function(){ clearTimeout(pt); };
  d.addEventListener("touchstart", start, { passive: true });
  d.addEventListener("touchend", end);
  d.addEventListener("mousedown", start);
  d.addEventListener("mouseup", end);
  c.appendChild(d);
  rendered[m.id] = true;
  if (scroll) c.scrollTop = c.scrollHeight;
}

function showActions(d, m) {
  haptic();
  document.querySelectorAll(".msg-actions").forEach(function(e){ e.remove(); });
  var own = ME && m.sender_id === ME.id;
  var a = document.createElement("div");
  a.className = "msg-actions active";
  var t = esc(m.text || "").substring(0, 40);
  var btns = "";
  btns += '<button onclick="setReply(' + m.id + ',\'' + t.replace(/'/g, "\\'").replace(/"/g, "&quot;") + '\')">↩ Javob</button>';
  btns += '<button onclick="quickReact(' + m.id + ')">😀 Reaksiya</button>';
  btns += '<button onclick="copyText(\'' + esc(m.text || "").replace(/'/g, "\\'").replace(/"/g, "&quot;") + '\')">📋 Nusxalash</button>';
  if (own) btns += '<button class="danger" onclick="delMsg(' + m.id + ')">🗑 O\'chirish</button>';
  a.innerHTML = btns;
  d.appendChild(a);
  setTimeout(function(){
    document.addEventListener("click", function close(e){
      if (!a.contains(e.target) && e.target !== d) {
        a.remove();
        document.removeEventListener("click", close);
      }
    });
  }, 100);
}

function setReply(id, text) {
  replyId = id;
  document.getElementById("replyName").textContent = "Javob";
  document.getElementById("replyText").textContent = text;
  document.getElementById("replyBar").classList.add("active");
  document.getElementById("msgInput").focus();
  document.querySelectorAll(".msg-actions").forEach(function(e){ e.remove(); });
}

function cancelReply() {
  replyId = null;
  document.getElementById("replyBar").classList.remove("active");
}

function quickReact(id) {
  var e = prompt("Emoji:", "👍");
  if (e) react(id, e);
}

function react(id, emoji) {
  haptic();
  api("/api/messages/" + id + "/react", {
    method: "POST",
    body: JSON.stringify({ initData: tg.initData, emoji: emoji })
  });
}

function delMsg(id) {
  document.querySelectorAll(".msg-actions").forEach(function(e){ e.remove(); });
  if (!confirm("O'chirishni xohlaysizmi?")) return;
  api("/api/messages/" + id, {
    method: "DELETE",
    body: JSON.stringify({ initData: tg.initData })
  }).then(function(r){
    if (r.ok) toast("O'chirildi");
  });
}

function copyText(t) {
  if (navigator.clipboard) navigator.clipboard.writeText(t).then(function(){ toast("Nusxalandi"); });
  document.querySelectorAll(".msg-actions").forEach(function(e){ e.remove(); });
}

function scrollTo(id) {
  var el = document.querySelector('.msg[data-id="' + id + '"]');
  if (el) {
    el.scrollIntoView({ behavior: "smooth", block: "center" });
    el.style.boxShadow = "0 0 0 3px #8b5cf6";
    setTimeout(function(){ el.style.boxShadow = ""; }, 1200);
  }
}

function sendMsg() {
  if (!currentChatId) return;
  var inp = document.getElementById("msgInput");
  var t = inp.value.trim();
  if (!t) return;
  inp.value = "";
  var rid = replyId;
  cancelReply();
  api("/api/chats/" + currentChatId + "/messages", {
    method: "POST",
    body: JSON.stringify({ initData: tg.initData, text: t, reply_to_id: rid })
  }).then(function(d){
    if (d.ok && d.message) {
      if (!rendered[d.message.id]) {
        if (d.message.id > lastMsgId) lastMsgId = d.message.id;
        appendMsg(d.message, true);
      }
      haptic();
    } else {
      toast(d.error || "Xato");
      inp.value = t;
    }
    inp.focus();
  });
}

function openProfile() {
  if (!ME) return;
  document.getElementById("screenProfile").classList.remove("hidden");
  document.getElementById("pfInit").textContent = getInitials(ME);
  if (ME.photo_url) document.getElementById("pfAvatar").innerHTML = '<img src="' + ME.photo_url + '">';
  document.getElementById("pfNameDisplay").textContent = ME.first_name || "User";
  document.getElementById("pfUsernameDisplay").textContent = ME.username ? "@" + ME.username : "";
  document.getElementById("pfName").value = ME.first_name || "";
  document.getElementById("pfBio").value = ME.bio || "";
  document.getElementById("pfEmoji").value = ME.status_emoji || "";
  document.getElementById("pfStatus").value = ME.status_text || "";
  haptic();
}

function closeProfile() {
  document.getElementById("screenProfile").classList.add("hidden");
}

function saveProfile() {
  api("/api/me", {
    method: "PUT",
    body: JSON.stringify({
      initData: tg.initData,
      first_name: document.getElementById("pfName").value.trim(),
      bio: document.getElementById("pfBio").value.trim(),
      status_emoji: document.getElementById("pfEmoji").value.trim(),
      status_text: document.getElementById("pfStatus").value.trim()
    })
  }).then(function(d){
    if (d.ok) {
      ME = d.user;
      updateHeader();
      closeProfile();
      toast("Saqlandi");
      haptic("success");
    }
  });
}

function connectWS() {
  if (!tg || !tg.initData) return;
  var proto = location.protocol === "https:" ? "wss:" : "ws:";
  var url = proto + "//" + location.host + "/ws?token=" + encodeURIComponent(tg.initData);
  try {
    ws = new WebSocket(url);
    ws.onopen = function(){
      reconnectAttempts = 0;
      document.getElementById("statusBar").classList.remove("show");
    };
    ws.onmessage = function(ev){
      try { handleWS(JSON.parse(ev.data)); } catch (e) {}
    };
    ws.onclose = function(){
      document.getElementById("statusBar").classList.add("show");
      clearTimeout(reconnectTimer);
      reconnectAttempts++;
      reconnectTimer = setTimeout(connectWS, Math.min(3000 * reconnectAttempts, 15000));
    };
  } catch (e) {
    reconnectTimer = setTimeout(connectWS, 3000);
  }
}

function handleWS(m) {
  if (m.type === "new_message") {
    var msg = m.data;
    if (currentChatId && msg.chat_id === currentChatId) {
      if (!rendered[msg.id]) {
        if (msg.id > lastMsgId) lastMsgId = msg.id;
        appendMsg(msg, true);
      }
    } else if (msg.sender_id !== ME.id) {
      haptic("medium");
      loadChats();
    }
  } else if (m.type === "delete_message") {
    if (m.data.chat_id === currentChatId) {
      var el = document.querySelector('.msg[data-id="' + m.data.id + '"]');
      if (el) el.remove();
      delete rendered[m.data.id];
    }
  } else if (m.type === "reaction") {
    if (m.data.chat_id === currentChatId) {
      var el = document.querySelector('.msg[data-id="' + m.data.id + '"]');
      if (el) {
        var r = {};
        try { r = JSON.parse(m.data.reactions || "{}"); } catch (e) {}
        var h = "";
        if (Object.keys(r).length) {
          h = '<div class="reactions">';
          Object.entries(r).forEach(function(p){
            var e = p[0], u = p[1];
            var mine = (ME && u.indexOf(ME.id) !== -1) ? " mine" : "";
            h += '<span class="reaction' + mine + '" onclick="react(' + m.data.id + ',\'' + e + '\')">' + e + ' ' + u.length + '</span>';
          });
          h += '</div>';
        }
        var old = el.querySelector(".reactions");
        if (old) old.remove();
        if (h) el.insertAdjacentHTML("beforeend", h);
      }
    }
  } else if (m.type === "ping" && ws) {
    ws.send(JSON.stringify({ type: "pong" }));
  }
}

document.getElementById("sendBtn").addEventListener("click", sendMsg);
document.getElementById("msgInput").addEventListener("keypress", function(e){
  if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendMsg(); }
});
window.addEventListener("beforeunload", function(){ if (ws) ws.close(); });

init();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(HTML_PAGE)


@app.get("/health")
async def health():
    return {"status": "ok"}


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


@app.post("/api/init")
async def api_init(req: InitReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "initData yaroqsiz"}, status_code=401)
    async for s in get_session():
        u = await get_or_create_user(s, tgu)
        if u.is_banned:
            return JSONResponse({"ok": False, "error": "Siz banlangansiz"}, status_code=403)
        return {"ok": True, "user": user_dict(u)}


@app.put("/api/me")
async def api_me_update(req: ProfileReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    async for s in get_session():
        u = await get_or_create_user(s, tgu)
        if req.first_name is not None: u.first_name = req.first_name[:100]
        if req.bio is not None: u.bio = req.bio[:500]
        if req.status_emoji is not None: u.status_emoji = req.status_emoji[:10]
        if req.status_text is not None: u.status_text = req.status_text[:100]
        await s.commit()
        await s.refresh(u)
        return {"ok": True, "user": user_dict(u)}


@app.get("/api/users/search")
async def api_search(q: str = ""):
    q = q.strip()
    if not q:
        return {"ok": True, "users": []}
    q = q.lstrip("@")
    if "t.me/" in q:
        q = q.split("t.me/")[-1]
    if "telegram.me/" in q:
        q = q.split("telegram.me/")[-1]
    q = q.split("?")[0].strip()
    if len(q) < 1:
        return {"ok": True, "users": []}
    async for s in get_session():
        r = await s.execute(select(User).where(or_(
            User.username.ilike(f"%{q}%"),
            User.first_name.ilike(f"%{q}%"),
            User.last_name.ilike(f"%{q}%"),
        )).where(User.is_banned == False).limit(20))
        return {"ok": True, "users": [user_dict(u) for u in r.scalars().all()]}


@app.post("/api/chats")
async def api_chats(req: InitReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    my_id = tgu["id"]
    async for s in get_session():
        r = await s.execute(select(Chat).where(or_(
            Chat.user1_id == my_id, Chat.user2_id == my_id
        )).order_by(desc(Chat.last_message_at), desc(Chat.created_at)))
        chats = r.scalars().all()
        result = []
        for c in chats:
            other_id = c.user2_id if c.user1_id == my_id else c.user1_id
            ur = await s.execute(select(User).where(User.telegram_id == other_id))
            other = ur.scalar_one_or_none()
            unread = await s.scalar(select(func.count(Message.id)).where(and_(
                Message.chat_id == c.id,
                Message.sender_id != my_id,
                Message.is_read == False,
                Message.is_deleted == False,
            ))) or 0
            result.append({
                "chat_id": c.id,
                "other_user": user_dict(other) if other else None,
                "last_message_text": c.last_message_text,
                "last_message_at": c.last_message_at.isoformat() if c.last_message_at else None,
                "unread": unread,
            })
        return {"ok": True, "chats": result}


@app.post("/api/chats/open")
async def api_open_chat(req: OpenChatReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    my_id = tgu["id"]
    other_id = req.user_id
    if my_id == other_id:
        return JSONResponse({"ok": False, "error": "O'zingiz bilan chat ocholmaysiz"}, status_code=400)
    a, b = min(my_id, other_id), max(my_id, other_id)
    key = f"{a}_{b}"
    async for s in get_session():
        r = await s.execute(select(User).where(User.telegram_id == other_id))
        other = r.scalar_one_or_none()
        if not other:
            return JSONResponse({"ok": False, "error": "Foydalanuvchi topilmadi"}, status_code=404)
        r = await s.execute(select(Chat).where(Chat.chat_key == key))
        chat = r.scalar_one_or_none()
        if not chat:
            chat = Chat(user1_id=a, user2_id=b, chat_key=key)
            s.add(chat)
            await s.commit()
            await s.refresh(chat)
        return {"ok": True, "chat_id": chat.id, "other_user": user_dict(other)}


@app.get("/api/chats/{chat_id}/messages")
async def api_messages(chat_id: int, initData: str = "", after_id: int = 0):
    tgu = verify_init_data(initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    my_id = tgu["id"]
    async for s in get_session():
        r = await s.execute(select(Chat).where(Chat.id == chat_id))
        c = r.scalar_one_or_none()
        if not c or my_id not in (c.user1_id, c.user2_id):
            return JSONResponse({"ok": False, "error": "Ruxsat yo'q"}, status_code=403)
        if after_id > 0:
            r = await s.execute(select(Message).where(and_(
                Message.chat_id == chat_id, Message.id > after_id, Message.is_deleted == False
            )).order_by(Message.id.asc()).limit(200))
        else:
            r = await s.execute(select(Message).where(and_(
                Message.chat_id == chat_id, Message.is_deleted == False
            )).order_by(desc(Message.id)).limit(100))
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
            result.append(msg_dict(m, rp))
        await s.execute(
            Message.__table__.update().where(and_(
                Message.chat_id == chat_id, Message.sender_id != my_id, Message.is_read == False
            )).values(is_read=True)
        )
        await s.commit()
        return {"ok": True, "messages": result}


@app.post("/api/chats/{chat_id}/messages")
async def api_send(chat_id: int, req: SendMsgReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    my_id = tgu["id"]
    text = (req.text or "").strip()
    if not text:
        return JSONResponse({"ok": False, "error": "Bo'sh xabar"}, status_code=400)
    if len(text) > 4000:
        return JSONResponse({"ok": False, "error": "Juda uzun"}, status_code=400)
    async for s in get_session():
        r = await s.execute(select(Chat).where(Chat.id == chat_id))
        c = r.scalar_one_or_none()
        if not c or my_id not in (c.user1_id, c.user2_id):
            return JSONResponse({"ok": False, "error": "Ruxsat yo'q"}, status_code=403)
        u = await get_or_create_user(s, tgu)
        rp = None
        if req.reply_to_id:
            rr = await s.execute(select(Message).where(Message.id == req.reply_to_id))
            p = rr.scalar_one_or_none()
            if p:
                rp = p.text[:60] + ("..." if len(p.text) > 60 else "")
        m = Message(chat_id=chat_id, sender_id=my_id, text=text, reply_to_id=req.reply_to_id)
        s.add(m)
        c.last_message_text = text[:200]
        c.last_message_at = datetime.utcnow()
        u.xp += 5
        u.level = 1 + u.xp // 100
        u.coins += 2
        await s.commit()
        await s.refresh(m)
        pl = msg_dict(m, rp)
        await manager.send_to_many([c.user1_id, c.user2_id], {"type": "new_message", "data": pl})
        return {"ok": True, "message": pl}


@app.delete("/api/messages/{mid}")
async def api_del(mid: int, req: InitReq):
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
        r = await s.execute(select(Chat).where(Chat.id == m.chat_id))
        c = r.scalar_one_or_none()
        m.is_deleted = True
        await s.commit()
        if c:
            await manager.send_to_many([c.user1_id, c.user2_id],
                {"type": "delete_message", "data": {"id": mid, "chat_id": m.chat_id}})
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
        r = await s.execute(select(Chat).where(Chat.id == m.chat_id))
        c = r.scalar_one_or_none()
        if c:
            await manager.send_to_many([c.user1_id, c.user2_id],
                {"type": "reaction", "data": {"id": mid, "chat_id": m.chat_id, "reactions": m.reactions}})
        return {"ok": True, "reactions": reacts}


@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...), initData: str = Form(...)):
    tgu = verify_init_data(initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    contents = await file.read()
    if len(contents) > 10 * 1024 * 1024:
        return JSONResponse({"ok": False, "error": "Fayl 10 MB dan oshmasin"}, status_code=400)
    ext = os.path.splitext(file.filename or "file")[1].lower()
    allowed = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mov", ".mp3", ".wav", ".ogg", ".pdf", ".docx", ".xlsx", ".txt", ".zip")
    if ext not in allowed:
        return JSONResponse({"ok": False, "error": "Fayl turi ruxsat etilmagan"}, status_code=400)
    fname = uuid.uuid4().hex + ext
    with open(os.path.join(UPLOAD_DIR, fname), "wb") as f:
        f.write(contents)
    return {"ok": True, "url": "/uploads/" + fname}


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
    except Exception:
        pass
    finally:
        manager.disconnect(uid, ws)
        async for s in get_session():
            r = await s.execute(select(User).where(User.telegram_id == uid))
            u = r.scalar_one_or_none()
            if u:
                u.is_online = False
                u.last_seen = datetime.utcnow()
                await s.commit()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
