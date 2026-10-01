"""
Telegram-ga o'xshash shaxsiy chat Mini App
"""
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
    language: Mapped[str] = mapped_column(String(5), default="uz")
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
    message_type: Mapped[str] = mapped_column(String(20), default="text")
    media_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    reply_to_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_edited: Mapped[bool] = mapped_column(Boolean, default=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False)
    reactions: Mapped[Optional[str]] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("✅ DB tayyor")


async def get_session():
    async with async_session() as session:
        yield session


class Manager:
    def __init__(self):
        self.active: Dict[int, set] = {}

    async def connect(self, uid: int, ws: WebSocket):
        await ws.accept()
        self.active.setdefault(uid, set()).add(ws)

    def disconnect(self, uid: int, ws: WebSocket):
        if uid in self.active:
            self.active[uid].discard(ws)
            if not self.active[uid]:
                del self.active[uid]

    async def send_to(self, uid: int, msg: dict):
        if uid not in self.active:
            return
        data = json.dumps(msg, default=str)
        for ws in list(self.active[uid]):
            try:
                await ws.send_text(data)
            except Exception:
                self.disconnect(uid, ws)

    async def send_to_many(self, uids: list, msg: dict):
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
    except Exception as e:
        logger.error(f"initData: {e}")
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


def user_dict(u: User) -> dict:
    return {
        "id": u.telegram_id, "first_name": u.first_name, "last_name": u.last_name,
        "username": u.username, "photo_url": u.photo_url, "bio": u.bio,
        "is_online": u.is_online, "is_admin": u.is_admin,
        "xp": u.xp, "level": u.level, "coins": u.coins,
        "status_emoji": u.status_emoji, "status_text": u.status_text,
        "last_seen": u.last_seen.isoformat() if u.last_seen else None,
    }


def msg_dict(m: Message, reply_preview: Optional[str] = None) -> dict:
    return {
        "id": m.id, "chat_id": m.chat_id, "sender_id": m.sender_id,
        "text": m.text, "message_type": m.message_type, "media_url": m.media_url,
        "reply_to_id": m.reply_to_id, "reply_preview": reply_preview,
        "reactions": m.reactions or "{}", "is_edited": m.is_edited,
        "is_read": m.is_read,
        "created_at": m.created_at.isoformat(),
    }


bot: Optional[Bot] = None
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
            "🚀 Chat Mini App'ga xush kelibsiz!\n\n"
            "Qidiruv orqali boshqa foydalanuvchilarni toping va ular bilan yozishing.",
            reply_markup=kb, parse_mode="HTML"
        )

    @dp.message(Command("help"))
    async def cmd_help(message: types.Message):
        await message.answer("Yordam: /start /help /stats /top", parse_mode="HTML")

    @dp.message(Command("stats"))
    async def cmd_stats(message: types.Message):
        async for s in get_session():
            u = await s.scalar(select(func.count(User.id)))
            m = await s.scalar(select(func.count(Message.id)))
            await message.answer(
                f"👥 Foydalanuvchilar: <b>{u or 0}</b>\n"
                f"💬 Xabarlar: <b>{m or 0}</b>\n"
                f"🟢 Online: <b>{len(manager.active)}</b>",
                parse_mode="HTML"
            )

    @dp.message(Command("top"))
    async def cmd_top(message: types.Message):
        async for s in get_session():
            r = await s.execute(select(User).order_by(desc(User.xp)).limit(10))
            us = r.scalars().all()
            text = "🏆 <b>TOP 10</b>\n\n"
            for i, u in enumerate(us, 1):
                text += f"{i}. {u.first_name} — Lv {u.level} ({u.xp} XP)\n"
            await message.answer(text, parse_mode="HTML")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global bot
    await init_db()
    if BOT_TOKEN and WEBAPP_URL:
        bot = Bot(token=BOT_TOKEN)
        try:
            await bot.set_webhook(
                url=f"{WEBAPP_URL}{WEBHOOK_PATH}",
                secret_token=WEBHOOK_SECRET,
                drop_pending_updates=True,
                allowed_updates=dp.resolve_used_update_types() if dp else None,
            )
            await bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(text="💬 Chat", web_app=WebAppInfo(url=WEBAPP_URL))
            )
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
        if req.first_name is not None:
            u.first_name = req.first_name[:100]
        if req.bio is not None:
            u.bio = req.bio[:500]
        if req.status_emoji is not None:
            u.status_emoji = req.status_emoji[:10]
        if req.status_text is not None:
            u.status_text = req.status_text[:100]
        await s.commit()
        await s.refresh(u)
        return {"ok": True, "user": user_dict(u)}


@app.get("/api/users/search")
async def api_search(q: str = ""):
    q = q.strip()
    if not q:
        return {"ok": True, "users": []}
    # @ belgisi va t.me linkni tozalash
    q = q.lstrip("@")
    if "t.me/" in q:
        q = q.split("t.me/")[-1]
    if "telegram.me/" in q:
        q = q.split("telegram.me/")[-1]
    q = q.split("?")[0].strip()
    if len(q) < 1:
        return {"ok": True, "users": []}

    async for s in get_session():
        r = await s.execute(
            select(User).where(or_(
                User.username.ilike(f"%{q}%"),
                User.first_name.ilike(f"%{q}%"),
                User.last_name.ilike(f"%{q}%"),
            )).where(User.is_banned == False).limit(20)
        )
        users = r.scalars().all()
        return {"ok": True, "users": [user_dict(u) for u in users]}


@app.get("/api/users/{user_id}")
async def api_user(user_id: int):
    async for s in get_session():
        r = await s.execute(select(User).where(User.telegram_id == user_id))
        u = r.scalar_one_or_none()
        if not u:
            return JSONResponse({"ok": False, "error": "Topilmadi"}, status_code=404)
        return {"ok": True, "user": user_dict(u)}


@app.get("/api/chats")
async def api_chats(req: InitReq):
    tgu = verify_init_data(req.initData)
    if not tgu:
        return JSONResponse({"ok": False, "error": "Auth"}, status_code=401)
    my_id = tgu["id"]
    async for s in get_session():
        r = await s.execute(
            select(Chat).where(or_(
                Chat.user1_id == my_id, Chat.user2_id == my_id
            )).order_by(desc(Chat.last_message_at), desc(Chat.created_at))
        )
        chats = r.scalars().all()
        result = []
        for c in chats:
            other_id = c.user2_id if c.user1_id == my_id else c.user1_id
            ur = await s.execute(select(User).where(User.telegram_id == other_id))
            other = ur.scalar_one_or_none()
            # o'qilmagan xabarlar soni
            unread = await s.scalar(
                select(func.count(Message.id)).where(and_(
                    Message.chat_id == c.id,
                    Message.sender_id != my_id,
                    Message.is_read == False,
                    Message.is_deleted == False,
                ))
            ) or 0
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
        # other user bazada bormi?
        r = await s.execute(select(User).where(User.telegram_id == other_id))
        other = r.scalar_one_or_none()
        if not other:
            return JSONResponse({"ok": False, "error": "Foydalanuvchi topilmadi. U botga /start bosmagan bo'lishi mumkin."}, status_code=404)
        # mavjud chatni topish
        r = await s.execute(select(Chat).where(Chat.chat_key == key))
        chat = r.scalar_one_or_none()
        if not chat:
            chat = Chat(user1_id=a, user2_id=b, chat_key=key)
            s.add(chat)
            await s.commit()
            await s.refresh(chat)
        return {
            "ok": True,
            "chat_id": chat.id,
            "other_user": user_dict(other),
        }


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
            r = await s.execute(
                select(Message).where(and_(
                    Message.chat_id == chat_id,
                    Message.id > after_id,
                    Message.is_deleted == False,
                )).order_by(Message.id.asc()).limit(200)
            )
        else:
            r = await s.execute(
                select(Message).where(and_(
                    Message.chat_id == chat_id,
                    Message.is_deleted == False,
                )).order_by(desc(Message.id)).limit(100)
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
            result.append(msg_dict(m, rp))

        # o'qilgan deb belgilash
        await s.execute(
            Message.__table__.update().where(and_(
                Message.chat_id == chat_id,
                Message.sender_id != my_id,
                Message.is_read == False,
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

        m = Message(
            chat_id=chat_id, sender_id=my_id, text=text,
            reply_to_id=req.reply_to_id
        )
        s.add(m)
        c.last_message_text = text[:200]
        c.last_message_at = datetime.utcnow()
        u.xp += 5
        u.level = 1 + u.xp // 100
        u.coins += 2
        await s.commit()
        await s.refresh(m)

        pl = msg_dict(m, rp)
        # ikkala foydalanuvchiga yuborish
        await manager.send_to_many(
            [c.user1_id, c.user2_id],
            {"type": "new_message", "data": pl}
        )
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
            await manager.send_to_many(
                [c.user1_id, c.user2_id],
                {"type": "delete_message", "data": {"id": mid, "chat_id": m.chat_id}}
            )
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
            await manager.send_to_many(
                [c.user1_id, c.user2_id],
                {"type": "reaction", "data": {"id": mid, "chat_id": m.chat_id, "reactions": m.reactions}}
            )
        return {"ok": True, "reactions": reacts}


@app.get("/api/stats")
async def api_stats():
    async for s in get_session():
        u = await s.scalar(select(func.count(User.id)))
        m = await s.scalar(select(func.count(Message.id)))
        c = await s.scalar(select(func.count(Chat.id)))
        return {"ok": True, "users": u or 0, "messages": m or 0, "chats": c or 0, "online": len(manager.active)}


@app.get("/api/leaderboard")
async def api_leaderboard():
    async for s in get_session():
        r = await s.execute(select(User).order_by(desc(User.xp)).limit(10))
        return {"ok": True, "users": [user_dict(u) for u in r.scalars().all()]}


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


# ═══════════════════════ HTML_PAGE ═══════════════════════
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="uz">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,user-scalable=no,viewport-fit=cover">
<title>Chat</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
body{font-family:-apple-system,BlinkMacSystemFont,'Inter','Segoe UI',Roboto,sans-serif;background:#0f0f14;color:#f5f5f7;height:100vh;overflow:hidden;font-size:15px}
.app{display:flex;flex-direction:column;height:100vh;max-width:820px;margin:0 auto;position:relative}
.screen{position:absolute;inset:0;display:flex;flex-direction:column;background:#0f0f14;transition:transform .3s ease}
.screen.hidden{transform:translateX(100%);pointer-events:none}
.header{display:flex;align-items:center;gap:10px;padding:12px 14px;background:rgba(26,26,36,.98);border-bottom:1px solid rgba(255,255,255,.06);flex-shrink:0;min-height:60px}
.back-btn{background:none;border:none;color:#f5f5f7;font-size:22px;cursor:pointer;padding:6px 10px;border-radius:10px;display:flex;align-items:center}
.back-btn:hover{background:rgba(255,255,255,.08)}
.header h1{font-size:17px;font-weight:600;flex:1}
.icon-btn{width:38px;height:38px;border:none;background:transparent;border-radius:10px;font-size:17px;cursor:pointer;color:#f5f5f7;display:flex;align-items:center;justify-content:center}
.icon-btn:hover{background:rgba(255,255,255,.08)}
.avatar{width:44px;height:44px;border-radius:50%;background:linear-gradient(135deg,#7c5cff,#ff5c8a);display:flex;align-items:center;justify-content:center;font-weight:700;font-size:16px;color:#fff;flex-shrink:0;position:relative;overflow:hidden}
.avatar img{width:100%;height:100%;object-fit:cover}
.avatar.sm{width:38px;height:38px;font-size:14px}
.online-dot{position:absolute;bottom:0;right:0;width:11px;height:11px;background:#31c48d;border-radius:50%;border:2px solid #1a1a24}
.list{flex:1;overflow-y:auto;padding:8px 0}
.empty{display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;padding:40px 20px;text-align:center;color:#8e8e93}
.empty .big{font-size:64px;margin-bottom:16px;opacity:.6}
.empty h2{font-size:18px;color:#f5f5f7;margin-bottom:8px}
.empty p{font-size:14px;line-height:1.6;max-width:280px;margin-bottom:24px}
.chat-item{display:flex;gap:12px;padding:12px 16px;cursor:pointer;transition:background .12s;align-items:center}
.chat-item:hover{background:rgba(124,92,255,.08)}
.chat-item:active{background:rgba(124,92,255,.15)}
.ci-info{flex:1;min-width:0}
.ci-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}
.ci-name{font-weight:600;font-size:15px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ci-time{font-size:11px;color:#8e8e93;flex-shrink:0;margin-left:8px}
.ci-bottom{display:flex;justify-content:space-between;align-items:center;gap:8px}
.ci-last{font-size:13px;color:#8e8e93;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1}
.unread{background:#7c5cff;color:#fff;font-size:11px;font-weight:700;min-width:20px;height:20px;border-radius:10px;display:flex;align-items:center;justify-content:center;padding:0 6px;flex-shrink:0}
.fab{position:absolute;bottom:24px;right:20px;width:56px;height:56px;border-radius:50%;background:linear-gradient(135deg,#7c5cff,#ff5c8a);border:none;color:#fff;font-size:26px;cursor:pointer;box-shadow:0 6px 20px rgba(124,92,255,.5);display:flex;align-items:center;justify-content:center;z-index:20}
.fab:active{transform:scale(.92)}
.messages{flex:1;overflow-y:auto;padding:14px;display:flex;flex-direction:column;gap:6px}
.msg{max-width:78%;padding:8px 12px;border-radius:16px;font-size:14.5px;line-height:1.45;word-wrap:break-word;position:relative;animation:slide .2s ease}
@keyframes slide{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
.msg.own{align-self:flex-end;background:linear-gradient(135deg,#7c5cff,#9c7cff);color:#fff;border-bottom-right-radius:4px}
.msg.other{align-self:flex-start;background:#232330;color:#f5f5f7;border-bottom-left-radius:4px}
.msg .text{white-space:pre-wrap;word-break:break-word}
.msg .time{font-size:10px;opacity:.65;margin-top:4px;text-align:right;display:flex;justify-content:flex-end;gap:5px}
.msg .reply-prev{font-size:11.5px;border-left:3px solid;padding:4px 8px;margin-bottom:5px;border-radius:6px;opacity:.85;background:rgba(0,0,0,.2);cursor:pointer}
.msg .reactions{display:flex;gap:4px;margin-top:5px;flex-wrap:wrap}
.reaction{background:rgba(255,255,255,.12);padding:2px 8px;border-radius:10px;font-size:11.5px;cursor:pointer;display:inline-flex;align-items:center;gap:3px}
.reaction.mine{background:rgba(124,92,255,.4);border:1px solid #7c5cff}
.input-area{display:flex;gap:8px;padding:10px 12px;background:rgba(26,26,36,.98);border-top:1px solid rgba(255,255,255,.06);padding-bottom:max(10px,env(safe-area-inset-bottom));align-items:flex-end;flex-shrink:0}
.input-wrap{flex:1;display:flex;align-items:center;background:#232330;border-radius:22px;padding:4px 6px 4px 12px;transition:box-shadow .2s}
.input-wrap:focus-within{box-shadow:0 0 0 2px #7c5cff}
.input-wrap input{flex:1;padding:10px 6px;border:none;background:transparent;color:#f5f5f7;font-size:15px;outline:none;min-width:0}
.send{width:46px;height:46px;border-radius:50%;background:linear-gradient(135deg,#7c5cff,#ff5c8a);border:none;color:#fff;font-size:17px;cursor:pointer;flex-shrink:0;display:flex;align-items:center;justify-content:center;box-shadow:0 4px 14px rgba(124,92,255,.5)}
.send:active{transform:scale(.9)}
.reply-bar{display:none;background:#1a1a24;border-left:3px solid #7c5cff;padding:8px 14px;margin:0 12px;border-radius:10px;font-size:12.5px;justify-content:space-between;align-items:center;margin-bottom:6px}
.reply-bar.active{display:flex}
.reply-bar .info{flex:1;min-width:0}
.reply-bar b{color:#7c5cff;display:block;font-size:11.5px;margin-bottom:2px}
.reply-bar .rtext{opacity:.8;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.reply-bar button{background:none;border:none;color:#8e8e93;font-size:20px;cursor:pointer;padding:0 6px}
.emoji-bar{display:flex;gap:4px;padding:6px 12px;background:#1a1a24;overflow-x:auto;border-top:1px solid rgba(255,255,255,.05);scrollbar-width:none}
.emoji-bar::-webkit-scrollbar{display:none}
.emoji-bar button{background:transparent;border:none;font-size:20px;cursor:pointer;padding:4px 6px;border-radius:8px;flex-shrink:0}
.emoji-bar button:hover{background:rgba(255,255,255,.1);transform:scale(1.15)}
.search-box{padding:12px 14px}
.search-box input{width:100%;padding:12px 16px;border:none;border-radius:14px;background:#232330;color:#f5f5f7;font-size:15px;outline:none;font-family:inherit}
.search-box input:focus{box-shadow:0 0 0 2px #7c5cff}
.search-hint{padding:10px 14px;font-size:12px;color:#8e8e93;text-align:center;line-height:1.6}
.user-item{display:flex;gap:12px;padding:12px 16px;cursor:pointer;align-items:center;transition:background .12s}
.user-item:hover{background:rgba(124,92,255,.08)}
.ui-info{flex:1;min-width:0}
.ui-name{font-weight:600;font-size:15px;margin-bottom:3px}
.ui-sub{font-size:12px;color:#8e8e93}
.msg-actions{position:absolute;top:100%;right:0;background:#2a2a3a;border-radius:12px;box-shadow:0 10px 40px rgba(0,0,0,.6);display:none;flex-direction:column;padding:6px;z-index:200;min-width:170px;margin-top:4px}
.msg-actions.active{display:flex}
.msg-actions button{background:none;border:none;font-size:13.5px;padding:9px 12px;cursor:pointer;border-radius:8px;text-align:left;color:#fff;display:flex;align-items:center;gap:10px;font-family:inherit}
.msg-actions button:hover{background:rgba(124,92,255,.25)}
.msg-actions button.danger:hover{background:rgba(255,71,87,.25)}
.toast{position:fixed;bottom:30px;left:50%;transform:translateX(-50%) translateY(100px);background:#232330;padding:12px 20px;border-radius:12px;font-size:14px;box-shadow:0 8px 30px rgba(0,0,0,.5);z-index:2000;opacity:0;transition:all .3s;pointer-events:none;max-width:90%}
.toast.show{transform:translateX(-50%) translateY(0);opacity:1}
.status-bar{padding:6px 14px;text-align:center;font-size:11px;background:#ffa502;color:#000;display:none}
.status-bar.show{display:block}
.loading{text-align:center;color:#8e8e93;padding:40px;font-size:14px}
.messages::-webkit-scrollbar{width:5px}
.messages::-webkit-scrollbar-thumb{background:rgba(255,255,255,.15);border-radius:3px}
@media(max-width:500px){.msg{max-width:85%}}
</style>
</head>
<body>
<div class="app">
  <div class="status-bar" id="statusBar">Qayta ulanmoqda...</div>

  <!-- CHAT LIST SCREEN -->
  <div class="screen" id="screenChats">
    <div class="header">
      <div class="avatar" id="myAvatar" onclick="openProfile()"><span id="myInit">?</span></div>
      <h1 id="myTitle">Chatlar</h1>
      <button class="icon-btn" onclick="showSearch()" title="Qidiruv">🔍</button>
    </div>
    <div class="list" id="chatList">
      <div class="loading">Yuklanmoqda...</div>
    </div>
    <button class="fab" onclick="showSearch()">+</button>
  </div>

  <!-- SEARCH SCREEN -->
  <div class="screen hidden" id="screenSearch">
    <div class="header">
      <button class="back-btn" onclick="hideSearch()">←</button>
      <h1>Yangi chat</h1>
    </div>
    <div class="search-box">
      <input type="text" id="searchInput" placeholder="@username yoki ism..." oninput="doSearch()" autocomplete="off" autofocus>
    </div>
    <div class="search-hint" id="searchHint">
      @username, t.me/username yoki ism kiriting.<br>
      Faqat botga /start bosgan foydalanuvchilar topiladi.
    </div>
    <div class="list" id="searchResults"></div>
  </div>

  <!-- CHAT SCREEN -->
  <div class="screen hidden" id="screenChat">
    <div class="header">
      <button class="back-btn" onclick="closeChat()">←</button>
      <div class="avatar sm" id="chatAvatar"><span id="chatInit">?</span></div>
      <div style="flex:1;min-width:0">
        <h1 id="chatName" style="font-size:15px;margin-bottom:2px">...</h1>
        <div id="chatStatus" style="font-size:11px;color:#8e8e93">...</div>
      </div>
    </div>
    <div class="messages" id="chatMessages">
      <div class="loading">Yuklanmoqda...</div>
    </div>
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
        <input type="text" id="msgInput" placeholder="Xabar..." maxlength="4000" autocomplete="off">
      </div>
      <button class="send" id="sendBtn">➤</button>
    </div>
  </div>

  <!-- PROFILE SCREEN -->
  <div class="screen hidden" id="screenProfile">
    <div class="header">
      <button class="back-btn" onclick="closeProfile()">←</button>
      <h1>Profil</h1>
    </div>
    <div style="padding:20px;overflow-y:auto;flex:1">
      <label style="font-size:12px;color:#8e8e93;display:block;margin-bottom:6px">Ism</label>
      <input id="pfName" style="width:100%;padding:11px 14px;border:1px solid rgba(255,255,255,.1);border-radius:10px;background:#232330;color:#f5f5f7;font-size:14px;outline:none;font-family:inherit;margin-bottom:14px">
      <label style="font-size:12px;color:#8e8e93;display:block;margin-bottom:6px">Bio</label>
      <textarea id="pfBio" style="width:100%;padding:11px 14px;border:1px solid rgba(255,255,255,.1);border-radius:10px;background:#232330;color:#f5f5f7;font-size:14px;outline:none;font-family:inherit;min-height:80px;resize:vertical;margin-bottom:14px"></textarea>
      <label style="font-size:12px;color:#8e8e93;display:block;margin-bottom:6px">Status emoji</label>
      <input id="pfEmoji" maxlength="4" style="width:100%;padding:11px 14px;border:1px solid rgba(255,255,255,.1);border-radius:10px;background:#232330;color:#f5f5f7;font-size:14px;outline:none;font-family:inherit;margin-bottom:14px">
      <label style="font-size:12px;color:#8e8e93;display:block;margin-bottom:6px">Status matn</label>
      <input id="pfStatus" maxlength="50" style="width:100%;padding:11px 14px;border:1px solid rgba(255,255,255,.1);border-radius:10px;background:#232330;color:#f5f5f7;font-size:14px;outline:none;font-family:inherit;margin-bottom:14px">
      <button onclick="saveProfile()" style="width:100%;padding:14px;border:none;border-radius:12px;background:linear-gradient(135deg,#7c5cff,#ff5c8a);color:#fff;font-size:15px;font-weight:600;cursor:pointer;font-family:inherit">Saqlash</button>
    </div>
  </div>

  <div class="toast" id="toast"></div>
</div>

<script>
var tg = window.Telegram ? window.Telegram.WebApp : null;
if (tg) { tg.ready(); tg.expand(); }

var ME = null;
var currentChatId = null;
var currentChatUser = null;
var lastMsgId = 0;
var replyId = null;
var ws = null;
var reconnectTimer = null;
var reconnectAttempts = 0;
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
  if (u && u.photo_url) {
    html += '<img src="' + u.photo_url + '">';
  } else {
    html += '<span>' + init + '</span>';
  }
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
    document.body.innerHTML = '<div style="padding:40px;text-align:center;color:#8e8e93">Telegram orqali oching</div>';
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

// ═══════════════ CHAT LIST ═══════════════
function loadChats() {
  api("/api/chats", { method: "POST", body: JSON.stringify({ initData: tg.initData }) })
    .then(function(d){
      if (!d.ok) return;
      var c = document.getElementById("chatList");
      if (!d.chats.length) {
        c.innerHTML = '<div class="empty"><div class="big">💬</div><h2>Hozircha chatlar yo\'q</h2><p>Qidiruv orqali yangi chat boshlang va boshqa foydalanuvchilar bilan yozishing</p><button onclick="showSearch()" style="padding:12px 24px;border:none;border-radius:12px;background:linear-gradient(135deg,#7c5cff,#ff5c8a);color:#fff;font-size:14px;font-weight:600;cursor:pointer;font-family:inherit">🔍 Qidirish</button></div>';
        return;
      }
      var h = "";
      d.chats.forEach(function(ch){
        var o = ch.other_user || {};
        h += '<div class="chat-item" onclick="openChat(' + ch.chat_id + ')">';
        h += renderAvatar(o);
        h += '<div class="ci-info">';
        h += '<div class="ci-top"><div class="ci-name">' + esc(o.first_name || "User") + '</div><div class="ci-time">' + (ch.last_message_at ? fmtTime(ch.last_message_at) : "") + '</div></div>';
        h += '<div class="ci-bottom"><div class="ci-last">' + esc((ch.last_message_text || "Chat boshlash").substring(0, 50)) + '</div>';
        if (ch.unread > 0) h += '<div class="unread">' + ch.unread + '</div>';
        h += '</div></div></div>';
      });
      c.innerHTML = h;
    });
}

// ═══════════════ SEARCH ═══════════════
function showSearch() {
  document.getElementById("screenSearch").classList.remove("hidden");
  document.getElementById("searchResults").innerHTML = "";
  document.getElementById("searchInput").value = "";
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
        r.innerHTML = '<div class="empty"><div class="big">🔍</div><h2>Topilmadi</h2><p>"' + esc(q) + '" bo\'yicha hech kim topilmadi.<br>Faqat botga /start bosgan odamlar qidiriladi.</p></div>';
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
  api("/api/chats/open", {
    method: "POST",
    body: JSON.stringify({ initData: tg.initData, user_id: userId })
  }).then(function(d){
    if (!d.ok) { toast(d.error || "Xato"); return; }
    hideSearch();
    openChat(d.chat_id, d.other_user);
  });
}

// ═══════════════ CHAT ═══════════════
function openChat(chatId, otherUser) {
  currentChatId = chatId;
  lastMsgId = 0;
  rendered = {};
  document.getElementById("screenChat").classList.remove("hidden");
  document.getElementById("chatMessages").innerHTML = '<div class="loading">Yuklanmoqda...</div>';

  if (otherUser) {
    setChatHeader(otherUser);
  } else {
    // chat listdan chaqirilganda userni topish
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
  if (u.photo_url) {
    document.getElementById("chatAvatar").innerHTML = '<img src="' + u.photo_url + '">' + (u.is_online ? '<div class="online-dot"></div>' : '');
  }
  document.getElementById("chatName").textContent = u.first_name || "User";
  var status = u.is_online ? "🟢 Online" : (u.last_seen ? "oxirgi: " + fmtTime(u.last_seen) : "");
  if (u.status_emoji || u.status_text) {
    status = (u.status_emoji || "") + " " + (u.status_text || status);
  }
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
    h += '<div class="reply-prev" onclick="scrollTo(' + m.reply_to_id + ')">↩ ' + esc(m.reply_preview) + '</div>';
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
  var btns = "";
  btns += '<button onclick="setReply(' + m.id + ',\'' + esc((m.text||"").substring(0,40)).replace(/'/g,"\\'") + '\')">↩ Javob</button>';
  btns += '<button onclick="quickReact(' + m.id + ')">😀 Reaksiya</button>';
  btns += '<button onclick="copyText(\'' + esc(m.text||"").replace(/'/g,"\\'").replace(/"/g,"&quot;") + '\')">📋 Nusxalash</button>';
  if (own) {
    btns += '<button class="danger" onclick="delMsg(' + m.id + ')">🗑 O\'chirish</button>';
  }
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
    el.style.boxShadow = "0 0 0 3px #7c5cff";
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

// ═══════════════ PROFILE ═══════════════
function openProfile() {
  if (!ME) return;
  document.getElementById("screenProfile").classList.remove("hidden");
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

// ═══════════════ WEBSOCKET ═══════════════
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


if __name__ == "__main__":
    import uvicorn
    logger.info(f"🚀 http://0.0.0.0:{PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
