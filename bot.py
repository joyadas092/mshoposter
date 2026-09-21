"""
MeeshoPoster — send a Meesho link, get product image + deal caption.

Admin-only:
  Only ADMIN_USER_IDS (from env) can send Meesho links.
  All other users who /start the bot are saved to DB for broadcast.

Commands (admin only):
  /broadcast           — reply to a post or add text to broadcast to all users
  /status              — show user count
  /screenshot_on       — enable real webpage screenshot mode (~15s per product)
  /screenshot_off      — switch back to fast CDN image mode (default, ~3s)

How it works:
  - curl_cffi + Dalvik/Android UA bypasses Akamai bot protection on meesho.com
  - Product data parsed from __NEXT_DATA__ JSON in SSR HTML
  - Screenshot mode: product page captured via Playwright (waits for hero image)
  - Fast mode: product image downloaded directly from images.meesho.com CDN
  - After every deal, the post is also sent to DEAL_CHANNEL_ID for distribution
"""

import os
import re
import json
import logging
import asyncio
import tempfile
import time
from pathlib import Path
from typing import Any

# Fix: set a fresh event loop BEFORE Pyrogram Client is created.
# Without this, Python 3.10 may bind Pyrogram to a stale/different loop.
_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)

from pyrogram import Client, filters
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    Message,
)
from pyrogram.errors import (
    FloodWait, UserIsBlocked, InputUserDeactivated,
    PeerIdInvalid, ChatWriteForbidden,
)
from dotenv import load_dotenv
from curl_cffi import requests as cffi_requests
import httpx
from PIL import Image

try:
    from motor.motor_asyncio import AsyncIOMotorClient
except ImportError:
    AsyncIOMotorClient = None

load_dotenv()

# ── config ──────────────────────────────────────────────────────────────────

API_ID    = int(os.getenv("API_ID", "0"))
API_HASH  = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MONGO_URI = os.getenv("MONGO_URI", "").strip()
MONGO_DB  = os.getenv("MONGO_DB_NAME", "viralbots").strip()
DEAL_CHANNEL_ID = int(os.getenv("DEAL_CHANNEL_ID", "-1004383366007"))

if not API_ID or not API_HASH or not BOT_TOKEN:
    raise SystemExit("Missing API_ID / API_HASH / BOT_TOKEN in .env")


def _parse_admin_ids() -> set[int]:
    ids: set[int] = set()
    raw = os.getenv("ADMIN_USER_IDS", "").strip()
    for part in re.split(r"[,;\s]+", raw):
        part = part.strip()
        if part.lstrip("-").isdigit():
            ids.add(int(part))
    return ids


ADMIN_USER_IDS: set[int] = _parse_admin_ids()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("meesho")

MEESHO_RE = re.compile(r"https?://(?:www\.)?meesho\.com/\S+", re.IGNORECASE)

# Dalvik Android UA — bypasses Akamai WAF on www.meesho.com
DALVIK_UA = (
    "Dalvik/2.1.0 (Linux; U; Android 13; Pixel 7 Build/TQ3A.230705.001)"
)
MOBILE_UA = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Mobile Safari/537.36"
)
DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

FETCH_HEADERS = {
    "User-Agent": DALVIK_UA,
    "X-MEESHO-APP-VERSION": "18.8.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
}

# Per-chat screenshot mode toggle {chat_id: bool}
screenshot_mode: dict[int, bool] = {}


# ── DB / user management (bot-key aware, like Amazon Pricehist) ─────────────

_mongo_client = None
_mongo_db_obj = None
_users_col = None
_memory_users: set[int] = set()

BOT_KEY      = ""
BOT_KEY_SAFE = ""
BOT_USERNAME = ""
BOT_ID       = 0

USER_GONE = (UserIsBlocked, InputUserDeactivated, PeerIdInvalid, ChatWriteForbidden)

_broadcast_jobs: dict[str, Any] = {}


def _safe_field(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", (value or "").strip().lstrip("@"))
    return cleaned or "unknown_bot"


def _utc_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


async def _init_db(bot_client) -> None:
    global _mongo_client, _mongo_db_obj, _users_col
    global BOT_KEY, BOT_KEY_SAFE, BOT_USERNAME, BOT_ID

    me = await bot_client.get_me()
    BOT_USERNAME = getattr(me, "username", "") or ""
    BOT_ID       = int(getattr(me, "id", 0) or 0)
    BOT_KEY      = BOT_USERNAME or str(BOT_ID)
    BOT_KEY_SAFE = _safe_field(BOT_KEY)

    if not MONGO_URI or AsyncIOMotorClient is None:
        log.warning("No MONGO_URI — users stored in memory only.")
        return

    _mongo_client = AsyncIOMotorClient(MONGO_URI)
    _mongo_db_obj = _mongo_client[MONGO_DB]
    _users_col    = _mongo_db_obj["multi_bot_users"]

    await _users_col.create_index("user_id", unique=True)
    await _users_col.create_index(f"bots.{BOT_KEY_SAFE}.last_seen_at")
    await _users_col.create_index("bot_keys")
    log.info("MongoDB connected. BOT_KEY=%s", BOT_KEY)


async def _save_user(user_id: int, username: str = "",
                     first_name: str = "", last_name: str = "") -> None:
    if not user_id:
        return
    if _users_col is None:
        _memory_users.add(int(user_id))
        return
    now = _utc_now()
    bk  = BOT_KEY_SAFE
    await _users_col.update_one(
        {"user_id": int(user_id)},
        {
            "$set": {
                "user_id":    int(user_id),
                "username":   username,
                "first_name": first_name,
                "last_name":  last_name,
                "updated_at": now,
                f"bots.{bk}.bot_key":      BOT_KEY,
                f"bots.{bk}.bot_username": BOT_USERNAME,
                f"bots.{bk}.bot_id":       BOT_ID,
                f"bots.{bk}.last_seen_at": now,
            },
            "$setOnInsert": {"created_at": now},
            "$addToSet":    {"bot_keys": BOT_KEY},
        },
        upsert=True,
    )


async def _save_user_from_event(user) -> None:
    if user is None:
        return
    await _save_user(
        int(user.id),
        username=getattr(user, "username", None) or "",
        first_name=getattr(user, "first_name", None) or "",
        last_name=getattr(user, "last_name", None) or "",
    )


async def _remove_user(user_id: int) -> None:
    if _users_col is None:
        _memory_users.discard(int(user_id))
        return
    bk = BOT_KEY_SAFE
    await _users_col.update_one(
        {"user_id": int(user_id)},
        {
            "$unset": {f"bots.{bk}": ""},
            "$pull":  {"bot_keys": BOT_KEY},
            "$set":   {"updated_at": _utc_now()},
        },
    )
    doc = await _users_col.find_one({"user_id": int(user_id)}, {"bot_keys": 1})
    if doc and not doc.get("bot_keys"):
        await _users_col.delete_one({"user_id": int(user_id)})


async def _count_users() -> int:
    if _users_col is None:
        return len(_memory_users)
    return await _users_col.count_documents(
        {f"bots.{BOT_KEY_SAFE}.last_seen_at": {"$exists": True}}
    )


async def _iter_users():
    if _users_col is None:
        for uid in list(_memory_users):
            yield uid
        return
    query     = {f"bots.{BOT_KEY_SAFE}.last_seen_at": {"$exists": True}}
    page_size = 500
    last_id   = None
    while True:
        page_q = dict(query)
        if last_id is not None:
            page_q["_id"] = {"$gt": last_id}
        docs = await (
            _users_col.find(page_q, {"_id": 1, "user_id": 1})
            .sort("_id", 1)
            .limit(page_size)
            .to_list(length=page_size)
        )
        if not docs:
            return
        for doc in docs:
            last_id = doc["_id"]
            yield int(doc["user_id"])
        if len(docs) < page_size:
            return


# ── Meesho scraping ──────────────────────────────────────────────────────────

def clean_url(url: str) -> str:
    return url.rstrip(").,;]\n \t>")


def _walk(obj, keys: set, depth: int = 0, max_depth: int = 10):
    """Recursively walk JSON and yield (key, value) for matching keys."""
    if depth > max_depth:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys and v not in (None, "", 0, [], {}):
                yield k, v
            yield from _walk(v, keys, depth + 1, max_depth)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v, keys, depth + 1, max_depth)


def parse_product(html: str) -> dict:
    """
    Extract product info from __NEXT_DATA__ JSON embedded in Meesho SSR HTML.
    Returns dict: name, price, mrp, discount, images, url
    """
    info = {"name": "", "price": 0, "mrp": 0, "discount": 0, "images": [], "url": ""}

    # OG canonical URL
    m = re.search(r'og:url[^>]*content="([^"]+)"', html, re.I)
    if m:
        info["url"] = m.group(1)

    # Title fallback
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    if m:
        info["name"] = re.sub(
            r"\s*[\|·\-]\s*Meesho.*$", "", m.group(1), flags=re.I
        ).strip()

    # Parse __NEXT_DATA__
    pat = r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>'
    m = re.search(pat, html, re.S)
    if not m:
        return info

    try:
        data = json.loads(m.group(1))
    except json.JSONDecodeError:
        return info

    product = (
        data.get("props", {})
            .get("pageProps", {})
            .get("initialState", {})
            .get("product", {})
            .get("details", {})
            .get("data", {})
    )

    if product:
        if product.get("name"):
            info["name"] = product["name"]

        p = product.get("price") or product.get("sellingPrice")
        if p:
            info["price"] = int(p)

        mrp_details = product.get("mrp_details", {})
        mrp = mrp_details.get("mrp") or product.get("mrp")
        if mrp:
            info["mrp"] = int(mrp)

        imgs = product.get("images", [])
        seen: set = set()
        for img in imgs:
            if isinstance(img, str) and img.startswith("http") and img not in seen:
                seen.add(img)
                upgraded = re.sub(r"_\d+(?=\.\w+)", "_800", img)
                info["images"].append(upgraded)

    # Fallback: walk entire data tree if primary path found nothing
    if not info["price"]:
        for k, v in _walk(data, {"price", "sellingPrice", "finalPrice", "discountedPrice"}):
            if isinstance(v, (int, float)) and v > 0:
                info["price"] = int(v)
                break

    if not info["mrp"]:
        for k, v in _walk(data, {"mrp", "originalPrice", "maxRetailPrice"}):
            if isinstance(v, (int, float)) and v > 0:
                info["mrp"] = int(v)
                break

    if not info["images"]:
        seen = set()
        for k, v in _walk(data, {"images", "imageUrl", "image_url", "heroImage", "productImages"}):
            if isinstance(v, str) and v.startswith("http") and v not in seen:
                seen.add(v)
                info["images"].append(v)
            elif isinstance(v, list):
                for x in v:
                    if isinstance(x, str) and x.startswith("http") and x not in seen:
                        seen.add(x)
                        info["images"].append(x)

    if info["mrp"] and info["price"] and info["mrp"] > info["price"]:
        info["discount"] = round(100 - (info["price"] / info["mrp"] * 100))

    return info


def fetch_html(url: str) -> str:
    """
    Fetch Meesho product page using curl_cffi (Chrome TLS fingerprint)
    + Dalvik Android UA — bypasses Akamai bot protection.
    """
    resp = cffi_requests.get(
        url,
        impersonate="chrome131",
        headers=FETCH_HEADERS,
        allow_redirects=True,
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} from Meesho")
    if "access denied" in resp.text.lower() and "edgesuite" in resp.text.lower():
        raise RuntimeError("Akamai blocked — unable to fetch page")
    return resp.text


async def download_image(url: str, dest: Path) -> bool:
    """Download a product image from images.meesho.com CDN."""
    try:
        async with httpx.AsyncClient(
            headers={
                "User-Agent": DALVIK_UA,
                "Accept": "image/avif,image/webp,image/jpeg,image/png,*/*",
                "Referer": "https://www.meesho.com/",
            },
            follow_redirects=True,
            timeout=30,
        ) as client:
            r = await client.get(url)
            r.raise_for_status()
            if len(r.content) < 1000:
                return False
            dest.write_bytes(r.content)
            return True
    except Exception as e:
        log.warning("Image download failed: %s", e)
        return False


def sanitize_photo(image_path: Path, max_dim: int = 2048) -> Path:
    """
    Ensure image complies with Telegram photo dimension limits:
    - Sum of width + height <= 10000
    - Aspect ratio <= 20:1
    - Max dimension <= 2048
    """
    try:
        with Image.open(image_path) as img:
            w, h = img.size
            if w <= 0 or h <= 0:
                return image_path

            modified = False
            # Telegram requires aspect ratio <= 20:1. We cap at 15:1 for safety.
            max_ratio = 15.0
            if h > w * max_ratio:
                img = img.crop((0, 0, w, int(w * max_ratio)))
                w, h = img.size
                modified = True
            elif w > h * max_ratio:
                img = img.crop((0, 0, int(h * max_ratio), h))
                w, h = img.size
                modified = True

            # Telegram requires width + height <= 10000; keep max dimension <= 2048
            if max(w, h) > max_dim or (w + h) > 8000:
                scale = min(max_dim / max(w, h), 8000 / (w + h))
                new_w = max(1, int(w * scale))
                new_h = max(1, int(h * scale))
                img = img.resize((new_w, new_h), Image.Resampling.LANCZOS)
                modified = True

            if modified or img.format not in ("JPEG", "JPG"):
                if img.mode in ("RGBA", "P"):
                    img = img.convert("RGB")
                img.save(image_path, format="JPEG", quality=92)
    except Exception as e:
        log.warning("sanitize_photo failed for %s: %s", image_path, e)
    return image_path


async def take_screenshot(html: str, canonical_url: str) -> bytes | None:
    """
    Render pre-fetched HTML locally in Playwright and capture a clean screenshot of
    the product view.
    """
    try:
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            ctx = await browser.new_context(
                viewport={"width": 1150, "height": 760},
                user_agent=DESKTOP_UA,
                locale="en-IN",
                timezone_id="Asia/Kolkata",
                device_scale_factor=1.5,
            )
            page = await ctx.new_page()

            # Intercept the initial document request only — sub-resources pass through
            async def handle_route(route):
                if route.request.resource_type == "document":
                    await route.fulfill(
                        body=html,
                        content_type="text/html; charset=utf-8",
                        status=200,
                    )
                else:
                    await route.continue_()

            await page.route("**", handle_route)
            await page.goto(
                canonical_url or "https://www.meesho.com/",
                wait_until="domcontentloaded",
                timeout=30_000,
            )

            # Wait for network to mostly settle and images to load
            try:
                await page.wait_for_load_state("networkidle", timeout=6_000)
            except Exception:
                pass
            await asyncio.sleep(1)

            # Hide top navigation bar, search bar, and app download banner
            await page.evaluate('''() => {
                const toHide = document.querySelectorAll(
                    'header, nav, div[class*="header"], div[class*="Header"], img[src*="marketing"], [id*="header"]'
                );
                toHide.forEach(el => { el.style.display = 'none'; });
            }''')
            await asyncio.sleep(0.5)

            # Viewport screenshot has controlled dimensions (1725 x 1140 px at 1.5x)
            screenshot_bytes = await page.screenshot()
            await browser.close()
            return screenshot_bytes

    except Exception as e:
        log.exception("Screenshot rendering failed: %s", e)
        return None


def build_caption(info: dict, original_url: str) -> str:
    """Build a formatted Meesho deal caption."""
    name = re.sub(r"\s+", " ", info["name"]).strip()
    if len(name) > 100:
        name = name[:97].rstrip() + "..."

    price = info["price"]
    mrp   = info["mrp"]

    lines = [f"**{name}**\n"]

    if price:
        if mrp and mrp > price:
            lines.append(f"Price: ~~Rs.{mrp}~~ ❌ → **Rs.{price}**")
        else:
            lines.append(f"Price: **Rs.{price}**")

    lines.append("Get additional Discount in Mobile App")
    lines.append("")
    link = info.get("url") or original_url
    lines.append(link)

    return "\n".join(lines)



# ── Broadcast helpers ────────────────────────────────────────────────────────

def _bcast_txt(s: dict, running: bool = True) -> str:
    if s.get("cancelled"):
        status = "🛑 Broadcast cancelled"
    elif running:
        status = "🚀 Broadcast running"
    else:
        status = "✅ Broadcast finished"
    elapsed = max(1, time.time() - s["started_at"])
    speed   = s["done"] / elapsed
    return (
        f"{status}\n\n"
        f"👥 Total: {s['total']}\n"
        f"📨 Done:  {s['done']}\n"
        f"✅ Sent:  {s['sent']}\n"
        f"❌ Failed:{s['failed']}\n"
        f"🗑 Deleted:{s['deleted']}\n"
        f"⚡ Speed: {speed:.1f}/s"
    )


def _cancel_kb(job_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🛑 Cancel Broadcast", callback_data=f"cancel_broadcast:{job_id}")]]
    )


async def _run_broadcast(
    pyrogram_client, admin_chat_id: int, status_msg_id: int, source: dict
) -> None:
    job_id = str(status_msg_id)
    cancel = asyncio.Event()
    stats  = {
        "started_at": time.time(),
        "total": await _count_users(),
        "done": 0, "sent": 0, "failed": 0, "deleted": 0, "cancelled": 0,
    }
    _broadcast_jobs[job_id] = {"cancel": cancel, "stats": stats}
    last_edit = 0.0

    try:
        try:
            await pyrogram_client.edit_message_text(
                chat_id=admin_chat_id, message_id=status_msg_id,
                text=_bcast_txt(stats), reply_markup=_cancel_kb(job_id),
            )
        except Exception:
            pass

        async for uid in _iter_users():
            if cancel.is_set():
                stats["cancelled"] = 1
                break
            try:
                if source["type"] == "copy":
                    src: Message = source["message"]
                    await pyrogram_client.copy_message(
                        chat_id=uid,
                        from_chat_id=src.chat.id,
                        message_id=src.id,
                        reply_markup=None,
                    )
                else:
                    await pyrogram_client.send_message(uid, source["text"])
                stats["sent"] += 1
            except FloodWait as e:
                await asyncio.sleep(e.value)
                try:
                    if source["type"] == "copy":
                        src = source["message"]
                        await pyrogram_client.copy_message(
                            chat_id=uid, from_chat_id=src.chat.id,
                            message_id=src.id, reply_markup=None,
                        )
                    else:
                        await pyrogram_client.send_message(uid, source["text"])
                    stats["sent"] += 1
                except Exception as e2:
                    stats["failed"] += 1
                    if isinstance(e2, USER_GONE):
                        await _remove_user(uid)
                        stats["deleted"] += 1
            except Exception as e:
                stats["failed"] += 1
                if isinstance(e, USER_GONE):
                    await _remove_user(uid)
                    stats["deleted"] += 1

            stats["done"] += 1
            now = time.time()
            if now - last_edit >= 2 or stats["done"] == stats["total"]:
                last_edit = now
                try:
                    await pyrogram_client.edit_message_text(
                        chat_id=admin_chat_id, message_id=status_msg_id,
                        text=_bcast_txt(stats), reply_markup=_cancel_kb(job_id),
                    )
                except Exception:
                    pass
            await asyncio.sleep(0.04)

    finally:
        _broadcast_jobs.pop(job_id, None)
        try:
            await pyrogram_client.edit_message_text(
                chat_id=admin_chat_id, message_id=status_msg_id,
                text=_bcast_txt(stats, running=False),
            )
        except Exception:
            pass


# ── Pyrogram bot ─────────────────────────────────────────────────────────────

app = Client("meeshoposter_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


@app.on_message(filters.command("start") & filters.private)
async def cmd_start(client, message: Message):
    user = message.from_user
    # Save every user who starts the bot for broadcast
    asyncio.create_task(_save_user_from_event(user))
    if user and user.id in ADMIN_USER_IDS:
        await message.reply(
            "👋 **MeeshoPoster Admin** ready!\n\n"
            "Send any Meesho link to get the deal post.\n\n"
            "**Commands:**\n"
            "• `/broadcast` — reply to a post or add text\n"
            "• `/status` — show user count\n"
            "• `/screenshot_on` / `/screenshot_off` — toggle screenshot mode"
        )
    else:
        await message.reply(
            "�️ **Meesho Loot Deals Bot**\n\n"
            "This bot is used by our team to generate Meesho deal posts.\n\n"
            "📢 **Want the best Meesho deals?**\n"
            "Join our deals channel where we post the hottest offers every day:\n\n"
            "👉 **[MeeshoLootOffers](https://t.me/MeeshoLootOffers)** — Free deals, loot prices & discount alerts on Meesho!\n\n"
            "🔔 You've been registered — we may send you deal updates directly too!"
        )


@app.on_message(filters.command("screenshot_on"))
async def cmd_screenshot_on(client, message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_USER_IDS:
        return
    screenshot_mode[message.chat.id] = True
    await message.reply(
        "📸 **Screenshot mode ON**\n"
        "I'll render a real webpage screenshot for each product (~15s).\n"
        "Use /screenshot_off to go back to fast CDN image mode."
    )


@app.on_message(filters.command("screenshot_off"))
async def cmd_screenshot_off(client, message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_USER_IDS:
        return
    screenshot_mode[message.chat.id] = False
    await message.reply(
        "⚡ **Screenshot mode OFF**\n"
        "Back to fast CDN image mode (default).\n"
        "Use /screenshot_on to enable webpage screenshots."
    )


@app.on_message(filters.command("status"))
async def cmd_status(client, message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_USER_IDS:
        return
    count = await _count_users()
    await message.reply(
        f"📊 **Bot Status**\n\n"
        f"👥 Users in DB: `{count}`\n"
        f"🔑 Bot key: `{BOT_KEY_SAFE}`\n"
        f"💾 Storage: `{'MongoDB' if _users_col is not None else 'Memory'}`\n"
        f"📢 Deal channel: `{DEAL_CHANNEL_ID}`"
    )


@app.on_message(filters.command("broadcast"))
async def cmd_broadcast(client, message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_USER_IDS:
        return

    if _broadcast_jobs:
        await message.reply("⚠️ A broadcast is already running.")
        return

    reply = message.reply_to_message
    payload = ""
    if message.text and len(message.text.split(maxsplit=1)) > 1:
        payload = message.text.split(maxsplit=1)[1].strip()

    if reply is not None:
        source = {"type": "copy", "message": reply}
    elif payload:
        source = {"type": "text", "text": payload}
    else:
        await message.reply(
            "📣 **How to broadcast:**\n"
            "• Reply to any post with `/broadcast` to forward it to all users\n"
            "• Or: `/broadcast your message here` for plain text"
        )
        return

    total = await _count_users()
    status = await message.reply(
        f"🚀 **Broadcast starting...**\n\n"
        f"🤖 Bot: @{BOT_USERNAME or BOT_KEY}\n"
        f"👥 Users: `{total}`",
        reply_markup=_cancel_kb("pending"),
    )
    asyncio.create_task(_run_broadcast(client, message.chat.id, status.id, source))


@app.on_callback_query(filters.regex(r"^cancel_broadcast:"))
async def cb_cancel_broadcast(client, callback_query):
    user_id = callback_query.from_user.id if callback_query.from_user else 0
    if user_id not in ADMIN_USER_IDS:
        await callback_query.answer("Only admin can cancel.", show_alert=True)
        return

    job_id = callback_query.data.split(":", 1)[1]
    if job_id == "pending":
        await callback_query.answer("Starting soon…")
        return

    job = _broadcast_jobs.get(job_id)
    if job:
        job["cancel"].set()
    await callback_query.answer("🛑 Cancelling broadcast…")


@app.on_message(filters.private & ~filters.command(
    ["start", "broadcast", "status", "screenshot_on", "screenshot_off"]
))
async def on_message(client, message: Message):
    user = message.from_user
    text = message.text or message.caption or ""

    # Save any user who interacts
    if user:
        asyncio.create_task(_save_user_from_event(user))

    # Only admins can use the Meesho link feature
    if not user or user.id not in ADMIN_USER_IDS:
        return

    m = MEESHO_RE.search(text)
    if not m:
        return

    url = clean_url(m.group(0))
    use_screenshot = screenshot_mode.get(message.chat.id, False)

    status_text = (
        "⏳ Fetching product & rendering screenshot..."
        if use_screenshot
        else "⏳ Fetching product info..."
    )
    status = await message.reply(status_text)

    tmp: Path | None = None
    try:
        html = await asyncio.to_thread(fetch_html, url)
        info = parse_product(html)

        log.info(
            "Product: %s | Rs.%s (MRP Rs.%s, %s%% off) | %d images | screenshot=%s",
            info["name"][:50], info["price"], info["mrp"],
            info["discount"], len(info["images"]), use_screenshot,
        )

        caption = build_caption(info, url)
        sent_message = None

        if use_screenshot:
            canonical = info.get("url") or url
            screenshot_bytes = await take_screenshot(html, canonical)

            if screenshot_bytes:
                with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as t:
                    tmp = Path(t.name)
                tmp.write_bytes(screenshot_bytes)
                sanitize_photo(tmp)
                sent_message = await client.send_photo(
                    chat_id=message.chat.id,
                    photo=str(tmp),
                    caption=caption,
                    reply_to_message_id=message.id,
                )
            else:
                sent_message = await message.reply(
                    caption + "\n\n_(Screenshot failed — caption only)_",
                )

        else:
            sent = False
            for img_url in info["images"][:6]:
                with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as t:
                    tmp = Path(t.name)
                if await download_image(img_url, tmp):
                    sanitize_photo(tmp)
                    sent_message = await client.send_photo(
                        chat_id=message.chat.id,
                        photo=str(tmp),
                        caption=caption,
                        reply_to_message_id=message.id,
                    )
                    sent = True
                    break
                else:
                    try:
                        tmp.unlink()
                    except OSError:
                        pass
                    tmp = None

            if not sent:
                sent_message = await message.reply(caption)

        # ── Auto-post to deal channel ────────────────────────────────────────
        if sent_message is not None:
            try:
                await client.copy_message(
                    chat_id=DEAL_CHANNEL_ID,
                    from_chat_id=sent_message.chat.id,
                    message_id=sent_message.id,
                )
                log.info("Deal posted to channel %s", DEAL_CHANNEL_ID)
            except Exception as e:
                log.warning("Failed to post to deal channel: %s", e)

        try:
            await status.delete()
        except Exception:
            pass

    except Exception as e:
        log.exception("Failed to process: %s", url)
        try:
            await status.edit_text(f"❌ Failed: {e}")
        except Exception:
            await message.reply(f"❌ Failed: {e}")
    finally:
        if tmp and tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


async def _start_health_server():
    port_str = os.getenv("PORT")
    if not port_str:
        return None
    try:
        port = int(port_str)

        async def handle_client(reader, writer):
            try:
                await reader.read(1024)
                response = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: text/plain\r\n"
                    b"Content-Length: 2\r\n"
                    b"Connection: close\r\n\r\n"
                    b"OK"
                )
                writer.write(response)
                await writer.drain()
            except Exception:
                pass
            finally:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

        server = await asyncio.start_server(handle_client, "0.0.0.0", port)
        log.info("Health server running on port %d", port)
        return server
    except Exception as e:
        log.warning("Failed to start health server: %s", e)
        return None


async def main():
    health_server = await _start_health_server()
    async with app:
        await _init_db(app)
        log.info("MeeshoPoster bot running.")
        log.info("Admins: %s", ADMIN_USER_IDS)
        log.info("Deal channel: %s", DEAL_CHANNEL_ID)
        try:
            await asyncio.Event().wait()  # block until Ctrl+C
        finally:
            if health_server:
                health_server.close()
                await health_server.wait_closed()


if __name__ == "__main__":
    try:
        _loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        _loop.close()
