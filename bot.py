"""
Consume — бот заявок для Discord (discord.py 2.x).
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import os
import re
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timezone, timedelta, time
from pathlib import Path
from typing import Any, Literal

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

load_dotenv()

BOT_DIR = Path(__file__).resolve().parent
STATE_PATH = BOT_DIR / "data" / "state.json"
TICKETS_PATH = BOT_DIR / "data" / "tickets.json"
SQLITE_PATH = BOT_DIR / "data" / "bot.sqlite3"

logger = logging.getLogger("consume")
INTENTS = discord.Intents.default()


class _JsonLogFormatter(logging.Formatter):
    """Одна строка = один JSON-объект (удобно для парсинга и внешних сборщиков логов)."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def _configure_json_logging() -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonLogFormatter())
    root.addHandler(handler)


def _start_fly_health_server_if_needed() -> None:
    """На Fly.io health-check идёт на internal_port (обычно 8080); бот сам HTTP не поднимает."""
    if not os.getenv("FLY_APP_NAME"):
        return
    raw = (os.getenv("PORT") or "8080").strip()
    try:
        port = int(raw)
    except ValueError:
        port = 8080

    class _HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *_args: object) -> None:
            pass

    def _serve() -> None:
        httpd = HTTPServer(("0.0.0.0", port), _HealthHandler)
        httpd.serve_forever()

    threading.Thread(target=_serve, name="fly-health", daemon=True).start()
INTENTS.message_content = True  # нужен для слежения за сообщениями в канале
INTENTS.members = True

TicketPhase = Literal["initial", "interview"]
TicketKind = Literal["rp", "vzp"]


def _parse_id_list(raw: str) -> list[int]:
    out: list[int] = []
    for part in raw.replace(" ", "").split(","):
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return out


def get_env_int(name: str, default: int = 0) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(f"{name}: ожидается целое число, получено {raw!r}.")


def get_env_int_set(name: str) -> frozenset[int]:
    return frozenset(_parse_id_list(os.getenv(name, "")))


def _mod_role_ids() -> frozenset[int]:
    return frozenset(_parse_id_list(os.getenv("MODERATOR_ROLE_ID", "")))


MODERATION_ROLE_IDS = _mod_role_ids()


def role_ids_or_moderation(role_ids: frozenset[int]) -> frozenset[int]:
    return role_ids if role_ids else MODERATION_ROLE_IDS


async def _safe_interaction_ephemeral(interaction: discord.Interaction, content: str) -> bool:
    """Пытается отправить ephemeral-ответ и не валит обработчик при сбоях API."""
    try:
        if interaction.response.is_done():
            await interaction.followup.send(content, ephemeral=True)
        else:
            await interaction.response.send_message(content, ephemeral=True)
        return True
    except discord.HTTPException as e:
        # Частые гонки: interaction/канал/вебхук уже недоступны.
        if getattr(e, "code", None) in {10003, 10015, 10062}:
            logger.warning("Не удалось отправить ephemeral-ответ: %s (%s)", e, getattr(e, "code", "n/a"))
            return False
        logger.exception("Ошибка отправки ephemeral-ответа")
        return False
    except Exception:
        logger.exception("Неожиданная ошибка отправки ephemeral-ответа")
        return False


def _is_stale_interaction_error(error: Exception) -> bool:
    if not isinstance(error, discord.HTTPException):
        return False
    return getattr(error, "code", None) in {10003, 10015, 10062}


class SafeView(discord.ui.View):
    async def on_error(
        self, interaction: discord.Interaction, error: Exception, item: discord.ui.Item[Any]
    ) -> None:
        if _is_stale_interaction_error(error):
            logger.warning("Устаревшее взаимодействие в view %s: %s", type(self).__name__, error)
            return
        logger.exception("Ошибка в view %s (item=%s)", type(self).__name__, type(item).__name__, exc_info=error)
        await _safe_interaction_ephemeral(
            interaction,
            "Взаимодействие не удалось обработать. Попробуйте снова через пару секунд.",
        )


class SafeModal(discord.ui.Modal):
    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        if _is_stale_interaction_error(error):
            logger.warning("Устаревшее взаимодействие в modal %s: %s", type(self).__name__, error)
            return
        logger.exception("Ошибка в modal %s", type(self).__name__, exc_info=error)
        await _safe_interaction_ephemeral(
            interaction,
            "Не удалось обработать форму. Откройте её заново и повторите отправку.",
        )


def _ticket_staff_role_ids() -> frozenset[int]:
    return frozenset(_parse_id_list(os.getenv("TICKET_STAFF_ROLE_IDS", "")))


def _ticket_ping_role_ids() -> list[int]:
    """Пинг при новом тикете. Если TICKET_PING_ROLE_IDS пуст — берутся TICKET_STAFF_ROLE_IDS."""
    raw = os.getenv("TICKET_PING_ROLE_IDS", "").strip()
    if raw:
        return _parse_id_list(raw)
    return sorted(_ticket_staff_role_ids())


ROLE_MENTION_DM_TARGET_ROLE_IDS = frozenset(
    _parse_id_list(os.getenv("ROLE_MENTION_DM_TARGET_ROLE_IDS", ""))
)
ROLE_MENTION_DM_CATEGORY_IDS = frozenset(
    _parse_id_list(os.getenv("ROLE_MENTION_DM_CATEGORY_IDS", ""))
)
ROLE_MENTION_DM_CHANNEL_IDS = frozenset(
    _parse_id_list(os.getenv("ROLE_MENTION_DM_CHANNEL_IDS", ""))
)
ROLE_MENTION_DM_TRIGGER_ROLE_IDS = frozenset(
    _parse_id_list(os.getenv("ROLE_MENTION_DM_TRIGGER_ROLE_IDS", ""))
)
BOT_ACTION_LOG_CHANNEL_ID = get_env_int("BOT_ACTION_LOG_CHANNEL_ID")


def role_mention_dm_watchlist_matches_channel(
    ch: discord.TextChannel | discord.VoiceChannel | discord.StageChannel | discord.Thread,
) -> bool:
    channel_id = ch.id
    parent_id: int | None = None
    category_id: int | None = None

    if isinstance(ch, discord.Thread):
        parent_id = ch.parent_id
        if ch.parent is not None:
            category_id = ch.parent.category_id
    else:
        category_id = ch.category_id

    if ROLE_MENTION_DM_CHANNEL_IDS:
        if channel_id in ROLE_MENTION_DM_CHANNEL_IDS:
            return True
        if parent_id is not None and parent_id in ROLE_MENTION_DM_CHANNEL_IDS:
            return True
    if ROLE_MENTION_DM_CATEGORY_IDS and category_id is not None:
        if category_id in ROLE_MENTION_DM_CATEGORY_IDS:
            return True
    return False


def user_can_trigger_role_mention_dm(member: discord.Member) -> bool:
    if not ROLE_MENTION_DM_TRIGGER_ROLE_IDS:
        return True
    return any(r.id in ROLE_MENTION_DM_TRIGGER_ROLE_IDS for r in member.roles)


_TIME_HHMM_RE = re.compile(r"^(\d{1,2}):(\d{2})$")


def parse_daily_role_ping_times(raw: str) -> list[tuple[int, int]]:
    """DAILY_ROLE_PING_TIMES: «09:00, 18:30» -> список (час, минута), без дублей."""
    out: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        m = _TIME_HHMM_RE.match(part)
        if not m:
            raise RuntimeError(
                f"DAILY_ROLE_PING_TIMES: неверный фрагмент {part!r}. Нужен формат ЧЧ:ММ, несколько через запятую."
            )
        h, minute = int(m.group(1)), int(m.group(2))
        if h > 23 or minute > 59:
            raise RuntimeError(
                f"DAILY_ROLE_PING_TIMES: недопустимое время {part!r} (часы 0-23, минуты 0-59)."
            )
        key = (h, minute)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def next_daily_role_ping_fire(tz: ZoneInfo, times: list[tuple[int, int]]) -> datetime:
    """Ближайший момент срабатывания из списка времен суток (timezone-aware)."""
    now = datetime.now(tz)
    today = now.date()
    best: datetime | None = None
    for h, m in times:
        cand = datetime.combine(today, time(h, m, 0), tzinfo=tz)
        if cand > now and (best is None or cand < best):
            best = cand
    if best is not None:
        return best
    tomorrow = today + timedelta(days=1)
    for h, m in times:
        cand = datetime.combine(tomorrow, time(h, m, 0), tzinfo=tz)
        if best is None or cand < best:
            best = cand
    assert best is not None
    return best


# Пост с <@&роль> в канале -> удалить предыдущий пост бота -> новый (по кругу).
# Режим 1: DAILY_ROLE_PING_TIMES=09:00,21:30 (часовой пояс DAILY_ROLE_PING_TIMEZONE).
# Режим 2: если DAILY_ROLE_PING_TIMES пуст — через DAILY_ROLE_PING_INTERVAL_HOURS часов.
DAILY_ROLE_PING_CHANNEL_ID = get_env_int("DAILY_ROLE_PING_CHANNEL_ID")
DAILY_ROLE_PING_ROLE_ID = get_env_int("DAILY_ROLE_PING_ROLE_ID")
_drp_iv = get_env_int("DAILY_ROLE_PING_INTERVAL_HOURS", 23)
DAILY_ROLE_PING_INTERVAL_HOURS = max(1, min(168, _drp_iv if _drp_iv > 0 else 23))
DAILY_ROLE_PING_MESSAGE = os.getenv("DAILY_ROLE_PING_MESSAGE", "").strip()
_drp_times_raw = os.getenv("DAILY_ROLE_PING_TIMES", "").strip()
DAILY_ROLE_PING_SCHEDULE = (
    parse_daily_role_ping_times(_drp_times_raw) if _drp_times_raw else []
)
_drp_tz_name = (
    os.getenv("DAILY_ROLE_PING_TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"
)
try:
    DAILY_ROLE_PING_TZ: ZoneInfo | None = (
        ZoneInfo(_drp_tz_name) if DAILY_ROLE_PING_SCHEDULE else None
    )
except ZoneInfoNotFoundError as exc:
    raise RuntimeError(
        f"DAILY_ROLE_PING_TIMEZONE: неизвестная зона {_drp_tz_name!r}."
    ) from exc

try:
    SBOR_TZ = ZoneInfo("Europe/Moscow")
except ZoneInfoNotFoundError as exc:
    raise RuntimeError("Не удалось загрузить таймзону Europe/Moscow для сборов.") from exc


# Контракты: /kontrakt — панель; заявки с Участвовать / Пикнул / Отказ.
# Пустые KONTRAKT_*_ROLE_IDS -> MODERATION_ROLE_IDS.
KONTRAKT_CHANNEL_ID = get_env_int("KONTRAKT_CHANNEL_ID")
KONTRAKT_POST_ROLE_IDS = get_env_int_set("KONTRAKT_POST_ROLE_IDS")
KONTRAKT_MANAGER_ROLE_IDS = get_env_int_set("KONTRAKT_MANAGER_ROLE_IDS")
KONTRAKT_NEW_CONTRACT_PING_ROLE_IDS = get_env_int_set(
    "KONTRAKT_NEW_CONTRACT_PING_ROLE_IDS"
)
_KONTRAKT_RULES_DEFAULT = (
    "Здесь будут правила контрактов.\n\n"
    "Задайте текст в **KONTRAKT_RULES_TEXT** в .env (несколько строк через \\n)."
)
KONTRAKT_RULES_TEXT = (
    os.getenv("KONTRAKT_RULES_TEXT", "").strip() or _KONTRAKT_RULES_DEFAULT
)
_KONTRAKT_PEOPLE_RANGE = re.compile(r"(?i)(?:от\s)?(\d+)\s*[-–]\s*(\d+)")


@dataclasses.dataclass
class ContractState:
    channel_id: int
    creator_id: int
    creator_tag: str
    title: str
    veksels: str
    time_slot: str
    razdel_100: str
    people_note: str
    max_participants: int
    participant_ids: list[int] = dataclasses.field(default_factory=list)
    status_open: bool = True
    status_note: str = "Открыт"


CONTRACT_MESSAGES: dict[int, ContractState] = {}


def _sqlite_connect() -> sqlite3.Connection:
    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init_sqlite() -> None:
    with _sqlite_connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contracts (
                message_id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                creator_id INTEGER NOT NULL,
                creator_tag TEXT NOT NULL,
                title TEXT NOT NULL,
                veksels TEXT NOT NULL,
                time_slot TEXT NOT NULL,
                razdel_100 TEXT NOT NULL,
                people_note TEXT NOT NULL,
                max_participants INTEGER NOT NULL,
                participant_ids TEXT NOT NULL,
                status_open INTEGER NOT NULL,
                status_note TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS autopark_cars (
                guild_id INTEGER NOT NULL,
                car_key TEXT NOT NULL,
                label TEXT NOT NULL,
                note TEXT NOT NULL,
                role_ids_json TEXT NOT NULL,
                reserved_by INTEGER,
                reserved_until_ts INTEGER,
                PRIMARY KEY (guild_id, car_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS autopark_panels (
                message_id INTEGER PRIMARY KEY,
                guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL
            )
            """
        )
        conn.commit()


def _kv_set_json(key: str, value: dict[str, Any]) -> None:
    payload = json.dumps(value, ensure_ascii=False)
    with _sqlite_connect() as conn:
        conn.execute(
            """
            INSERT INTO kv_store (key, value_json) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json
            """,
            (key, payload),
        )
        conn.commit()


def _kv_get_json(key: str) -> dict[str, Any] | None:
    with _sqlite_connect() as conn:
        row = conn.execute(
            "SELECT value_json FROM kv_store WHERE key = ?",
            (key,),
        ).fetchone()
    if row is None:
        return None
    try:
        data = json.loads(str(row["value_json"]))
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def _contract_store(message_id: int, state: ContractState) -> None:
    with _sqlite_connect() as conn:
        conn.execute(
            """
            INSERT INTO contracts (
                message_id, channel_id, creator_id, creator_tag, title, veksels,
                time_slot, razdel_100, people_note, max_participants,
                participant_ids, status_open, status_note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(message_id) DO UPDATE SET
                channel_id=excluded.channel_id,
                creator_id=excluded.creator_id,
                creator_tag=excluded.creator_tag,
                title=excluded.title,
                veksels=excluded.veksels,
                time_slot=excluded.time_slot,
                razdel_100=excluded.razdel_100,
                people_note=excluded.people_note,
                max_participants=excluded.max_participants,
                participant_ids=excluded.participant_ids,
                status_open=excluded.status_open,
                status_note=excluded.status_note
            """,
            (
                message_id,
                state.channel_id,
                state.creator_id,
                state.creator_tag,
                state.title,
                state.veksels,
                state.time_slot,
                state.razdel_100,
                state.people_note,
                state.max_participants,
                json.dumps(state.participant_ids, ensure_ascii=False),
                1 if state.status_open else 0,
                state.status_note,
            ),
        )
        conn.commit()
    CONTRACT_MESSAGES[message_id] = state


def _contract_load(message_id: int) -> ContractState | None:
    cached = CONTRACT_MESSAGES.get(message_id)
    if cached is not None:
        return cached
    with _sqlite_connect() as conn:
        row = conn.execute(
            "SELECT * FROM contracts WHERE message_id = ?",
            (message_id,),
        ).fetchone()
    if row is None:
        return None
    participants_raw = row["participant_ids"] or "[]"
    try:
        participant_ids = [int(x) for x in json.loads(participants_raw)]
    except (ValueError, json.JSONDecodeError, TypeError):
        participant_ids = []
    state = ContractState(
        channel_id=int(row["channel_id"]),
        creator_id=int(row["creator_id"]),
        creator_tag=str(row["creator_tag"]),
        title=str(row["title"]),
        veksels=str(row["veksels"]),
        time_slot=str(row["time_slot"]),
        razdel_100=str(row["razdel_100"]),
        people_note=str(row["people_note"]),
        max_participants=int(row["max_participants"]),
        participant_ids=participant_ids,
        status_open=bool(row["status_open"]),
        status_note=str(row["status_note"]),
    )
    CONTRACT_MESSAGES[message_id] = state
    return state


_init_sqlite()


def kontrakt_allowed_in_channel(interaction: discord.Interaction) -> bool:
    if not KONTRAKT_CHANNEL_ID:
        return True
    return interaction.channel_id == KONTRAKT_CHANNEL_ID


def kontrakt_channel_restriction_message() -> str:
    if not KONTRAKT_CHANNEL_ID:
        return ""
    return f"Контракт можно отправить только в <#{KONTRAKT_CHANNEL_ID}>."


def user_can_post_kontrakt_panel(interaction: discord.Interaction) -> bool:
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        return False
    member = interaction.user
    if interaction.guild.owner_id == member.id:
        return True
    if member.guild_permissions.manage_guild:
        return True
    post = role_ids_or_moderation(KONTRAKT_POST_ROLE_IDS)
    if not post:
        return False
    return any(r.id in post for r in member.roles)


def user_can_manage_kontrakt_contract(
    interaction: discord.Interaction, state: ContractState
) -> bool:
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        return False
    member = interaction.user
    if interaction.guild.owner_id == member.id:
        return True
    if member.guild_permissions.manage_guild:
        return True
    mgr = role_ids_or_moderation(KONTRAKT_MANAGER_ROLE_IDS)
    if mgr and any(r.id in mgr for r in member.roles):
        return True
    return False


def _kontrakt_manage_forbidden_embed() -> discord.Embed:
    return discord.Embed(
        title="Нет доступа",
        description=(
            "Пикнул и Отказ: MODERATOR_ROLE_ID или KONTRAKT_MANAGER_ROLE_IDS (если задан)."
        ),
        color=discord.Color.dark_red(),
    )


def parse_kontrakt_people_cap(raw: str) -> tuple[int, str]:
    """Верхняя граница набора из «От 2-6» и т.п.; подпись для футера."""
    s = raw.strip()
    note = (s[:100] + "...") if len(s) > 100 else (s or "—")
    if not s:
        return 6, "—"
    m = _KONTRAKT_PEOPLE_RANGE.search(s)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        cap = max(lo, hi)
        cap = min(max(cap, 1), 40)
        return cap, note
    m2 = re.search(r"\d+", s)
    if m2:
        cap = min(max(int(m2.group(0)), 1), 40)
        return cap, note
    return 6, note


def build_kontrakt_panel_embed() -> discord.Embed:
    return discord.Embed(
        title="📋 Контракт",
        description=KONTRAKT_RULES_TEXT[:4000],
        color=discord.Color.dark_theme(),
    )


def _kontrakt_participants_render(ids: list[int]) -> str:
    if not ids:
        return "—"
    return "\n".join(f"<@{uid}>" for uid in ids)


def build_kontrakt_contract_embed(state: ContractState) -> discord.Embed:
    emb = discord.Embed(
        title=state.title[:256] or "Контракт",
        color=discord.Color.dark_theme(),
    )
    emb.add_field(
        name="Автор",
        value=f"<@{state.creator_id}>\nКонтракт: {state.title[:256] or '—'}",
        inline=False,
    )
    emb.add_field(name="На 100%", value=state.razdel_100 or "—", inline=False)
    emb.add_field(
        name=f"Участники ({len(state.participant_ids)}/{state.max_participants})",
        value=_kontrakt_participants_render(state.participant_ids),
        inline=False,
    )
    emb.set_footer(text=f"Людей: {state.people_note} | Статус: {state.status_note}")
    emb.timestamp = datetime.now(timezone.utc)
    return emb

def _ticket_open_message_and_mentions(
    guild: discord.Guild,
    applicant: discord.Member,
) -> tuple[str, discord.AllowedMentions]:
    role_parts: list[str] = []
    role_objs: list[discord.Object] = []
    for rid in _ticket_ping_role_ids():
        if guild.get_role(rid) is not None:
            role_parts.append(f"<@&{rid}>")
            role_objs.append(discord.Object(id=rid))
    line = " ".join(role_parts)
    if line:
        content = line
        allowed = discord.AllowedMentions(everyone=False, roles=role_objs)
    else:
        content = applicant.mention
        allowed = discord.AllowedMentions(everyone=False, users=[applicant])
    return content, allowed


def _ticket_category_id() -> int | None:
    raw = os.getenv("TICKET_CATEGORY_ID", "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _accept_role_ids(track: Literal["academy", "main"]) -> list[int]:
    """
    Роли, выдаваемые при принятии заявки.
    academy -> ACCEPT_ROLE_ID_ACADEMY (fallback: ACCEPT_ROLE_ID)
    main -> ACCEPT_ROLE_ID_MAIN (fallback: ACCEPT_ROLE_ID)
    """
    key = "ACCEPT_ROLE_ID_ACADEMY" if track == "academy" else "ACCEPT_ROLE_ID_MAIN"
    raw = os.getenv(key, "").strip()
    if raw:
        return _parse_id_list(raw)
    return _parse_id_list(os.getenv("ACCEPT_ROLE_ID", ""))


def _vzp_maps_dir() -> Path:
    raw = os.getenv("VZP_MAPS_DIR", "maps_vzp").strip()
    p = Path(raw)
    return p if p.is_absolute() else BOT_DIR / p


# Номера 1–16 = файлы 1.png … 16.png (или .jpg и т.д.). У карты 11 второй кадр: 11_2.png
_MAP_EXTS = (".png", ".jpg", ".jpeg", ".webp", ".gif")

VZP_MAP_LABELS: list[str] = [
    "Байкерка",
    "Большой миррор",
    "Веспуччи",
    "Ветряки",
    "Киностудия",
    "Лесопилка",
    "Маленький миррор",
    "Муравейник",
    "Мусорка",
    "Мясо",
    "Нефть",
    "Палетка",
    "Порт бизвар",
    "Сендик",
    "Стройка",
    "Татушка",
]


def _map_label(num: int) -> str:
    if 1 <= num <= len(VZP_MAP_LABELS):
        return VZP_MAP_LABELS[num - 1]
    return str(num)


def _map_image_paths(num: int) -> list[Path]:
    """Пути к картинкам: `{num}.*` и для 11 также `11_2.*`."""
    d = _vzp_maps_dir()
    paths: list[Path] = []
    for ext in _MAP_EXTS:
        p = d / f"{num}{ext}"
        if p.is_file():
            paths.append(p)
            break
    if num == 11:
        for ext in _MAP_EXTS:
            p2 = d / f"11_2{ext}"
            if p2.is_file():
                paths.append(p2)
                break
    return paths


def _load_state() -> dict[str, Any]:
    data = _kv_get_json("state")
    if data is not None:
        data.setdefault("guilds", {})
        return data
    # Миграция legacy JSON -> SQLite (однократно, если в SQLite еще пусто).
    if STATE_PATH.exists():
        try:
            with STATE_PATH.open(encoding="utf-8") as f:
                legacy = json.load(f)
            if isinstance(legacy, dict):
                legacy.setdefault("guilds", {})
                _kv_set_json("state", legacy)
                return legacy
        except (json.JSONDecodeError, OSError):
            pass
    base = {"guilds": {}}
    _kv_set_json("state", base)
    return base


def _save_state(data: dict[str, Any]) -> None:
    _kv_set_json("state", data)


_state_lock = asyncio.Lock()


def guild_acceptance(guild_id: int) -> tuple[bool, bool]:
    data = _load_state()
    g = data["guilds"].setdefault(str(guild_id), {"rp": True, "vzp": True})
    return bool(g.get("rp", True)), bool(g.get("vzp", True))


async def set_guild_acceptance(guild_id: int, *, rp: bool | None = None, vzp: bool | None = None) -> tuple[bool, bool]:
    async with _state_lock:
        data = _load_state()
        g = data["guilds"].setdefault(str(guild_id), {"rp": True, "vzp": True})
        if rp is not None:
            g["rp"] = rp
        if vzp is not None:
            g["vzp"] = vzp
        _save_state(data)
        return bool(g["rp"]), bool(g["vzp"])


def _load_tickets() -> dict[str, Any]:
    data = _kv_get_json("tickets")
    if data is not None:
        data.setdefault("by_channel", {})
        data.setdefault("counter", {})
        data.setdefault("pending", {})
        return data
    # Миграция legacy JSON -> SQLite (однократно, если в SQLite еще пусто).
    if TICKETS_PATH.exists():
        try:
            with TICKETS_PATH.open(encoding="utf-8") as f:
                legacy = json.load(f)
            if isinstance(legacy, dict):
                legacy.setdefault("by_channel", {})
                legacy.setdefault("counter", {})
                legacy.setdefault("pending", {})
                _kv_set_json("tickets", legacy)
                return legacy
        except (json.JSONDecodeError, OSError):
            pass
    base = {"by_channel": {}, "counter": {}, "pending": {}}
    _kv_set_json("tickets", base)
    return base


def _save_tickets(data: dict[str, Any]) -> None:
    _kv_set_json("tickets", data)


_ticket_lock = asyncio.Lock()


async def _next_ticket_no(guild_id: int) -> int:
    async with _ticket_lock:
        data = _load_tickets()
        key = str(guild_id)
        n = int(data["counter"].get(key, 0)) + 1
        data["counter"][key] = n
        _save_tickets(data)
        return n


async def _ticket_put(channel_id: int, record: dict[str, Any]) -> None:
    async with _ticket_lock:
        data = _load_tickets()
        data["by_channel"][str(channel_id)] = record
        _save_tickets(data)


async def _ticket_get(channel_id: int) -> dict[str, Any] | None:
    data = _load_tickets()
    return data["by_channel"].get(str(channel_id))


async def _ticket_delete(channel_id: int) -> None:
    async with _ticket_lock:
        data = _load_tickets()
        data["by_channel"].pop(str(channel_id), None)
        _save_tickets(data)


async def _ticket_update_phase(channel_id: int, phase: TicketPhase) -> None:
    async with _ticket_lock:
        data = _load_tickets()
        key = str(channel_id)
        if key in data["by_channel"]:
            data["by_channel"][key]["phase"] = phase
            _save_tickets(data)


def _pending_key(guild_id: int, ticket_no: int) -> str:
    return f"{guild_id}_{ticket_no}"


def _ticket_get_pending(guild_id: int, ticket_no: int) -> dict[str, Any] | None:
    data = _load_tickets()
    return data.get("pending", {}).get(_pending_key(guild_id, ticket_no))


async def _ticket_put_pending(guild_id: int, ticket_no: int, record: dict[str, Any]) -> None:
    async with _ticket_lock:
        data = _load_tickets()
        data.setdefault("pending", {})[_pending_key(guild_id, ticket_no)] = record
        _save_tickets(data)


async def _ticket_delete_pending(guild_id: int, ticket_no: int) -> None:
    async with _ticket_lock:
        data = _load_tickets()
        data.get("pending", {}).pop(_pending_key(guild_id, ticket_no), None)
        _save_tickets(data)


def _application_review_channel_id() -> int | None:
    raw = os.getenv("APPLICATION_REVIEW_CHANNEL_ID", "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _channel_slug(name: str, ticket_no: int, kind: str) -> str:
    base = re.sub(r"[^a-z0-9\-]", "", name.lower().replace(" ", "-"))[:60] or "user"
    return f"{kind}-{base}-{ticket_no}"[:100]


async def _safe_dm(user: discord.abc.User, content: str) -> bool:
    try:
        await user.send(content)
        return True
    except discord.HTTPException as e:
        logger.warning("DM не доставлено %s: %s", user.id, e)
        return False


async def _safe_dm_embed(user: discord.abc.User, embed: discord.Embed) -> bool:
    try:
        await user.send(embed=embed)
        return True
    except discord.HTTPException as e:
        logger.warning("DM (embed) не доставлено %s: %s", user.id, e)
        return False


def _reason_in_code_block(reason: str) -> str:
    r = reason.strip().replace("```", "'''")
    if len(r) > 900:
        r = r[:897] + "…"
    return f"```{r}```"


def _embed_rejection_dm(*, reason: str, after_interview: bool) -> discord.Embed:
    title = "❌ Отказ после обзвона" if after_interview else "❌ Заявка отклонена"
    desc = (
        "Модератор рассмотрел анкету после обзвона. Главное — блок ниже."
        if after_interview
        else "Модератор рассмотрел анкету. Главное — блок ниже."
    )
    emb = discord.Embed(
        title=title,
        description=desc,
        color=discord.Color.red(),
        timestamp=datetime.now(timezone.utc),
    )
    emb.add_field(name="Причина отказа", value=_reason_in_code_block(reason), inline=False)
    emb.add_field(
        name="Дальше",
        value="Повторная заявка — через 1–3 дня.\nИсправь то, что указано в причине.",
        inline=False,
    )
    emb.set_footer(text=datetime.now().strftime("%d.%m.%Y"))
    return emb


def _embed_interview_invite_dm(guild_name: str, channel: discord.abc.GuildChannel) -> discord.Embed:
    emb = discord.Embed(
        title="🕒 Тикет на рассмотрении",
        description=(
            f"Заявку в **{guild_name}** приняли на рассмотрение. "
            "Зайди в канал ниже — там продолжится общение."
        ),
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc),
    )
    emb.add_field(name="Канал", value=channel.mention, inline=False)
    emb.set_footer(text=datetime.now().strftime("%d.%m.%Y"))
    return emb


def _guild_member_from_interaction(interaction: discord.Interaction) -> discord.Member | None:
    m = getattr(interaction, "member", None)
    if m is not None:
        return m
    if interaction.guild is None or interaction.user is None:
        return None
    return interaction.guild.get_member(interaction.user.id)


async def _resolve_interaction_member(interaction: discord.Interaction) -> discord.Member | None:
    m = _guild_member_from_interaction(interaction)
    if m is not None:
        return m
    if interaction.guild is None:
        return None
    try:
        return await interaction.guild.fetch_member(interaction.user.id)
    except discord.NotFound:
        return None


async def user_can_moderate(interaction: discord.Interaction) -> bool:
    if interaction.guild is None:
        return False
    if interaction.user.id == interaction.guild.owner_id:
        return True
    member = _guild_member_from_interaction(interaction)
    if member is None:
        try:
            member = await interaction.guild.fetch_member(interaction.user.id)
        except discord.NotFound:
            return False
    if member.guild_permissions.manage_guild or member.guild_permissions.administrator:
        return True
    mod_ids = _mod_role_ids()
    if mod_ids and any(r.id in mod_ids for r in member.roles):
        return True
    return False


async def user_can_handle_ticket(interaction: discord.Interaction) -> bool:
    if interaction.guild is None:
        return False
    if interaction.user.id == interaction.guild.owner_id:
        return True
    member = _guild_member_from_interaction(interaction)
    if member is None:
        try:
            member = await interaction.guild.fetch_member(interaction.user.id)
        except discord.NotFound:
            return False
    if member.guild_permissions.manage_guild or member.guild_permissions.administrator:
        return True
    staff = _ticket_staff_role_ids()
    if staff and any(r.id in staff for r in member.roles):
        return True
    return False


def status_line(enabled: bool) -> str:
    return "✅ Включено" if enabled else "❌ Выключено"


def _embed_field_codeblock(text: str) -> str:
    """Значение поля в виде блока кода (как в превью Discord)."""
    raw = (text or "—").strip()
    raw = raw.replace("```", "'''")
    max_inner = 1016
    if len(raw) > max_inner:
        raw = raw[: max_inner - 1] + "…"
    return f"```{raw}```"


def _normalize_ticket_fields(kind: TicketKind, fields: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """
    Приводит поля тикета к единому формату.
    Нужен для совместимости со старыми заявками/старыми заголовками.
    """
    values = [str(v) for _, v in fields]
    if kind == "rp":
        names = [
            "Возраст",
            "Онлайн",
            "Семьи",
            "Откуда",
            "Откат",
        ]
    else:
        names = [
            "Возраст",
            "Онлайн",
            "Семьи",
            "Откат",
        ]
    out: list[tuple[str, str]] = []
    for i, name in enumerate(names):
        out.append((name, values[i] if i < len(values) else "—"))
    return out


def _embed_lines_value(lines: list[str], *, empty: str = "—", limit: int = 1024) -> str:
    """Безопасно собирает строки в значение поля embed (<=1024 символов)."""
    if not lines:
        return empty
    out: list[str] = []
    used = 0
    total = len(lines)
    for idx, line in enumerate(lines):
        chunk = line if not out else f"\n{line}"
        if used + len(chunk) > limit:
            left = total - idx
            if left > 0:
                suffix = f"\n…и еще {left}"
                if used + len(suffix) <= limit:
                    out.append(suffix)
            break
        out.append(chunk)
        used += len(chunk)
    return "".join(out) if out else empty


def moderation_embed(guild_id: int) -> discord.Embed:
    rp_on, vzp_on = guild_acceptance(guild_id)
    e = discord.Embed(
        title="Модерация заявок",
        description="Переключите прием заявок по типам.",
        color=discord.Color.orange(),
    )
    e.add_field(name="Статус", value=f"РП: **{status_line(rp_on)}**\nVZP: **{status_line(vzp_on)}**", inline=False)
    e.set_footer(text=datetime.now().strftime("%d.%m.%Y"))
    return e


def _build_ticket_embed(
    *,
    kind: TicketKind,
    ticket_no: int,
    applicant: discord.abc.User,
    fields: list[tuple[str, str]],
    phase: TicketPhase,
) -> discord.Embed:
    label = "РП" if kind == "rp" else "VZP"
    # Синий акцент для РП / зелёный для VZP (полоса слева в embed)
    color = discord.Color(0x3498DB) if kind == "rp" else discord.Color(0x2ECC71)
    emb = discord.Embed(
        title=f"Новая заявка: {label} · #{ticket_no}",
        color=color,
        timestamp=datetime.now(timezone.utc),
    )
    emb.add_field(name="ПОЛЬЗОВАТЕЛЬ", value=applicant.mention, inline=False)
    fields = _normalize_ticket_fields(kind, fields)
    for name, value in fields:
        emb.add_field(name=name, value=_embed_field_codeblock(value), inline=False)
    emb.set_footer(text=f"User ID: {applicant.id} - Тикет №{ticket_no} - {datetime.now().strftime('%d.%m.%Y')}")
    return emb


class RejectReasonModal(SafeModal, title="Причина отказа"):
    reason = discord.ui.TextInput(
        label="Причина отказа",
        style=discord.TextStyle.paragraph,
        placeholder="Укажи причину отказа для заявителя…",
        max_length=1000,
        required=True,
    )

    def __init__(self, *, guild_id: int, ticket_no: int | None = None, private_channel_id: int | None = None) -> None:
        super().__init__(custom_id="consume:ticket_reject_reason")
        self.guild_id = guild_id
        self.ticket_no = ticket_no
        self.private_channel_id = private_channel_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await user_can_handle_ticket(interaction):
            await interaction.response.send_message("Нет прав на работу с заявками.", ephemeral=True)
            return

        guild = interaction.client.get_guild(self.guild_id)
        if guild is None:
            await interaction.response.send_message("Сервер недоступен.", ephemeral=True)
            return

        reason_text = str(self.reason.value).strip()
        if not reason_text:
            await interaction.response.send_message("Причина не может быть пустой.", ephemeral=True)
            return

        if self.private_channel_id is not None:
            rec = await _ticket_get(self.private_channel_id)
            if not rec or rec.get("phase") != "interview":
                await interaction.response.send_message("Заявка уже закрыта или устарела.", ephemeral=True)
                return

            applicant_id = int(rec["applicant_id"])
            applicant = guild.get_member(applicant_id)
            if applicant is None:
                try:
                    applicant = await guild.fetch_member(applicant_id)
                except discord.NotFound:
                    applicant = None

            await interaction.response.defer(ephemeral=True)
            if applicant is not None:
                emb = _embed_rejection_dm(reason=reason_text, after_interview=True)
                await _safe_dm_embed(applicant, emb)

            await _ticket_delete(self.private_channel_id)
            await _safe_interaction_ephemeral(interaction, "Отказ с причиной отправлен заявителю в ЛС.")
            ch = guild.get_channel(self.private_channel_id)
            if isinstance(ch, discord.TextChannel):
                try:
                    await ch.delete(reason="Отказ после обзвона")
                except (discord.Forbidden, discord.NotFound):
                    logger.exception("Не удалось удалить канал после отказа")
            return

        if self.ticket_no is None:
            await interaction.response.send_message("Внутренняя ошибка тикета.", ephemeral=True)
            return

        rec = _ticket_get_pending(self.guild_id, self.ticket_no)
        if not rec or rec.get("phase") != "initial":
            await interaction.response.send_message("Заявка уже обработана или устарела.", ephemeral=True)
            return

        applicant_id = int(rec["applicant_id"])
        applicant = guild.get_member(applicant_id)
        if applicant is None:
            try:
                applicant = await guild.fetch_member(applicant_id)
            except discord.NotFound:
                applicant = None

        staff_ch_id = int(rec["staff_channel_id"])
        staff_msg_id = int(rec["staff_message_id"])

        await interaction.response.defer(ephemeral=True)
        if applicant is not None:
            emb = _embed_rejection_dm(reason=reason_text, after_interview=False)
            await _safe_dm_embed(applicant, emb)

        await _ticket_delete_pending(self.guild_id, self.ticket_no)

        staff_ch = guild.get_channel(staff_ch_id)
        if isinstance(staff_ch, discord.TextChannel):
            try:
                msg = await staff_ch.fetch_message(staff_msg_id)
                await msg.delete()
            except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                logger.warning("Не удалось удалить сообщение заявки в канале модерации")

        await _safe_interaction_ephemeral(interaction, "Отказ с причиной отправлен заявителю в ЛС.")


class TicketViewInitial(SafeView):
    def __init__(self, guild_id: int, ticket_no: int) -> None:
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.ticket_no = ticket_no

        b1 = discord.ui.Button(
            label="Обзвон",
            style=discord.ButtonStyle.success,
            custom_id=f"consume:tk_o:{guild_id}:{ticket_no}",
            emoji="📞",
        )
        b1.callback = self._on_obzvon
        self.add_item(b1)

        b2 = discord.ui.Button(
            label="Отказать",
            style=discord.ButtonStyle.danger,
            custom_id=f"consume:tk_r1:{guild_id}:{ticket_no}",
            emoji="❌",
        )
        b2.callback = self._on_reject_initial
        self.add_item(b2)

    async def _on_obzvon(self, interaction: discord.Interaction) -> None:
        if not await user_can_handle_ticket(interaction):
            await interaction.response.send_message("Нет прав на работу с заявками.", ephemeral=True)
            return
        if interaction.guild is None or interaction.guild.id != self.guild_id:
            await interaction.response.send_message("Неверный сервер.", ephemeral=True)
            return
        msg = interaction.message
        if msg is None:
            await interaction.response.send_message("Сообщение заявки не найдено.", ephemeral=True)
            return

        rec = _ticket_get_pending(self.guild_id, self.ticket_no)
        if not rec or rec.get("phase") != "initial":
            await interaction.response.send_message("Заявка уже обработана или устарела.", ephemeral=True)
            return
        if int(rec["staff_message_id"]) != msg.id or int(rec["staff_channel_id"]) != interaction.channel_id:
            await interaction.response.send_message("Это не актуальное сообщение заявки.", ephemeral=True)
            return

        if not _ticket_category_id():
            await interaction.response.send_message(
                "Администратор не настроил **TICKET_CATEGORY_ID** — нельзя создать канал обзвона.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        applicant = guild.get_member(int(rec["applicant_id"]))
        if applicant is None:
            try:
                applicant = await guild.fetch_member(int(rec["applicant_id"]))
            except discord.NotFound:
                await _safe_interaction_ephemeral(interaction, "Пользователь вышел с сервера.")
                return

        kind: TicketKind = rec["kind"]
        ticket_no = int(rec["ticket_no"])
        fields = _normalize_ticket_fields(kind, list(rec["embed_fields"]))

        try:
            private_ch = await _create_ticket_channel(
                guild,
                applicant,
                kind=kind,
                ticket_no=ticket_no,
                embed_fields=fields,
            )
        except discord.Forbidden:
            await interaction.followup.send(
                "У бота нет прав создать канал в категории тикетов.",
                ephemeral=True,
            )
            return
        except RuntimeError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            return
        except Exception:
            logger.exception("create ticket channel (обзвон)")
            await interaction.followup.send("Ошибка при создании канала. См. консоль бота.", ephemeral=True)
            return

        dm_emb = _embed_interview_invite_dm(guild.name, private_ch)
        await _safe_dm_embed(applicant, dm_emb)

        await _ticket_delete_pending(self.guild_id, self.ticket_no)

        emb, ticket_files = _build_ticket_embed_with_banner(
            kind=kind,
            ticket_no=ticket_no,
            applicant=applicant,
            fields=fields,
            phase="interview",
        )
        try:
            await msg.edit(embed=emb, view=None, attachments=msg.attachments)
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            logger.warning("Не удалось обновить сообщение заявки в канале модерации")

        v2 = TicketViewFinal(private_ch.id)
        bot.add_view(v2)
        try:
            send_kw: dict[str, Any] = {"embed": emb, "view": v2}
            if ticket_files:
                send_kw["files"] = ticket_files
            await private_ch.send(**send_kw)
        except discord.HTTPException:
            logger.exception("Не удалось отправить эмбед в канал обзвона")

        new_rec = {
            "guild_id": guild.id,
            "applicant_id": applicant.id,
            "kind": kind,
            "ticket_no": ticket_no,
            "phase": "interview",
            "embed_fields": fields,
        }
        await _ticket_put(private_ch.id, new_rec)

        await interaction.followup.send(f"Канал обзвона: {private_ch.mention}", ephemeral=True)

    async def _on_reject_initial(self, interaction: discord.Interaction) -> None:
        if not await user_can_handle_ticket(interaction):
            await interaction.response.send_message("Нет прав на работу с заявками.", ephemeral=True)
            return
        if interaction.guild is None or interaction.guild.id != self.guild_id:
            await interaction.response.send_message("Неверный сервер.", ephemeral=True)
            return
        msg = interaction.message
        if msg is None:
            await interaction.response.send_message("Сообщение заявки не найдено.", ephemeral=True)
            return

        rec = _ticket_get_pending(self.guild_id, self.ticket_no)
        if not rec or rec.get("phase") != "initial":
            await interaction.response.send_message("Заявка уже обработана.", ephemeral=True)
            return
        if int(rec["staff_message_id"]) != msg.id or int(rec["staff_channel_id"]) != interaction.channel_id:
            await interaction.response.send_message("Это не актуальное сообщение заявки.", ephemeral=True)
            return

        await interaction.response.send_modal(RejectReasonModal(guild_id=self.guild_id, ticket_no=self.ticket_no))


class TicketViewFinal(SafeView):
    def __init__(self, channel_id: int) -> None:
        super().__init__(timeout=None)
        self.channel_id = channel_id

        b1 = discord.ui.Button(
            label="Принять в академию",
            style=discord.ButtonStyle.success,
            custom_id=f"consume:tk_a_ac:{channel_id}",
            emoji="🎓",
        )
        b1.callback = self._on_accept_academy
        self.add_item(b1)

        b2 = discord.ui.Button(
            label="Принять в основу",
            style=discord.ButtonStyle.success,
            custom_id=f"consume:tk_a_main:{channel_id}",
            emoji="✅",
        )
        b2.callback = self._on_accept_main
        self.add_item(b2)

        b3 = discord.ui.Button(
            label="Отказать",
            style=discord.ButtonStyle.danger,
            custom_id=f"consume:tk_r2:{channel_id}",
            emoji="❌",
        )
        b3.callback = self._on_reject_after
        self.add_item(b3)

    async def _handle_accept(self, interaction: discord.Interaction, *, track: Literal["academy", "main"]) -> None:
        if not await user_can_handle_ticket(interaction):
            await interaction.response.send_message("Нет прав на работу с заявками.", ephemeral=True)
            return
        if interaction.channel_id != self.channel_id:
            await interaction.response.send_message("Неверный канал тикета.", ephemeral=True)
            return
        rec = await _ticket_get(self.channel_id)
        if not rec or rec.get("phase") != "interview":
            await interaction.response.send_message("Сначала нажмите «Обзвон» или заявка уже закрыта.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        guild = interaction.guild
        ch = interaction.channel
        if guild is None or not isinstance(ch, discord.TextChannel):
            await _safe_interaction_ephemeral(interaction, "Канал тикета недоступен. Попробуйте снова.")
            return

        role_ids = _accept_role_ids(track)
        if not role_ids:
            key = "ACCEPT_ROLE_ID_ACADEMY" if track == "academy" else "ACCEPT_ROLE_ID_MAIN"
            await _safe_interaction_ephemeral(
                interaction,
                f"В `.env` не задан **{key}** (или общий **ACCEPT_ROLE_ID**) — укажите ID роли "
                "(или несколько через запятую), которую выдавать после принятия.",
            )
            return

        roles_to_add: list[discord.Role] = []
        missing_ids: list[int] = []
        for rid in role_ids:
            r = guild.get_role(rid)
            if r is None:
                missing_ids.append(rid)
            else:
                roles_to_add.append(r)

        if missing_ids:
            key = "ACCEPT_ROLE_ID_ACADEMY" if track == "academy" else "ACCEPT_ROLE_ID_MAIN"
            await _safe_interaction_ephemeral(
                interaction,
                f"На сервере не найдены роли с ID: {', '.join(str(x) for x in missing_ids)}. "
                f"Проверьте {key} (или ACCEPT_ROLE_ID) в `.env`.",
            )
            return

        applicant_id = int(rec["applicant_id"])
        member = guild.get_member(applicant_id)
        if member is None:
            try:
                member = await guild.fetch_member(applicant_id)
            except discord.NotFound:
                await _safe_interaction_ephemeral(interaction, "Пользователь не на сервере — роль не выдана.")
                return

        try:
            reason = "Заявка принята в академию" if track == "academy" else "Заявка принята в основу"
            await member.add_roles(*roles_to_add, reason=reason)
        except discord.Forbidden:
            await _safe_interaction_ephemeral(
                interaction,
                "Не удалось выдать роль: проверьте иерархию ролей (роль бота выше всех выдаваемых).",
            )
            return

        if track == "academy":
            await _safe_dm(member, "> **Вас приняли в академию. Добро пожаловать!**")
        else:
            await _safe_dm(member, "> **Вас приняли в основу. Добро пожаловать!**")
        await _ticket_delete(self.channel_id)
        try:
            await ch.delete(reason="Заявка принята")
        except (discord.Forbidden, discord.NotFound):
            logger.exception("Не удалось удалить канал после принятия")

    async def _on_accept_academy(self, interaction: discord.Interaction) -> None:
        await self._handle_accept(interaction, track="academy")

    async def _on_accept_main(self, interaction: discord.Interaction) -> None:
        await self._handle_accept(interaction, track="main")

    async def _on_reject_after(self, interaction: discord.Interaction) -> None:
        if not await user_can_handle_ticket(interaction):
            await interaction.response.send_message("Нет прав на работу с заявками.", ephemeral=True)
            return
        if interaction.channel_id != self.channel_id:
            await interaction.response.send_message("Неверный канал тикета.", ephemeral=True)
            return
        rec = await _ticket_get(self.channel_id)
        if not rec or rec.get("phase") != "interview":
            await interaction.response.send_message("Заявка уже закрыта.", ephemeral=True)
            return

        guild = interaction.guild
        if guild is None:
            return

        await interaction.response.send_modal(
            RejectReasonModal(guild_id=guild.id, private_channel_id=self.channel_id),
        )


async def _create_ticket_channel(
    guild: discord.Guild,
    applicant: discord.Member,
    *,
    kind: TicketKind,
    ticket_no: int,
    embed_fields: list[tuple[str, str]],
) -> discord.TextChannel:
    cat_id = _ticket_category_id()
    category = guild.get_channel(cat_id) if cat_id else None
    if cat_id and not isinstance(category, discord.CategoryChannel):
        raise RuntimeError("TICKET_CATEGORY_ID не указывает на категорию.")

    overwrites: dict[Any, discord.PermissionOverwrite] = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
        applicant: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
    }
    for rid in _ticket_staff_role_ids():
        role = guild.get_role(rid)
        if role:
            overwrites[role] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                manage_messages=True,
            )

    name = _channel_slug(applicant.display_name, ticket_no, kind)
    ch = await guild.create_text_channel(
        name=name,
        category=category,
        overwrites=overwrites,
        reason=f"Заявка {kind.upper()} #{ticket_no}",
    )
    return ch


async def _restore_ticket_views() -> None:
    data = _load_tickets()
    for key, rec in data.get("pending", {}).items():
        if rec.get("phase") != "initial":
            continue
        try:
            gid_str, _, tno_str = key.rpartition("_")
            gid = int(gid_str)
            tno = int(tno_str)
        except ValueError:
            continue
        bot.add_view(TicketViewInitial(gid, tno))
    for cid_str, rec in data.get("by_channel", {}).items():
        try:
            cid = int(cid_str)
        except ValueError:
            continue
        phase = rec.get("phase")
        if phase == "interview":
            bot.add_view(TicketViewFinal(cid))


class RPApplicationModal(SafeModal, title="Заявка РП"):
    f1 = discord.ui.TextInput(
        label="Возраст",
        placeholder="18",
        style=discord.TextStyle.short,
        max_length=200,
        required=True,
    )
    f2 = discord.ui.TextInput(
        label="Онлайн",
        placeholder="Пример: 4-6 часов",
        style=discord.TextStyle.short,
        max_length=200,
        required=True,
    )
    f3 = discord.ui.TextInput(
        label="Список семей в которых были",
        placeholder="Пример: Killa, Kai, Black",
        style=discord.TextStyle.short,
        max_length=100,
        required=True,
    )
    f4 = discord.ui.TextInput(
        label="Откуда узнали о семье Consume",
        placeholder="Пример: От друга | Из рекламы",
        style=discord.TextStyle.paragraph,
        max_length=1000,
        required=True,
    )
    f5 = discord.ui.TextInput(
        label="Откат стрельбы DM 10.500 урона",
        placeholder="Ссылка на YouTube | Нету = academy",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=True,
    )

    def __init__(self) -> None:
        super().__init__(custom_id="consume:ticket_apply_rp")

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await _submit_ticket_modal(
            interaction,
            kind="rp",
            embed_fields=[
                ("ВОЗРАСТ", str(self.f1.value)),
                ("ОНЛАЙН", str(self.f2.value)),
                ("СПИСОК СЕМЕЙ, В КОТОРЫХ БЫЛИ", str(self.f3.value)),
                ("ОТКУДА УЗНАЛИ", str(self.f4.value)),
                ("ОТКАТ СТРЕЛЬБЫ DM 10.500 УРОНА", str(self.f5.value)),
            ],
        )


class VZPApplicationModal(SafeModal, title="Форма заявки VZP"):
    f1 = discord.ui.TextInput(
        label="Возраст",
        placeholder="Пример: 18",
        style=discord.TextStyle.short,
        max_length=200,
        required=True,
    )
    f2 = discord.ui.TextInput(
        label="Онлайн",
        placeholder="Пример: 4-6 часов",
        style=discord.TextStyle.short,
        max_length=200,
        required=True,
    )
    f3 = discord.ui.TextInput(
        label="В каких семьях были",
        placeholder="Пример: Killa, Kai, Black",
        style=discord.TextStyle.short,
        max_length=100,
        required=True,
    )
    f4 = discord.ui.TextInput(
        label="Откат с ВЗП/DM",
        placeholder="Ссылка на YouTube",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=True,
    )

    def __init__(self) -> None:
        super().__init__(custom_id="consume:ticket_apply_vzp")

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await _submit_ticket_modal(
            interaction,
            kind="vzp",
            embed_fields=[
                ("ВОЗРАСТ", str(self.f1.value)),
                ("ОНЛАЙН", str(self.f2.value)),
                ("В КАКИХ СЕМЬЯХ БЫЛИ", str(self.f3.value)),
                ("ОТКАТ С ВЗП/DM", str(self.f4.value)),
            ],
        )


async def _submit_ticket_modal(
    interaction: discord.Interaction,
    *,
    kind: TicketKind,
    embed_fields: list[tuple[str, str]],
) -> None:
    if interaction.guild is None:
        await interaction.response.send_message("Заявки только на сервере.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    applicant = _guild_member_from_interaction(interaction)
    if applicant is None:
        try:
            applicant = await interaction.guild.fetch_member(interaction.user.id)
        except discord.NotFound:
            await _safe_interaction_ephemeral(interaction, "Не удалось определить участника.")
            return

    guild = interaction.guild
    embed_fields = _normalize_ticket_fields(kind, embed_fields)

    ticket_no = await _next_ticket_no(guild.id)

    emb, ticket_files = _build_ticket_embed_with_banner(
        kind=kind,
        ticket_no=ticket_no,
        applicant=applicant,
        fields=embed_fields,
        phase="interview",
    )
    final_view: TicketViewFinal | None = None
    try:
        private_ch = await _create_ticket_channel(
            guild,
            applicant,
            kind=kind,
            ticket_no=ticket_no,
            embed_fields=embed_fields,
        )
        final_view = TicketViewFinal(private_ch.id)
        bot.add_view(final_view)
        send_kwargs: dict[str, Any] = {"embed": emb, "view": final_view}
        if ticket_files:
            send_kwargs["files"] = ticket_files
        await private_ch.send(**send_kwargs)
        dm_emb = _embed_interview_invite_dm(guild.name, private_ch)
        await _safe_dm_embed(applicant, dm_emb)
        await _ticket_put(
            private_ch.id,
            {
                "guild_id": guild.id,
                "applicant_id": applicant.id,
                "kind": kind,
                "ticket_no": ticket_no,
                "phase": "interview",
                "embed_fields": embed_fields,
            },
        )
    except discord.Forbidden:
        await interaction.followup.send(
            "У бота нет прав создать/настроить канал тикета. Проверьте права и `TICKET_CATEGORY_ID`.",
            ephemeral=True,
        )
        return
    except RuntimeError as e:
        await interaction.followup.send(str(e), ephemeral=True)
        return
    except discord.HTTPException as e:
        logger.warning("Создание/публикация тикета: %s", e)
        await interaction.followup.send("Не удалось создать тикет-канал.", ephemeral=True)
        return
    except Exception:
        logger.exception("Неизвестная ошибка при создании тикета")
        await interaction.followup.send("Ошибка при создании тикета. См. консоль бота.", ephemeral=True)
        return

    await interaction.followup.send(
        f"Тикет создан: {private_ch.mention}",
        ephemeral=True,
    )


class ModerationPanelView(SafeView):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(label="РП", style=discord.ButtonStyle.secondary, custom_id="consume:mod_rp", emoji="📝")
    async def toggle_rp(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await user_can_moderate(interaction):
            await interaction.response.send_message("Нет прав.", ephemeral=True)
            return
        if interaction.guild is None:
            return
        rp_on, vzp_on = guild_acceptance(interaction.guild.id)
        rp_on, vzp_on = await set_guild_acceptance(interaction.guild.id, rp=not rp_on)
        await interaction.response.edit_message(embed=moderation_embed(interaction.guild.id), view=self)

    @discord.ui.button(label="VZP", style=discord.ButtonStyle.secondary, custom_id="consume:mod_vzp", emoji="📋")
    async def toggle_vzp(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await user_can_moderate(interaction):
            await interaction.response.send_message("Нет прав.", ephemeral=True)
            return
        if interaction.guild is None:
            return
        rp_on, vzp_on = guild_acceptance(interaction.guild.id)
        rp_on, vzp_on = await set_guild_acceptance(interaction.guild.id, vzp=not vzp_on)
        await interaction.response.edit_message(embed=moderation_embed(interaction.guild.id), view=self)


class ApplicationPanelView(SafeView):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.select(
        placeholder="📋 Подать Заявку VZP",
        custom_id="consume:app_select",
        options=[
            discord.SelectOption(
                label="Подать Заявку РП",
                description="Нажмите, чтобы заполнить анкету RP",
                value="rp",
                emoji="📝",
            ),
            discord.SelectOption(
                label="Подать Заявку VZP",
                description="Нажмите, чтобы заполнить анкету VZP",
                value="vzp",
                emoji="📋",
            ),
            discord.SelectOption(
                label="Модерация",
                description="Управление приемом заявок",
                value="mod",
                emoji="⚙️",
            ),
        ],
        min_values=1,
        max_values=1,
    )
    async def app_select(self, interaction: discord.Interaction, select: discord.ui.Select) -> None:
        val = select.values[0]
        if interaction.guild is None:
            await interaction.response.send_message("Используйте на сервере.", ephemeral=True)
            return

        rp_on, vzp_on = guild_acceptance(interaction.guild.id)

        if val == "rp":
            if not rp_on:
                await interaction.response.send_message("Приём заявок РП временно закрыт.", ephemeral=True)
                return
            await interaction.response.send_modal(RPApplicationModal())
            return

        if val == "vzp":
            if not vzp_on:
                await interaction.response.send_message("Приём заявок VZP временно закрыт.", ephemeral=True)
                return
            await interaction.response.send_modal(VZPApplicationModal())
            return

        if val == "mod":
            if not await user_can_moderate(interaction):
                await interaction.response.send_message("У вас нет доступа к панели модерации.", ephemeral=True)
                return
            view = ModerationPanelView()
            await interaction.response.send_message(embed=moderation_embed(interaction.guild.id), view=view, ephemeral=True)
            return


class VZPMapsView(SafeView):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.select(
        placeholder="Выбирай",
        custom_id="consume:map_select",
        options=[
            discord.SelectOption(label=label[:100], value=str(n))
            for n, label in enumerate(VZP_MAP_LABELS, start=1)
        ],
        min_values=1,
        max_values=1,
    )
    async def map_select(self, interaction: discord.Interaction, select: discord.ui.Select) -> None:
        num = int(select.values[0])
        label = _map_label(num)
        paths = _map_image_paths(num)
        if paths:
            files = [discord.File(p) for p in paths]
            await interaction.response.send_message(
                content=f"**{label}**",
                files=files,
                ephemeral=True,
            )
        else:
            d = _vzp_maps_dir()
            hint = f"`{num}.png`"
            if num == 11:
                hint += " и при необходимости `11_2.png`"
            await interaction.response.send_message(
                f"Картинки для **{label}** нет. Положите {hint} в `{d}`.",
                ephemeral=True,
            )


async def _kontrakt_refresh_message(message_id: int) -> bool:
    state = _contract_load(message_id)
    if state is None:
        return False
    ch = bot.get_channel(state.channel_id)
    if not isinstance(ch, discord.TextChannel):
        return False
    try:
        msg = await ch.fetch_message(message_id)
    except (discord.NotFound, discord.HTTPException):
        return False
    view: discord.ui.View | None = KontraktContractView() if state.status_open else None
    await msg.edit(embed=build_kontrakt_contract_embed(state), view=view)
    return True


async def _kontrakt_open_thread_and_notify(
    message_id: int,
    *,
    decision_text: str,
    reason: str = "",
    actor_id: int | None = None,
) -> None:
    state = _contract_load(message_id)
    if state is None:
        return
    ch = bot.get_channel(state.channel_id)
    if not isinstance(ch, discord.TextChannel):
        return
    try:
        msg = await ch.fetch_message(message_id)
    except (discord.NotFound, discord.HTTPException):
        return
    try:
        thread = await msg.create_thread(
            name=f"Контракт · {decision_text}"[:100],
            auto_archive_duration=1440,
        )
    except discord.HTTPException:
        return

    targets = [f"<@{uid}>" for uid in state.participant_ids]
    if not targets:
        targets = [f"<@{state.creator_id}>"]
    who = " ".join(targets)
    actor_part = f"\nМодератор: <@{actor_id}>" if actor_id else ""
    reason_part = f"\nПричина: {reason}" if reason else ""
    text = f"{who}\nВас {decision_text.lower()} по контракту **{state.title}**.{actor_part}{reason_part}"
    await thread.send(text[:2000], allowed_mentions=discord.AllowedMentions(users=True))


class KontraktRejectModal(SafeModal, title="Причина отказа"):
    reason = discord.ui.TextInput(
        label="Причина",
        placeholder="Коротко: почему отказ",
        max_length=300,
        style=discord.TextStyle.paragraph,
    )

    def __init__(self, message_id: int) -> None:
        super().__init__(timeout=300, custom_id="consume:kontrakt_reject_reason")
        self.message_id = message_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        state = _contract_load(self.message_id)
        if state is None:
            await interaction.response.send_message("Контракт не найден или уже закрыт.", ephemeral=True)
            return
        if not user_can_manage_kontrakt_contract(interaction, state):
            await interaction.response.send_message(embed=_kontrakt_manage_forbidden_embed(), ephemeral=True)
            return
        state.status_open = False
        reason = str(self.reason).strip()[:300]
        state.status_note = f"Отказ: {reason[:120] or 'без причины'}"
        _contract_store(self.message_id, state)
        await _kontrakt_refresh_message(self.message_id)
        await _kontrakt_open_thread_and_notify(
            self.message_id,
            decision_text="Отказали",
            reason=reason or "без причины",
            actor_id=interaction.user.id if isinstance(interaction.user, discord.Member) else None,
        )
        await interaction.response.send_message("Контракт закрыт с отказом.", ephemeral=True)


class KontraktProposeModal(SafeModal, title="Предложить контракт"):
    title_input = discord.ui.TextInput(label="Название", max_length=120, placeholder="Например: Ограбление фуры")
    razdel_100 = discord.ui.TextInput(label="На 100%", max_length=100, placeholder="Да / Нет")
    people = discord.ui.TextInput(
        label="Люди",
        max_length=100,
        placeholder="Например: От 2-6",
    )

    def __init__(self) -> None:
        super().__init__(custom_id="consume:kontrakt_propose_form")

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or interaction.channel is None:
            await interaction.response.send_message("Используйте на сервере.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("Контракт можно публиковать только в текстовом канале.", ephemeral=True)
            return
        if not kontrakt_allowed_in_channel(interaction):
            await interaction.response.send_message(kontrakt_channel_restriction_message(), ephemeral=True)
            return
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Не удалось определить участника.", ephemeral=True)
            return

        cap, people_note = parse_kontrakt_people_cap(str(self.people))
        state = ContractState(
            channel_id=interaction.channel.id,
            creator_id=interaction.user.id,
            creator_tag=interaction.user.display_name,
            title=str(self.title_input).strip(),
            veksels="—",
            time_slot="—",
            razdel_100=str(self.razdel_100).strip(),
            people_note=people_note,
            max_participants=cap,
        )
        emb = build_kontrakt_contract_embed(state)

        ping_mentions: list[str] = []
        allowed_roles: list[discord.Role] = []
        for rid in KONTRAKT_NEW_CONTRACT_PING_ROLE_IDS:
            role = interaction.guild.get_role(rid)
            if role is not None:
                ping_mentions.append(role.mention)
                allowed_roles.append(role)
        content = " ".join(ping_mentions) if ping_mentions else None

        msg = await interaction.channel.send(
            content=content,
            embed=emb,
            view=KontraktContractView(),
            allowed_mentions=discord.AllowedMentions(roles=allowed_roles) if allowed_roles else discord.AllowedMentions.none(),
        )
        _contract_store(msg.id, state)
        await interaction.response.send_message("Контракт опубликован.", ephemeral=True)


class KontraktPanelView(SafeView):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(label="Предложить", style=discord.ButtonStyle.primary, custom_id="consume:kontrakt_propose")
    async def propose(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not kontrakt_allowed_in_channel(interaction):
            await interaction.response.send_message(kontrakt_channel_restriction_message(), ephemeral=True)
            return
        await interaction.response.send_modal(KontraktProposeModal())


class KontraktContractView(SafeView):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(label="Участвовать", style=discord.ButtonStyle.success, custom_id="consume:kontrakt_join")
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.message is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Не удалось обработать нажатие.", ephemeral=True)
            return
        state = _contract_load(interaction.message.id)
        if state is None:
            await interaction.response.send_message("Контракт не найден (после перезапуска опубликуйте новый).", ephemeral=True)
            return
        if not state.status_open:
            await interaction.response.send_message("Контракт уже закрыт.", ephemeral=True)
            return
        if interaction.user.id in state.participant_ids:
            await interaction.response.send_message("Вы уже в списке.", ephemeral=True)
            return
        if len(state.participant_ids) >= state.max_participants:
            await interaction.response.send_message("Список уже заполнен.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        state.participant_ids.append(interaction.user.id)
        _contract_store(interaction.message.id, state)
        await _kontrakt_refresh_message(interaction.message.id)
        await _safe_interaction_ephemeral(interaction, "Вы добавлены в контракт.")

    @discord.ui.button(label="Пикнул", style=discord.ButtonStyle.secondary, custom_id="consume:kontrakt_pinged")
    async def pinged(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.message is None:
            await interaction.response.send_message("Не удалось обработать нажатие.", ephemeral=True)
            return
        state = _contract_load(interaction.message.id)
        if state is None:
            await interaction.response.send_message("Контракт не найден.", ephemeral=True)
            return
        if not user_can_manage_kontrakt_contract(interaction, state):
            await interaction.response.send_message(embed=_kontrakt_manage_forbidden_embed(), ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        state.status_open = False
        state.status_note = "Пикнул"
        _contract_store(interaction.message.id, state)
        await _kontrakt_refresh_message(interaction.message.id)
        await _kontrakt_open_thread_and_notify(
            interaction.message.id,
            decision_text="Пикнули",
            actor_id=interaction.user.id if isinstance(interaction.user, discord.Member) else None,
        )
        await _safe_interaction_ephemeral(interaction, "Контракт отмечен: Пикнул.")

    @discord.ui.button(label="Отказ", style=discord.ButtonStyle.danger, custom_id="consume:kontrakt_reject")
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.message is None:
            await interaction.response.send_message("Не удалось обработать нажатие.", ephemeral=True)
            return
        state = _contract_load(interaction.message.id)
        if state is None:
            await interaction.response.send_message("Контракт не найден.", ephemeral=True)
            return
        if not user_can_manage_kontrakt_contract(interaction, state):
            await interaction.response.send_message(embed=_kontrakt_manage_forbidden_embed(), ephemeral=True)
            return
        await interaction.response.send_modal(KontraktRejectModal(interaction.message.id))


@dataclasses.dataclass
class SborState:
    guild_id: int
    role_id: int
    message_id: int
    channel_id: int
    kind: str
    max_main: int
    max_reserve: int
    start_at: datetime
    open: bool = True
    main_ids: set[int] = dataclasses.field(default_factory=set)
    reserve_ids: set[int] = dataclasses.field(default_factory=set)


_sbor_sessions: dict[int, SborState] = {}
_sbor_lock = asyncio.Lock()
_sbor_tasks: dict[int, asyncio.Task] = {}


def _sbor_render_mentions(ids: set[int]) -> str:
    if not ids:
        return "—"
    return "\n".join(f"<@{uid}>" for uid in sorted(ids))


def _sbor_format_remaining(seconds: float) -> str:
    if seconds <= 0:
        return "0 минут"
    total_minutes = int(seconds // 60)
    hours = total_minutes // 60
    minutes = total_minutes % 60
    parts: list[str] = []
    if hours > 0:
        parts.append(f"{hours} ч")
    parts.append(f"{minutes} мин")
    return " ".join(parts)


def _sbor_embed(state: SborState) -> discord.Embed:
    now = datetime.now(timezone.utc)
    remaining_sec = max(0.0, (state.start_at - now).total_seconds())
    remaining_str = _sbor_format_remaining(remaining_sec)
    when_str = state.start_at.astimezone(SBOR_TZ).strftime("%d.%m.%Y г. %H:%M МСК")
    emb = discord.Embed(
        title=f"Сбор · {state.kind}",
        color=discord.Color.dark_theme(),
        timestamp=now,
    )
    emb.add_field(name="Время", value=f"{when_str}\nчерез {remaining_str}", inline=False)
    emb.add_field(
        name=f"Участники ({len(state.main_ids)}/{state.max_main})",
        value=_sbor_render_mentions(state.main_ids),
        inline=False,
    )
    emb.add_field(
        name=f"Замены ({len(state.reserve_ids)}/{state.max_reserve})",
        value=_sbor_render_mentions(state.reserve_ids),
        inline=False,
    )
    emb.set_footer(text=datetime.now(SBOR_TZ).strftime("%d.%m.%Y"))
    return emb


async def _sbor_refresh_message(state: SborState) -> bool:
    guild = bot.get_guild(state.guild_id)
    if guild is None:
        return False
    ch = guild.get_channel(state.channel_id)
    if not isinstance(ch, discord.TextChannel):
        return False
    try:
        msg = await ch.fetch_message(state.message_id)
    except (discord.NotFound, discord.HTTPException):
        return False
    # Если запись закрыта — показываем только кнопку модерации
    view: discord.ui.View
    now = datetime.now(timezone.utc)
    if state.open and now <= state.start_at:
        view = SborPublicView()
    else:
        view = SborPublicViewClosed()
    try:
        await msg.edit(embed=_sbor_embed(state), view=view)
        return True
    except discord.HTTPException:
        return False


async def _sbor_announce_start(state: SborState) -> None:
    """Отправить сообщение о старте сбора с пингом роли и списком основы."""
    guild = bot.get_guild(state.guild_id)
    if guild is None:
        return
    ch = guild.get_channel(state.channel_id)
    if not isinstance(ch, discord.TextChannel):
        return
    role = guild.get_role(state.role_id)
    if role is None:
        return

    if state.main_ids:
        mains_line = " ".join(f"<@{uid}>" for uid in sorted(state.main_ids))
    else:
        mains_line = "—"

    try:
        await ch.send(
            content=role.mention,
            allowed_mentions=discord.AllowedMentions(everyone=False, roles=[role]),
            embed=discord.Embed(
                title=f"Сбор · {state.kind} — старт",
                description=f"**Основа:**\n{mains_line}",
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc),
            ),
        )
    except discord.HTTPException:
        pass


async def _sbor_notify_role_dm(
    *,
    guild: discord.Guild,
    role: discord.Role,
    channel: discord.TextChannel,
    kind: str,
    start_at: datetime,
    jump_url: str,
) -> None:
    start_local = start_at.astimezone(SBOR_TZ)
    start_str = start_local.strftime("%H:%M")
    date_str = start_local.strftime("%d.%m.%Y")
    dm_text = (
        f"Тебя собирают на **{kind}** в **{guild.name}**.\n"
        f"Канал: {channel.mention}\n"
        f"Старт: **{date_str} {start_str} МСК**\n"
        f"Записаться: {jump_url}"
    )
    for member in role.members:
        if member.bot:
            continue
        try:
            await member.send(dm_text)
        except discord.HTTPException as e:
            logger.warning("Не удалось отправить ЛС о сборе %s: %s", member.id, e)


def _sbor_start_countdown(message_id: int) -> None:
    if message_id in _sbor_tasks:
        return

    async def _runner(msg_id: int) -> None:
        try:
            while True:
                async with _sbor_lock:
                    state = _sbor_sessions.get(msg_id)
                    if state is None:
                        break
                    now = datetime.now(timezone.utc)
                    if not state.open or now > state.start_at:
                        state.open = False
                        await _sbor_refresh_message(state)
                        await _sbor_announce_start(state)
                        break
                    await _sbor_refresh_message(state)
                await asyncio.sleep(60)
        finally:
            _sbor_tasks.pop(msg_id, None)

    _sbor_tasks[message_id] = asyncio.create_task(_runner(message_id))


def _sbor_join(state: SborState, user_id: int, target: str) -> None:
    if target == "main":
        state.reserve_ids.discard(user_id)
        state.main_ids.add(user_id)
    else:
        state.main_ids.discard(user_id)
        state.reserve_ids.add(user_id)


def _sbor_remove(state: SborState, user_id: int) -> bool:
    removed = user_id in state.main_ids or user_id in state.reserve_ids
    state.main_ids.discard(user_id)
    state.reserve_ids.discard(user_id)
    return removed


class SborPublicView(SafeView):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    async def _handle_join(self, interaction: discord.Interaction, target: str) -> None:
        if interaction.message is None:
            await interaction.response.send_message("Сообщение не найдено.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        async with _sbor_lock:
            state = _sbor_sessions.get(interaction.message.id)
            if state is None:
                await _safe_interaction_ephemeral(interaction, "Этот сбор уже не активен.")
                return
            # авто-закрытие по времени (при строго прошедшем времени)
            if datetime.now(timezone.utc) > state.start_at or not state.open:
                state.open = False
                await _sbor_refresh_message(state)
                await _safe_interaction_ephemeral(interaction, "Запись на этот сбор уже закрыта.")
                return
            if target == "main" and len(state.main_ids) >= state.max_main:
                await _safe_interaction_ephemeral(interaction, "Все места в основе уже заняты.")
                return
            if target == "reserve" and len(state.reserve_ids) >= state.max_reserve:
                await _safe_interaction_ephemeral(interaction, "Все места на замене уже заняты.")
                return
            _sbor_join(state, interaction.user.id, target)
            ok = await _sbor_refresh_message(state)
        if not ok:
            await _safe_interaction_ephemeral(interaction, "Не удалось обновить сообщение сбора.")
            return
        label = "основу" if target == "main" else "замену"
        await _safe_interaction_ephemeral(interaction, f"Вы записаны в **{label}**.")

    @discord.ui.button(label="В основу", style=discord.ButtonStyle.success, custom_id="consume:sbor_join_main")
    async def join_main(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._handle_join(interaction, "main")

    @discord.ui.button(label="На замену", style=discord.ButtonStyle.secondary, custom_id="consume:sbor_join_reserve")
    async def join_reserve(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._handle_join(interaction, "reserve")

    @discord.ui.button(label="Выйти", style=discord.ButtonStyle.danger, custom_id="consume:sbor_leave")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.message is None:
            await interaction.response.send_message("Сообщение не найдено.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        async with _sbor_lock:
            state = _sbor_sessions.get(interaction.message.id)
            if state is None:
                await _safe_interaction_ephemeral(interaction, "Этот сбор уже не активен.")
                return
            changed = _sbor_remove(state, interaction.user.id)
            ok = await _sbor_refresh_message(state)
        if not changed:
            await _safe_interaction_ephemeral(interaction, "Вы не были записаны в этот сбор.")
            return
        if not ok:
            await _safe_interaction_ephemeral(interaction, "Не удалось обновить сообщение сбора.")
            return
        await _safe_interaction_ephemeral(interaction, "Вы выписаны из сбора.")

    @discord.ui.button(label="Модерация списка", style=discord.ButtonStyle.primary, custom_id="consume:sbor_moderation")
    async def moderation(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await user_can_moderate(interaction):
            await interaction.response.send_message("Только модераторы могут открыть модерацию.", ephemeral=True)
            return
        if interaction.message is None:
            await interaction.response.send_message("Сообщение не найдено.", ephemeral=True)
            return
        async with _sbor_lock:
            state = _sbor_sessions.get(interaction.message.id)
            if state is None:
                await interaction.response.send_message("Этот сбор уже не активен.", ephemeral=True)
                return
        await interaction.response.send_message(
            "Панель модерации сбора:",
            view=SborModerationView(state.message_id),
            ephemeral=True,
        )


class SborPublicViewClosed(SafeView):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(label="Модерация списка", style=discord.ButtonStyle.primary, custom_id="consume:sbor_moderation_closed")
    async def moderation(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await user_can_moderate(interaction):
            await interaction.response.send_message("Только модераторы могут открыть модерацию.", ephemeral=True)
            return
        if interaction.message is None:
            await interaction.response.send_message("Сообщение не найдено.", ephemeral=True)
            return
        async with _sbor_lock:
            state = _sbor_sessions.get(interaction.message.id)
            if state is None:
                await interaction.response.send_message("Этот сбор уже не активен.", ephemeral=True)
                return
        await interaction.response.send_message(
            "Панель модерации сбора:",
            view=SborModerationView(state.message_id),
            ephemeral=True,
        )


class SborApproveSelect(discord.ui.Select):
    def __init__(self, message_id: int, state: SborState) -> None:
        guild = bot.get_guild(state.guild_id)
        registered_ids = sorted(state.main_ids | state.reserve_ids)
        options: list[discord.SelectOption] = []
        for uid in registered_ids:
            name = str(uid)
            if guild is not None:
                member = guild.get_member(uid)
                if member is not None:
                    name = member.display_name
            options.append(discord.SelectOption(label=name[:100], value=str(uid)))
        if not options:
            options = [discord.SelectOption(label="Никто не записан", value="0")]
        max_values = min(25, len(options))
        super().__init__(
            placeholder="Отметьте, кто идёт в основу",
            min_values=0,
            max_values=max_values,
            options=options,
            disabled=not registered_ids,
        )
        self.message_id = message_id

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await user_can_moderate(interaction):
            await interaction.response.send_message("Нет прав.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        async with _sbor_lock:
            state = _sbor_sessions.get(self.message_id)
            if state is None:
                await _safe_interaction_ephemeral(interaction, "Сбор не найден.")
                return
            if not (state.main_ids or state.reserve_ids):
                await _safe_interaction_ephemeral(interaction, "Никто ещё не записался.")
                return
            # выбранные идут в основу, остальные из записавшихся — в замены
            selected_ids = {int(v) for v in self.values if v != "0"}
            registered_ids = list(sorted(state.main_ids | state.reserve_ids))
            new_main: list[int] = []
            for uid in registered_ids:
                if uid in selected_ids and len(new_main) < state.max_main:
                    new_main.append(uid)
            new_reserve: list[int] = []
            for uid in registered_ids:
                if uid not in selected_ids and len(new_reserve) < state.max_reserve:
                    new_reserve.append(uid)
            state.main_ids = set(new_main)
            state.reserve_ids = set(new_reserve)
            ok = await _sbor_refresh_message(state)
        if not ok:
            await _safe_interaction_ephemeral(interaction, "Не удалось обновить список.")
            return
        await _safe_interaction_ephemeral(interaction, "Список основы и замен обновлён.")


class SborRemoveSelect(discord.ui.Select):
    def __init__(self, message_id: int, state: SborState) -> None:
        guild = bot.get_guild(state.guild_id)

        def member_name(uid: int) -> str:
            if guild is not None:
                member = guild.get_member(uid)
                if member is not None:
                    return member.display_name
            return str(uid)

        options: list[discord.SelectOption] = []
        for uid in sorted(state.main_ids):
            options.append(
                discord.SelectOption(label=f"Основа: {member_name(uid)}"[:100], value=str(uid))
            )
        for uid in sorted(state.reserve_ids):
            options.append(
                discord.SelectOption(label=f"Замена: {member_name(uid)}"[:100], value=str(uid))
            )
        if not options:
            options = [discord.SelectOption(label="Список пуст", value="0")]
        super().__init__(
            placeholder="Выписать участника",
            min_values=1,
            max_values=1,
            options=options[:25],
            disabled=not (state.main_ids or state.reserve_ids),
        )
        self.message_id = message_id

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await user_can_moderate(interaction):
            await interaction.response.send_message("Нет прав.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        uid = int(self.values[0])
        if uid == 0:
            await _safe_interaction_ephemeral(interaction, "Выписывать некого.")
            return
        async with _sbor_lock:
            state = _sbor_sessions.get(self.message_id)
            if state is None:
                await _safe_interaction_ephemeral(interaction, "Сбор не найден.")
                return
            removed = _sbor_remove(state, uid)
            ok = await _sbor_refresh_message(state)
        if not removed:
            await _safe_interaction_ephemeral(interaction, "Участник не найден в списках.")
            return
        if not ok:
            await _safe_interaction_ephemeral(interaction, "Удалено, но не удалось обновить сообщение.")
            return
        await _safe_interaction_ephemeral(interaction, f"<@{uid}> выписан(а) из списков.")


class SborModerationView(SafeView):
    def __init__(self, message_id: int) -> None:
        super().__init__(timeout=300)
        self.message_id = message_id
        state = _sbor_sessions.get(message_id)
        if state is not None:
            self.add_item(SborApproveSelect(message_id, state))
            self.add_item(SborRemoveSelect(message_id, state))

    @discord.ui.button(label="Закрыть/открыть запись", style=discord.ButtonStyle.danger)
    async def toggle_open(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await user_can_moderate(interaction):
            await interaction.response.send_message("Нет прав.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        async with _sbor_lock:
            state = _sbor_sessions.get(self.message_id)
            if state is None:
                await _safe_interaction_ephemeral(interaction, "Сбор не найден.")
                return
            was_open = state.open
            state.open = not state.open
            ok = await _sbor_refresh_message(state)
            if was_open and not state.open:
                await _sbor_announce_start(state)
        if not ok:
            await _safe_interaction_ephemeral(interaction, "Не удалось обновить сообщение.")
            return
        await _safe_interaction_ephemeral(
            interaction,
            "Запись открыта." if state.open else "Запись закрыта.",
        )


def _resolve_banner_path(raw: str) -> Path | None:
    raw = raw.strip()
    if not raw:
        return None
    p = Path(raw)
    if p.is_file():
        return p
    p2 = BOT_DIR / raw
    if p2.is_file():
        return p2
    return None


_TICKET_BANNER_FILENAME = "ticket_banner.png"


def _ticket_banner_image_path() -> Path | None:
    for key in ("APPLICATION_EMBED_IMAGE_PATH", "BANNER_IMAGE_PATH"):
        raw = os.getenv(key, "").strip()
        if not raw:
            continue
        p = _resolve_banner_path(raw)
        if p is not None:
            return p
    return None


def _ticket_banner_files() -> list[discord.File]:
    path = _ticket_banner_image_path()
    if path is None:
        return []
    try:
        return [discord.File(path, filename=_TICKET_BANNER_FILENAME)]
    except OSError as e:
        logger.warning("Картинка заявки не прочитана (%s): %s", path, e)
        return []


def _build_ticket_embed_with_banner(
    *,
    kind: TicketKind,
    ticket_no: int,
    applicant: discord.abc.User,
    fields: list[tuple[str, str]],
    phase: TicketPhase,
) -> tuple[discord.Embed, list[discord.File]]:
    emb = _build_ticket_embed(
        kind=kind,
        ticket_no=ticket_no,
        applicant=applicant,
        fields=fields,
        phase=phase,
    )
    # Для карточки новой заявки баннер отключен полностью.
    return emb, []


def build_application_embed(bot_user: discord.abc.User) -> discord.Embed:
    emb = discord.Embed(
        title="Оформление заявки.",
        color=discord.Color.dark_theme(),
    )
    if bot_user.display_avatar:
        emb.set_author(name="Consume famq", icon_url=bot_user.display_avatar.url)
    else:
        emb.set_author(name="Consume famq")
    emb.description = (
        "**После отправки анкеты сразу создаётся отдельный тикет-канал с вами.**\n\n"
        "> В канале команда рассматривает заявку и выносит решение: **Принять / Отказать**.\n\n"
        "**Также продублируем ссылку на тикет в личные сообщения, чтобы вы ничего не пропустили.**"
    )
    emb.set_footer(text="Подать заявку:")
    return emb


def build_maps_embed() -> discord.Embed:
    desc = (
        "**Все карты VZP**\n\n"
        "> **Байкерка | Большой миррор | Веспуччи**\n"
        "> **Ветряки | Киностудия | Лесопилка |  Маленький миррор**\n"
        "> **Муравейник | Мусорка | Мясо | Нефть | Палетка **\n"
        "> **Порт бизвар | Сендик | Стройка | Татушка**\n\n"
        "**Выбери карту:**\n"
    )
    return discord.Embed(description=desc, color=discord.Color.dark_theme())


class ConsumeBot(commands.Bot):
    def __init__(self) -> None:
        super().__init__(command_prefix="!", intents=INTENTS)

    async def setup_hook(self) -> None:
        self.add_view(ApplicationPanelView())
        self.add_view(ModerationPanelView())
        self.add_view(VZPMapsView())
        self.add_view(KontraktPanelView())
        self.add_view(KontraktContractView())
        self.add_view(AutoparkPanelView())
        await _restore_ticket_views()
        guild_raw = os.getenv("DEV_GUILD_ID", "").strip()
        if guild_raw:
            try:
                g = discord.Object(id=int(guild_raw))
                # В dev-гильдии ранее могли остаться копии guild-команд,
                # из-за чего в UI видно дубли (global + guild).
                # Очищаем guild-оверлей и оставляем единый набор global-команд.
                self.tree.clear_commands(guild=g)
                await self.tree.sync(guild=g)
                await self.tree.sync()
            except ValueError:
                await self.tree.sync()
        else:
            await self.tree.sync()

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or message.webhook_id or message.guild is None:
            return
        ch = message.channel
        if not isinstance(
            ch,
            (
                discord.TextChannel,
                discord.VoiceChannel,
                discord.StageChannel,
                discord.Thread,
            ),
        ):
            return
        if not ROLE_MENTION_DM_TARGET_ROLE_IDS:
            return
        if not ROLE_MENTION_DM_CATEGORY_IDS and not ROLE_MENTION_DM_CHANNEL_IDS:
            return
        if not role_mention_dm_watchlist_matches_channel(ch):
            return
        if not message.role_mentions:
            return
        target_roles = [
            r
            for r in message.role_mentions
            if r.id in ROLE_MENTION_DM_TARGET_ROLE_IDS
        ]
        if not target_roles:
            return
        if not isinstance(message.author, discord.Member):
            return
        if not user_can_trigger_role_mention_dm(message.author):
            return
        asyncio.create_task(dm_role_mention_channel_broadcast(message, target_roles))

    async def on_interaction(self, interaction: discord.Interaction) -> None:
        asyncio.create_task(_log_bot_interaction_action(interaction))


bot = ConsumeBot()
_daily_role_ping_task: asyncio.Task[None] | None = None
_autopark_task: asyncio.Task[None] | None = None


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
    original = error.original if isinstance(error, app_commands.CommandInvokeError) else error
    if isinstance(original, Exception) and _is_stale_interaction_error(original):
        logger.warning("Устаревшее slash-взаимодействие: %s", original)
        return
    logger.exception("Ошибка app command: %s", error)
    await _safe_interaction_ephemeral(interaction, "Команда временно недоступна. Попробуйте еще раз.")


def _interaction_action_name(interaction: discord.Interaction) -> str | None:
    data = interaction.data if isinstance(interaction.data, dict) else {}
    action_labels: dict[str, str] = {
        "mod_rp": "Модерация: переключить прием РП",
        "mod_vzp": "Модерация: переключить прием VZP",
        "app_select": "Панель заявок: выбрать тип заявки",
        "map_select": "Карты VZP: выбрать карту",
        "kontrakt_propose": "Контракты: открыть форму предложения",
        "kontrakt_propose_form": "Контракты: отправить форму предложения",
        "kontrakt_join": "Контракты: записаться",
        "kontrakt_pinged": "Контракты: отметить 'Пикнул'",
        "kontrakt_reject": "Контракты: открыть форму отказа",
        "kontrakt_reject_reason": "Контракты: отправить причину отказа",
        "sbor_join_main": "Сбор: записаться в основу",
        "sbor_join_reserve": "Сбор: записаться на замену",
        "sbor_leave": "Сбор: выйти из списка",
        "sbor_moderation": "Сбор: открыть модерацию списка",
        "sbor_moderation_closed": "Сбор: открыть модерацию закрытого сбора",
        "autopark_take": "Автопарк: открыть выбор авто для брони",
        "autopark_release": "Автопарк: открыть выбор авто для освобождения",
        "autopark_edit": "Автопарк: открыть редактор списка",
        "autopark_claim_select": "Автопарк: выбрать авто для брони",
        "autopark_release_select": "Автопарк: выбрать авто для освобождения",
        "autopark_delete_select": "Автопарк: выбрать авто для удаления",
        "autopark_add_car": "Автопарк: отправить форму добавления авто",
        "ticket_reject_reason": "Тикеты: отправить причину отказа",
        "ticket_apply_rp": "Тикеты: отправить РП-заявку",
        "ticket_apply_vzp": "Тикеты: отправить VZP-заявку",
    }

    def _looks_dynamic_token(token: str) -> bool:
        t = token.strip().lower()
        if not t:
            return True
        if re.fullmatch(r"\d{6,}", t):
            return True
        if re.fullmatch(r"[0-9a-f]{16,}", t):
            return True
        if re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", t):
            return True
        if len(t) >= 24 and re.fullmatch(r"[a-z0-9_-]+", t):
            return True
        return False

    def _normalize_custom_id(raw_custom_id: str) -> str:
        cid = raw_custom_id.strip()
        if not cid:
            return "unknown"
        if cid.startswith("consume:"):
            cid = cid[len("consume:") :]
        parts = cid.split(":")
        clean_parts: list[str] = []
        for part in parts:
            clean_parts.append("<id>" if _looks_dynamic_token(part) else part)
        return ":".join(clean_parts)

    if interaction.type == discord.InteractionType.application_command:
        cmd_name = str(data.get("name", "")).strip() or "unknown"
        return f"Команда /{cmd_name}"
    if interaction.type == discord.InteractionType.component:
        custom_id = _normalize_custom_id(str(data.get("custom_id", "")))
        component_type = int(data.get("component_type", 0) or 0)
        label = action_labels.get(custom_id)
        if label:
            return label
        if component_type == int(discord.ComponentType.button):
            return f"Кнопка: {custom_id}"
        return f"Выбор: {custom_id}"
    if interaction.type == discord.InteractionType.modal_submit:
        custom_id = _normalize_custom_id(str(data.get("custom_id", "")))
        label = action_labels.get(custom_id)
        if label:
            return label
        return f"Форма: {custom_id}"
    return None


async def _log_bot_interaction_action(interaction: discord.Interaction) -> None:
    if not BOT_ACTION_LOG_CHANNEL_ID:
        return
    action = _interaction_action_name(interaction)
    if action is None:
        return
    user = interaction.user
    if user is None or getattr(user, "bot", False):
        return

    log_ch = bot.get_channel(BOT_ACTION_LOG_CHANNEL_ID)
    if not isinstance(log_ch, (discord.TextChannel, discord.Thread)):
        try:
            fetched = await bot.fetch_channel(BOT_ACTION_LOG_CHANNEL_ID)
        except discord.HTTPException:
            logger.warning("Не удалось получить канал логов действий бота: %s", BOT_ACTION_LOG_CHANNEL_ID)
            return
        if not isinstance(fetched, (discord.TextChannel, discord.Thread)):
            logger.warning("BOT_ACTION_LOG_CHANNEL_ID=%s не текстовый канал.", BOT_ACTION_LOG_CHANNEL_ID)
            return
        log_ch = fetched

    data_raw = interaction.data if isinstance(interaction.data, dict) else {}
    custom_id_raw = str(data_raw.get("custom_id", "") or "")[:300] or None
    it = interaction.type
    type_name = getattr(it, "name", None) or str(it)

    payload: dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "source": "interaction",
        "interaction_id": str(interaction.id),
        "interaction_type": type_name,
        "action": action,
        "user": {
            "id": user.id,
            "username": getattr(user, "name", None),
            "global_name": getattr(user, "global_name", None),
        },
        "channel_id": interaction.channel_id,
    }
    if interaction.guild is not None:
        payload["guild"] = {
            "id": interaction.guild.id,
            "name": interaction.guild.name,
        }
    else:
        payload["guild"] = None
    if custom_id_raw:
        payload["custom_id"] = custom_id_raw

    line = json.dumps(payload, ensure_ascii=False)
    if len(line) > 1980:
        payload["action"] = (action[:400] + "…") if len(action) > 400 else action
        payload.pop("custom_id", None)
        line = json.dumps(payload, ensure_ascii=False)
        if len(line) > 1980:
            line = line[:1977] + "…"

    discord_body = f"```json\n{line}\n```"
    try:
        await log_ch.send(discord_body[:2000])
    except discord.HTTPException as e:
        logger.warning("Не удалось отправить лог действия бота в канал %s: %s", BOT_ACTION_LOG_CHANNEL_ID, e)


async def dm_role_mention_channel_broadcast(
    message: discord.Message,
    target_roles: list[discord.Role],
) -> None:
    if message.guild is None:
        return
    ch = message.channel
    if not isinstance(
        ch,
        (
            discord.TextChannel,
            discord.VoiceChannel,
            discord.StageChannel,
            discord.Thread,
        ),
    ):
        return

    # Берем исходный текст и удаляем упомянутые целевые роли.
    content = (message.content or "").strip()
    for role in target_roles:
        content = content.replace(role.mention, " ")
    content = re.sub(r"\s+", " ", content).strip()
    body = content[:1700] if content else "(без текста)"
    body = f"> ## {body}"
    author_label = (
        message.author.display_name
        if isinstance(message.author, discord.Member)
        else str(message.author)
    )
    header = f"**{author_label}** · сообщение в <#{ch.id}>:\n\n"
    footer = f"\n\n{message.jump_url}"
    dm_content = (header + body + footer)[:2000]

    recipients: dict[int, discord.Member] = {}
    for role in target_roles:
        for m in role.members:
            if m.bot or m.id == message.author.id:
                continue
            recipients[m.id] = m

    for member in recipients.values():
        try:
            await member.send(dm_content)
        except discord.HTTPException:
            pass
        await asyncio.sleep(0.35)


async def daily_role_ping_loop() -> None:
    await bot.wait_until_ready()
    if not DAILY_ROLE_PING_CHANNEL_ID or not DAILY_ROLE_PING_ROLE_ID:
        return
    ch_id = DAILY_ROLE_PING_CHANNEL_ID
    rid = DAILY_ROLE_PING_ROLE_ID
    interval_sec = DAILY_ROLE_PING_INTERVAL_HOURS * 3600
    tz = DAILY_ROLE_PING_TZ
    schedule = DAILY_ROLE_PING_SCHEDULE

    async def send_role_ping() -> discord.Message | None:
        ch = bot.get_channel(ch_id)
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            return None
        role = ch.guild.get_role(rid)
        if role is None:
            logger.warning(
                "daily_role_ping: role id %s not found in guild %s",
                rid,
                ch.guild.id,
            )
            return None
        body = DAILY_ROLE_PING_MESSAGE or "Напоминание."
        content = f"{role.mention}\n{body}"
        return await ch.send(
            content=content,
            allowed_mentions=discord.AllowedMentions(roles=[role]),
        )

    async def delete_bot_message(mid: int) -> None:
        if bot.user is None:
            return
        ch_del = bot.get_channel(ch_id)
        if not isinstance(ch_del, (discord.TextChannel, discord.Thread)):
            return
        try:
            old = await ch_del.fetch_message(mid)
            if old.author.id == bot.user.id:
                await old.delete()
        except discord.HTTPException:
            pass

    if tz is not None and schedule:
        last_mid: int | None = None
        while not bot.is_closed():
            try:
                nxt = next_daily_role_ping_fire(tz, schedule)
                delay = max(1.0, (nxt - datetime.now(tz)).total_seconds())
                await asyncio.sleep(delay)
                if last_mid is not None:
                    await delete_bot_message(last_mid)
                msg = await send_role_ping()
                last_mid = msg.id if msg is not None else None
            except Exception:
                logger.exception("daily_role_ping_loop(schedule): unexpected error")
                await asyncio.sleep(60)
        return

    while not bot.is_closed():
        try:
            msg = await send_role_ping()
            if msg is None:
                await asyncio.sleep(60)
                continue
            await asyncio.sleep(interval_sec)
            await delete_bot_message(msg.id)
        except Exception:
            logger.exception("daily_role_ping_loop(interval): unexpected error")
            await asyncio.sleep(60)


def _member_may_post_panel(member: discord.Member) -> bool:
    g = member.guild
    if member.id == g.owner_id:
        return True
    p = member.guild_permissions
    return p.administrator or p.manage_guild


def _spam_command_role_ids() -> frozenset[int]:
    return frozenset(_parse_id_list(os.getenv("SPAM_COMMAND_ROLE_IDS", "")))


def _member_may_spam(member: discord.Member) -> bool:
    g = member.guild
    if member.id == g.owner_id:
        return True
    p = member.guild_permissions
    if p.administrator or p.manage_guild:
        return True
    allowed = _spam_command_role_ids()
    if allowed and any(r.id in allowed for r in member.roles):
        return True
    return False


def _announce_channel_id() -> int | None:
    raw = os.getenv("ANNOUNCE_CHANNEL_ID", "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _announce_role_id() -> int | None:
    raw = os.getenv("ANNOUNCE_ROLE_ID", "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _format_spam_dm(text: str) -> str:
    """Оформление ЛС: первая строка — `> # ...`, остальные — `> ...` (blockquote в Discord)."""
    raw = text.strip().replace("\r\n", "\n")[:1600]
    if not raw:
        return "> #"
    lines = raw.split("\n")
    out: list[str] = []
    for i, line in enumerate(lines):
        if i == 0:
            out.append(f"> # {line}" if line else "> #")
        else:
            out.append(f"> {line}" if line else ">")
    result = "\n".join(out)
    return result[:1990]


async def _broadcast_dm_spam(
    guild: discord.Guild,
    role: discord.Role,
    content: str,
) -> tuple[int, int]:
    """Возвращает (успешно, ошибок)."""
    if guild.large and not guild.chunked:
        try:
            await guild.chunk()
        except discord.HTTPException:
            logger.warning("guild.chunk() не удался, рассылка только по кэшу участников")

    targets = [
        m
        for m in guild.members
        if role in m.roles and not m.bot and m.id != guild.me.id
    ]
    ok = 0
    fail = 0
    delay = float(os.getenv("SPAM_DM_DELAY_SEC", "0.65").strip() or "0.65")
    for m in targets:
        try:
            await m.send(content)
            ok += 1
        except discord.HTTPException:
            fail += 1
        if delay > 0:
            await asyncio.sleep(delay)
    return ok, fail


@dataclasses.dataclass
class AutoparkCar:
    key: str
    label: str
    note: str
    role_ids: list[int]
    reserved_by: int | None
    reserved_until_ts: int | None


AUTOPARK_RESERVE_MINUTES = max(1, get_env_int("AUTOPARK_RESERVE_MINUTES", 60))


def _autopark_manager_role_ids() -> frozenset[int]:
    return frozenset(_parse_id_list(os.getenv("AUTOPARK_MANAGER_ROLE_IDS", "")))


def _autopark_user_has_access(member: discord.Member, car: AutoparkCar) -> bool:
    if not car.role_ids:
        return True
    return any(r.id in car.role_ids for r in member.roles)


def _autopark_user_can_manage(member: discord.Member) -> bool:
    if member.id == member.guild.owner_id:
        return True
    if member.guild_permissions.manage_guild or member.guild_permissions.administrator:
        return True
    allowed = _autopark_manager_role_ids()
    return bool(allowed and any(r.id in allowed for r in member.roles))


def _autopark_load_cars(guild_id: int) -> list[AutoparkCar]:
    now_ts = int(datetime.now(timezone.utc).timestamp())
    out: list[AutoparkCar] = []
    with _sqlite_connect() as conn:
        rows = conn.execute(
            """
            SELECT car_key, label, note, role_ids_json, reserved_by, reserved_until_ts
            FROM autopark_cars
            WHERE guild_id = ?
            ORDER BY label COLLATE NOCASE, car_key COLLATE NOCASE
            """,
            (guild_id,),
        ).fetchall()
        for row in rows:
            try:
                role_ids = [int(x) for x in json.loads(str(row["role_ids_json"]) or "[]")]
            except (TypeError, ValueError, json.JSONDecodeError):
                role_ids = []
            reserved_by = int(row["reserved_by"]) if row["reserved_by"] is not None else None
            reserved_until = (
                int(row["reserved_until_ts"])
                if row["reserved_until_ts"] is not None
                else None
            )
            # Авто-очистка протухшей брони на чтении.
            if reserved_until is not None and reserved_until <= now_ts:
                conn.execute(
                    """
                    UPDATE autopark_cars
                    SET reserved_by = NULL, reserved_until_ts = NULL
                    WHERE guild_id = ? AND car_key = ?
                    """,
                    (guild_id, str(row["car_key"])),
                )
                reserved_by = None
                reserved_until = None
            out.append(
                AutoparkCar(
                    key=str(row["car_key"]),
                    label=str(row["label"]),
                    note=str(row["note"]),
                    role_ids=role_ids,
                    reserved_by=reserved_by,
                    reserved_until_ts=reserved_until,
                )
            )
        conn.commit()
    return out


def _autopark_upsert_car(guild_id: int, car: AutoparkCar) -> None:
    with _sqlite_connect() as conn:
        conn.execute(
            """
            INSERT INTO autopark_cars (
                guild_id, car_key, label, note, role_ids_json, reserved_by, reserved_until_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, car_key) DO UPDATE SET
                label=excluded.label,
                note=excluded.note,
                role_ids_json=excluded.role_ids_json,
                reserved_by=excluded.reserved_by,
                reserved_until_ts=excluded.reserved_until_ts
            """,
            (
                guild_id,
                car.key,
                car.label,
                car.note,
                json.dumps(car.role_ids, ensure_ascii=False),
                car.reserved_by,
                car.reserved_until_ts,
            ),
        )
        conn.commit()


def _autopark_delete_car(guild_id: int, car_key: str) -> int:
    with _sqlite_connect() as conn:
        cur = conn.execute(
            "DELETE FROM autopark_cars WHERE guild_id = ? AND car_key = ?",
            (guild_id, car_key),
        )
        conn.commit()
    return int(cur.rowcount)


def _autopark_register_panel(guild_id: int, channel_id: int, message_id: int) -> None:
    with _sqlite_connect() as conn:
        conn.execute(
            """
            INSERT INTO autopark_panels (message_id, guild_id, channel_id)
            VALUES (?, ?, ?)
            ON CONFLICT(message_id) DO UPDATE SET
                guild_id=excluded.guild_id,
                channel_id=excluded.channel_id
            """,
            (message_id, guild_id, channel_id),
        )
        conn.commit()


def _autopark_list_panels(guild_id: int) -> list[tuple[int, int]]:
    with _sqlite_connect() as conn:
        rows = conn.execute(
            "SELECT channel_id, message_id FROM autopark_panels WHERE guild_id = ?",
            (guild_id,),
        ).fetchall()
    return [(int(r["channel_id"]), int(r["message_id"])) for r in rows]


def _autopark_remove_panel(message_id: int) -> None:
    with _sqlite_connect() as conn:
        conn.execute("DELETE FROM autopark_panels WHERE message_id = ?", (message_id,))
        conn.commit()


def _autopark_claim(guild_id: int, car_key: str, user_id: int, minutes: int) -> bool:
    now_ts = int(datetime.now(timezone.utc).timestamp())
    until_ts = now_ts + int(minutes) * 60
    with _sqlite_connect() as conn:
        row = conn.execute(
            """
            SELECT reserved_by, reserved_until_ts FROM autopark_cars
            WHERE guild_id = ? AND car_key = ?
            """,
            (guild_id, car_key),
        ).fetchone()
        if row is None:
            return False
        reserved_by = row["reserved_by"]
        reserved_until = row["reserved_until_ts"]
        if reserved_by is not None and reserved_until is not None and int(reserved_until) > now_ts:
            return False
        conn.execute(
            """
            UPDATE autopark_cars
            SET reserved_by = ?, reserved_until_ts = ?
            WHERE guild_id = ? AND car_key = ?
            """,
            (user_id, until_ts, guild_id, car_key),
        )
        conn.commit()
    return True


def _autopark_release(guild_id: int, car_key: str, actor_id: int, force: bool = False) -> bool:
    with _sqlite_connect() as conn:
        row = conn.execute(
            """
            SELECT reserved_by FROM autopark_cars
            WHERE guild_id = ? AND car_key = ?
            """,
            (guild_id, car_key),
        ).fetchone()
        if row is None or row["reserved_by"] is None:
            return False
        if (not force) and int(row["reserved_by"]) != actor_id:
            return False
        conn.execute(
            """
            UPDATE autopark_cars
            SET reserved_by = NULL, reserved_until_ts = NULL
            WHERE guild_id = ? AND car_key = ?
            """,
            (guild_id, car_key),
        )
        conn.commit()
    return True


def _autopark_expire_overdue() -> list[int]:
    now_ts = int(datetime.now(timezone.utc).timestamp())
    touched: set[int] = set()
    with _sqlite_connect() as conn:
        rows = conn.execute(
            """
            SELECT guild_id, car_key
            FROM autopark_cars
            WHERE reserved_until_ts IS NOT NULL AND reserved_until_ts <= ?
            """,
            (now_ts,),
        ).fetchall()
        for row in rows:
            touched.add(int(row["guild_id"]))
            conn.execute(
                """
                UPDATE autopark_cars
                SET reserved_by = NULL, reserved_until_ts = NULL
                WHERE guild_id = ? AND car_key = ?
                """,
                (int(row["guild_id"]), str(row["car_key"])),
            )
        conn.commit()
    return sorted(touched)


def _autopark_embed(guild: discord.Guild) -> discord.Embed:
    cars = _autopark_load_cars(guild.id)
    free = [c for c in cars if c.reserved_by is None]
    busy = [c for c in cars if c.reserved_by is not None]

    def car_line(c: AutoparkCar) -> str:
        parts: list[str] = [f"• {c.label}"]
        if c.note:
            parts.append(c.note)
        if c.role_ids:
            role_mentions = " ".join(
                f"<@&{rid}>" for rid in c.role_ids if guild.get_role(rid) is not None
            )
            if role_mentions:
                parts.append(role_mentions)
        return "\n".join(parts)

    lines_free = [car_line(c) for c in free[:40]]
    lines_busy: list[str] = []
    now_ts = int(datetime.now(timezone.utc).timestamp())
    for c in busy[:20]:
        left = ""
        if c.reserved_until_ts is not None:
            mins = max(0, int((c.reserved_until_ts - now_ts) // 60))
            if mins >= 60:
                hrs = max(1, round(mins / 60))
                left = f"(через {hrs} часа)"
            else:
                left = f"(через {mins} мин)"
        who = f" <@{c.reserved_by}>" if c.reserved_by is not None else ""
        lines_busy.append(f"• {c.label}{who}\n{left}" if left else f"• {c.label}{who}")

    emb = discord.Embed(
        title="🚗 Автопарк: Car",
        description="Актуальный статус автомобилей.",
        color=discord.Color.dark_theme(),
        timestamp=datetime.now(timezone.utc),
    )
    emb.add_field(
        name=f"🟢 Свободные ({len(free)})",
        value=_embed_lines_value(lines_free),
        inline=False,
    )
    emb.add_field(
        name=f"🔴 Занятые ({len(busy)})",
        value=_embed_lines_value(lines_busy),
        inline=False,
    )
    emb.set_footer(text="")
    return emb


async def _autopark_refresh_panels(guild_id: int) -> None:
    guild = bot.get_guild(guild_id)
    if guild is None:
        return
    emb = _autopark_embed(guild)
    for channel_id, message_id in _autopark_list_panels(guild_id):
        ch = guild.get_channel(channel_id)
        if not isinstance(ch, discord.TextChannel):
            _autopark_remove_panel(message_id)
            continue
        try:
            msg = await ch.fetch_message(message_id)
            await msg.edit(embed=emb, view=AutoparkPanelView())
        except (discord.NotFound, discord.Forbidden):
            _autopark_remove_panel(message_id)
        except discord.HTTPException:
            pass


class AutoparkAddModal(SafeModal, title="Добавить авто в список"):
    car_key = discord.ui.TextInput(label="Ключ (уникальный ID)", placeholder="Например: PESTONA01", max_length=60)
    label = discord.ui.TextInput(label="Как показывать в списке", placeholder="BMW M5 H90 LCI - PESTONA01", max_length=120)
    note = discord.ui.TextInput(label="Текст под строкой (необязательно)", required=False, style=discord.TextStyle.paragraph, max_length=250)
    role_ids = discord.ui.TextInput(label="ID ролей доступа (через запятую)", required=False, max_length=300, placeholder="Пусто = доступно всем")

    def __init__(self) -> None:
        super().__init__(custom_id="consume:autopark_add_car")

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        if not _autopark_user_can_manage(interaction.user):
            await interaction.response.send_message("Нет доступа к редактированию автопарка.", ephemeral=True)
            return
        key = str(self.car_key).strip().upper()
        label = str(self.label).strip()
        note = str(self.note).strip()
        if not key or not label:
            await interaction.response.send_message("Нужны ключ и название.", ephemeral=True)
            return
        ids = _parse_id_list(str(self.role_ids))
        # Удобный fallback: если ID не указали отдельно, попробуем извлечь из
        # текста под строкой упоминания вида <@&123...>.
        if not ids and note:
            ids = [int(x) for x in re.findall(r"<@&(\d+)>", note)]
        _autopark_upsert_car(
            interaction.guild.id,
            AutoparkCar(
                key=key,
                label=label,
                note=note,
                role_ids=ids,
                reserved_by=None,
                reserved_until_ts=None,
            ),
        )
        await _autopark_refresh_panels(interaction.guild.id)
        await interaction.response.send_message(f"Добавил авто **{label}**.", ephemeral=True)


class AutoparkEditModal(SafeModal, title="Изменить авто в списке"):
    label = discord.ui.TextInput(label="Как показывать в списке", placeholder="BMW M5 H90 LCI - PESTONA01", max_length=120)
    note = discord.ui.TextInput(label="Текст под строкой (необязательно)", required=False, style=discord.TextStyle.paragraph, max_length=250)
    role_ids = discord.ui.TextInput(label="ID ролей доступа (через запятую)", required=False, max_length=300, placeholder="Пусто = доступно всем")

    def __init__(self, car: AutoparkCar) -> None:
        super().__init__(custom_id="consume:autopark_edit_car")
        self.car_key = car.key
        self.label.default = car.label
        self.note.default = car.note
        self.role_ids.default = ",".join(str(x) for x in car.role_ids)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        if not _autopark_user_can_manage(interaction.user):
            await interaction.response.send_message("Нет доступа к редактированию автопарка.", ephemeral=True)
            return
        label = str(self.label).strip()
        note = str(self.note).strip()
        if not label:
            await interaction.response.send_message("Название не может быть пустым.", ephemeral=True)
            return
        ids = _parse_id_list(str(self.role_ids))
        if not ids and note:
            ids = [int(x) for x in re.findall(r"<@&(\d+)>", note)]

        cars = _autopark_load_cars(interaction.guild.id)
        existing = next((c for c in cars if c.key == self.car_key), None)
        if existing is None:
            await interaction.response.send_message("Позиция не найдена (возможно, уже удалена).", ephemeral=True)
            return

        _autopark_upsert_car(
            interaction.guild.id,
            AutoparkCar(
                key=existing.key,
                label=label,
                note=note,
                role_ids=ids,
                reserved_by=existing.reserved_by,
                reserved_until_ts=existing.reserved_until_ts,
            ),
        )
        await _autopark_refresh_panels(interaction.guild.id)
        await interaction.response.send_message(f"Обновил авто **{label}**.", ephemeral=True)


class AutoparkEditSelect(discord.ui.Select):
    def __init__(self, cars: list[AutoparkCar]) -> None:
        options = [
            discord.SelectOption(label=c.label[:100], value=c.key, description=c.key[:100], emoji="✏️")
            for c in cars[:25]
        ]
        super().__init__(
            placeholder="Выбери авто для изменения...",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="consume:autopark_edit_select",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        if not _autopark_user_can_manage(interaction.user):
            await interaction.response.send_message("Нет доступа к редактированию автопарка.", ephemeral=True)
            return
        car_key = self.values[0]
        cars = _autopark_load_cars(interaction.guild.id)
        car = next((c for c in cars if c.key == car_key), None)
        if car is None:
            await interaction.response.send_message("Позиция не найдена.", ephemeral=True)
            return
        await interaction.response.send_modal(AutoparkEditModal(car))


class AutoparkEditView(SafeView):
    def __init__(self, cars: list[AutoparkCar]) -> None:
        super().__init__(timeout=120)
        self.add_item(AutoparkEditSelect(cars))


class AutoparkDeleteSelect(discord.ui.Select):
    def __init__(self, cars: list[AutoparkCar]) -> None:
        options = [
            discord.SelectOption(label=c.label[:100], value=c.key, description=c.key[:100], emoji="🗑️")
            for c in cars[:25]
        ]
        super().__init__(
            placeholder="Выбери авто, чтобы убрать из списка...",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="consume:autopark_delete_select",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        if not _autopark_user_can_manage(interaction.user):
            await interaction.response.send_message("Нет доступа к редактированию автопарка.", ephemeral=True)
            return
        car_key = self.values[0]
        deleted = _autopark_delete_car(interaction.guild.id, car_key)
        await _autopark_refresh_panels(interaction.guild.id)
        if deleted:
            await interaction.response.send_message("Позиция удалена.", ephemeral=True)
        else:
            await interaction.response.send_message("Не удалось удалить позицию.", ephemeral=True)


class AutoparkDeleteView(SafeView):
    def __init__(self, cars: list[AutoparkCar]) -> None:
        super().__init__(timeout=120)
        self.add_item(AutoparkDeleteSelect(cars))


class AutoparkEditorView(SafeView):
    def __init__(self) -> None:
        super().__init__(timeout=180)

    @discord.ui.button(label="Добавить авто", emoji="➕", style=discord.ButtonStyle.success)
    async def add_car(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_modal(AutoparkAddModal())

    @discord.ui.button(label="Изменить позицию", emoji="✏️", style=discord.ButtonStyle.primary)
    async def edit_car(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        cars = _autopark_load_cars(interaction.guild.id)
        if not cars:
            await interaction.response.send_message("Список пуст.", ephemeral=True)
            return
        await interaction.response.send_message("Выбери позицию для изменения:", ephemeral=True, view=AutoparkEditView(cars))

    @discord.ui.button(label="Удалить из списка", emoji="➖", style=discord.ButtonStyle.danger)
    async def remove_car(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        cars = _autopark_load_cars(interaction.guild.id)
        if not cars:
            await interaction.response.send_message("Список пуст.", ephemeral=True)
            return
        await interaction.response.send_message("Выбери позицию для удаления:", ephemeral=True, view=AutoparkDeleteView(cars))


class AutoparkClaimSelect(discord.ui.Select):
    def __init__(self, cars: list[AutoparkCar]) -> None:
        options = [discord.SelectOption(label=c.label[:100], value=c.key, description=c.note[:100] or c.key[:100]) for c in cars[:25]]
        super().__init__(placeholder="Выбери авто для брони...", min_values=1, max_values=1, options=options, custom_id="consume:autopark_claim_select")

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        key = self.values[0]
        if not _autopark_claim(interaction.guild.id, key, interaction.user.id, AUTOPARK_RESERVE_MINUTES):
            await _safe_interaction_ephemeral(interaction, "Не удалось занять авто (возможно, уже занято).")
            return
        cars = _autopark_load_cars(interaction.guild.id)
        car = next((c for c in cars if c.key == key), None)
        await _autopark_refresh_panels(interaction.guild.id)
        label = car.label if car is not None else key
        await _safe_interaction_ephemeral(interaction, f"Ты занял(а) **{label}** на {AUTOPARK_RESERVE_MINUTES} мин.")


class AutoparkClaimView(SafeView):
    def __init__(self, cars: list[AutoparkCar]) -> None:
        super().__init__(timeout=120)
        self.add_item(AutoparkClaimSelect(cars))


class AutoparkReleaseSelect(discord.ui.Select):
    def __init__(self, cars: list[AutoparkCar], force: bool) -> None:
        self.force = force
        options = [discord.SelectOption(label=c.label[:100], value=c.key, description=c.note[:100] or c.key[:100]) for c in cars[:25]]
        super().__init__(placeholder="Выбери авто для освобождения...", min_values=1, max_values=1, options=options, custom_id="consume:autopark_release_select")

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        key = self.values[0]
        ok = _autopark_release(interaction.guild.id, key, interaction.user.id, force=self.force)
        if not ok:
            await _safe_interaction_ephemeral(interaction, "Не удалось освободить авто.")
            return
        cars = _autopark_load_cars(interaction.guild.id)
        car = next((c for c in cars if c.key == key), None)
        await _autopark_refresh_panels(interaction.guild.id)
        label = car.label if car is not None else key
        await _safe_interaction_ephemeral(interaction, f"✅ Ты освободил **{label}**. Бронь снята.")


class AutoparkReleaseView(SafeView):
    def __init__(self, cars: list[AutoparkCar], force: bool) -> None:
        super().__init__(timeout=120)
        self.add_item(AutoparkReleaseSelect(cars, force))


class AutoparkPanelView(SafeView):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(label="Занять авто", style=discord.ButtonStyle.success, custom_id="consume:autopark_take")
    async def take(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        cars = _autopark_load_cars(interaction.guild.id)
        free = [c for c in cars if c.reserved_by is None and _autopark_user_has_access(interaction.user, c)]
        if not free:
            await interaction.response.send_message("Нет доступных машин.", ephemeral=True)
            return
        await interaction.response.send_message("Выбери авто, которое хочешь занять:", ephemeral=True, view=AutoparkClaimView(free))

    @discord.ui.button(label="Освободить авто", style=discord.ButtonStyle.danger, custom_id="consume:autopark_release")
    async def release(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        cars = _autopark_load_cars(interaction.guild.id)
        manager = _autopark_user_can_manage(interaction.user)
        busy = [c for c in cars if c.reserved_by is not None and (manager or c.reserved_by == interaction.user.id)]
        if not busy:
            await interaction.response.send_message("У тебя нет активной брони.", ephemeral=True)
            return
        await interaction.response.send_message(
            "Выбери авто для освобождения:",
            ephemeral=True,
            view=AutoparkReleaseView(busy, force=manager),
        )

    @discord.ui.button(label="Изменить список", style=discord.ButtonStyle.secondary, emoji="✏️", custom_id="consume:autopark_edit")
    async def edit(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        if not _autopark_user_can_manage(interaction.user):
            await interaction.response.send_message("Редактировать список может только администрация.", ephemeral=True)
            return
        emb = discord.Embed(
            title="Редактирование списка (видно только тебе)",
            description=(
                "Добавляй, изменяй или удаляй позиции — все панели автопарка на сервере обновятся."
            ),
            color=discord.Color.dark_theme(),
        )
        await interaction.response.send_message(embed=emb, ephemeral=True, view=AutoparkEditorView())


async def autopark_expire_sweep_loop() -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            touched = _autopark_expire_overdue()
            for guild_id in touched:
                await _autopark_refresh_panels(guild_id)
        except Exception:
            logger.exception("autopark_expire_sweep_loop: unexpected error")
        await asyncio.sleep(30)


@bot.tree.command(name="panel", description="Опубликовать панель заявок в этом канале")
async def slash_panel(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message("Команда только на сервере.", ephemeral=True)
        return
    member = await _resolve_interaction_member(interaction)
    if member is None or not _member_may_post_panel(member):
        await interaction.response.send_message(
            "Нужно право **«Управлять сервером»** или роль **«Администратор»** на сервере.",
            ephemeral=True,
        )
        return
    if bot.user is None:
        await interaction.response.send_message("Бот ещё не готов.", ephemeral=True)
        return

    ch = interaction.channel
    if not isinstance(ch, discord.TextChannel):
        await interaction.response.send_message("Сюда нельзя отправить панель.", ephemeral=True)
        return

    emb = build_application_embed(bot.user)
    view = ApplicationPanelView()
    banner_raw = os.getenv("BANNER_IMAGE_PATH", "")
    banner_path = _resolve_banner_path(banner_raw)
    files: list[discord.File] = []
    if banner_path is not None:
        try:
            files.append(discord.File(banner_path, filename="banner.png"))
            # Превью (thumbnail) отображается вверху embed, а не внизу.
            emb.set_thumbnail(url="attachment://banner.png")
        except OSError as e:
            logger.warning("Баннер не прочитан (%s): %s", banner_path, e)

    await interaction.response.defer(ephemeral=True)
    try:
        if files:
            await ch.send(embed=emb, view=view, files=files)
        else:
            await ch.send(embed=emb, view=view)
    except discord.Forbidden:
        await interaction.followup.send(
            "У **бота** нет прав в этом канале: **отправка сообщений**, **встраивание ссылок**, "
            "**файлы** (если есть баннер).",
            ephemeral=True,
        )
        return
    except discord.HTTPException as e:
        logger.exception("panel: %s", e)
        await interaction.followup.send(f"Ошибка Discord: `{e.text[:400]}`", ephemeral=True)
        return
    await interaction.followup.send("Панель отправлена в канал.", ephemeral=True)


@slash_panel.error
async def slash_panel_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
    if isinstance(error, app_commands.CommandInvokeError) and error.original is not None:
        logger.exception("/panel: %s", error.original)
    else:
        logger.exception("/panel: %s", error)
    try:
        if interaction.response.is_done():
            await interaction.followup.send("Ошибка команды. См. консоль бота.", ephemeral=True)
        else:
            await interaction.response.send_message("Ошибка команды. См. консоль бота.", ephemeral=True)
    except discord.DiscordException:
        pass


@bot.tree.command(name="maps", description="Опубликовать карты VZP в этом канале")
async def slash_maps(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message("Команда только на сервере.", ephemeral=True)
        return
    ch = interaction.channel
    if not isinstance(ch, discord.TextChannel):
        await interaction.response.send_message("Сюда нельзя отправить сообщение.", ephemeral=True)
        return

    emb = build_maps_embed()
    view = VZPMapsView()
    await interaction.response.defer(ephemeral=True)
    try:
        await ch.send(embed=emb, view=view)
    except discord.Forbidden:
        await interaction.followup.send("У бота нет прав отправлять сообщения здесь.", ephemeral=True)
        return
    except discord.HTTPException as e:
        logger.exception("maps: %s", e)
        await interaction.followup.send(f"Ошибка Discord: `{e.text[:400]}`", ephemeral=True)
        return
    await interaction.followup.send("Карты отправлены в канал.", ephemeral=True)


@slash_maps.error
async def slash_maps_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
    if isinstance(error, app_commands.CommandInvokeError) and error.original is not None:
        logger.exception("/maps: %s", error.original)
    else:
        logger.exception("/maps: %s", error)
    try:
        if interaction.response.is_done():
            await interaction.followup.send("Ошибка команды. См. консоль бота.", ephemeral=True)
        else:
            await interaction.response.send_message("Ошибка команды. См. консоль бота.", ephemeral=True)
    except discord.DiscordException:
        pass


@bot.tree.command(name="sbor", description="Опубликовать сбор (основа/замена) в этом канале")
@app_commands.rename(
    kind="тип",
    participants="участников",
    reserves="замены",
    start_in="время",
)
@app_commands.choices(
    kind=[
        app_commands.Choice(name="ВЗХ", value="ВЗХ"),
        app_commands.Choice(name="МП", value="МП"),
        app_commands.Choice(name="Поставка", value="Поставка"),
    ]
)
@app_commands.describe(
    role="Роль для пинга",
    kind="Вид сбора",
    participants="Количество мест в основе",
    reserves="Количество мест на замене",
    start_in="Время начала: либо минуты (15), либо ЧЧ:ММ (19:30)",
)
async def slash_sbor(
    interaction: discord.Interaction,
    role: discord.Role,
    kind: app_commands.Choice[str],
    participants: app_commands.Range[int, 1, 100],
    reserves: app_commands.Range[int, 0, 100],
    start_in: str,
) -> None:
    if interaction.guild is None:
        await interaction.response.send_message("Команда только на сервере.", ephemeral=True)
        return
    member = await _resolve_interaction_member(interaction)
    if member is None or not await user_can_moderate(interaction):
        await interaction.response.send_message("Команда доступна только модераторам.", ephemeral=True)
        return
    ch = interaction.channel
    if not isinstance(ch, discord.TextChannel):
        await interaction.response.send_message("Сюда нельзя отправить сбор.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    # сначала пробуем трактовать как количество минут
    start_in = start_in.strip()
    start_local: datetime
    now_local = datetime.now(SBOR_TZ)
    if re.fullmatch(r"\d{1,4}", start_in):
        # минуты до начала
        minutes = int(start_in)
        if minutes <= 0:
            await interaction.followup.send("Количество минут должно быть больше 0.", ephemeral=True)
            return
        start_local = now_local + timedelta(minutes=minutes)
    else:
        # парсим время ЧЧ:ММ в МСК
        m = re.fullmatch(r"(\d{1,2}):(\d{2})", start_in)
        if not m:
            await interaction.followup.send(
                "Неверный формат времени. Укажите либо минуты (например, `15`), либо ЧЧ:ММ (например, `19:30`).",
                ephemeral=True,
            )
            return
        hour = int(m.group(1))
        minute = int(m.group(2))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            await interaction.followup.send("Неверное время. Часы 0–23, минуты 0–59.", ephemeral=True)
            return
        start_local = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if start_local <= now_local:
            # если время уже прошло сегодня по МСК — берем завтра
            start_local = start_local + timedelta(days=1)
    start_at = start_local.astimezone(timezone.utc)
    view = SborPublicView()
    try:
        msg = await ch.send(
            content=role.mention,
            embed=discord.Embed(title="Сбор участников", color=discord.Color.dark_theme()),
            view=view,
            allowed_mentions=discord.AllowedMentions(everyone=False, roles=[role]),
        )
    except discord.Forbidden:
        await interaction.followup.send("У бота нет прав отправлять сообщения в этом канале.", ephemeral=True)
        return
    except discord.HTTPException as e:
        await interaction.followup.send(f"Ошибка Discord: `{e}`", ephemeral=True)
        return

    state = SborState(
        guild_id=interaction.guild.id,
        role_id=role.id,
        message_id=msg.id,
        channel_id=ch.id,
        kind=kind.value,
        max_main=int(participants),
        max_reserve=int(reserves),
        start_at=start_at,
    )
    async with _sbor_lock:
        _sbor_sessions[msg.id] = state
    await _sbor_refresh_message(state)
    _sbor_start_countdown(msg.id)
    if kind.value == "ВЗХ":
        asyncio.create_task(
            _sbor_notify_role_dm(
                guild=interaction.guild,
                role=role,
                channel=ch,
                kind=kind.value,
                start_at=start_at,
                jump_url=msg.jump_url,
            )
        )
    await interaction.followup.send(f"Сбор опубликован в {ch.mention}.", ephemeral=True)


@bot.tree.command(name="autopark", description="Опубликовать панель автопарка")
async def slash_autopark(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message("Команда только на сервере.", ephemeral=True)
        return
    member = await _resolve_interaction_member(interaction)
    if member is None or not _autopark_user_can_manage(member):
        await interaction.response.send_message(
            "Нет прав. Нужны **Управлять сервером** или роль из **AUTOPARK_MANAGER_ROLE_IDS**.",
            ephemeral=True,
        )
        return
    ch = interaction.channel
    if not isinstance(ch, discord.TextChannel):
        await interaction.response.send_message("Сюда нельзя отправить панель.", ephemeral=True)
        return
    emb = _autopark_embed(interaction.guild)
    msg = await ch.send(embed=emb, view=AutoparkPanelView())
    _autopark_register_panel(interaction.guild.id, ch.id, msg.id)
    await interaction.response.send_message("Панель автопарка опубликована.", ephemeral=True)


@bot.tree.command(
    name="kontrakt",
    description="Панель контрактов: правила и кнопка «Предложить»",
)
async def kontrakt(interaction: discord.Interaction) -> None:
    if interaction.guild is None or interaction.channel is None:
        await interaction.response.send_message("Только на сервере.", ephemeral=True)
        return
    if not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message("Команда доступна только в текстовом канале.", ephemeral=True)
        return
    if not kontrakt_allowed_in_channel(interaction):
        await interaction.response.send_message(kontrakt_channel_restriction_message(), ephemeral=True)
        return
    if not user_can_post_kontrakt_panel(interaction):
        await interaction.response.send_message(
            "Нет прав. Нужны **Управлять сервером** или роль из **KONTRAKT_POST_ROLE_IDS** "
            "(если пусто — берутся MODERATOR_ROLE_ID).",
            ephemeral=True,
        )
        return
    embed = build_kontrakt_panel_embed()
    view = KontraktPanelView()
    await interaction.channel.send(embed=embed, view=view)
    await interaction.response.send_message("Панель контрактов опубликована.", ephemeral=True)


@bot.tree.command(
    name="spam",
    description="Рассылка в ЛС всем участникам с выбранной ролью (кроме ботов)",
)
@app_commands.describe(
    role="Роль получателей",
    text="Текст сообщения (в ЛС: первая строка как > # …, остальные как > …)",
)
async def slash_spam(interaction: discord.Interaction, role: discord.Role, text: str) -> None:
    if interaction.guild is None:
        await interaction.response.send_message("Только на сервере.", ephemeral=True)
        return
    guild = interaction.guild
    member = await _resolve_interaction_member(interaction)
    if member is None or not _member_may_spam(member):
        await interaction.response.send_message(
            "Нет прав на рассылку. Нужны **Администратор** или **Управлять сервером**, "
            "или роль из **SPAM_COMMAND_ROLE_IDS** в `.env`.",
            ephemeral=True,
        )
        return

    if role.guild.id != guild.id:
        await interaction.response.send_message("Роль должна быть с этого сервера.", ephemeral=True)
        return
    if role.is_default():
        await interaction.response.send_message("Нельзя выбрать роль **@everyone**.", ephemeral=True)
        return
    if member.id != guild.owner_id and not member.guild_permissions.administrator:
        if role >= member.top_role:
            await interaction.response.send_message(
                "Нельзя рассылать по роли **выше или равной** вашей высшей роли.",
                ephemeral=True,
            )
            return

    if not text or not text.strip():
        await interaction.response.send_message("Текст не может быть пустым.", ephemeral=True)
        return

    formatted = _format_spam_dm(text)
    await interaction.response.defer(ephemeral=True)
    ok, fail = await _broadcast_dm_spam(guild, role, formatted)
    await interaction.followup.send(
        f"Рассылка по роли {role.mention}: **{ok}** доставлено, **{fail}** не удалось (ЛС закрыты и т.п.).",
        ephemeral=True,
    )


@slash_spam.error
async def slash_spam_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
    if isinstance(error, app_commands.CommandInvokeError) and error.original is not None:
        logger.exception("/spam: %s", error.original)
    else:
        logger.exception("/spam: %s", error)
    try:
        if interaction.response.is_done():
            await interaction.followup.send("Ошибка команды. См. консоль бота.", ephemeral=True)
        else:
            await interaction.response.send_message("Ошибка команды. См. консоль бота.", ephemeral=True)
    except discord.DiscordException:
        pass


@bot.event
async def on_ready() -> None:
    global _daily_role_ping_task, _autopark_task
    if _daily_role_ping_task is None or _daily_role_ping_task.done():
        _daily_role_ping_task = asyncio.create_task(daily_role_ping_loop())
    if _autopark_task is None or _autopark_task.done():
        _autopark_task = asyncio.create_task(autopark_expire_sweep_loop())
    logger.info("Бот запущен: %s (%s)", bot.user, bot.user.id if bot.user else None)


def main() -> None:
    _configure_json_logging()
    _start_fly_health_server_if_needed()
    token = (os.getenv("DISCORD_TOKEN") or "").strip().strip('"').strip("'")
    if not token:
        raise SystemExit("Задайте DISCORD_TOKEN в .env или окружении.")
    bot.run(token)


if __name__ == "__main__":
    main()
