#!/usr/bin/env python3
"""
uploader.py - Sincroniza carpetas de series (Unraid) con un canal de Telegram
con temas (un tema por serie).

Arquitectura optimizada con cursor persistente (top_message):
  * Almacena en /data/topic_cache.json el estado analizado y el último top_message.
  * Si el tema remoto no tiene mensajes nuevos, se omite GetReplies por completo.
  * Reduce drásticamente las llamadas a la API de Telegram de O(N) a O(modificados).
  * Control previo de cuota: no crea temas si el archivo no cabe en la cuota restante.
  * Mensajes e informes con formato estético limpio y barras adaptadas a móvil.
"""
from __future__ import annotations

import asyncio
import hashlib
import html
import inspect
import json
import logging
import math
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import unicodedata
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from hydrogram import Client, raw
from hydrogram.enums import ChatMemberStatus, ParseMode
from hydrogram.errors import FloodWait
from hydrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaDocument,
    Message,
)

log = logging.getLogger("uploader")

EXIT_CONFIG_ERROR = 2

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

class ConfigError(Exception):
    pass

class RemoteStateError(Exception):
    """No se pudo leer el estado remoto de Telegram de forma fiable."""

@dataclass(frozen=True)
class Config:
    api_id: int
    api_hash: str
    group_id: int
    notify_channel_id: int
    notify_thread_id: int | None
    bot_token: str | None
    tmdb_api_key: str | None
    tmdb_language: str
    compress_files: bool
    archive_password: str
    daily_quota_gb: float
    split_threshold_gb: float | None
    archive_volume_size: str
    host_media_path: str
    media_dir: Path
    data_dir: Path
    temp_dir: Path
    min_file_age_minutes: float
    allowed_user_ids: frozenset[int]
    run_at: str | None
    run_interval_hours: float
    max_consecutive_failures: int
    tz: ZoneInfo

    @property
    def daily_quota_bytes(self) -> int:
        return int(self.daily_quota_gb * (1024 ** 3))

def load_config(env=None) -> Config:
    env = os.environ if env is None else env
    errors: list[str] = []

    def raw_val(key: str) -> str:
        return (env.get(key) or "").strip()

    def req_str(key: str) -> str:
        v = raw_val(key)
        if not v:
            errors.append(f"Falta la variable obligatoria {key}")
        return v

    def opt_int(key: str, default: int | None = None) -> int | None:
        v = raw_val(key)
        if not v:
            return default
        try:
            return int(v)
        except ValueError:
            errors.append(f"{key} debe ser un entero (valor actual: {v!r})")
            return default

    def opt_float(key: str, default: float | None) -> float | None:
        v = raw_val(key)
        if not v:
            return default
        try:
            return float(v)
        except ValueError:
            errors.append(f"{key} debe ser un número (valor actual: {v!r})")
            return default

    def opt_bool(key: str, default: bool) -> bool:
        v = raw_val(key)
        if not v:
            return default
        normalized = v.lower()
        if normalized in ("true", "1", "yes", "on", "t"):
            return True
        if normalized in ("false", "0", "no", "off", "f"):
            return False
        errors.append(f"{key} debe ser un booleano (true/false, 1/0, yes/no u on/off)")
        return default

    def finite_float(key: str, value: float | None, *, minimum: float | None = None,
                     allow_zero: bool = True) -> float | None:
        if value is None:
            return None
        if not math.isfinite(value):
            errors.append(f"{key} debe ser un número finito")
            return None
        if minimum is not None and (value < minimum or (not allow_zero and value == minimum)):
            comparator = "mayor que" if not allow_zero else "igual o mayor que"
            errors.append(f"{key} debe ser {comparator} {minimum}")
            return None
        return value

    api_id = opt_int("TELEGRAM_API_ID")
    if api_id is None:
        errors.append("Falta la variable obligatoria TELEGRAM_API_ID")
    api_hash = req_str("TELEGRAM_API_HASH")
    group_id = opt_int("TELEGRAM_GROUP_ID")
    if group_id is None:
        errors.append("Falta la variable obligatoria TELEGRAM_GROUP_ID")

    notify_channel = opt_int("TELEGRAM_NOTIFY_CHANNEL_ID", group_id)
    thread_id = opt_int("TELEGRAM_NOTIFY_THREAD_ID")
    if thread_id is not None and thread_id <= 0:
        thread_id = None

    allowed: set[int] = set()
    for chunk in raw_val("ALLOWED_USER_IDS").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            allowed.add(int(chunk))
        except ValueError:
            errors.append(f"ALLOWED_USER_IDS contiene un valor no numérico: {chunk!r}")

    run_at = raw_val("RUN_AT") or None
    if run_at:
        m = re.fullmatch(r"(\d{1,2}):(\d{2})", run_at)
        if not m or int(m.group(1)) > 23 or int(m.group(2)) > 59:
            errors.append(f"RUN_AT debe tener formato HH:MM (valor actual: {run_at!r})")
            run_at = None

    tz_str = raw_val("TZ") or "Europe/Madrid"
    try:
        tz = ZoneInfo(tz_str)
    except Exception:
        errors.append(f"Zona horaria TZ desconocida o inválida: {tz_str!r}")
        tz = ZoneInfo("UTC")

    split_gb = finite_float(
        "SPLIT_THRESHOLD_GB", opt_float("SPLIT_THRESHOLD_GB", None),
        minimum=0.0, allow_zero=False
    )

    daily_quota_gb = opt_float("DAILY_QUOTA_GB", 100.0)
    if daily_quota_gb is None:
        daily_quota_gb = 100.0
    daily_quota_gb = finite_float(
        "DAILY_QUOTA_GB", daily_quota_gb, minimum=0.0, allow_zero=True
    )
    if daily_quota_gb is None:
        daily_quota_gb = 100.0

    run_interval = opt_float("RUN_INTERVAL_HOURS", 24.0)
    if run_interval is None:
        run_interval = 24.0
    run_interval = finite_float(
        "RUN_INTERVAL_HOURS", run_interval, minimum=0.0, allow_zero=False
    )
    if run_interval is None:
        run_interval = 24.0

    min_file_age = opt_float("MIN_FILE_AGE_MINUTES", 10.0)
    if min_file_age is None:
        min_file_age = 10.0
    min_file_age = finite_float(
        "MIN_FILE_AGE_MINUTES", min_file_age, minimum=0.0, allow_zero=True
    )
    if min_file_age is None:
        min_file_age = 10.0

    max_failures = opt_int("MAX_CONSECUTIVE_FAILURES", 10)
    if max_failures is None:
        max_failures = 10
    if max_failures < 1:
        errors.append("MAX_CONSECUTIVE_FAILURES debe ser un entero mayor o igual que 1")
        max_failures = 10

    volume_size = raw_val("ARCHIVE_VOLUME_SIZE") or "1850m"
    volume_match = re.fullmatch(r"(?i)(\d+)(?:b|k|m|g)?", volume_size)
    if not volume_match or int(volume_match.group(1)) <= 0:
        errors.append(
            "ARCHIVE_VOLUME_SIZE debe ser un tamaño 7z positivo (ejemplos: 1850m, 2g, 500m)"
        )

    compress_files = opt_bool("COMPRESS_FILES", True)

    if errors:
        raise ConfigError("\n".join(f"  - {e}" for e in errors))

    data_dir = Path(raw_val("DATA_DIR") or "/data")
    return Config(
        api_id=api_id,
        api_hash=api_hash,
        group_id=group_id,
        notify_channel_id=notify_channel,
        notify_thread_id=thread_id,
        bot_token=raw_val("TELEGRAM_BOT_TOKEN") or None,
        tmdb_api_key=raw_val("TMDB_API_KEY") or None,
        tmdb_language=raw_val("TMDB_LANGUAGE") or "es-ES",
        compress_files=compress_files,
        archive_password=raw_val("ARCHIVE_PASSWORD"),
        daily_quota_gb=daily_quota_gb,
        split_threshold_gb=split_gb,
        archive_volume_size=volume_size,
        host_media_path=raw_val("HOST_MEDIA_PATH") or "/mnt/user/data/media/tv/",
        media_dir=Path(raw_val("MEDIA_DIR") or "/media"),
        data_dir=data_dir,
        temp_dir=Path(raw_val("TEMP_DIR") or "/tmp/splits"),
        min_file_age_minutes=min_file_age,
        allowed_user_ids=frozenset(allowed),
        run_at=run_at,
        run_interval_hours=run_interval,
        max_consecutive_failures=max_failures,
        tz=tz,
    )

@dataclass(frozen=True)
class Limits:
    premium: bool
    threshold_bytes: int
    volume_size: str

def compute_limits(premium: bool, threshold_gb_override: float | None, volume_size: str) -> Limits:
    safe_threshold_gb = 3.70 if premium else 1.80
    max_upload_bytes = 4_000_000_000 if premium else 2_000_000_000
    threshold_gb = threshold_gb_override if threshold_gb_override is not None else safe_threshold_gb

    if threshold_gb > safe_threshold_gb:
        log.warning(
            "SPLIT_THRESHOLD_GB=%.2f supera el umbral seguro para una cuenta %s. "
            "Se limita a %.2f GiB para evitar rechazos de Telegram.",
            threshold_gb, "Premium" if premium else "estándar", safe_threshold_gb
        )
        threshold_gb = safe_threshold_gb

    unit_factors = {"b": 1, "k": 1024, "m": 1024**2, "g": 1024**3}
    m = re.fullmatch(r"(?i)(\d+)([bkmg]?)", volume_size.strip())
    volume_bytes = None
    if m:
        unit = m.group(2).lower()
        volume_bytes = int(m.group(1)) * unit_factors.get(unit, 1)

    safe_volume_bytes = max_upload_bytes - 32 * 1024 * 1024
    if volume_bytes is None or volume_bytes > safe_volume_bytes:
        fallback = "3700m" if premium else "1850m"
        log.warning(
            "ARCHIVE_VOLUME_SIZE=%s es inválido o demasiado grande para Telegram. "
            "Se utilizará %s.",
            volume_size, fallback
        )
        volume_size = fallback

    return Limits(premium, int(threshold_gb * (1024 ** 3)), volume_size)

CFG: Config
LIMITS: Limits
QUOTA: DailyQuota
ABORT_CURRENT_RUN = False
SHUTDOWN_REQUESTED = False
MAIN_LOOP: asyncio.AbstractEventLoop | None = None

VIDEO_EXTS = {
    ".mkv", ".mp4", ".m4v", ".avi", ".mov",
    ".ogm", ".ogv", ".webm", ".flv", ".wmv", ".asf",
    ".ts", ".mts", ".m2ts",
    ".mpg", ".mpeg", ".m2v", ".vob", ".evo",
    ".3gp", ".3g2"
}
SUB_EXTS = {".srt", ".ass", ".ssa", ".sub", ".idx", ".vtt", ".sup"}
VALID_EXTS = VIDEO_EXTS | SUB_EXTS

TS_LIKE_EXTS = {".ts", ".mts", ".m2ts"}
MIN_TS_BYTES = 20 * 1024 * 1024
TOPIC_TITLE_MAX = 120

ORPHAN_PREFIXES = ("⏳ Iniciando subida", "📤 Subiendo archivo")
HEADER_MARKERS = ("the movie database", "themoviedb.org")

MAX_UPLOAD_RETRIES = 3
MAX_FLOODWAITS_PER_FILE = 10
MAX_POSTER_BYTES = 5 * 1024 * 1024

def flood_seconds(e: FloodWait) -> int:
    val = getattr(e, "value", None)
    if val is None:
        val = getattr(e, "x", 5)
    try:
        sec = int(val)
        return max(sec, 5)
    except (TypeError, ValueError):
        return 5

async def interruptible_sleep(seconds: float) -> bool:
    if SHUTDOWN_REQUESTED or ABORT_CURRENT_RUN:
        return False
    if seconds <= 0:
        return not (SHUTDOWN_REQUESTED or ABORT_CURRENT_RUN)
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if SHUTDOWN_REQUESTED or ABORT_CURRENT_RUN:
            return False
        step = min(1.0, end - time.monotonic())
        await asyncio.sleep(step)
    return True

async def shutdown_only_sleep(seconds: float) -> bool:
    if SHUTDOWN_REQUESTED:
        return False
    end = time.monotonic() + max(0.0, seconds)
    while time.monotonic() < end:
        if SHUTDOWN_REQUESTED:
            return False
        await asyncio.sleep(min(1.0, end - time.monotonic()))
    return not SHUTDOWN_REQUESTED

# ==============================================================================
# DETECCIÓN DE EPISODIOS Y CLAVES CANÓNICAS
# ==============================================================================

SEASON_EP_REGEX = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,3})")
ALT_SEASON_EP_REGEX = re.compile(r"\b(\d{1,2})x(\d{2,3})\b", re.IGNORECASE)
DATE_EP_REGEX = re.compile(r"\b(\d{4})[-._](\d{2})[-._](\d{2})\b")
ABSOLUTE_EP_REGEX = re.compile(
    r"(?:\b(?:episode|ep|e)\s*|\s-\s(?!(?:19|20)\d{2}\b)(?!(?:480|576|720|1080|2160)\b))(\d{1,3})\b",
    re.IGNORECASE,
)

PART_CAPTION_RE = re.compile(
    r"^(?P<base>.*?)\s*\(Parte (?P<i>\d+) de (?P<n>\d+)\)\s*$", re.DOTALL | re.IGNORECASE
)
VOLUME_SUFFIX_RE = re.compile(r"\.7z(?:\.(\d{2,4}))?$", re.IGNORECASE)
ARCHIVE_SUFFIX_RE = re.compile(r"\.7z(?:\.\d{2,4})?$", re.IGNORECASE)
SAMPLE_STEM_RE = re.compile(r"^(?:sample|trailer)$|(?:^|[-._ ])(?:sample|trailer)$", re.IGNORECASE)
SUB_CLEAN_RE = re.compile(
    r"(\.(?:[a-z]{2,3}(?:-[a-z]{2,3})?|forced|completo|default))*\.(?:srt|ass|ssa|sub|idx|vtt|sup)$",
    re.IGNORECASE,
)

def get_file_clean_base(path: Path | str) -> str:
    name = Path(path).name
    name = ARCHIVE_SUFFIX_RE.sub("", name)
    cleaned = SUB_CLEAN_RE.sub("", name)
    if cleaned != name:
        suffix = Path(cleaned).suffix.lower()
        if suffix in VIDEO_EXTS:
            return Path(cleaned).stem
        return cleaned
    return Path(name).stem

def extract_season_episode(name: str) -> str | None:
    m = SEASON_EP_REGEX.search(name)
    if m:
        return f"S{int(m.group(1)):02d}E{int(m.group(2)):02d}"
    m = ALT_SEASON_EP_REGEX.search(name)
    if m:
        return f"S{int(m.group(1)):02d}E{int(m.group(2)):02d}"
    m = DATE_EP_REGEX.search(name)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = ABSOLUTE_EP_REGEX.search(name)
    if m:
        return f"E{int(m.group(1)):03d}"
    return None

def extract_ep_sort_key(p: Path) -> tuple:
    stem = p.stem
    is_sub = 1 if p.suffix.lower() in SUB_EXTS else 0

    m = SEASON_EP_REGEX.search(stem) or ALT_SEASON_EP_REGEX.search(stem)
    if m:
        return (0, int(m.group(1)), int(m.group(2)), is_sub, p.name.lower())

    m = DATE_EP_REGEX.search(stem)
    if m:
        return (1, int(m.group(1)), int(m.group(2)), int(m.group(3)), is_sub, p.name.lower())

    m = ABSOLUTE_EP_REGEX.search(stem)
    if m:
        return (2, int(m.group(1)), is_sub, p.name.lower())

    return (3, 0, is_sub, p.name.lower())

def norm(text: str) -> str:
    return unicodedata.normalize("NFC", text).strip().casefold()

@dataclass
class DocRecord:
    msg_id: int
    filename: str
    caption: str

@dataclass
class TopicInfo:
    complete_keys: set[str] = field(default_factory=set)
    video_episode_codes: set[str] = field(default_factory=set)
    partials: dict[str, list[int]] = field(default_factory=dict)
    has_header: bool = False
    top_message: int = 0

    def to_dict(self) -> dict:
        return {
            "complete_keys": list(self.complete_keys),
            "video_episode_codes": list(self.video_episode_codes),
            "partials": self.partials,
            "has_header": self.has_header,
            "top_message": self.top_message,
        }

    @classmethod
    def from_dict(cls, data: dict) -> TopicInfo:
        return cls(
            complete_keys=set(data.get("complete_keys", [])),
            video_episode_codes=set(data.get("video_episode_codes", [])),
            partials=data.get("partials", {}),
            has_header=data.get("has_header", False),
            top_message=data.get("top_message", 0),
        )

# ==============================================================================
# GESTIÓN PERSISTENTE DE LA CACHÉ DE TEMAS (CURSOR top_message)
# ==============================================================================

class TopicCacheStore:
    def __init__(self, path: Path):
        self.path = path
        self.cache: dict[int, TopicInfo] = {}
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw_content = self.path.read_text(encoding="utf-8").strip()
            if not raw_content:
                return
            data = json.loads(raw_content)
            for tid_str, topic_data in data.items():
                self.cache[int(tid_str)] = TopicInfo.from_dict(topic_data)
        except Exception as e:
            log.warning("No se pudo cargar topic_cache.json (%s). Se reconstruirá en memoria.", e)

    def save(self) -> None:
        if not self._dirty:
            return
        try:
            payload = {str(tid): info.to_dict() for tid, info in self.cache.items()}
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload), encoding="utf-8")
            os.replace(tmp, self.path)
            self._dirty = False
        except Exception as e:
            log.warning("No se pudo guardar topic_cache.json: %s", e)

    def get(self, topic_id: int) -> TopicInfo | None:
        return self.cache.get(topic_id)

    def update(self, topic_id: int, info: TopicInfo, flush: bool = False) -> None:
        self.cache[topic_id] = info
        self._dirty = True
        if flush:
            self.save()

    def cleanup_untracked(self, active_topic_ids: set[int]) -> None:
        initial_len = len(self.cache)
        self.cache = {tid: info for tid, info in self.cache.items() if tid in active_topic_ids}
        if len(self.cache) != initial_len:
            self._dirty = True
            self.save()
            log.info("topic_cache.json limpiado: %d temas obsoletos eliminados.", initial_len - len(self.cache))

TOPIC_STORE: TopicCacheStore

def analyze_documents(records: list[DocRecord]) -> TopicInfo:
    groups: dict[str, dict] = {}
    volume_bases = {
        norm(get_file_clean_base(rec.filename or ""))
        for rec in records
        if VOLUME_SUFFIX_RE.search(rec.filename or "")
    }
    for r in records:
        caption = (r.caption or "").strip()
        fname = (r.filename or "").strip()
        vol = VOLUME_SUFFIX_RE.search(fname)
        idx = 1

        is_sub = Path(fname).suffix.lower() in SUB_EXTS
        category = "sub" if is_sub else "vid"

        if is_sub:
            base = Path(fname).name
            idx, total = 1, 1
            has_explicit_total = True
        else:
            m = PART_CAPTION_RE.match(caption) if caption else None
            if m:
                base, idx, total = m.group("base").strip(), int(m.group("i")), int(m.group("n"))
                has_explicit_total = True
            elif caption:
                base = caption
                vol_num = vol.group(1) if vol else None
                idx, total = (int(vol_num), None) if vol_num else (1, 1)
                has_explicit_total = vol_num is None
            else:
                base = get_file_clean_base(fname)
                vol_num = vol.group(1) if vol else None
                idx, total = (int(vol_num), None) if vol_num else (1, 1)
                has_explicit_total = vol_num is None

        if not base:
            continue

        c_key = f"{norm(base)}|{category}"
        g = groups.setdefault(
            c_key,
            {
                "parts": set(),
                "total": None,
                "ids": [],
                "category": category,
                "base": base,
                "has_explicit_total": has_explicit_total,
            },
        )
        g["parts"].add(idx)
        g["ids"].append(r.msg_id)
        if total is not None:
            g["total"] = max(g["total"] or 0, total)
        if has_explicit_total and total is not None:
            g["has_explicit_total"] = True

    info = TopicInfo()
    for group_key, g in groups.items():
        parts: set[int] = g["parts"]
        total = g["total"] if g["total"] is not None else (max(parts) if parts else 0)

        looks_like_volume = norm(g["base"]) in volume_bases
        if looks_like_volume and not g["has_explicit_total"]:
            complete = False
        else:
            complete = bool(parts) and parts >= set(range(1, total + 1))

        if complete:
            info.complete_keys.add(group_key)
            if g["category"] == "vid":
                code = extract_season_episode(g["base"])
                if code:
                    info.video_episode_codes.add(code)
        else:
            info.partials[group_key] = list(g["ids"])
    return info

def is_uploaded(file_path: Path, info: TopicInfo) -> bool:
    is_sub = file_path.suffix.lower() in SUB_EXTS
    clean_base = file_path.name if is_sub else get_file_clean_base(file_path)
    target_key = f"{norm(clean_base)}|{'sub' if is_sub else 'vid'}"

    if target_key in info.complete_keys:
        return True
    if target_key in info.partials:
        return False

    if is_sub:
        return False

    code = extract_season_episode(file_path.stem)
    return bool(code and code in info.video_episode_codes)

def truncate_utf8(text: str, max_bytes: int) -> str:
    return text.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore").strip()

def make_topic_title(folder_name: str) -> str:
    return truncate_utf8(folder_name, TOPIC_TITLE_MAX)

@dataclass(frozen=True)
class Series:
    folder: Path
    title: str

def assign_titles(folders: list[Path]) -> list[Series]:
    by_title: dict[str, list[Path]] = {}
    for d in folders:
        title = make_topic_title(d.name)
        if title:
            by_title.setdefault(title, []).append(d)
    out: list[Series] = []
    for title, group in by_title.items():
        for i, d in enumerate(sorted(group, key=lambda p: p.name)):
            if i == 0:
                out.append(Series(d, title))
            else:
                h = hashlib.sha1(d.name.encode()).hexdigest()[:6]
                suffix = f" #{h}"
                new_title = f"{truncate_utf8(title, TOPIC_TITLE_MAX - len(suffix.encode('utf-8')))}{suffix}"
                log.warning("Colisión de título de tema: '%s' -> '%s'", d.name, new_title)
                out.append(Series(d, new_title))
    out.sort(key=lambda s: s.folder.name.lower())
    return out

def is_sample(rel_parts: tuple[str, ...]) -> bool:
    *dirs, name = rel_parts
    if any(d.lower() in ("sample", "samples") for d in dirs):
        return True
    return bool(SAMPLE_STEM_RE.search(Path(name).stem))

@dataclass
class ScanResult:
    files: list[Path]
    sizes: dict[Path, int]
    mtimes: dict[Path, float]

def scan_series(folder: Path) -> ScanResult:
    files: list[Path] = []
    sizes: dict[Path, int] = {}
    mtimes: dict[Path, float] = {}
    for p in folder.rglob("*"):
        try:
            if not p.is_file():
                continue
            rel = p.relative_to(folder).parts
            if any(part in ("@eaDir", "metadata", "@tmp") for part in rel) or p.name.startswith("._"):
                continue
            ext = p.suffix.lower()
            if ext not in VALID_EXTS or is_sample(rel):
                continue
            st = p.stat()
            if ext in TS_LIKE_EXTS and st.st_size < MIN_TS_BYTES:
                continue
            files.append(p)
            sizes[p] = st.st_size
            mtimes[p] = st.st_mtime
        except OSError as e:
            log.warning("No se pudo inspeccionar %s: %s", p, e)

    files.sort(key=extract_ep_sort_key)
    return ScanResult(files, sizes, mtimes)

def next_run_delay(run_at: str | None, interval_hours: float, tz: ZoneInfo, now: datetime | None = None) -> float:
    now = now or datetime.now(tz)
    if run_at:
        hh, mm = (int(x) for x in run_at.split(":"))
        target = datetime(now.year, now.month, now.day, hh, mm, 0, tzinfo=tz)
        if target <= now:
            tomorrow = now.date() + timedelta(days=1)
            target = datetime(tomorrow.year, tomorrow.month, tomorrow.day, hh, mm, 0, tzinfo=tz)
        return (target - now).total_seconds()
    return interval_hours * 3600

# ==============================================================================
# CUOTA DIARIA
# ==============================================================================

class DailyQuota:
    def __init__(self, path: Path, limit_bytes: int, tz: ZoneInfo):
        self.path = path
        self.bak_path = path.with_suffix(".json.bak")
        self.limit = limit_bytes
        self.tz = tz
        self._date = datetime.now(self.tz).strftime("%Y-%m-%d")
        self._bytes = 0
        self._load()

    def _today_str(self) -> str:
        return datetime.now(self.tz).strftime("%Y-%m-%d")

    def _load(self) -> None:
        today = self._today_str()
        for p in (self.path, self.bak_path):
            if not p.exists():
                continue
            try:
                raw_content = p.read_text(encoding="utf-8").strip()
                if not raw_content:
                    continue
                data = json.loads(raw_content)
                if not isinstance(data, dict):
                    raise ValueError("el estado no es un objeto JSON")
                stored_date = data.get("date")
                try:
                    parsed_date = datetime.strptime(stored_date, "%Y-%m-%d").date()
                except (TypeError, ValueError) as err:
                    raise ValueError("fecha de cuota inválida") from err
                current_date = datetime.now(self.tz).date()
                if parsed_date > current_date:
                    raise ValueError(
                        "la fecha guardada está en el futuro; posible retroceso del reloj"
                    )
                if stored_date == today:
                    stored_bytes = int(data.get("bytes", 0))
                    if stored_bytes < 0:
                        raise ValueError("el contador de bytes no puede ser negativo")
                    self._bytes = stored_bytes
                else:
                    self._bytes = 0
                self._date = today
                return
            except json.JSONDecodeError as err:
                log.error("Fichero de cuota corrupto (%s): %s", p, err)
            except Exception as e:
                log.warning("No se pudo leer el estado de cuota desde %s: %s", p, e)

        if self.path.exists() and self.path.stat().st_size > 0:
            raise RuntimeError(
                f"El fichero de cuota {self.path} existe pero está corrupto y no hay respaldo válido. "
                "Para evitar sobrepasar los límites de subida de Telegram, se detiene la ejecución. "
                "Revisa o elimina el archivo manualmente si deseas restablecer a 0."
            )

        self._date = today
        self._bytes = 0

    def _roll(self) -> None:
        today = self._today_str()
        if today != self._date:
            self._date, self._bytes = today, 0

    @property
    def used(self) -> int:
        self._roll()
        return self._bytes

    @property
    def exhausted(self) -> bool:
        return self.limit > 0 and self.used >= self.limit

    def add(self, n: int) -> None:
        self._roll()
        self._bytes += n
        try:
            payload = json.dumps({"date": self._date, "bytes": self._bytes})
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(payload, encoding="utf-8")
            if self.path.exists():
                shutil.copyfile(self.path, self.bak_path)
            os.replace(tmp, self.path)
        except Exception as e:
            log.warning("No se pudo guardar el estado de cuota: %s", e)

def gb(n: float) -> float:
    return n / (1024 ** 3)

# ==============================================================================
# FICHEROS DE ESTADO: LIBERABLES
# ==============================================================================

def state_file() -> Path:
    return CFG.data_dir / "liberables.txt"

def header_liberables() -> str:
    path_str = CFG.host_media_path.replace("\n", " ").strip()
    return f"Todos los archivos liberables se encuentran en la ruta {path_str}\n" + ("=" * 80) + "\n"

def header_pendientes() -> str:
    path_str = CFG.host_media_path.replace("\n", " ").strip()
    return f"Todos los archivos pendientes de subir se encuentran en la ruta {path_str}\n" + ("=" * 80) + "\n"

def parse_liberables_entries() -> dict[str, float]:
    entries: dict[str, float] = {}
    path = state_file()
    if not path.exists():
        return entries
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("Todos los archivos") or line.startswith("="):
                continue
            body = line[2:].strip() if line.startswith("- ") else line
            if " [" in body and body.endswith(" GB]"):
                name, size = body.rsplit(" [", 1)
                try:
                    value = float(size.replace(" GB]", "").strip())
                except ValueError:
                    value = 0.0
                entries[name.strip()] = value
    return entries

def write_liberables_file(entries: dict[str, float]) -> None:
    path = state_file()
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(header_liberables())
        for folder in sorted(entries, key=str.lower):
            f.write(f"- {folder} [{entries[folder]:.2f} GB]\n")
    os.replace(tmp, path)

def append_to_liberables(folder_name: str, total_gb: float) -> None:
    entries = parse_liberables_entries()
    entries[folder_name] = total_gb
    write_liberables_file(entries)

def remove_from_liberables(folder_name: str) -> None:
    entries = parse_liberables_entries()
    if folder_name in entries:
        del entries[folder_name]
        write_liberables_file(entries)

# ==============================================================================
# LLAMADAS A TELEGRAM
# ==============================================================================

async def tg_call(factory, what: str, attempts: int = 4, max_floodwaits: int = 20):
    delay, failures, floods = 5, 0, 0
    while True:
        if SHUTDOWN_REQUESTED or ABORT_CURRENT_RUN:
            raise RemoteStateError(f"{what}: cancelado por parada solicitada")
        try:
            return await factory()
        except FloodWait as e:
            floods += 1
            if floods > max_floodwaits:
                raise RemoteStateError(f"{what}: demasiados FloodWait") from e
            wait_time = flood_seconds(e) + 2
            log.warning("%s: FloodWait de %ds...", what, wait_time)
            if not await interruptible_sleep(wait_time):
                raise RemoteStateError(f"{what}: interrumpido durante FloodWait")
        except Exception as e:
            failures += 1
            if failures >= attempts:
                raise RemoteStateError(f"{what}: {e}") from e
            log.warning("%s falló (%s). Reintento %d/%d en %ds...", what, e, failures, attempts - 1, delay)
            if not await interruptible_sleep(delay):
                raise RemoteStateError(f"{what}: interrumpido durante reintento")
            delay = min(delay * 2, 60)

def make_forum_raw_request(name: str, peer, **kwargs):
    messages_ns = getattr(raw.functions, "messages", None)
    messages_method = getattr(messages_ns, name, None) if messages_ns else None
    if messages_method is not None:
        try:
            return messages_method(peer=peer, **kwargs)
        except TypeError as exc:
            raise RuntimeError(
                f"Hydrogram expone messages.{name}, pero no acepta los argumentos "
                f"esperados (peer={type(peer).__name__}): {exc}"
            ) from exc

    channels_ns = getattr(raw.functions, "channels", None)
    channels_method = getattr(channels_ns, name, None) if channels_ns else None
    if channels_method is not None:
        input_channel = getattr(raw.types, "InputChannel", None)
        input_peer_channel = getattr(raw.types, "InputPeerChannel", None)
        if input_channel is None:
            raise RuntimeError(
                f"Hydrogram expone channels.{name}, pero no define raw.types.InputChannel"
            )

        if input_peer_channel is not None and isinstance(peer, input_peer_channel):
            try:
                peer = input_channel(
                    channel_id=peer.channel_id,
                    access_hash=peer.access_hash,
                )
            except (AttributeError, TypeError) as exc:
                raise RuntimeError(
                    f"No se pudo convertir InputPeerChannel a InputChannel para {name}: {exc}"
                ) from exc
        elif not isinstance(peer, input_channel):
            try:
                peer = input_channel(
                    channel_id=peer.channel_id,
                    access_hash=peer.access_hash,
                )
            except (AttributeError, TypeError) as exc:
                raise RuntimeError(
                    f"El peer {type(peer).__name__} no es válido para channels.{name}; "
                    "se esperaba InputChannel o InputPeerChannel convertible"
                ) from exc

        try:
            return channels_method(channel=peer, **kwargs)
        except TypeError as exc:
            raise RuntimeError(
                f"Hydrogram expone channels.{name}, pero rechazó los argumentos "
                f"channel={type(peer).__name__}: {exc}"
            ) from exc

    raise RuntimeError(
        f"La versión instalada de Hydrogram no ofrece raw.functions.messages/channels.{name}"
    )

@dataclass
class RemoteTopicMeta:
    id: int
    top_message: int

async def get_remote_topics(user: Client) -> dict[str, RemoteTopicMeta]:
    topics: dict[str, RemoteTopicMeta] = {}
    seen_ids: set[int] = set()
    peer = await tg_call(lambda: user.resolve_peer(CFG.group_id), "resolve_peer")
    offset_date = offset_id = offset_topic = 0

    while True:
        res = await tg_call(
            lambda: user.invoke(
                make_forum_raw_request(
                    "GetForumTopics", peer,
                    offset_date=offset_date,
                    offset_id=offset_id,
                    offset_topic=offset_topic,
                    limit=100,
                )
            ),
            "GetForumTopics",
        )
        batch = getattr(res, "topics", None) or []
        if not batch:
            break

        new_in_page = 0
        last_topic = None

        for t in batch:
            tid = getattr(t, "id", None)
            if tid is not None and tid not in seen_ids:
                seen_ids.add(tid)
                new_in_page += 1
                title = getattr(t, "title", None)
                top_msg = getattr(t, "top_message", 0) or 0
                if title:
                    topics[title.strip()] = RemoteTopicMeta(id=tid, top_message=top_msg)
            last_topic = t

        total_count = getattr(res, "count", None)
        if new_in_page == 0 or last_topic is None:
            break
        if total_count is not None and len(seen_ids) >= total_count:
            break

        offset_topic = getattr(last_topic, "id", 0) or 0
        offset_id = getattr(last_topic, "top_message", 0) or 0
        offset_date = getattr(last_topic, "date", 0) or 0

        if not getattr(res, "order_by_create_date", False) and offset_id:
            last_message = next(
                (m for m in (getattr(res, "messages", None) or [])
                 if getattr(m, "id", None) == offset_id),
                None,
            )
            if last_message is not None:
                offset_date = getattr(last_message, "date", offset_date) or offset_date

    return topics

async def delete_remote_messages(user: Client, ids: list[int]) -> bool:
    if not ids:
        return True
    success = True
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        try:
            await user.delete_messages(chat_id=CFG.group_id, message_ids=chunk)
        except Exception as e:
            success = False
            log.warning("No se pudieron borrar %d mensajes: %s", len(chunk), e)
    return success

async def get_topic_info(user: Client, topic_id: int) -> TopicInfo:
    records: list[DocRecord] = []
    orphan_ids: list[int] = []
    seen_msg_ids: set[int] = set()
    has_header = False
    max_remote_id = 0
    my_id = user.me.id if getattr(user, "me", None) else None
    peer = await tg_call(lambda: user.resolve_peer(CFG.group_id), "resolve_peer")
    offset_id = 0

    while True:
        res = await tg_call(
            lambda: user.invoke(
                raw.functions.messages.GetReplies(
                    peer=peer, msg_id=topic_id, offset_id=offset_id, offset_date=0,
                    add_offset=0, limit=100, max_id=0, min_id=0, hash=0,
                )
            ),
            f"GetReplies(tema {topic_id})",
        )
        messages = getattr(res, "messages", []) or []
        if not messages:
            break

        new_msgs_in_batch = 0
        min_id_in_batch: int | None = None

        for m in messages:
            mid = getattr(m, "id", None)
            if not mid or mid in seen_msg_ids:
                continue
            seen_msg_ids.add(mid)
            new_msgs_in_batch += 1
            if mid > max_remote_id:
                max_remote_id = mid
            if min_id_in_batch is None or mid < min_id_in_batch:
                min_id_in_batch = mid

            text = getattr(m, "message", "") or ""
            media = getattr(m, "media", None)
            doc = getattr(media, "document", None) if media else None
            from_obj = getattr(m, "from_id", None)
            from_id = getattr(from_obj, "user_id", None) if from_obj else None

            if my_id and from_id == my_id and not doc and text.startswith(ORPHAN_PREFIXES):
                orphan_ids.append(mid)
                continue

            low = text.lower()
            if any(h in low for h in HEADER_MARKERS):
                has_header = True

            if doc and hasattr(doc, "attributes"):
                filename = ""
                for attr in doc.attributes:
                    if isinstance(attr, raw.types.DocumentAttributeFilename):
                        filename = attr.file_name
                        break
                records.append(DocRecord(mid, filename, text))

        if new_msgs_in_batch == 0 or min_id_in_batch is None:
            break

        if min_id_in_batch == offset_id:
            break
        offset_id = min_id_in_batch

    if orphan_ids:
        await delete_remote_messages(user, orphan_ids)

    info = analyze_documents(records)
    info.has_header = has_header
    info.top_message = max_remote_id
    return info

async def get_cached_topic_info(user: Client, topic_id: int, current_top_message: int = 0) -> TopicInfo:
    cached = TOPIC_STORE.get(topic_id)
    if cached is not None:
        if current_top_message > 0 and cached.top_message >= current_top_message:
            return cached

    await asyncio.sleep(0.15)
    info = await get_topic_info(user, topic_id)
    if current_top_message > 0:
        info.top_message = max(info.top_message, current_top_message)
    TOPIC_STORE.update(topic_id, info, flush=True)
    return info

# ==============================================================================
# ANÁLISIS DE UNA SERIE
# ==============================================================================

@dataclass
class SeriesState:
    series: Series
    scan: ScanResult
    total_gb: float
    topic_id: int | None
    info: TopicInfo
    pending: list[Path]
    recent: list[Path]

    @property
    def pending_gb(self) -> float:
        return gb(sum(self.scan.sizes[f] for f in self.pending))

    @property
    def liberable(self) -> bool:
        return bool(self.scan.files) and not self.pending

async def analyze_series(user: Client, series: Series, remote_topics: dict[str, RemoteTopicMeta]) -> SeriesState | None:
    scan = await asyncio.to_thread(scan_series, series.folder)
    if not scan.files:
        return None
    meta = remote_topics.get(series.title)
    topic_id = meta.id if meta else None
    top_msg = meta.top_message if meta else 0

    info = await get_cached_topic_info(user, topic_id, top_msg) if topic_id else TopicInfo()
    pending = [f for f in scan.files if not is_uploaded(f, info)]
    limit = time.time() - CFG.min_file_age_minutes * 60
    recent = [f for f in pending if scan.mtimes[f] > limit]
    return SeriesState(
        series=series, scan=scan,
        total_gb=gb(sum(scan.sizes.values())),
        topic_id=topic_id, info=info, pending=pending, recent=recent,
    )

# ==============================================================================
# NOTIFICACIONES Y PANEL
# ==============================================================================

def get_control_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⏹ Cancelar jornada", callback_data="abort_run")]]
    )

def panel_text() -> str:
    used = QUOTA.used
    pct = (used / QUOTA.limit) * 100 if QUOTA.limit else 0
    return (
        f"🚀 <b>Sincronización de Series a Telegram</b>\n\n"
        f"📊 <b>Estado de cuota hoy:</b> {gb(used):.2f} GB / {CFG.daily_quota_gb:.1f} GB ({pct:.1f}%)\n"
        f"Puedes interactuar con los siguientes controles:"
    )

async def send_start_panel(bot: Client | None, user: Client):
    try:
        return await (bot or user).send_message(
            chat_id=CFG.notify_channel_id,
            text=panel_text(),
            reply_markup=get_control_buttons() if bot else None,
            message_thread_id=CFG.notify_thread_id,
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.warning("No se pudo enviar el panel interactivo: %s", e)
        return None

async def update_start_panel(bot: Client | None, panel_msg) -> None:
    if not bot or not panel_msg:
        return
    try:
        await panel_msg.edit_text(panel_text(), reply_markup=get_control_buttons(), parse_mode=ParseMode.HTML)
    except FloodWait as e:
        await interruptible_sleep(flood_seconds(e) + 1)
    except Exception:
        pass

async def close_start_panel(bot: Client | None, panel_msg) -> None:
    if not panel_msg or not bot:
        return
    status_label = "⏹ <b>Jornada de subida cancelada/interrumpida.</b>" if (SHUTDOWN_REQUESTED or ABORT_CURRENT_RUN) else "✅ <b>Jornada de subida cerrada.</b>"
    text = (
        f"{status_label}\n"
        f"📊 Volumen total acumulado hoy: <b>{gb(QUOTA.used):.2f} GB</b> / {CFG.daily_quota_gb:.1f} GB."
    )
    try:
        await panel_msg.edit_text(text, reply_markup=None, parse_mode=ParseMode.HTML)
    except FloodWait as e:
        log.warning("FloodWait (%ds) al cerrar el panel de inicio.", flood_seconds(e))
        await interruptible_sleep(flood_seconds(e) + 1)
        try:
            await panel_msg.edit_text(text, reply_markup=None, parse_mode=ParseMode.HTML)
        except Exception as err:
            log.warning("No se pudo cerrar el panel tras FloodWait: %s", err)
    except Exception as e:
        log.warning("No se pudo editar el panel de inicio al cierre: %s", e)

async def is_authorized(client: Client, callback: CallbackQuery) -> bool:
    uid = callback.from_user.id if callback.from_user else None
    if uid is None:
        return False
    if CFG.allowed_user_ids:
        return uid in CFG.allowed_user_ids
    try:
        member = await client.get_chat_member(CFG.notify_channel_id, uid)
        return member.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR)
    except Exception as e:
        log.warning("No se pudo verificar permisos de %s: %s", uid, e)
        return False

class UploadProgressTracker:
    def __init__(self, client: Client, chat_id: int, thread_id: int, filename: str,
                 loop: asyncio.AbstractEventLoop | None):
        self.client = client
        self.chat_id = chat_id
        self.thread_id = thread_id
        self.filename = filename.replace("`", "'")
        self.loop = loop
        self.status_msg = None
        self.last_percent_step = 0
        self.last_edit_time = 0.0
        self.updating_telegram = False
        self._last_log_step = -1
        self._lock = threading.Lock()

    async def init_message(self) -> None:
        try:
            self.status_msg = await self.client.send_message(
                chat_id=self.chat_id,
                text=f"⏳ <b>Iniciando subida:</b>\n<code>{html.escape(self.filename)}</code>\n[░░░░░░░░░░] 0%",
                message_thread_id=self.thread_id,
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            log.warning("No se pudo crear el mensaje de progreso: %s", e)

    async def _async_edit_text(self, text: str) -> None:
        try:
            if self.status_msg:
                await self.status_msg.edit_text(text, parse_mode=ParseMode.HTML)
        except Exception:
            pass
        finally:
            with self._lock:
                self.updating_telegram = False

    def callback(self, current: int, total: int) -> None:
        if not total:
            return
        pct = (current / total) * 100
        step = int(pct // 10) * 10

        with self._lock:
            if step in (0, 50, 100) and step > self._last_log_step:
                self._last_log_step = step
                log.info("   -> Subiendo %s: %d%% (%d MB / %d MB)", self.filename, step,
                         current // (1024 * 1024), total // (1024 * 1024))

            if not self.status_msg or self.updating_telegram or not self.loop or self.loop.is_closed():
                return
            now = time.time()
            if step > self.last_percent_step and (now - self.last_edit_time) >= 5:
                self.last_percent_step = step
                self.last_edit_time = now
                self.updating_telegram = True
                bar = "█" * (step // 10) + "░" * (10 - step // 10)
                text = (
                    f"📤 <b>Subiendo archivo:</b>\n<code>{html.escape(self.filename)}</code>\n\n"
                    f"[{bar}] {step}%\n"
                    f"📊 {gb(current):.2f} GB / {gb(total):.2f} GB"
                )
                try:
                    asyncio.run_coroutine_threadsafe(self._async_edit_text(text), self.loop)
                except Exception:
                    with self._lock:
                        self.updating_telegram = False

    async def cleanup(self) -> None:
        if self.status_msg:
            try:
                await self.status_msg.delete()
            except Exception:
                pass

# ==============================================================================
# EMPAQUETADO (7z) Y SUBIDA
# ==============================================================================

def clean_temp_directory() -> None:
    d = CFG.temp_dir
    d.mkdir(parents=True, exist_ok=True)
    generated_archive = re.compile(r"_[0-9a-f]{6}\.7z(?:\.\d{2,4})?$", re.IGNORECASE)
    generated_poster = re.compile(r"poster_\d+\.jpg$", re.IGNORECASE)
    generated_report = re.compile(r"pendientes_\d+\.txt$", re.IGNORECASE)
    removed = 0
    for item in d.iterdir():
        name = item.name
        if not (
            generated_archive.search(name)
            or generated_poster.fullmatch(name)
            or generated_report.fullmatch(name)
        ):
            continue
        try:
            if item.is_file() or item.is_symlink():
                item.unlink()
                removed += 1
        except OSError as e:
            log.warning("No se pudo borrar el temporal %s: %s", item.name, e)
    if removed:
        log.info("Se limpiaron %d artefactos temporales propios en %s.", removed, d)

def archive_base_path(source: Path) -> Path:
    stem = source.stem.encode("utf-8")[:180].decode("utf-8", errors="ignore")
    suffix = hashlib.sha1(source.name.encode()).hexdigest()[:6]
    return CFG.temp_dir / f"{stem}_{suffix}.7z"

def archive_files_for(base: Path) -> list[Path]:
    if not base.parent.exists():
        return []
    return sorted(
        p for p in base.parent.iterdir()
        if p.name == base.name or p.name.startswith(base.name + ".")
    )

def remove_archive_files(base: Path) -> None:
    for p in archive_files_for(base):
        try:
            p.unlink()
        except Exception as e:
            log.warning("No se pudo borrar %s: %s", p.name, e)

async def prepare_7z_file(source: Path, force_no_password: bool = False) -> list[Path]:
    CFG.temp_dir.mkdir(parents=True, exist_ok=True)
    size = source.stat().st_size
    free = shutil.disk_usage(CFG.temp_dir).free
    required_space = int(size * 1.05) + (512 * 1024 * 1024)
    if free < required_space:
        raise RuntimeError(
            f"Espacio insuficiente en {CFG.temp_dir}: libres {gb(free):.2f} GB, "
            f"hacen falta ~{gb(required_space):.2f} GB. Monta TEMP_DIR en un volumen con más espacio."
        )

    base = archive_base_path(source)
    split = size > int(LIMITS.threshold_bytes * 0.98)

    async def _run_7z(do_split: bool) -> list[Path]:
        remove_archive_files(base)
        cmd = ["7z", "a", "-mx=0", "-y"]
        if do_split:
            log.info("[7Z] %.2f GB > %.2f GB. Dividiendo en tomos de %s...",
                     gb(size), gb(LIMITS.threshold_bytes), LIMITS.volume_size)
            cmd.append(f"-v{LIMITS.volume_size}")
        else:
            log.info("[7Z] %.2f GB. Empaquetando en un único archivo...", gb(size))
        if CFG.archive_password and not force_no_password:
            cmd += [f"-p{CFG.archive_password}", "-mhe=on"]
            log.info("[7Z] Cifrado AES-256 aplicado.")
        else:
            log.info("[7Z] Empaquetado sin contraseña.")
        cmd += [str(base), str(source)]

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )
        communicate_task = asyncio.create_task(proc.communicate())
        while not communicate_task.done():
            if SHUTDOWN_REQUESTED or ABORT_CURRENT_RUN:
                log.warning("[7Z] Cancelación detectada; deteniendo el empaquetado de %s...", source.name)
                try:
                    proc.terminate()
                except ProcessLookupError:
                    pass
                try:
                    _, stderr = await asyncio.wait_for(asyncio.shield(communicate_task), timeout=10)
                except asyncio.TimeoutError:
                    try:
                        proc.kill()
                    except ProcessLookupError:
                        pass
                    _, stderr = await communicate_task
                raise RemoteStateError("Empaquetado interrumpido por cancelación/parada")
            await asyncio.sleep(0.5)

        _, stderr = await communicate_task
        if proc.returncode != 0:
            raise RuntimeError(f"7z falló con código {proc.returncode}: {stderr.decode(errors='replace')}")

        parts = archive_files_for(base)
        if not parts:
            raise RuntimeError("7z terminó sin errores pero no generó archivos")
        return parts

    parts = await _run_7z(split)

    safety_margin = 2 * 1024 * 1024
    if not split and len(parts) == 1 and parts[0].stat().st_size >= (LIMITS.threshold_bytes - safety_margin):
        log.warning("[7Z] El archivo individual (%d bytes) superó el umbral seguro. Reempaquetando en tomos...", parts[0].stat().st_size)
        parts = await _run_7z(True)

    return parts

async def send_file_with_retry(user: Client, topic_id: int, file_path: Path, caption: str) -> Message | None:
    expected = file_path.stat().st_size
    attempts = failures = floods = 0
    tracker = UploadProgressTracker(user, CFG.group_id, topic_id, file_path.name, MAIN_LOOP)
    await tracker.init_message()
    try:
        while failures < MAX_UPLOAD_RETRIES:
            if SHUTDOWN_REQUESTED:
                log.info("Apagado del proceso solicitado; se interrumpe la subida de %s.", caption)
                return None
            try:
                attempts += 1
                log.info(
                    "Subiendo: %s (llamada %d, fallos %d/%d, tema %s)",
                    caption, attempts, failures, MAX_UPLOAD_RETRIES, topic_id
                )
                msg = await user.send_document(
                    chat_id=CFG.group_id,
                    document=str(file_path),
                    caption=caption,
                    message_thread_id=topic_id,
                    progress=tracker.callback,
                )
                if msg is None:
                    log.warning("Subida interrumpida: %s", caption)
                    return None

                remote_size = getattr(getattr(msg, "document", None), "file_size", None)
                if remote_size is not None and remote_size != expected:
                    log.error("Tamaño distinto tras subir %s: local %d vs Telegram %d. Se borra y se reintenta.",
                              caption, expected, remote_size)
                    if not await delete_remote_messages(user, [msg.id]):
                        log.error("No se pudo retirar la copia con tamaño incorrecto; se detiene el archivo.")
                        return None
                    failures += 1
                    if failures >= MAX_UPLOAD_RETRIES:
                        log.error("Se agotaron los intentos por discrepancia de tamaño en %s.", caption)
                        return None
                    if not await shutdown_only_sleep(10):
                        return None
                    continue

                log.info("[OK] %s subido correctamente.", caption)
                await interruptible_sleep(3)
                return msg
            except FloodWait as e:
                floods += 1
                if floods > MAX_FLOODWAITS_PER_FILE:
                    log.error("Demasiados FloodWait seguidos subiendo %s. Se abandona.", caption)
                    return None
                wait_time = flood_seconds(e)
                if wait_time > 3600:
                    log.error("Telegram solicitó un FloodWait excesivo (%ds). Abortando archivo.", wait_time)
                    return None
                log.warning("[FLOOD WAIT] Telegram pide esperar %ds...", wait_time)
                if not await shutdown_only_sleep(wait_time):
                    log.warning("FloodWait interrumpido por SIGTERM/SIGINT; se abandona el archivo.")
                    return None
            except Exception as e:
                failures += 1
                log.error(
                    "Llamada %d falló para %s (%s). Fallo %d/%d; reintentando...",
                    attempts, caption, e, failures, MAX_UPLOAD_RETRIES
                )
                if failures >= MAX_UPLOAD_RETRIES:
                    break
                if not await shutdown_only_sleep(10):
                    return None
        log.error("Se agotaron los %d intentos fallidos para %s.", MAX_UPLOAD_RETRIES, caption)
        return None
    finally:
        await tracker.cleanup()

async def upload_episode(user: Client, topic_id: int, f: Path) -> int | None:
    if SHUTDOWN_REQUESTED or ABORT_CURRENT_RUN:
        return None
    size = f.stat().st_size
    is_sub = f.suffix.lower() in SUB_EXTS

    if is_sub:
        caption = f.name
        msg = await send_file_with_retry(user, topic_id, f, caption)
        if not msg:
            return None
        QUOTA.add(size)
        return size

    need_archive = CFG.compress_files or size > LIMITS.threshold_bytes
    base = archive_base_path(f) if need_archive else None
    uploaded_msg_ids: list[int] = []
    try:
        if need_archive:
            parts = await prepare_7z_file(f, force_no_password=not CFG.compress_files)
        else:
            parts = [f]
        total, uploaded = len(parts), 0
        for idx, part in enumerate(parts, start=1):
            part_size = part.stat().st_size
            if QUOTA.limit > 0:
                remaining = max(0, QUOTA.limit - QUOTA.used)
                if part_size > remaining:
                    log.warning(
                        "[CUOTA] Se detiene el archivo multipart '%s': el tomo %d/%d "
                        "ocupa %.2f GB y solo quedan %.2f GB. No se subirá un tomo incompleto.",
                        f.stem, idx, total, gb(part_size), gb(remaining)
                    )
                    if uploaded_msg_ids:
                        deleted = await delete_remote_messages(user, uploaded_msg_ids)
                        if not deleted:
                            log.error(
                                "No se pudieron borrar todos los tomos parciales de '%s'; "
                                "se requiere revisión manual.", f.stem
                            )
                    return None

            caption = f.stem if total == 1 else f"{f.stem} (Parte {idx} de {total})"
            msg = await send_file_with_retry(user, topic_id, part, caption)
            if not msg:
                if uploaded_msg_ids:
                    log.warning(
                        "Borrando %d tomos previos de '%s' tras fallo en tomo %d. "
                        "Cuota ya consumida NO recuperable: %.2f GB.",
                        len(uploaded_msg_ids), f.stem, idx, gb(uploaded),
                    )
                    deleted = await delete_remote_messages(user, uploaded_msg_ids)
                    if not deleted:
                        log.error(
                            "No se pudieron retirar todos los tomos parciales de '%s'. "
                            "Se requiere revisión manual antes de reintentar.",
                            f.stem
                        )
                return None
            uploaded_msg_ids.append(msg.id)
            n = part_size
            QUOTA.add(n)
            uploaded += n
            if SHUTDOWN_REQUESTED and idx < total:
                deleted = await delete_remote_messages(user, uploaded_msg_ids)
                if not deleted:
                    log.error(
                        "No se pudieron borrar todos los tomos tras la señal de apagado de '%s'.",
                        f.stem
                    )
                return None
        return uploaded
    finally:
        if base is not None:
            remove_archive_files(base)

# ==============================================================================
# CABECERA TMDb
# ==============================================================================

def fetch_tmdb_metadata(serie_name: str) -> dict | None:
    if not CFG.tmdb_api_key:
        return None
    m = re.match(r"^(.*?)\s*\((\d{4})\)$", serie_name.strip())
    clean_title, year = (m.group(1).strip(), m.group(2)) if m else (serie_name.strip(), None)
    key_clean = CFG.tmdb_api_key.strip().strip("'\"")

    def _do_request(title_query: str, query_year: str | None, use_bearer: bool) -> dict | None:
        params = {"query": title_query, "language": CFG.tmdb_language}
        if query_year:
            params["first_air_date_year"] = query_year

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Accept": "application/json"}
        if use_bearer:
            headers["Authorization"] = f"Bearer {key_clean}"
        else:
            params["api_key"] = key_clean

        url = f"https://api.themoviedb.org/3/search/tv?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))

    def query(title_query: str, query_year: str | None) -> dict | None:
        data = None
        for use_bearer in (False, True):
            try:
                data = _do_request(title_query, query_year, use_bearer=use_bearer)
                break
            except urllib.error.HTTPError as e:
                if e.code == 401 and not use_bearer:
                    continue
                log.warning("TMDb: error consultando '%s' (Bearer=%s): %s", title_query, use_bearer, e)
                return None
            except Exception as e:
                log.warning("TMDb: fallo de conexión con '%s': %s", title_query, e)
                return None

        if not data:
            return None

        results = data.get("results", [])
        if results:
            target_norm = norm(clean_title)
            best = results[0]
            for item in results:
                name_norm = norm(item.get("name", ""))
                orig_norm = norm(item.get("original_name", ""))
                if target_norm in (name_norm, orig_norm):
                    best = item
                    break
            poster = best.get("poster_path")
            return {
                "id": best.get("id"),
                "title": best.get("name", clean_title),
                "overview": (best.get("overview") or "").strip(),
                "rating": best.get("vote_average", 0.0),
                "poster_url": f"https://image.tmdb.org/t/p/w780{poster}" if poster else None,
            }
        return None

    res = query(clean_title, year)
    if not res and "&" in clean_title:
        res = query(clean_title.replace("&", "and"), year)
    if not res and year:
        res = query(clean_title, None)

    return res

def download_file(url: str, dest: Path, max_bytes: int = MAX_POSTER_BYTES) -> bool:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        total = 0
        with urllib.request.urlopen(req, timeout=15) as resp, open(dest, "wb") as out:
            while True:
                chunk = resp.read(64 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    raise ValueError(f"El póster supera el límite de {max_bytes} bytes")
                out.write(chunk)
        return total > 0 and dest.exists() and dest.stat().st_size == total
    except Exception as e:
        log.warning("No se pudo descargar el póster de %s: %s", url, e)
        try:
            dest.unlink(missing_ok=True)
        except Exception:
            pass
        return False

async def post_header_message(user: Client, topic_id: int, title: str) -> int | None:
    tmdb = await asyncio.to_thread(fetch_tmdb_metadata, title)
    poster: Path | None = None

    search_title = re.sub(r"\s*\(\d{4}\)\s*$", "", title).strip() or title
    tmdb_url = f"https://www.themoviedb.org/search/tv?query={urllib.parse.quote(search_title)}"
    if tmdb and tmdb.get("id"):
        tmdb_url = f"https://www.themoviedb.org/tv/{tmdb['id']}"

    try:
        if tmdb and tmdb.get("poster_url"):
            CFG.temp_dir.mkdir(parents=True, exist_ok=True)
            candidate = CFG.temp_dir / f"poster_{topic_id}.jpg"
            if await asyncio.to_thread(download_file, tmdb["poster_url"], candidate):
                poster = candidate

        overview = ""
        if tmdb and tmdb.get("overview"):
            overview = tmdb["overview"]
            if len(overview) > 450:
                overview = overview[:447] + "..."
        rating = f" ⭐ {tmdb['rating']:.1f}/10" if tmdb and tmdb.get("rating") else ""

        caption_html = (
            f"🎬 <b>{html.escape(title)}</b>{rating}\n\n"
            + (f"<i>{html.escape(overview)}</i>\n\n" if overview else "")
            + f'🔗 <a href="{html.escape(tmdb_url, quote=True)}">Ficha en The Movie Database (TMDb)</a>'
        )
        caption_plain = (
            f"🎬 {title}{rating}\n\n"
            + (f"{overview}\n\n" if overview else "")
            + f"🔗 Ficha en The Movie Database (TMDb): {tmdb_url}"
        )

        async def send(formatted: bool) -> Message:
            cap = caption_html if formatted else caption_plain
            mode = ParseMode.HTML if formatted else ParseMode.DISABLED
            if poster and poster.exists():
                return await user.send_photo(chat_id=CFG.group_id, photo=str(poster), caption=cap,
                                             message_thread_id=topic_id, parse_mode=mode)
            else:
                return await user.send_message(chat_id=CFG.group_id, text=cap, message_thread_id=topic_id,
                                               parse_mode=mode, disable_web_page_preview=False)

        for formatted in (True, False):
            try:
                log.info("Publicando ficha en el tema %s (%s)...", topic_id,
                         "HTML" if formatted else "texto plano")
                try:
                    sent_msg = await send(formatted)
                except FloodWait as e:
                    if not await interruptible_sleep(flood_seconds(e) + 2):
                        return None
                    sent_msg = await send(formatted)
                return getattr(sent_msg, "id", None)
            except Exception as e:
                log.warning("Cabecera fallida (%s): %s", "HTML" if formatted else "texto plano", e)
        return None
    finally:
        if poster:
            poster.unlink(missing_ok=True)
        await asyncio.sleep(2)

async def ensure_header(user: Client, topic_id: int, title: str, info: TopicInfo) -> None:
    if info.has_header:
        return
    msg_id = await post_header_message(user, topic_id, title)
    if msg_id:
        info.has_header = True
        if msg_id > info.top_message:
            info.top_message = msg_id
        TOPIC_STORE.update(topic_id, info, flush=False)

# ==============================================================================
# TEMAS
# ==============================================================================

async def create_single_topic(user: Client, title: str, max_attempts: int = 3) -> tuple[int, int] | tuple[None, None]:
    log.info("[NUEVO] Creando tema en Telegram para: '%s'...", title)
    attempt = 0
    delay = 3
    rnd_id = random.randrange(-2**63, 2**63 - 1)

    while attempt < max_attempts:
        if SHUTDOWN_REQUESTED or ABORT_CURRENT_RUN:
            return None, None

        attempt += 1
        try:
            peer = await user.resolve_peer(CFG.group_id)
            res = await user.invoke(
                make_forum_raw_request(
                    "CreateForumTopic", peer, title=title, random_id=rnd_id
                )
            )
            topic_id = None
            top_msg_id = 0
            for upd in getattr(res, "updates", []) or []:
                msg = getattr(upd, "message", None)
                if msg is not None and hasattr(msg, "id"):
                    topic_id = msg.id
                    top_msg_id = max(top_msg_id, msg.id)
                    break

            if topic_id is None:
                log.warning("No se pudo extraer el ID del update de CreateForumTopic; re-listando topics...")
                await asyncio.sleep(2)
                try:
                    refreshed = await get_remote_topics(user)
                    meta = refreshed.get(title)
                    if meta:
                        topic_id = meta.id
                        top_msg_id = meta.top_message
                except Exception as e:
                    log.warning("Re-listado de topics falló: %s", e)

            if isinstance(topic_id, int) and topic_id > 0:
                log.info("Tema creado. ID confirmado: %s (top_message: %s)", topic_id, top_msg_id or topic_id)
                await asyncio.sleep(2)
                return topic_id, (top_msg_id or topic_id)

            log.warning("Intento %d/%d: respuesta inesperada al crear tema '%s'", attempt, max_attempts, title)

        except FloodWait as e:
            wait_time = flood_seconds(e) + 5
            log.warning("[FLOOD WAIT] Esperando %ds antes de reintentar crear tema...", wait_time)
            if not await interruptible_sleep(wait_time):
                return None, None
            continue
        except Exception as e:
            log.warning("Intento %d/%d falló al crear tema '%s': %s", attempt, max_attempts, title, e)

        if attempt < max_attempts:
            if not await interruptible_sleep(delay):
                return None, None
            delay = min(delay * 2, 30)

    log.error("Se agotaron los reintentos para crear el tema '%s'.", title)
    return None, None

# ==============================================================================
# PROCESADO DE UNA SERIE
# ==============================================================================

@dataclass
class SeriesResult:
    episodes: int = 0
    bytes: int = 0
    total_gb: float = 0.0
    fatal: bool = False
    error: bool = False

def estimate_upload_size(f: Path, raw_size: int) -> int:
    needs_archive = CFG.compress_files or raw_size > LIMITS.threshold_bytes
    if f.suffix.lower() in SUB_EXTS or not needs_archive:
        return raw_size
    return int(raw_size * 1.01) + 1024 * 1024

async def process_serie(user: Client, bot: Client | None, panel_msg, series: Series,
                        remote_topics: dict[str, RemoteTopicMeta]) -> SeriesResult:
    if ABORT_CURRENT_RUN:
        return SeriesResult()

    try:
        state = await analyze_series(user, series, remote_topics)
    except RemoteStateError as e:
        log.error("No se pudo leer el estado de '%s': %s. Se omite esta serie.", series.folder.name, e)
        return SeriesResult(error=True)
    if state is None:
        return SeriesResult()

    name = series.folder.name

    if not state.pending:
        if state.topic_id and not state.info.has_header and not ABORT_CURRENT_RUN:
            await ensure_header(user, state.topic_id, series.title, state.info)
        if state.liberable:
            append_to_liberables(name, state.total_gb)
        else:
            remove_from_liberables(name)
        TOPIC_STORE.save()
        return SeriesResult(total_gb=state.total_gb)

    uploadable = [f for f in state.pending if f not in state.recent]
    if state.recent:
        log.info("'%s': %d archivo(s) modificados hace menos de %.0f min; se dejan para otra pasada.",
                 name, len(state.recent), CFG.min_file_age_minutes)
    remove_from_liberables(name)
    if not uploadable:
        TOPIC_STORE.save()
        return SeriesResult(total_gb=state.total_gb)

    topic_id, info = state.topic_id, state.info

    # Verificación preventiva de cuota antes de crear el tema
    if QUOTA.limit > 0:
        first_file = uploadable[0]
        first_size = state.scan.sizes.get(first_file, first_file.stat().st_size)
        est_first_size = estimate_upload_size(first_file, first_size)
        remaining_now = max(0, QUOTA.limit - QUOTA.used)
        if est_first_size > remaining_now:
            log.info(
                "[CUOTA] Se pospone '%s': el primer archivo requiere %.2f GB y solo quedan %.2f GB. "
                "No se crea el tema en Telegram para evitar dejar hilos vacíos.",
                name, gb(est_first_size), gb(remaining_now)
            )
            TOPIC_STORE.save()
            return SeriesResult(total_gb=state.total_gb, fatal=True)

    if not topic_id:
        topic_id, real_top_msg = await create_single_topic(user, series.title)
        if not topic_id:
            if SHUTDOWN_REQUESTED or ABORT_CURRENT_RUN:
                return SeriesResult(fatal=True)
            log.error("No se pudo crear el tema para '%s'. Se omite la serie.", series.folder.name)
            return SeriesResult(error=True)
        remote_topics[series.title] = RemoteTopicMeta(id=topic_id, top_message=real_top_msg)
        info = TopicInfo(top_message=real_top_msg)
        TOPIC_STORE.update(topic_id, info, flush=True)

    await ensure_header(user, topic_id, series.title, info)

    log.info("[PROCESANDO] '%s': %d archivo(s) pendiente(s)...", name, len(uploadable))
    uploaded_count = bytes_uploaded = 0

    for f in uploadable:
        if ABORT_CURRENT_RUN:
            log.info("[CANCELACIÓN] Deteniendo subida tras finalizar el archivo en curso.")
            break
        if QUOTA.exhausted:
            log.info("[CUPO ALCANZADO] Se superaron los %.1f GB del día.", CFG.daily_quota_gb)
            break
        if not f.exists():
            log.warning("'%s' ya no existe; se omite.", f.name)
            continue

        if QUOTA.limit > 0:
            source_size = state.scan.sizes.get(f, f.stat().st_size)
            estimated_size = estimate_upload_size(f, source_size)
            remaining = max(0, QUOTA.limit - QUOTA.used)
            if estimated_size > remaining:
                log.info(
                    "[CUOTA] Se pospone '%s': estimación %.2f GB, quedan %.2f GB. "
                    "No se inicia para evitar superar la cuota diaria.",
                    f.name, gb(estimated_size), gb(remaining)
                )
                break

        is_sub = f.suffix.lower() in SUB_EXTS
        clean_base = f.name if is_sub else get_file_clean_base(f)
        c_key = f"{norm(clean_base)}|{'sub' if is_sub else 'vid'}"

        stale = info.partials.pop(c_key, [])
        if stale:
            log.info("Subida incompleta previa de '%s': se borran %d parte(s) antes de rehacerla.",
                     f.stem, len(stale))
            if not await delete_remote_messages(user, stale):
                log.error(
                    "No se pudieron eliminar todas las partes antiguas de '%s'; "
                    "se omite para evitar mezclar versiones.",
                    f.stem
                )
                return SeriesResult(error=True)

        try:
            sent = await upload_episode(user, topic_id, f)
        except Exception as e:
            log.error("[ERROR PROCESANDO ARCHIVO] '%s': %s", f.name, e)
            sent = None

        if sent is not None:
            uploaded_count += 1
            bytes_uploaded += sent
            info.complete_keys.add(c_key)
            if not is_sub:
                code = extract_season_episode(f.stem)
                if code:
                    info.video_episode_codes.add(code)
            TOPIC_STORE.update(topic_id, info, flush=False)
            await update_start_panel(bot, panel_msg)
        else:
            log.warning("No se pudo completar la subida de '%s'.", f.name)

        log.info(" -> [CONSUMO HOY] %.2f GB / %.1f GB", gb(QUOTA.used), CFG.daily_quota_gb)

    TOPIC_STORE.save()

    complete = (
        not ABORT_CURRENT_RUN
        and uploaded_count == len(uploadable)
        and not state.recent
    )
    if complete:
        append_to_liberables(name, state.total_gb)
        log.info("[COMPLETA] Todos los archivos de '%s' están en Telegram.", name)

    return SeriesResult(episodes=uploaded_count, bytes=bytes_uploaded, total_gb=state.total_gb)

# ==============================================================================
# INFORMES
# ==============================================================================

def list_series() -> list[Series]:
    if not CFG.media_dir.exists() or not CFG.media_dir.is_dir():
        raise RuntimeError(
            f"MEDIA_DIR no existe o no es un directorio accesible: {CFG.media_dir}. "
            "Comprueba el montaje del volumen en Docker."
        )
    try:
        folders = [
            d for d in CFG.media_dir.iterdir()
            if d.is_dir() and not d.name.startswith((".", "@"))
        ]
    except OSError as e:
        raise RuntimeError(f"No se pudo listar MEDIA_DIR {CFG.media_dir}: {e}") from e
    return assign_titles(folders)

async def send_reports_album(client: Client, user: Client, remote_topics: dict[str, RemoteTopicMeta],
                             series_list: list[Series]) -> None:
    old_entries = parse_liberables_entries()
    new_entries: dict[str, float] = {}
    pending_map: dict[str, tuple[int, float]] = {}
    errors: list[str] = []

    for s in series_list:
        name = s.folder.name
        try:
            state = await analyze_series(user, s, remote_topics)
        except RemoteStateError as e:
            log.error("Informe: no se pudo verificar '%s': %s", name, e)
            errors.append(name)
            if name in old_entries:
                new_entries[name] = old_entries[name]
            continue
        if state is None:
            continue
        if state.pending:
            pending_map[name] = (len(state.pending), state.pending_gb)
        else:
            new_entries[name] = state.total_gb

    write_liberables_file(new_entries)
    liberables_gb = sum(new_entries.values())
    total_pending_eps = sum(c for c, _ in pending_map.values())
    total_pending_gb = sum(g for _, g in pending_map.values())

    CFG.temp_dir.mkdir(parents=True, exist_ok=True)
    pendientes_file = CFG.temp_dir / f"pendientes_{int(time.time())}.txt"
    with open(pendientes_file, "w", encoding="utf-8") as fo:
        fo.write(header_pendientes())
        for n in sorted(pending_map, key=str.lower):
            c, g = pending_map[n]
            fo.write(f"- {n} [{g:.2f} GB] ({c} archivos pendientes)\n")

    extra = ""
    if errors:
        extra += f"⚠️ Series no verificadas por error de red: {len(errors)}\n"
    
    caption = (
        f"📊 ESTADO GLOBAL DEL ALMACENAMIENTO\n"
        f"────────────────────────\n\n"
        f"🗑 Series que pueden ser purgadas (Liberables)\n"
        f"- Carpetas completas: {len(new_entries)}\n"
        f"- Espacio recuperable: {liberables_gb:.2f} GB\n"
        f"- Incluye los vídeos y los subtítulos\n\n"
        f"⏳ Cola de Subida (Pendientes)\n"
        f"- Series en espera: {len(pending_map)}\n"
        f"- Archivos pendientes: {total_pending_eps}\n"
        f"- Volumen pendiente: {total_pending_gb:.2f} GB\n\n"
        f"{extra}"
        f"────────────────────────\n"
        f"📁 Adjuntos: informes con el desglose detallado."
    )

    media = [
        InputMediaDocument(media=str(state_file()), caption=caption, parse_mode=ParseMode.DISABLED),
        InputMediaDocument(media=str(pendientes_file)),
    ]

    try:
        await client.send_media_group(
            chat_id=CFG.notify_channel_id, media=media, message_thread_id=CFG.notify_thread_id
        )
        log.info("[NOTIFICACIÓN] Informes enviados.")
        pendientes_file.unlink(missing_ok=True)
    except Exception as e:
        log.error("No se pudo enviar el grupo de documentos: %s", e)

# ==============================================================================
# WORKER
# ==============================================================================

def request_shutdown() -> None:
    global ABORT_CURRENT_RUN, SHUTDOWN_REQUESTED
    log.warning("Señal de parada recibida: terminando archivo actual antes de cerrar...")
    SHUTDOWN_REQUESTED = True
    ABORT_CURRENT_RUN = True

async def run_worker() -> None:
    global MAIN_LOOP, QUOTA, LIMITS, ABORT_CURRENT_RUN, TOPIC_STORE
    clean_temp_directory()
    CFG.data_dir.mkdir(parents=True, exist_ok=True)
    TOPIC_STORE = TopicCacheStore(CFG.data_dir / "topic_cache.json")

    user = Client(str(CFG.data_dir / "user_session"), api_id=CFG.api_id, api_hash=CFG.api_hash)
    bot = (
        Client(str(CFG.data_dir / "bot_session"), api_id=CFG.api_id, api_hash=CFG.api_hash,
               bot_token=CFG.bot_token)
        if CFG.bot_token else None
    )

    if bot:
        @bot.on_callback_query()
        async def handle_control_buttons(client: Client, callback: CallbackQuery):
            global ABORT_CURRENT_RUN
            if callback.data != "abort_run":
                return
            if not await is_authorized(client, callback):
                await callback.answer("⛔ No tienes permiso para cancelar la jornada.", show_alert=True)
                return
            ABORT_CURRENT_RUN = True
            await callback.answer("⏹ Cancelando jornada tras finalizar el archivo en curso...", show_alert=True)
            try:
                await callback.edit_message_text(
                    "⏹ <b>Jornada en pausa.</b>\nFinalizando la subida del archivo actual y cerrando...",
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass

    MAIN_LOOP = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            MAIN_LOOP.add_signal_handler(sig, request_shutdown)
        except (NotImplementedError, RuntimeError):
            pass

    user_started = False
    bot_started = False
    notify_client = bot if bot else user

    try:
        await user.start()
        user_started = True
        if bot:
            await bot.start()
            bot_started = True

        premium = bool(getattr(getattr(user, "me", None), "is_premium", False))
        LIMITS = compute_limits(premium, CFG.split_threshold_gb, CFG.archive_volume_size)
        log.info("Cuenta %s: umbral de división %.2f GB, tomos de %s.",
                 "Premium" if premium else "estándar", gb(LIMITS.threshold_bytes), LIMITS.volume_size)
        if not CFG.allowed_user_ids:
            log.info("ALLOWED_USER_IDS vacío: solo los administradores del canal podrán cancelar.")

        QUOTA = DailyQuota(CFG.data_dir / "cuota_hoy.json", CFG.daily_quota_bytes, CFG.tz)
        log.info("[ESTADO CUOTA] Consumo registrado hoy: %.2f GB / %.1f GB", gb(QUOTA.used), CFG.daily_quota_gb)

        log.info("Consultando temas existentes en Telegram...")
        remote_topics = await get_remote_topics(user)
        log.info("Se encontraron %d temas en el grupo.", len(remote_topics))

        active_ids = {meta.id for meta in remote_topics.values()}
        TOPIC_STORE.cleanup_untracked(active_ids)

        series_list = list_series()
        activity: list[dict] = []
        skipped_errors = 0

        panel_msg = await send_start_panel(bot, user)
        if QUOTA.exhausted:
            log.info("[CUPO COMPLETO] El cupo diario ya está alcanzado.")
        else:
            for series in series_list:
                if ABORT_CURRENT_RUN:
                    log.info("[ORDEN RECIBIDA] Se frena el ciclo por cancelación.")
                    break
                if QUOTA.exhausted:
                    log.info("[CUPO ALCANZADO] Se alcanzaron los %.1f GB diarios.", CFG.daily_quota_gb)
                    break
                res = await process_serie(user, bot, panel_msg, series, remote_topics)
                if res.fatal:
                    log.info("[PARADA DE CUOTA] No queda cuota suficiente para la siguiente serie. Deteniendo ciclo.")
                    break
                if res.error:
                    skipped_errors += 1
                if res.episodes > 0:
                    activity.append({"name": series.folder.name, "episodes": res.episodes,
                                     "gb": gb(res.bytes)})

        await close_start_panel(bot, panel_msg)

        if SHUTDOWN_REQUESTED:
            log.info("Parada del proceso solicitada: se omiten los informes para no bloquear el cierre.")
            return

        total_day = gb(QUOTA.used)
        warn = f"\n⚠️ {skipped_errors} serie(s) omitidas por error de lectura en Telegram." if skipped_errors else ""
        header = "⏹ Jornada Interrumpida (cancelada desde Telegram)" if ABORT_CURRENT_RUN else "📊 Informe de Sincronización Diaria"
        
        if activity:
            lines = [f"- {s['name']}: {s['episodes']} archivos ({s['gb']:.2f} GB)" for s in activity]
            msg_text = (
                f"{header}\n\n"
                f"📤  Transferencias realizadas hoy:\n" + "\n".join(lines) +
                f"\n\n📈 Volumen total transferido hoy: {total_day:.2f} GB / {CFG.daily_quota_gb:.1f} GB{warn}"
            )
        else:
            msg_text = (
                f"{header}\n"
                f"📊 Consumo acumulado hoy: {total_day:.2f} GB / {CFG.daily_quota_gb:.1f} GB.{warn}"
            )

        try:
            await notify_client.send_message(
                chat_id=CFG.notify_channel_id,
                text=msg_text,
                message_thread_id=CFG.notify_thread_id,
                parse_mode=ParseMode.DISABLED
            )
        except Exception as e:
            log.warning("No se pudo enviar el aviso final: %s", e)

        await asyncio.sleep(2)
        await send_reports_album(notify_client, user, remote_topics, series_list)

    except Exception as e:
        log.exception("Error fatal en el worker")
        err_msg = str(e)[:300].replace("`", "'")
        try:
            await notify_client.send_message(
                chat_id=CFG.notify_channel_id,
                text=f"❌ Error en el worker: {err_msg}",
                message_thread_id=CFG.notify_thread_id,
                parse_mode=ParseMode.DISABLED
            )
        except Exception:
            pass
        raise
    finally:
        if 'TOPIC_STORE' in globals():
            TOPIC_STORE.save()
        try:
            clean_temp_directory()
        except Exception:
            pass
        if bot_started and bot:
            try:
                await bot.stop()
            except Exception as e:
                log.warning("No se pudo detener el bot limpiamente: %s", e)
        if user_started:
            try:
                await user.stop()
            except Exception as e:
                log.warning("No se pudo detener la sesión de usuario limpiamente: %s", e)

# ==============================================================================
# SUPERVISOR
# ==============================================================================

def run_supervisor() -> None:
    try:
        cfg = load_config()
    except ConfigError as e:
        log.critical("Configuración inválida:\n%s", e)
        sys.exit(EXIT_CONFIG_ERROR)

    log.info("=" * 50)
    log.info(" [SUPERVISOR] Arrancando gestor de sincronización")
    log.info("=" * 50)

    stop = threading.Event()
    child: subprocess.Popen | None = None

    def handle_signal(signum, _frame):
        log.warning("[SUPERVISOR] Señal %s recibida. Parando...", signum)
        stop.set()
        if child is not None and child.poll() is None:
            child.terminate()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    failures = 0
    while not stop.is_set():
        log.info("[SUPERVISOR] Lanzando subproceso de subida (%s)...",
                 datetime.now(cfg.tz).strftime("%d/%m/%Y %H:%M"))
        child = subprocess.Popen([sys.executable, __file__, "--worker"])

        while child.poll() is None:
            try:
                child.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                if stop.is_set():
                    log.info("[SUPERVISOR] Esperando a que el worker cierre limpiamente...")
                    try:
                        child.wait(timeout=1800.0)
                    except subprocess.TimeoutExpired:
                        log.warning("[SUPERVISOR] Worker no cerró en 1800s. Forzando kill...")
                        child.kill()
                        child.wait()
                    break

        rc = child.returncode
        child = None
        if stop.is_set():
            break

        if rc == EXIT_CONFIG_ERROR:
            log.critical("[SUPERVISOR] El worker reporta configuración inválida. No se reintenta.")
            sys.exit(EXIT_CONFIG_ERROR)

        if rc != 0:
            failures += 1
            if failures >= cfg.max_consecutive_failures:
                log.critical("[SUPERVISOR] %d fallos consecutivos. Abandono.", failures)
                sys.exit(1)
            delay = min(60 * (2 ** (failures - 1)), 3600)
            log.error("[SUPERVISOR] El worker falló (código %s, fallo %d/%d). Reintento en %ds...",
                      rc, failures, cfg.max_consecutive_failures, delay)
            stop.wait(delay)
            continue

        failures = 0
        delay = next_run_delay(cfg.run_at, cfg.run_interval_hours, cfg.tz)
        next_run = (datetime.now(cfg.tz) + timedelta(seconds=delay)).strftime("%d/%m/%Y %H:%M")
        log.info("[SUPERVISOR] Lote terminado. En reposo hasta %s.", next_run)
        stop.wait(delay)

    log.info("[SUPERVISOR] Parado.")

def setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("hydrogram").setLevel(logging.ERROR)
    logging.getLogger("hydrogram.client").setLevel(logging.CRITICAL)

def main() -> None:
    global CFG
    setup_logging()
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        try:
            CFG = load_config()
        except ConfigError as e:
            log.critical("Configuración inválida:\n%s", e)
            sys.exit(EXIT_CONFIG_ERROR)
        asyncio.run(run_worker())
    else:
        run_supervisor()

if __name__ == "__main__":
    main()
