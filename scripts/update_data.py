#!/usr/bin/env python3
"""Refresh The Pacers Dashboard data from NBA + ESPN public endpoints."""

from __future__ import annotations

import json
import os
import re
import socket
import sys
import time
import random
import argparse
import hashlib
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import urljoin, urlparse

try:
    from pypdf import PdfReader
except ImportError:  # pragma: no cover
    PdfReader = None

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = PROJECT_ROOT / ".cache"
CONFIG_PATH = PROJECT_ROOT / "config.json"
LOG_PATH = PROJECT_ROOT / "update.log"

SAFE_MODE = False
FAST_MODE = False
TEST_FAIL_SECTION = ""
TEST_INVALID_SECTION = ""
TEST_429_PROVIDER = ""

DEFAULT_CONFIG = {
    "request": {
        "timeout_seconds": 8,
        "max_retries": 3,
        "backoff_seconds": 1.0,
        "jitter_seconds": 0.35,
        "global_min_delay_seconds": 0.5,
        "per_host_min_delay_seconds": 1.0,
    },
    "ttl_seconds": {
        "standings": 1800,
        "scoreboard": 180,
        "schedule": 43200,
        "player_profiles": 86400,
        "draft": 86400,
        "videos": 43200,
        "default": 1800,
    },
    "enabled_sections": {
        "standings": True,
        "schedule": True,
        "scoreboard": True,
        "boxscores": True,
        "players": True,
        "draft": True,
        "videos": True,
        "whatYouMissed": True,
        "pickProtection": True,
    },
    "team": {"abbr": "IND", "espn_id": "11", "nba_id": 1610612754},
    "sources": {
        "allow_html_scraping": False,
        "allow_pdf_scraping": False,
        "allow_stats_nba_com": False,
        "nba_stats": False,
        "youtube_data_api": False,
    },
    "youtube": {
        "channel_id": "",
        "uploads_playlist_id": "",
    },
    "policy": {
        "no_scraping": True,
        "require_compliant_sources": True,
    },
    "requirements": {
        "standings": "api",
        "schedule": "api",
        "scoreboard": "api",
        "boxscores": "api",
        "players": "api",
        "draft": "manual_or_paid_api",
        "videos": "youtube_embed_or_youtube_api_or_manual",
    },
}

CONFIG: Dict[str, object] = {}
RUN_CACHE: Dict[str, dict] = {}
LAST_REQUEST_AT = 0.0
LAST_REQUEST_BY_HOST: Dict[str, float] = {}
FORCE_FAIL_CONTAINS = os.getenv("PACERS_FORCE_FAIL_CONTAINS", "").strip().lower()

SECTION_META: Dict[str, dict] = {}
RUN_ERRORS: List[dict] = []


class PolicyViolation(Exception):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def log_event(level: str, message: str, **extra: object) -> None:
    ts = utc_now_iso()
    record = {"time_utc": ts, "level": level, "message": message}
    if extra:
        record.update(extra)
    try:
        with LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=True) + "\n")
    except Exception:
        pass


def _deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_PATH.exists():
        try:
            user_cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            if isinstance(user_cfg, dict):
                cfg = _deep_merge(cfg, user_cfg)
        except Exception as exc:
            log_event("warn", "failed to load config.json; using defaults", error=str(exc))
    return cfg


def init_runtime(args: argparse.Namespace) -> None:
    global CONFIG, SAFE_MODE, FAST_MODE, TEST_FAIL_SECTION, TEST_INVALID_SECTION, TEST_429_PROVIDER
    CONFIG = load_config()
    policy = CONFIG.get("policy", {}) if isinstance(CONFIG, dict) else {}
    no_scrape = bool(policy.get("no_scraping")) if isinstance(policy, dict) else False
    SAFE_MODE = bool(args.safe) or no_scrape
    FAST_MODE = bool(args.fast)
    TEST_FAIL_SECTION = str(getattr(args, "test_fail", "") or "").strip().lower()
    TEST_INVALID_SECTION = str(getattr(args, "test_invalid", "") or "").strip().lower()
    TEST_429_PROVIDER = str(getattr(args, "test_429", "") or "").strip().lower()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def section_status(section: str, status: str, source: str, fetched_at_utc: Optional[str] = None, error: Optional[str] = None) -> None:
    now = utc_now_iso()
    prev = SECTION_META.get(section, {})
    entry = {
        "status": status,
        "source": source,
        "fetched_at_utc": fetched_at_utc or now,
        "last_ok_at_utc": prev.get("last_ok_at_utc") if isinstance(prev, dict) else None,
    }
    if status == "ok":
        entry["last_ok_at_utc"] = now
    if error:
        entry["error"] = error
        RUN_ERRORS.append({"section": section, "source": source, "message": error, "time_utc": now})
    elif isinstance(prev, dict) and "error" in prev:
        # Preserve prior error only if still stale/error.
        if status in {"stale", "error"}:
            entry["error"] = prev.get("error")
    SECTION_META[section] = entry

# Draft section (existing model)
ESPN_BASE_URL = "https://site.api.espn.com"
ESPN_STANDINGS_URL = f"{ESPN_BASE_URL}/apis/site/v2/sports/basketball/nba/standings"
ESPN_STANDINGS_URLS = [
    ESPN_STANDINGS_URL,
    "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/standings",
]
ESPN_SCHEDULE_URL = f"{ESPN_BASE_URL}/apis/site/v2/sports/basketball/nba/teams/11/schedule"
ESPN_SCHEDULE_URLS = [
    ESPN_SCHEDULE_URL,
    "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/teams/11/schedule",
]
ESPN_SUMMARY_URL_TEMPLATE = f"{ESPN_BASE_URL}/apis/site/v2/sports/basketball/nba/summary?event={{event_id}}"
ESPN_SCOREBOARD_URL_TEMPLATE = f"{ESPN_BASE_URL}/apis/site/v2/sports/basketball/nba/scoreboard?dates={{yyyymmdd}}"
ESPN_STANDINGS_HTML_URL = "https://www.espn.com/nba/standings"

# NBA official sources for "What You Missed"
NBA_SCHEDULE_URL = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"
NBA_BOXSCORE_URL = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
NBA_PLAYERS_URL = "https://cdn.nba.com/static/json/staticData/players.json"
NBA_CDN_STANDINGS_URL = "https://cdn.nba.com/static/json/liveData/standings/standings.json"
NBA_ALT_STANDINGS_URL = "https://data.nba.net/prod/v1/current/standings_all.json"
NBA_ALT_SCOREBOARD_URL_TEMPLATE = "https://data.nba.net/prod/v2/{date}/scoreboard.json"
NBA_ALT_BOXSCORE_URL_TEMPLATE = "https://data.nba.net/prod/v1/{date}/{game_id}_boxscore.json"
NBA_PLAYER_PROFILE_URL_TEMPLATE = "https://data.nba.net/prod/v1/{season}/players/{player_id}_profile.json"
NBA_CDN_PLAYERPROFILE_URL_TEMPLATES = [
    "https://cdn.nba.com/static/json/liveData/playerprofile/playerprofile_{player_id}.json",
    "https://cdn.nba.com/static/json/liveData/playerProfile/playerProfile_{player_id}.json",
]
BBREF_PLAYER_GAMELOG_URL_TEMPLATE = "https://www.basketball-reference.com/players/{initial}/{slug}/gamelog/{season}"
BBREF_STANDINGS_URL_TEMPLATE = "https://www.basketball-reference.com/leagues/NBA_{season}_standings.html"
NBA_STATS_PLAYER_GAMELOG_URL = "https://stats.nba.com/stats/playergamelog"
TANKATHON_MOCK_URL = "https://www.tankathon.com/mock_draft"
TANKATHON_STANDINGS_URL = "https://www.tankathon.com/"
NBADRAFT_MOCK_URLS = [
    "https://www.nbadraft.net/nba-mock-drafts/",
    "https://www.nbadraft.net/nba-mock-draft/",
]
MOCK_OVERRIDE_FILE = "mock_override.json"
VIDEOS_OVERRIDE_FILE = "videos_override.json"
YOUTUBE_FALLBACK_HIGHLIGHT_URL = "https://youtu.be/nVnPeY3di08?si=-CGsEbxXmEJdrWuB"
HTTP_TIMEOUT_SECONDS = 8
HTTP_RETRY_ATTEMPTS = 3
HTTP_DNS_EXTRA_RETRIES = 2
LOCAL_DISPLAY_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone(timedelta(hours=-5))

PACERS_ABBR = "IND"
PACERS_TEAM_ID_STR = "11"
PACERS_NBA_TEAM_ID = 1610612754
FALLEN_DEFAULT_IDS = {
    "Myles Turner": "1626167",
    "Bennedict Mathurin": "1631097",
}
FALLEN_BBREF_SLUGS = {
    "Myles Turner": ("t", "turnemy01"),
    "Bennedict Mathurin": ("m", "mathube01"),
}
FALLEN_NBA_PLAYER_PAGES = {
    "Myles Turner": "https://www.nba.com/player/1626167/myles-turner",
    "Bennedict Mathurin": "https://www.nba.com/player/1631097/bennedict-mathurin",
}

LOTTERY_WEIGHTS = [14.0, 14.0, 14.0, 12.5, 10.5, 9.0, 7.5, 6.0, 4.5, 3.0, 2.0, 1.5, 1.0, 0.5]
ODDS_BY_SLOT: Dict[int, Dict[str, float]] = {
    1: {"top4": 52.1, "first": 14.0},
    2: {"top4": 52.1, "first": 14.0},
    3: {"top4": 52.1, "first": 14.0},
    4: {"top4": 48.1, "first": 12.5},
    5: {"top4": 42.1, "first": 10.5},
    6: {"top4": 37.2, "first": 9.0},
    7: {"top4": 32.0, "first": 7.5},
    8: {"top4": 26.3, "first": 6.0},
    9: {"top4": 20.3, "first": 4.5},
    10: {"top4": 13.9, "first": 3.0},
    11: {"top4": 9.4, "first": 2.0},
    12: {"top4": 7.1, "first": 1.5},
    13: {"top4": 4.8, "first": 1.0},
    14: {"top4": 2.4, "first": 0.5},
}

PICK_PROTECTION = {
    "text": "Reported protection in the Ivica Zubac deal: Indiana's 2026 first-round pick is protected 1-4 and 10-30.",
    "keepSlots": "1-4 and 10-30",
    "sendSlots": "5-9",
    "sourceTeam": "LAC",
}

DEFAULT_WHAT_YOU_MISSED = {
    "summary": "Latest completed game not available.",
    "leaders": [],
    "highlightVideoUrl": YOUTUBE_FALLBACK_HIGHLIGHT_URL,
}



BLOCKED_SCRAPE_HOST_TOKENS = (
    "tankathon.com",
    "nbadraft.net",
    "basketball-reference.com",
)


def _policy_dict() -> Dict[str, object]:
    p = CONFIG.get("policy", {}) if isinstance(CONFIG, dict) else {}
    return p if isinstance(p, dict) else {}


def _source_dict() -> Dict[str, object]:
    s = CONFIG.get("sources", {}) if isinstance(CONFIG, dict) else {}
    return s if isinstance(s, dict) else {}


def _requirements_dict() -> Dict[str, str]:
    req = CONFIG.get("requirements", {}) if isinstance(CONFIG, dict) else {}
    return req if isinstance(req, dict) else {}


def _youtube_cfg() -> Dict[str, object]:
    y = CONFIG.get("youtube", {}) if isinstance(CONFIG, dict) else {}
    return y if isinstance(y, dict) else {}


def _ensure_compliant_request(url: str) -> None:
    policy = _policy_dict()
    no_scraping = bool(policy.get("no_scraping"))
    if not no_scraping:
        return

    lower = url.lower()
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()

    if any(token in host for token in BLOCKED_SCRAPE_HOST_TOKENS):
        raise PolicyViolation(f"blocked host by policy_no_scrape: {host}")
    if path.endswith(".html") or path.endswith(".htm"):
        raise PolicyViolation(f"blocked html by policy_no_scrape: {url}")
    if path.endswith(".pdf"):
        raise PolicyViolation(f"blocked pdf by policy_no_scrape: {url}")
    if "youtube.com/@" in lower or "/videos" in path and "youtube.com" in host:
        raise PolicyViolation(f"blocked youtube html channel scrape by policy_no_scrape: {url}")
    if "stats.nba.com" in host and not bool(_source_dict().get("allow_stats_nba_com")):
        raise PolicyViolation("policy_blocked_stats_nba")


def _load_json_file(path: Path) -> Optional[object]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_videos_override(project_root: Path) -> List[dict]:
    path = project_root / VIDEOS_OVERRIDE_FILE
    raw = _load_json_file(path)
    if not isinstance(raw, list):
        return []
    out: List[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        vid = str(item.get("videoId") or "").strip()
        title = str(item.get("title") or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{11}", vid):
            continue
        out.append({"videoId": vid, "title": title})
    return out


DEFAULT_DRAFT = {
    "lotteryStandings": [
        {"slot": 1, "team": "SAC", "record": "12-45", "top4": 52.1, "first": 14.0},
        {"slot": 2, "team": "IND", "record": "15-41", "top4": 52.1, "first": 14.0},
        {"slot": 3, "team": "NO", "record": "15-41", "top4": 52.1, "first": 14.0},
        {"slot": 4, "team": "BKN", "record": "15-39", "top4": 48.1, "first": 12.5},
        {"slot": 5, "team": "WAS", "record": "15-39", "top4": 42.1, "first": 10.5},
        {"slot": 6, "team": "UTA", "record": "18-38", "top4": 37.2, "first": 9.0},
        {"slot": 7, "team": "DAL", "record": "19-35", "top4": 32.0, "first": 7.5},
        {"slot": 8, "team": "MEM", "record": "20-33", "top4": 26.3, "first": 6.0},
        {"slot": 9, "team": "CHI", "record": "24-32", "top4": 20.3, "first": 4.5},
        {"slot": 10, "team": "MIL", "record": "23-30", "top4": 13.9, "first": 3.0},
        {"slot": 11, "team": "ATL", "record": "27-30", "top4": 9.4, "first": 2.0},
        {"slot": 12, "team": "CHA", "record": "26-30", "top4": 7.1, "first": 1.5},
        {"slot": 13, "team": "POR", "record": "27-29", "top4": 4.8, "first": 1.0},
        {"slot": 14, "team": "LAC", "record": "27-28", "top4": 2.4, "first": 0.5},
    ],
    "pacersPickOdds": [
        {"pick": 1, "pct": 14.0},
        {"pick": 2, "pct": 13.4},
        {"pick": 3, "pct": 12.7},
        {"pick": 4, "pct": 12.0},
        {"pick": 5, "pct": 27.8},
        {"pick": 6, "pct": 20.0},
    ],
    "keyGames": [
        {
            "date": "Fri, Feb 20, 2026",
            "opponent": "Washington Wizards",
            "location": "Away",
            "why": "Washington is within 5 wins of Indiana and directly impacts nearby draft positioning.",
        },
        {
            "date": "Sun, Feb 22, 2026",
            "opponent": "Dallas Mavericks",
            "location": "Home",
            "why": "Dallas is within 5 wins of Indiana and a direct standings swing game.",
        },
    ],
}

# Backup mock map used only when live mock sources cannot be parsed.
DEFAULT_MOCK_MAP: Dict[int, Dict[str, str]] = {
    1: {"name": "Darryn Peterson", "url": "https://www.nbadraft.net/players/darryn-peterson/"},
    2: {"name": "AJ Dybantsa", "url": "https://www.nbadraft.net/players/aj-dybantsa/"},
    3: {"name": "Cameron Boozer", "url": "https://www.nbadraft.net/players/cameron-boozer/"},
    4: {"name": "Caleb Wilson", "url": "https://www.nbadraft.net/players/caleb-wilson/"},
    5: {"name": "Kingston Flemings", "url": "https://www.nbadraft.net/players/kingston-flemings/"},
    6: {"name": "Mikel Brown Jr.", "url": "https://www.nbadraft.net/players/mikel-brown-jr/"},
    7: {"name": "Keaton Wagler", "url": "https://www.nbadraft.net/players/keaton-wagler/"},
    8: {"name": "Nate Ament", "url": "https://www.nbadraft.net/players/nate-ament/"},
    9: {"name": "Darius Acuff", "url": "https://www.nbadraft.net/players/darius-acuff/"},
    10: {"name": "Labaron Philon", "url": "https://www.nbadraft.net/players/labaron-philon/"},
    11: {"name": "Braylon Mullins", "url": "https://www.nbadraft.net/players/braylon-mullins/"},
    12: {"name": "Hannes Steinbach", "url": "https://www.nbadraft.net/players/hannes-steinbach/"},
    13: {"name": "Brayden Burries", "url": "https://www.nbadraft.net/players/brayden-burries/"},
    14: {"name": "Yaxel Lendeborg", "url": "https://www.nbadraft.net/players/yaxel-lendeborg/"},
}

BLOCKED_MOCK_PLAYER_NAMES = {"cooper flagg"}


def is_blocked_mock_player(name: object) -> bool:
    return str(name or "").strip().lower() in BLOCKED_MOCK_PLAYER_NAMES


def sanitize_mock_map(mock_map: Dict[int, Dict[str, str]]) -> Dict[int, Dict[str, str]]:
    clean: Dict[int, Dict[str, str]] = {}
    for slot, info in (mock_map or {}).items():
        if not isinstance(info, dict):
            continue
        if is_blocked_mock_player(info.get("name")):
            continue
        clean[slot] = {"name": str(info.get("name") or "").strip(), "url": str(info.get("url") or "").strip()}
    return clean


TEAM_NAMES = {
    "Atlanta Hawks", "Boston Celtics", "Brooklyn Nets", "Charlotte Hornets", "Chicago Bulls",
    "Cleveland Cavaliers", "Dallas Mavericks", "Denver Nuggets", "Detroit Pistons",
    "Golden State Warriors", "Houston Rockets", "Indiana Pacers", "LA Clippers",
    "Los Angeles Lakers", "Memphis Grizzlies", "Miami Heat", "Milwaukee Bucks",
    "Minnesota Timberwolves", "New Orleans Pelicans", "New York Knicks",
    "Oklahoma City Thunder", "Orlando Magic", "Philadelphia 76ers", "Phoenix Suns",
    "Portland Trail Blazers", "Sacramento Kings", "San Antonio Spurs", "Toronto Raptors",
    "Utah Jazz", "Washington Wizards",
}
TEAM_NAME_TO_ABBR = {
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GS",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "LA Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NO",
    "New York Knicks": "NY",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SA",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
}
ALL_TEAM_ABBRS = [
    "ATL", "BOS", "BKN", "CHA", "CHI", "CLE", "DAL", "DEN", "DET", "GS",
    "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", "MIN", "NO", "NY",
    "OKC", "ORL", "PHI", "PHX", "POR", "SAC", "SA", "TOR", "UTA", "WAS",
]

STATUS_WORDS = ["Out", "Questionable", "Doubtful", "Probable", "Available", "Unavailable"]
STATUS_RE = re.compile(
    r"^([A-Za-z'\-. ]+,\s+[A-Za-z'\-. ]+)\s+(Out|Questionable|Doubtful|Probable|Available|Unavailable)\s*(.*)$"
)
ENTRY_RE = re.compile(
    r"([A-Za-z0-9'. -]+,\s*[A-Za-z0-9'. -]+|[A-Za-z0-9'. -]+\s+[A-Za-z0-9'. -]+)"
    r"(?:\s+[A-Z]{1,3}){0,3}\s+"
    r"(Out|Questionable|Doubtful|Probable|Available|Unavailable)\s*",
    re.IGNORECASE,
)


def _cache_key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _cache_path(url: str) -> Path:
    return CACHE_DIR / f"{_cache_key(url)}.json"


def _classify_url(url: str) -> str:
    u = url.lower()
    if "standings" in u:
        return "standings"
    if "scoreboard" in u:
        return "scoreboard"
    if "schedule" in u:
        return "schedule"
    if "player" in u and ("profile" in u or "players/" in u):
        return "player_profiles"
    if "tankathon" in u or "mock" in u or "nbadraft" in u:
        return "draft"
    if "youtube" in u or "youtu.be" in u:
        return "videos"
    return "default"


def _ttl_for_url(url: str) -> int:
    ttls = CONFIG.get("ttl_seconds", DEFAULT_CONFIG["ttl_seconds"])
    if not isinstance(ttls, dict):
        ttls = DEFAULT_CONFIG["ttl_seconds"]
    k = _classify_url(url)
    try:
        return int(ttls.get(k, ttls.get("default", 1800)))
    except Exception:
        return 1800


def _load_cache(url: str) -> Optional[dict]:
    if url in RUN_CACHE:
        return RUN_CACHE[url]
    p = _cache_path(url)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            RUN_CACHE[url] = data
            return data
    except Exception:
        return None
    return None


def _save_cache(url: str, body: bytes, headers: Dict[str, str]) -> None:
    p = _cache_path(url)
    record = {
        "url": url,
        "fetched_at_utc": utc_now_iso(),
        "headers": headers,
        "body_b64": body.hex(),
    }
    try:
        p.write_text(json.dumps(record), encoding="utf-8")
        RUN_CACHE[url] = record
    except Exception as exc:
        log_event("warn", "failed to write cache", url=url, error=str(exc))


def _decode_cached_body(record: dict) -> Optional[bytes]:
    hx = record.get("body_b64")
    if not isinstance(hx, str):
        return None
    try:
        return bytes.fromhex(hx)
    except Exception:
        return None


def _respect_rate_limits(url: str) -> None:
    global LAST_REQUEST_AT
    req_cfg = CONFIG.get("request", DEFAULT_CONFIG["request"])
    if not isinstance(req_cfg, dict):
        req_cfg = DEFAULT_CONFIG["request"]
    global_min = float(req_cfg.get("global_min_delay_seconds", 0.5))
    host_min = float(req_cfg.get("per_host_min_delay_seconds", 1.0))
    host = urlparse(url).netloc.lower()
    now = time.time()
    wait_global = max(0.0, (LAST_REQUEST_AT + global_min) - now)
    wait_host = max(0.0, (LAST_REQUEST_BY_HOST.get(host, 0.0) + host_min) - now)
    wait_for = max(wait_global, wait_host)
    if wait_for > 0:
        time.sleep(wait_for)
    now2 = time.time()
    LAST_REQUEST_AT = now2
    LAST_REQUEST_BY_HOST[host] = now2


def _is_dns_resolution_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    if isinstance(exc, URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, socket.gaierror):
            return True
    return (
        "nodename nor servname provided" in text
        or "temporary failure in name resolution" in text
        or "name or service not known" in text
        or "getaddrinfo failed" in text
    )


def fetch_url(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
    max_retries: Optional[int] = None,
    backoff: Optional[float] = None,
    jitter: Optional[float] = None,
    allow_stale_cache: bool = True,
    use_fresh_cache: bool = True,
) -> Optional[dict]:
    try:
        _ensure_compliant_request(url)
    except PolicyViolation as exc:
        log_event("error", "policy_violation", url=url, reason=str(exc))
        return {
            "ok": False,
            "status": 0,
            "body": None,
            "headers": {},
            "source": "none",
            "fetched_at_utc": utc_now_iso(),
            "error": f"policy_no_scrape: {exc}",
        }

    req_cfg = CONFIG.get("request", DEFAULT_CONFIG["request"])
    if not isinstance(req_cfg, dict):
        req_cfg = DEFAULT_CONFIG["request"]
    timeout_s = int(timeout if timeout is not None else req_cfg.get("timeout_seconds", HTTP_TIMEOUT_SECONDS))
    retries = int(max_retries if max_retries is not None else req_cfg.get("max_retries", HTTP_RETRY_ATTEMPTS))
    backoff_s = float(backoff if backoff is not None else req_cfg.get("backoff_seconds", 1.0))
    jitter_s = float(jitter if jitter is not None else req_cfg.get("jitter_seconds", 0.35))

    base_headers = {
        "User-Agent": "PacersDashboardBot/1.0 (+noncommercial fan project)",
        "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
    }
    host = urlparse(url).netloc.lower()
    if "cdn.nba.com" in host:
        base_headers.update(
            {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
                "Referer": "https://www.nba.com/",
            }
        )
    if headers:
        base_headers.update(headers)

    cached = _load_cache(url)
    if FORCE_FAIL_CONTAINS and FORCE_FAIL_CONTAINS in url.lower():
        if allow_stale_cache and cached:
            body = _decode_cached_body(cached)
            if body is not None:
                return {
                    "ok": False,
                    "status": 0,
                    "body": body,
                    "headers": cached.get("headers", {}),
                    "source": "cache:stale",
                    "fetched_at_utc": cached.get("fetched_at_utc"),
                    "error": f"forced failure for urls containing '{FORCE_FAIL_CONTAINS}'",
                }
        return {
            "ok": False,
            "status": 0,
            "body": None,
            "headers": {},
            "source": "none",
            "fetched_at_utc": utc_now_iso(),
            "error": f"forced failure for urls containing '{FORCE_FAIL_CONTAINS}'",
        }

    ttl = _ttl_for_url(url)
    now = datetime.now(timezone.utc)
    if use_fresh_cache and cached and isinstance(cached.get("fetched_at_utc"), str):
        try:
            fetched_at = datetime.fromisoformat(str(cached["fetched_at_utc"]).replace("Z", "+00:00"))
        except Exception:
            fetched_at = None
        if fetched_at and (now - fetched_at).total_seconds() <= ttl:
            body = _decode_cached_body(cached)
            if body is not None:
                age_s = (now - fetched_at).total_seconds()
                ttl_remaining_s = max(0.0, ttl - age_s)
                log_event(
                    "info",
                    "cache_hit",
                    url=url,
                    source="cache:fresh",
                    ttl_seconds=ttl,
                    ttl_remaining_seconds=round(ttl_remaining_s, 2),
                )
                return {
                    "ok": True,
                    "status": 200,
                    "body": body,
                    "headers": cached.get("headers", {}),
                    "source": "cache:fresh",
                    "fetched_at_utc": cached.get("fetched_at_utc"),
                }

    # Deterministic offline hook to exercise 429/backoff/retry path.
    if url.startswith("test429://"):
        last_err = None
        sim_retries = max(2, retries)
        for attempt in range(sim_retries):
            if attempt == 0:
                retry_after = 2.0
                computed_backoff = (backoff_s * (2 ** attempt)) + 0.0
                intended_sleep = max(retry_after, computed_backoff)
                log_event(
                    "warn",
                    "http_429_detected",
                    url=url,
                    status=429,
                    retry_after_seconds=retry_after,
                    computed_backoff_seconds=round(computed_backoff, 2),
                    intended_sleep_seconds=round(intended_sleep, 2),
                    attempt=attempt + 1,
                )
                time.sleep(intended_sleep)
                last_err = HTTPError(url, 429, "Too Many Requests", hdrs={"Retry-After": "2"}, fp=None)
                continue
            body = b"{\"ok\": true, \"source\": \"test429\"}"
            resp_headers = {"Content-Type": "application/json"}
            _save_cache(url, body, resp_headers)
            log_event("info", "network_fetch", url=url, status=200, ttl_seconds=ttl, simulated=True)
            return {
                "ok": True,
                "status": 200,
                "body": body,
                "headers": resp_headers,
                "source": "network",
                "fetched_at_utc": utc_now_iso(),
            }
        return {
            "ok": False,
            "status": 429,
            "body": None,
            "headers": {},
            "source": "none",
            "fetched_at_utc": utc_now_iso(),
            "error": str(last_err) if last_err else "simulated 429 failure",
        }

    last_err = None
    dns_extra_remaining = max(0, HTTP_DNS_EXTRA_RETRIES)
    attempt = 0
    while attempt < retries:
        try:
            _respect_rate_limits(url)
            final_url = str(url).strip()
            print(f"Final request URL: {repr(final_url)}", flush=True)
            parsed = urlparse(final_url)
            print(
                "Parsed URL components:",
                {
                    "scheme": parsed.scheme,
                    "netloc": parsed.netloc,
                    "path": parsed.path,
                    "params": parsed.params,
                    "query": parsed.query,
                    "fragment": parsed.fragment,
                    "hostname": parsed.hostname,
                    "port": parsed.port,
                    "username": parsed.username,
                },
                flush=True,
            )
            if parsed.hostname is None:
                raise ValueError(f"Malformed URL (missing hostname): {repr(final_url)}")
            req = Request(final_url, headers=base_headers)
            with urlopen(req, timeout=timeout_s) as response:
                status = int(getattr(response, "status", 200) or 200)
                resp_headers = {k: v for k, v in response.headers.items()} if response.headers else {}
                body = response.read()
                _save_cache(url, body, resp_headers)
                log_event("info", "network_fetch", url=url, status=status, ttl_seconds=ttl)
                return {
                    "ok": True,
                    "status": status,
                    "body": body,
                    "headers": resp_headers,
                    "source": "network",
                    "fetched_at_utc": utc_now_iso(),
                }
        except HTTPError as exc:
            status = int(getattr(exc, "code", 0) or 0)
            retry_after = 0.0
            try:
                rh = exc.headers.get("Retry-After") if exc.headers else None
                retry_after = float(rh) if rh else 0.0
            except Exception:
                retry_after = 0.0
            last_err = exc
            if status in (429, 503):
                wait = max(retry_after, (backoff_s * (2 ** attempt)) + random.uniform(0, jitter_s))
                time.sleep(wait)
                attempt += 1
                continue
            if attempt < retries - 1:
                time.sleep((backoff_s * (2 ** attempt)) + random.uniform(0, jitter_s))
                attempt += 1
                continue
        except (URLError, TimeoutError, OSError) as exc:
            last_err = exc
            if _is_dns_resolution_error(exc) and dns_extra_remaining > 0:
                dns_extra_remaining -= 1
                wait = (backoff_s * (2 ** attempt)) + random.uniform(0, jitter_s)
                log_event(
                    "warn",
                    "dns_retry",
                    url=url,
                    error=str(exc),
                    retries_remaining=dns_extra_remaining,
                    wait_seconds=round(wait, 2),
                )
                time.sleep(wait)
                continue
            if attempt < retries - 1:
                time.sleep((backoff_s * (2 ** attempt)) + random.uniform(0, jitter_s))
                attempt += 1
                continue
        attempt += 1

    if allow_stale_cache and cached:
        body = _decode_cached_body(cached)
        if body is not None:
            stale_age = None
            try:
                cached_ts = datetime.fromisoformat(str(cached.get("fetched_at_utc", "")).replace("Z", "+00:00"))
                stale_age = (datetime.now(timezone.utc) - cached_ts).total_seconds()
            except Exception:
                stale_age = None
            log_event(
                "warn",
                "cache_hit",
                url=url,
                source="cache:stale",
                ttl_seconds=ttl,
                stale_age_seconds=round(stale_age, 2) if stale_age is not None else None,
                error=str(last_err) if last_err else "network failure",
            )
            return {
                "ok": False,
                "status": 0,
                "body": body,
                "headers": cached.get("headers", {}),
                "source": "cache:stale",
                "fetched_at_utc": cached.get("fetched_at_utc"),
                "error": str(last_err) if last_err else "network failure",
            }
    return {
        "ok": False,
        "status": 0,
        "body": None,
        "headers": {},
        "source": "none",
        "fetched_at_utc": utc_now_iso(),
        "error": str(last_err) if last_err else "network failure",
    }


def fetch_bytes(url: str) -> bytes:
    res = fetch_url(url)
    if res and res.get("body") is not None:
        return res["body"]
    raise URLError(str(res.get("error") if isinstance(res, dict) else "Failed to fetch URL"))


def fetch_json(url: str) -> dict:
    return json.loads(fetch_bytes(url).decode("utf-8"))


def fetch_json_with_headers(url: str, headers: Dict[str, str]) -> dict:
    res = fetch_url(url, headers=headers)
    if not res or res.get("body") is None:
        raise URLError(str(res.get("error") if isinstance(res, dict) else "Failed to fetch URL"))
    return json.loads(res["body"].decode("utf-8"))


def fetch_text(url: str) -> str:
    return fetch_bytes(url).decode("utf-8", errors="ignore")


def fetch_json_any(urls: List[str], use_fresh_cache: bool = True) -> dict:
    payload, _meta = fetch_json_any_with_meta(urls, use_fresh_cache=use_fresh_cache)
    return payload


def fetch_json_any_with_meta(urls: List[str], use_fresh_cache: bool = True) -> Tuple[dict, dict]:
    last_exc: Optional[Exception] = None
    for url in urls:
        res = fetch_url(url, use_fresh_cache=use_fresh_cache)
        if not res or res.get("body") is None:
            last_exc = URLError(str((res or {}).get("error") if isinstance(res, dict) else "network failure"))
            continue
        try:
            payload = json.loads(res["body"].decode("utf-8"))
            return payload, {
                "url": url,
                "http_status": res.get("status"),
                "source": res.get("source"),
                "fetched_at_utc": res.get("fetched_at_utc"),
                "ok": bool(res.get("ok")),
            }
        except Exception as exc:
            last_exc = exc
            log_event("warn", "json_parse_failed", url=url, error=str(exc))
            continue
    if last_exc:
        raise last_exc
    raise URLError("No URL candidates provided")


def fetch_valid_standings_payload(urls: List[str], use_fresh_cache: bool = True) -> Tuple[dict, dict]:
    last_exc: Optional[Exception] = None
    for url in urls:
        res = fetch_url(url, use_fresh_cache=use_fresh_cache)
        if not res or res.get("body") is None:
            last_exc = URLError(str((res or {}).get("error") if isinstance(res, dict) else "network failure"))
            log_event("warn", "standings_fetch_failed", url=url, error=str(last_exc))
            continue
        try:
            payload = json.loads(res["body"].decode("utf-8"))
        except Exception as exc:
            last_exc = exc
            log_event("warn", "standings_json_parse_failed", url=url, error=str(exc))
            continue
        rows = parse_lottery_standings(payload if isinstance(payload, dict) else {})
        if isinstance(rows, list) and len(rows) >= 14:
            sac = next((r for r in rows if isinstance(r, dict) and r.get("team") == "SAC"), None)
            if isinstance(sac, dict):
                log_event(
                    "info",
                    "standings_parsed_sample",
                    team="SAC",
                    wins=int(sac.get("wins", 0)),
                    losses=int(sac.get("losses", 0)),
                    record=str(sac.get("record") or ""),
                )
            return payload, {
                "url": url,
                "http_status": res.get("status"),
                "source": res.get("source"),
                "fetched_at_utc": res.get("fetched_at_utc"),
                "ok": bool(res.get("ok")),
                "validated_rows": len(rows),
            }
        last_exc = ValueError(f"invalid standings payload shape from {url}")
        if isinstance(payload, dict):
            log_event("warn", "standings_payload_invalid", url=url, keys=list(payload.keys())[:20], rows=len(rows))
        else:
            log_event("warn", "standings_payload_invalid", url=url, payload_type=type(payload).__name__, rows=len(rows))
    if last_exc:
        raise last_exc
    raise URLError("No valid standings payload")


def _strip_tags(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_team_abbr(value: str) -> str:
    v = str(value or "").strip().upper()
    mapping = {
        "BRK": "BKN",
        "BKN": "BKN",
        "NJN": "BKN",
        "NOP": "NO",
        "NOH": "NO",
        "NYK": "NY",
        "GSW": "GS",
        "SAS": "SA",
        "PHO": "PHX",
        "PHX": "PHX",
        "LAL": "LAL",
        "LAC": "LAC",
    }
    return mapping.get(v, v)


def load_mock_override(project_root: Path) -> Dict[int, Dict[str, str]]:
    override_path = project_root / MOCK_OVERRIDE_FILE
    if not override_path.exists():
        return {}
    try:
        raw = json.loads(override_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    out: Dict[int, Dict[str, str]] = {}
    if not isinstance(raw, dict) or not isinstance(raw.get("draftOrder"), list):
        log_event("warn", "mock_override_invalid", reason="missing_draftOrder_array")
        return {}

    name_counts: Dict[str, int] = defaultdict(int)

    for row in raw.get("draftOrder", []):
        if not isinstance(row, dict):
            continue
        try:
            pick = int(row.get("slot"))
        except (TypeError, ValueError):
            continue
        if pick < 1 or pick > 14:
            continue

        name = str(row.get("mockPlayerName") or "").strip()
        if not name:
            continue
        if is_blocked_mock_player(name):
            continue
        name_counts[name.lower()] += 1

        url = str(row.get("mockPlayerUrl") or "").strip()
        if url and not url.startswith("https://"):
            log_event("warn", "mock_override_invalid_url", slot=pick, url=url)
            continue
        if url:
            try:
                domain = urlparse(url).netloc.lower()
            except Exception:
                domain = ""
            if domain and "nbadraft.net" not in domain:
                log_event("warn", "mock_override_url_domain_warning", slot=pick, domain=domain, url=url)
        out[pick] = {"name": name, "url": url}

    for n, c in name_counts.items():
        if c > 1:
            log_event("warn", "mock_override_duplicate_name_warning", name=n, count=c)

    if not out:
        log_event("warn", "mock_override_invalid", reason="no_valid_rows")
        return {}
    return out


def _slugify_for_nbadraft(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower())
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


def _nbadraft_profile_url(name: str) -> str:
    slug = _slugify_for_nbadraft(name)
    if not slug:
        return ""
    return f"https://www.nbadraft.net/players/{slug}/"


def _looks_like_player_name(text: str) -> bool:
    if not text:
        return False
    if len(text) < 5 or len(text) > 50:
        return False
    if re.search(r"\d", text):
        return False
    cleaned = re.sub(r"[^A-Za-z' -]", " ", text)
    words = [w for w in cleaned.split() if w]
    if len(words) < 2:
        return False
    return True


def extract_tankathon_lottery_rows() -> List[dict]:
    if SAFE_MODE:
        return []
    try:
        html = fetch_text(TANKATHON_STANDINGS_URL)
    except Exception:
        return []

    # Primary parser: walk /team/<abbr> entries in page order and read nearby record.
    ordered_rows: List[dict] = []
    seen_ordered = set()
    for m in re.finditer(r"/team/([a-z]{2,4})", html, flags=re.IGNORECASE):
        team = _normalize_team_abbr(m.group(1))
        if not team or team in seen_ordered:
            continue
        chunk = _strip_tags(html[m.start(): m.start() + 900])
        rec = re.search(r"\b(\d{1,2})-(\d{1,2})\b", chunk)
        if not rec:
            continue
        wins = int(rec.group(1))
        losses = int(rec.group(2))
        gp = wins + losses
        if gp <= 0:
            continue
        seen_ordered.add(team)
        ordered_rows.append(
            {
                "slot": len(ordered_rows) + 1,
                "team": team,
                "wins": wins,
                "losses": losses,
                "win_pct": wins / gp,
                "record": f"{wins}-{losses}",
            }
        )
        if len(ordered_rows) == 14:
            return ordered_rows

    # Secondary parser: page text with full team names and nearby records.
    text = _strip_tags(html)
    by_position: List[Tuple[int, str, int, int]] = []
    for team_name, abbr in TEAM_NAME_TO_ABBR.items():
        for m in re.finditer(re.escape(team_name), text, flags=re.IGNORECASE):
            start = m.start()
            nearby = text[max(0, start - 120): start + 220]
            rec = re.search(r"\b(\d{1,2})-(\d{1,2})\b", nearby)
            if not rec:
                continue
            wins = int(rec.group(1))
            losses = int(rec.group(2))
            if wins + losses <= 0:
                continue
            by_position.append((start, abbr, wins, losses))
            break
    if by_position:
        by_position.sort(key=lambda x: x[0])
        out2: List[dict] = []
        seen2 = set()
        for _, abbr, wins, losses in by_position:
            if abbr in seen2:
                continue
            seen2.add(abbr)
            gp = wins + losses
            out2.append(
                {
                    "slot": len(out2) + 1,
                    "team": abbr,
                    "wins": wins,
                    "losses": losses,
                    "win_pct": wins / gp if gp else 0.0,
                    "record": f"{wins}-{losses}",
                }
            )
            if len(out2) == 14:
                break
        if len(out2) >= 14:
            return out2

    # Tertiary parser: Next.js-style embedded JSON payloads.
    next_data_match = re.search(
        r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if next_data_match:
        try:
            blob = json.loads(next_data_match.group(1))
        except Exception:
            blob = None
        if isinstance(blob, dict):
            candidates: List[List[dict]] = []

            def _collect_lists(node: object) -> None:
                if isinstance(node, list):
                    parsed_list: List[dict] = []
                    for item in node:
                        if not isinstance(item, dict):
                            continue
                        team_raw = (
                            item.get("team")
                            or item.get("teamAbbrev")
                            or item.get("abbr")
                            or item.get("tricode")
                            or item.get("team_code")
                        )
                        team = _normalize_team_abbr(str(team_raw or ""))
                        wins = item.get("wins", item.get("win", item.get("w")))
                        losses = item.get("losses", item.get("loss", item.get("l")))
                        record = str(item.get("record") or "")
                        if (wins is None or losses is None) and record:
                            m = re.search(r"\b(\d{1,2})-(\d{1,2})\b", record)
                            if m:
                                wins = m.group(1)
                                losses = m.group(2)
                        try:
                            w = int(str(wins))
                            l = int(str(losses))
                        except Exception:
                            continue
                        if not team or len(team) > 4 or w + l <= 0:
                            continue
                        parsed_list.append(
                            {
                                "team": team,
                                "wins": w,
                                "losses": l,
                                "win_pct": w / (w + l),
                                "record": f"{w}-{l}",
                            }
                        )
                    if len(parsed_list) >= 14:
                        candidates.append(parsed_list)
                elif isinstance(node, dict):
                    for v in node.values():
                        _collect_lists(v)

            _collect_lists(blob)
            if candidates:
                best = candidates[0][:14]
                out3: List[dict] = []
                seen3 = set()
                for row in best:
                    t = row["team"]
                    if t in seen3:
                        continue
                    seen3.add(t)
                    out3.append(
                        {
                            "slot": len(out3) + 1,
                            "team": t,
                            "wins": row["wins"],
                            "losses": row["losses"],
                            "win_pct": row["win_pct"],
                            "record": row["record"],
                        }
                    )
                    if len(out3) == 14:
                        break
                if len(out3) >= 14:
                    return out3

    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE | re.DOTALL)
    out: List[dict] = []
    seen_teams = set()
    for row_html in rows:
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, flags=re.IGNORECASE | re.DOTALL)
        if len(cells) < 3:
            continue
        flat_cells = [re.sub(r"\s+", " ", _strip_tags(c)).strip() for c in cells]

        slot = None
        for c in flat_cells[:2]:
            m = re.search(r"^\D*(\d{1,2})\D*$", c)
            if m:
                v = int(m.group(1))
                if 1 <= v <= 14:
                    slot = v
                    break
        if slot is None:
            continue

        record = None
        wins = None
        losses = None
        for c in flat_cells:
            m = re.search(r"\b(\d{1,2})-(\d{1,2})\b", c)
            if m:
                wins = int(m.group(1))
                losses = int(m.group(2))
                record = f"{wins}-{losses}"
                break
        if record is None:
            continue

        team = None
        for c in flat_cells[1:5]:
            c2 = c.replace(".", "").strip().upper()
            if re.fullmatch(r"[A-Z]{2,3}", c2):
                team = c2
                break
        if team is None:
            m = re.search(r"/team/([a-z]{2,3})", row_html, flags=re.IGNORECASE)
            if m:
                team = m.group(1).upper()
        if not team:
            continue
        if team in seen_teams:
            continue
        seen_teams.add(team)

        gp = wins + losses
        win_pct = (wins / gp) if gp else 0.0
        out.append(
            {
                "slot": slot,
                "team": team,
                "wins": wins,
                "losses": losses,
                "win_pct": win_pct,
                "record": record,
            }
        )

    out.sort(key=lambda x: x["slot"])
    if len(out) >= 14:
        return out[:14]

    # Fallback parser for layout changes: infer order from /team/<abbr> and nearby W-L.
    loose: List[dict] = []
    seen_loose = set()
    for m in re.finditer(r"/team/([a-z]{2,4})", html, flags=re.IGNORECASE):
        abbr = _normalize_team_abbr(m.group(1))
        if not abbr or abbr in seen_loose:
            continue
        chunk = _strip_tags(html[m.start(): m.start() + 500])
        rec = re.search(r"\b(\d{1,2})-(\d{1,2})\b", chunk)
        if not rec:
            continue
        wins = int(rec.group(1))
        losses = int(rec.group(2))
        gp = wins + losses
        if gp <= 0:
            continue
        seen_loose.add(abbr)
        loose.append(
            {
                "slot": len(loose) + 1,
                "team": abbr,
                "wins": wins,
                "losses": losses,
                "win_pct": wins / gp,
                "record": f"{wins}-{losses}",
            }
        )
        if len(loose) == 14:
            break
    return loose


def extract_tankathon_mock_map() -> Tuple[Dict[int, Dict[str, str]], Optional[str]]:
    if SAFE_MODE:
        return {}, None
    try:
        html = fetch_text(TANKATHON_MOCK_URL)
    except Exception:
        return {}, None

    picks: Dict[int, Dict[str, str]] = {}
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE | re.DOTALL)
    for row_html in rows:
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, flags=re.IGNORECASE | re.DOTALL)
        if len(cells) < 2:
            continue

        first_cell = _strip_tags(cells[0])
        pick_match = re.search(r"^\D*(\d{1,2})\D*$", first_cell)
        if not pick_match:
            continue
        pick_num = int(pick_match.group(1))
        if pick_num < 1 or pick_num > 30:
            continue

        name = ""
        for cell_html in cells[1:6]:
            links = re.findall(r'<a[^>]*>(.*?)</a>', cell_html, flags=re.IGNORECASE | re.DOTALL)
            candidates = [_strip_tags(link) for link in links]
            if not candidates:
                candidates = [_strip_tags(cell_html)]
            for cand in candidates:
                cand = re.sub(r"\s+", " ", cand).strip()
                if _looks_like_player_name(cand):
                    name = cand
                    break
            if name:
                break

        if not name:
            continue
        picks[pick_num] = {"name": name, "url": _nbadraft_profile_url(name)}

    if picks:
        return picks, TANKATHON_MOCK_URL

    # Fallback: parse likely JSON/script patterns in the page payload.
    script_patterns = [
        r'"pick"\s*:\s*(\d{1,2})[^{}]{0,220}?"name"\s*:\s*"([^"]+)"',
        r'"pick_number"\s*:\s*(\d{1,2})[^{}]{0,220}?"player_name"\s*:\s*"([^"]+)"',
        r'"overallPick"\s*:\s*(\d{1,2})[^{}]{0,220}?"player"\s*:\s*"([^"]+)"',
    ]
    for pattern in script_patterns:
        for match in re.finditer(pattern, html, flags=re.IGNORECASE | re.DOTALL):
            pick_num = int(match.group(1))
            if pick_num < 1 or pick_num > 30:
                continue
            name = re.sub(r"\s+", " ", match.group(2)).strip()
            if not _looks_like_player_name(name):
                continue
            picks[pick_num] = {"name": name, "url": _nbadraft_profile_url(name)}
    if picks:
        return picks, TANKATHON_MOCK_URL

    # Fallback: "1. Player Name" style snippets in page scripts/markup.
    for match in re.finditer(r"(?:Pick\s*)?(\d{1,2})\s*[.\-:]\s*([A-Z][A-Za-z' -]{3,40})", html):
        pick_num = int(match.group(1))
        if pick_num < 1 or pick_num > 30:
            continue
        name = re.sub(r"\s+", " ", match.group(2)).strip()
        if not _looks_like_player_name(name):
            continue
        picks[pick_num] = {"name": name, "url": _nbadraft_profile_url(name)}

    if picks:
        return picks, TANKATHON_MOCK_URL

    # Fallback: text layout like "1 Darryn PetersonSG/PG | Kansas".
    text = _strip_tags(html)
    text = re.sub(r"\s+", " ", text)
    text_patterns = [
        r"\b([1-9]|1\d|2\d|30)\s+(?:Image:\s*[A-Z]{2,3}\s+)?([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+){1,3})\s*(?:PG|SG|SF|PF|C)(?:/[A-Z]{1,2})?\b",
        r"\b([1-9]|1\d|2\d|30)\s+([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+){1,3})\s*(?:PG|SG|SF|PF|C)(?:/[A-Z]{1,2})?\b",
        r"\b([1-9]|1\d|2\d|30)\s+([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+){1,3})\s*\|\s*[A-Za-z]",
    ]
    for pattern in text_patterns:
        for match in re.finditer(pattern, text):
            pick_num = int(match.group(1))
            if pick_num < 1 or pick_num > 30:
                continue
            name = re.sub(r"\s+", " ", match.group(2)).strip()
            if not _looks_like_player_name(name):
                continue
            if pick_num not in picks:
                picks[pick_num] = {"name": name, "url": _nbadraft_profile_url(name)}
    if picks:
        return picks, TANKATHON_MOCK_URL

    # Fallback: infer names from anchor text in page order, then map 1..30.
    name_candidates: List[str] = []
    seen_names = set()
    for anchor in re.findall(r"<a[^>]*>(.*?)</a>", html, flags=re.IGNORECASE | re.DOTALL):
        atext = _strip_tags(anchor)
        atext = re.sub(r"\s+", " ", atext).strip()
        if not atext:
            continue
        m = re.search(r"([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+){1,3})\s+(?:PG|SG|SF|PF|C)\b", atext)
        if not m:
            continue
        name = m.group(1).strip()
        if not _looks_like_player_name(name):
            continue
        lname = name.lower()
        if lname in seen_names:
            continue
        seen_names.add(lname)
        name_candidates.append(name)
        if len(name_candidates) >= 30:
            break
    if name_candidates:
        for i, name in enumerate(name_candidates, start=1):
            picks[i] = {"name": name, "url": _nbadraft_profile_url(name)}
        return picks, TANKATHON_MOCK_URL

    # Last resort: per-pick neighborhood scan for a plausible player name.
    team_words = {
        "hawks", "celtics", "nets", "hornets", "bulls", "cavaliers", "mavericks", "nuggets",
        "pistons", "warriors", "rockets", "pacers", "clippers", "lakers", "grizzlies", "heat",
        "bucks", "timberwolves", "pelicans", "knicks", "thunder", "magic", "76ers", "suns",
        "blazers", "kings", "spurs", "raptors", "jazz", "wizards",
    }
    for pick_num in range(1, 31):
        needle_patterns = [
            rf"\b{pick_num}\b",
            rf"pick[^a-z0-9]{{0,8}}{pick_num}\b",
            rf"\"pick\"\s*:\s*{pick_num}\b",
        ]
        for npat in needle_patterns:
            m = re.search(npat, html, flags=re.IGNORECASE)
            if not m:
                continue
            chunk = _strip_tags(html[m.start(): m.start() + 1200])
            for nm in re.finditer(r"\b([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+){1,3})\b", chunk):
                name = re.sub(r"\s+", " ", nm.group(1)).strip()
                if not _looks_like_player_name(name):
                    continue
                if any(w.lower() in team_words for w in name.split()):
                    continue
                picks[pick_num] = {"name": name, "url": _nbadraft_profile_url(name)}
                break
            if pick_num in picks:
                break
    if picks:
        return picks, TANKATHON_MOCK_URL
    return {}, None


def extract_nbadraft_mock_map() -> Tuple[Dict[int, Dict[str, str]], Optional[str]]:
    if SAFE_MODE:
        return {}, None
    link_pattern = re.compile(
        r'href="([^"]*?/(?:players?|player)/[^"]+)"[^>]*>([^<]+)</a>',
        flags=re.IGNORECASE,
    )
    for url in NBADRAFT_MOCK_URLS:
        try:
            html = fetch_text(url)
        except Exception:
            continue

        picks: Dict[int, Dict[str, str]] = {}
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE | re.DOTALL)
        for row_html in rows:
            if "/players/" not in row_html and "/player/" not in row_html:
                continue

            cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, flags=re.IGNORECASE | re.DOTALL)
            if not cells:
                continue
            first_cell = _strip_tags(cells[0])
            pick_match = re.search(r"^\D*(\d{1,2})\D*$", first_cell)
            if not pick_match:
                continue
            pick_num = int(pick_match.group(1))
            if pick_num < 1 or pick_num > 30:
                continue

            link_match = link_pattern.search(row_html)
            if not link_match:
                continue
            href = link_match.group(1).strip()
            name = _strip_tags(link_match.group(2))
            if not name:
                continue
            abs_url = urljoin(url, href)
            picks[pick_num] = {"name": name, "url": abs_url}

        if picks:
            return picks, url
    return {}, None


def get_mock_map(mock_override: Dict[int, Dict[str, str]]) -> Tuple[Dict[int, Dict[str, str]], Optional[str]]:
    # Compliance mode: no scraping and no embedded defaults for draft mocks.
    if bool(_policy_dict().get("no_scraping")):
        if mock_override:
            return sanitize_mock_map(dict(mock_override)), f"manual:{MOCK_OVERRIDE_FILE}"
        log_event("info", "section_skipped_policy", section="draft", reason="policy_no_scrape_no_manual_override")
        return {}, None

    mock_map, mock_source_url = extract_tankathon_mock_map()
    if not mock_map:
        mock_map, mock_source_url = extract_nbadraft_mock_map()
    if not mock_map:
        mock_map = dict(DEFAULT_MOCK_MAP)
        mock_source_url = "fallback:embedded-default-mock"
    if mock_override:
        mock_map.update(mock_override)
    return sanitize_mock_map(mock_map), mock_source_url


def apply_mock_to_lottery_rows(rows: List[dict], mock_map: Dict[int, Dict[str, str]]) -> List[dict]:
    out: List[dict] = []
    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        r = dict(row)
        slot = r.get("slot")
        try:
            pick = int(slot) if slot is not None else idx
        except (TypeError, ValueError):
            pick = idx
        if mock_map.get(pick, {}).get("name"):
            r["mockPlayerName"] = mock_map[pick]["name"]
        if "url" in mock_map.get(pick, {}):
            r["mockPlayerUrl"] = mock_map[pick].get("url", "")
        out.append(r)
    return out


def iter_dicts(node: object) -> Iterable[dict]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from iter_dicts(value)
    elif isinstance(node, list):
        for item in node:
            yield from iter_dicts(item)


# ---------- Draft section (unchanged model, ESPN) ----------
def collect_standings_entries(node: object) -> List[dict]:
    entries: List[dict] = []
    for item in iter_dicts(node):
        standings = item.get("standings")
        if isinstance(standings, dict):
            raw_entries = standings.get("entries")
            if isinstance(raw_entries, list):
                entries.extend([e for e in raw_entries if isinstance(e, dict)])
    return entries


def stat_map(stats: List[dict]) -> Dict[str, object]:
    mapping: Dict[str, object] = {}
    for item in stats:
        if not isinstance(item, dict):
            continue
        key = item.get("name")
        if key is not None:
            mapping[str(key)] = item.get("value")
    return mapping


def _extract_overall_wins_losses(entry: dict) -> Tuple[Optional[int], Optional[int]]:
    if not isinstance(entry, dict):
        return None, None
    # Primary: stats on entry.
    stats = stat_map(entry.get("stats", [])) if isinstance(entry.get("stats"), list) else {}
    wins = stats.get("wins")
    losses = stats.get("losses")
    try:
        if wins is not None and losses is not None:
            return int(float(wins)), int(float(losses))
    except (TypeError, ValueError):
        pass

    # ESPN often provides multiple records. Choose explicit overall record.
    records = entry.get("records", [])
    if isinstance(records, list):
        overall = None
        for rec in records:
            if not isinstance(rec, dict):
                continue
            rec_name = str(rec.get("name") or rec.get("type") or "").lower()
            if rec_name == "overall":
                overall = rec
                break
        if overall is None:
            for rec in records:
                if not isinstance(rec, dict):
                    continue
                if "overall" in str(rec.get("name") or rec.get("summary") or "").lower():
                    overall = rec
                    break
        if overall is None and records:
            overall = records[0] if isinstance(records[0], dict) else None
        if isinstance(overall, dict):
            r_stats = stat_map(overall.get("stats", [])) if isinstance(overall.get("stats"), list) else {}
            wins = r_stats.get("wins")
            losses = r_stats.get("losses")
            try:
                if wins is not None and losses is not None:
                    return int(float(wins)), int(float(losses))
            except (TypeError, ValueError):
                pass
            summary = str(overall.get("summary") or overall.get("displayValue") or "")
            m = re.search(r"\b(\d{1,2})-(\d{1,2})\b", summary)
            if m:
                return int(m.group(1)), int(m.group(2))
    return None, None


def parse_lottery_standings(payload: dict) -> List[dict]:
    entries = collect_standings_entries(payload)
    teams: List[dict] = []
    for entry in entries:
        team = entry.get("team", {})
        abbr = team.get("abbreviation")
        if not abbr:
            continue
        wins, losses = _extract_overall_wins_losses(entry)
        if wins is None or losses is None:
            continue
        gp = wins + losses
        if gp == 0:
            continue
        stats = stat_map(entry.get("stats", [])) if isinstance(entry.get("stats"), list) else {}
        win_pct = float(stats.get("winPercent", wins / gp) or 0)
        teams.append(
            {
                "team": str(abbr),
                "wins": wins,
                "losses": losses,
                "win_pct": win_pct,
                "record": f"{wins}-{losses}",
            }
        )

    if not teams:
        # Generic fallback for ESPN payload shape changes.
        for node in iter_dicts(payload):
            if not isinstance(node, dict):
                continue
            team_node = node.get("team", {}) if isinstance(node.get("team"), dict) else {}
            abbr = str(
                node.get("abbreviation")
                or team_node.get("abbreviation")
                or team_node.get("abbrev")
                or ""
            ).upper()
            if not abbr or len(abbr) > 4:
                continue
            wins = node.get("wins", node.get("win", node.get("w")))
            losses = node.get("losses", node.get("loss", node.get("l")))
            if wins is None or losses is None:
                stats = node.get("stats")
                if isinstance(stats, list):
                    sm = stat_map(stats)
                    wins = sm.get("wins", sm.get("win", sm.get("w")))
                    losses = sm.get("losses", sm.get("loss", sm.get("l")))
                elif isinstance(stats, dict):
                    wins = stats.get("wins", stats.get("win", stats.get("w")))
                    losses = stats.get("losses", stats.get("loss", stats.get("l")))
            if wins is None or losses is None:
                record_text = str(
                    node.get("summary")
                    or node.get("displayValue")
                    or (team_node.get("record", {}) if isinstance(team_node.get("record"), dict) else {}).get("summary")
                    or ""
                )
                m = re.search(r"\b(\d{1,2})-(\d{1,2})\b", record_text)
                if m:
                    wins = m.group(1)
                    losses = m.group(2)
            try:
                w = int(float(wins))
                l = int(float(losses))
            except (TypeError, ValueError):
                continue
            gp = w + l
            if gp <= 0:
                continue
            teams.append(
                {
                    "team": _normalize_team_abbr(abbr),
                    "wins": w,
                    "losses": l,
                    "win_pct": w / gp,
                    "record": f"{w}-{l}",
                }
            )

    if not teams:
        # Last-resort heuristic parse across raw JSON text.
        try:
            raw = json.dumps(payload)
        except Exception:
            raw = ""
        heuristic: List[dict] = []
        for ab in ALL_TEAM_ABBRS:
            patterns = [
                rf'"abbreviation"\s*:\s*"{ab}"',
                rf'"teamTricode"\s*:\s*"{ab}"',
                rf'"triCode"\s*:\s*"{ab}"',
            ]
            pos = -1
            for pat in patterns:
                m = re.search(pat, raw)
                if m:
                    pos = m.start()
                    break
            if pos < 0:
                continue
            chunk = raw[max(0, pos - 600): pos + 1200]
            win_m = re.search(r'"(?:wins|win|w)"\s*:\s*"?(\d{1,2})"?', chunk)
            loss_m = re.search(r'"(?:losses|loss|l)"\s*:\s*"?(\d{1,2})"?', chunk)
            if not win_m or not loss_m:
                rec_m = re.search(r'"(?:record|summary|displayValue)"\s*:\s*"(\d{1,2})-(\d{1,2})"', chunk)
                if rec_m:
                    w = int(rec_m.group(1))
                    l = int(rec_m.group(2))
                else:
                    continue
            else:
                w = int(win_m.group(1))
                l = int(loss_m.group(1))
            gp = w + l
            if gp <= 0:
                continue
            heuristic.append(
                {
                    "team": ab,
                    "wins": w,
                    "losses": l,
                    "win_pct": w / gp,
                    "record": f"{w}-{l}",
                }
            )
        teams = heuristic

    dedup = {team["team"]: team for team in teams}
    unique_teams = list(dedup.values())
    unique_teams.sort(key=lambda t: (t["win_pct"], t["wins"], -t["losses"]))
    if unique_teams:
        for row in unique_teams[:10]:
            if row.get("team") == "SAC":
                log_event(
                    "info",
                    "standings_parsed_sample",
                    team=row.get("team"),
                    wins=int(row.get("wins", 0)),
                    losses=int(row.get("losses", 0)),
                    record=str(row.get("record") or ""),
                )
                break
    return unique_teams[:14]


def parse_all_team_wins(payload: dict) -> Dict[str, int]:
    entries = collect_standings_entries(payload)
    wins_map: Dict[str, int] = {}
    for entry in entries:
        team = entry.get("team", {})
        abbr = team.get("abbreviation")
        if not abbr:
            continue
        stats = stat_map(entry.get("stats", []))
        wins = int(float(stats.get("wins", 0) or 0))
        wins_map[str(abbr)] = wins
    return wins_map


def _parse_slot_ranges(spec: str) -> List[Tuple[int, int]]:
    text = str(spec or "").lower().replace("and", ",")
    parts = [p.strip() for p in text.split(",") if p.strip()]
    ranges: List[Tuple[int, int]] = []
    for part in parts:
        m = re.match(r"^(\d{1,2})\s*-\s*(\d{1,2})$", part)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            lo, hi = min(a, b), max(a, b)
            ranges.append((lo, hi))
            continue
        if part.isdigit():
            v = int(part)
            ranges.append((v, v))
    return ranges


def _pick_in_ranges(pick: int, ranges: List[Tuple[int, int]]) -> bool:
    for lo, hi in ranges:
        if lo <= pick <= hi:
            return True
    return False


def build_pick_protection_impact(lottery_standings: List[dict], pacers_pick_odds: List[dict], pick_protection: dict) -> dict:
    source_team = str((pick_protection or {}).get("sourceTeam") or "LAC").upper()
    current_slot = None
    for row in lottery_standings:
        if isinstance(row, dict) and str(row.get("team") or "").upper() == source_team:
            try:
                current_slot = int(row.get("slot"))
            except (TypeError, ValueError):
                current_slot = None
            break
    current_top4 = ODDS_BY_SLOT.get(current_slot, {}).get("top4", None) if current_slot else None
    current_first = ODDS_BY_SLOT.get(current_slot, {}).get("first", None) if current_slot else None

    keep_ranges = _parse_slot_ranges((pick_protection or {}).get("keepSlots", ""))
    distributions = compute_lottery_distributions()
    source_distribution = distributions.get(current_slot, {}) if current_slot else {}
    protect_probability = 0.0
    if source_distribution:
        for pick, pct in source_distribution.items():
            if _pick_in_ranges(int(pick), keep_ranges):
                protect_probability += _as_float(pct)
    else:
        # Fallback approximation when slot distribution is unavailable.
        if current_top4 is not None and keep_ranges:
            top4_protected = any(_pick_in_ranges(p, keep_ranges) for p in [1, 2, 3, 4])
            protect_probability = float(current_top4 if top4_protected else max(0.0, 100.0 - current_top4))
    protect_probability = round(protect_probability, 1)
    convey_probability = round(max(0.0, 100.0 - protect_probability), 1)

    return {
        "sourceTeam": source_team,
        "currentSlot": current_slot,
        "currentTop4": current_top4,
        "currentFirst": current_first,
        "conveyProbability": convey_probability,
        "protectProbability": protect_probability,
        "note": "Auto-calculated from standings + fixed lottery odds table",
    }


def extract_lottery_standings_from_espn_html() -> List[dict]:
    if SAFE_MODE:
        return []
    try:
        html = fetch_text(ESPN_STANDINGS_HTML_URL)
    except Exception:
        return []

    slug_map = {
        "brk": "BKN",
        "bkn": "BKN",
        "gsw": "GS",
        "nop": "NO",
        "nyk": "NY",
        "sas": "SA",
        "phx": "PHX",
    }

    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE | re.DOTALL)
    teams: List[dict] = []
    seen = set()
    for row in rows:
        m = re.search(r"/nba/team/_/name/([a-z0-9]{2,4})/", row, flags=re.IGNORECASE)
        if not m:
            continue
        slug = m.group(1).lower()
        abbr = slug_map.get(slug, slug.upper())
        if not abbr or len(abbr) > 4 or abbr in seen:
            continue
        text = _strip_tags(row)
        rec = re.search(r"\b(\d{1,2})-(\d{1,2})\b", text)
        if not rec:
            continue
        wins = int(rec.group(1))
        losses = int(rec.group(2))
        gp = wins + losses
        if gp <= 0:
            continue
        seen.add(abbr)
        teams.append(
            {
                "team": _normalize_team_abbr(abbr),
                "wins": wins,
                "losses": losses,
                "win_pct": wins / gp,
                "record": f"{wins}-{losses}",
            }
        )

    teams.sort(key=lambda t: (t["win_pct"], t["wins"], -t["losses"]))
    out = []
    for i, t in enumerate(teams[:14], start=1):
        out.append({**t, "slot": i})
    return out


def extract_lottery_standings_from_bref() -> List[dict]:
    if SAFE_MODE:
        return []
    now_year = datetime.now(timezone.utc).year
    season_candidates = [now_year, now_year + 1, now_year - 1]
    slug_map = {
        "BRK": "BKN",
        "NOP": "NO",
        "NYK": "NY",
        "GSW": "GS",
        "PHO": "PHX",
        "SAS": "SA",
    }
    for season in season_candidates:
        url = BBREF_STANDINGS_URL_TEMPLATE.format(season=season)
        try:
            html = fetch_text(url)
        except Exception:
            continue
        html_clean = re.sub(r"<!--|-->", "", html)
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html_clean, flags=re.IGNORECASE | re.DOTALL)
        teams: List[dict] = []
        seen = set()
        for row in rows:
            tm = re.search(r'/teams/([A-Z]{3})/' , row)
            if not tm:
                continue
            abbr = slug_map.get(tm.group(1).upper(), tm.group(1).upper())
            if abbr in seen:
                continue
            w_m = re.search(r'data-stat="wins"[^>]*>(\d+)</td>', row, flags=re.IGNORECASE)
            l_m = re.search(r'data-stat="losses"[^>]*>(\d+)</td>', row, flags=re.IGNORECASE)
            if not w_m or not l_m:
                text = _strip_tags(row)
                rec = re.search(r"\b(\d{1,2})-(\d{1,2})\b", text)
                if not rec:
                    continue
                wins = int(rec.group(1))
                losses = int(rec.group(2))
            else:
                wins = int(w_m.group(1))
                losses = int(l_m.group(1))
            gp = wins + losses
            if gp <= 0:
                continue
            seen.add(abbr)
            teams.append(
                {
                    "team": abbr,
                    "wins": wins,
                    "losses": losses,
                    "win_pct": wins / gp,
                    "record": f"{wins}-{losses}",
                }
            )
        if len(teams) >= 14:
            teams.sort(key=lambda t: (t["win_pct"], t["wins"], -t["losses"]))
            out = []
            for i, t in enumerate(teams[:14], start=1):
                out.append({**t, "slot": i})
            return out
    return []


def parse_lottery_standings_nba_alt(payload: dict) -> List[dict]:
    teams = payload.get("league", {}).get("standard", {}).get("teams", [])
    out: List[dict] = []
    if not isinstance(teams, list):
        return out
    for t in teams:
        if not isinstance(t, dict):
            continue
        abbr = str(t.get("teamSitesOnly", {}).get("teamTricode") or t.get("triCode") or "").upper()
        if not abbr:
            continue
        try:
            wins = int(str(t.get("win") or 0))
            losses = int(str(t.get("loss") or 0))
        except ValueError:
            continue
        gp = wins + losses
        if gp == 0:
            continue
        win_pct = wins / gp
        out.append(
            {
                "team": abbr,
                "wins": wins,
                "losses": losses,
                "win_pct": win_pct,
                "record": f"{wins}-{losses}",
            }
        )
    dedup = {team["team"]: team for team in out}
    unique_teams = list(dedup.values())
    unique_teams.sort(key=lambda x: (x["win_pct"], x["wins"], -x["losses"]))
    return unique_teams[:14]


def parse_all_team_wins_nba_alt(payload: dict) -> Dict[str, int]:
    teams = payload.get("league", {}).get("standard", {}).get("teams", [])
    wins: Dict[str, int] = {}
    if not isinstance(teams, list):
        return wins
    for t in teams:
        if not isinstance(t, dict):
            continue
        abbr = str(t.get("teamSitesOnly", {}).get("teamTricode") or t.get("triCode") or "").upper()
        if not abbr:
            continue
        try:
            wins[abbr] = int(str(t.get("win") or 0))
        except ValueError:
            continue
    return wins


def parse_lottery_standings_nba_cdn(payload: dict) -> List[dict]:
    teams: List[dict] = []
    for node in iter_dicts(payload):
        # Handle likely shapes: team + record stats in one node.
        abbr = str(
            node.get("teamTricode")
            or node.get("teamCode")
            or node.get("teamAbbreviation")
            or (node.get("team", {}) if isinstance(node.get("team"), dict) else {}).get("teamTricode")
            or ""
        ).upper()
        if not abbr or len(abbr) > 4:
            continue
        wins_raw = node.get("wins", node.get("win", node.get("w")))
        losses_raw = node.get("losses", node.get("loss", node.get("l")))
        try:
            wins = int(float(wins_raw))
            losses = int(float(losses_raw))
        except (TypeError, ValueError):
            continue
        gp = wins + losses
        if gp <= 0:
            continue
        teams.append(
            {
                "team": abbr,
                "wins": wins,
                "losses": losses,
                "win_pct": wins / gp,
                "record": f"{wins}-{losses}",
            }
        )
    dedup = {t["team"]: t for t in teams}
    unique = list(dedup.values())
    unique.sort(key=lambda x: (x["win_pct"], x["wins"], -x["losses"]))
    return unique[:14]


def parse_all_team_wins_nba_cdn(payload: dict) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in parse_lottery_standings_nba_cdn(payload):
        out[row["team"]] = int(row["wins"])
    return out


def compute_lottery_distributions() -> Dict[int, Dict[int, float]]:
    slots = list(range(1, 15))
    weights = {slot: LOTTERY_WEIGHTS[slot - 1] for slot in slots}
    distributions: Dict[int, Dict[int, float]] = {slot: defaultdict(float) for slot in slots}

    def recurse(remaining: Tuple[int, ...], drawn: Tuple[int, ...], prob: float) -> None:
        if len(drawn) == 4:
            top4 = list(drawn)
            remaining_sorted = sorted([slot for slot in slots if slot not in top4])
            final_pick_by_slot: Dict[int, int] = {}
            for idx, slot in enumerate(top4, start=1):
                final_pick_by_slot[slot] = idx
            for idx, slot in enumerate(remaining_sorted, start=5):
                final_pick_by_slot[slot] = idx
            for slot, pick in final_pick_by_slot.items():
                distributions[slot][pick] += prob
            return

        total = sum(weights[slot] for slot in remaining)
        for slot in remaining:
            next_prob = prob * (weights[slot] / total)
            next_remaining = tuple(s for s in remaining if s != slot)
            recurse(next_remaining, drawn + (slot,), next_prob)

    recurse(tuple(slots), tuple(), 1.0)

    rounded: Dict[int, Dict[int, float]] = {}
    for slot, pick_probs in distributions.items():
        rounded[slot] = {pick: round(prob * 100, 1) for pick, prob in sorted(pick_probs.items())}
    return rounded


def build_upcoming_games(espn_schedule_payload: dict, lottery_slots: Dict[str, int], team_wins: Dict[str, int]) -> List[dict]:
    events = espn_schedule_payload.get("events", [])
    now = datetime.now(timezone.utc)
    pacers_slot = lottery_slots.get(PACERS_ABBR)
    pacers_wins = team_wins.get(PACERS_ABBR)

    candidates = []
    for event in events:
        date_str = event.get("date")
        if not date_str:
            continue
        try:
            tip = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except ValueError:
            continue
        status_name = str(event.get("status", {}).get("type", {}).get("name", "")).lower()
        comp_status = ""
        competitions = event.get("competitions", [])
        if competitions and isinstance(competitions[0], dict):
            comp_status = str(competitions[0].get("status", {}).get("type", {}).get("name", "")).lower()
        is_final = "final" in status_name or "final" in comp_status
        if is_final:
            continue

        if tip < now:
            continue

        competitions = event.get("competitions", [])
        if not competitions:
            continue
        competitors = competitions[0].get("competitors", [])

        pacers_comp = None
        opp_comp = None
        for comp in competitors:
            team = comp.get("team", {})
            if team.get("id") == PACERS_TEAM_ID_STR or team.get("abbreviation") == PACERS_ABBR:
                pacers_comp = comp
            else:
                opp_comp = comp

        if not pacers_comp or not opp_comp:
            continue

        opp_abbr = opp_comp.get("team", {}).get("abbreviation", "UNK")
        opp_name = opp_comp.get("team", {}).get("displayName", opp_abbr)
        opp_slot = lottery_slots.get(opp_abbr)
        opp_wins = team_wins.get(opp_abbr)
        is_key = False
        why = ""
        if opp_slot is not None and pacers_slot is not None and pacers_wins is not None and opp_wins is not None:
            if abs(opp_wins - pacers_wins) <= 5:
                is_key = True
                why = f"{opp_name} is within 5 wins of Indiana ({opp_wins} vs {pacers_wins}). Current lottery slot comparison: {opp_slot} vs {pacers_slot}."

        location = "Home" if pacers_comp.get("homeAway") == "home" else "Away"

        candidates.append(
            {
                "date_obj": tip,
                "date": _format_display_date(tip, "%a, %b %-d, %Y"),
                "opponent": opp_name,
                "location": location,
                "isKey": is_key,
                "why": why,
            }
        )

    candidates.sort(key=lambda g: g["date_obj"])
    picked = []
    seen = set()
    for item in candidates:
        key = (item["date"], item["opponent"])
        if key in seen:
            continue
        seen.add(key)
        picked.append({k: item[k] for k in ["date", "opponent", "location", "isKey", "why"]})
        if len(picked) == 10:
            break

    return picked


def build_upcoming_games_from_nba_schedule(schedule_payload: dict, lottery_slots: Dict[str, int], team_wins: Dict[str, int]) -> List[dict]:
    now = datetime.now(timezone.utc)
    pacers_slot = lottery_slots.get(PACERS_ABBR)
    pacers_wins = team_wins.get(PACERS_ABBR)
    candidates: List[dict] = []

    for game in _flatten_nba_games(schedule_payload):
        home = game.get("homeTeam", {}) if isinstance(game.get("homeTeam"), dict) else {}
        away = game.get("awayTeam", {}) if isinstance(game.get("awayTeam"), dict) else {}
        home_abbr = _get_team_tricode(home)
        away_abbr = _get_team_tricode(away)
        if home_abbr != PACERS_ABBR and away_abbr != PACERS_ABBR:
            continue
        if _is_final_game(game):
            continue
        game_dt = _game_date_utc(game)
        if not game_dt or game_dt < now:
            continue

        pacers_home = home_abbr == PACERS_ABBR
        opp = away if pacers_home else home
        opp_abbr = _get_team_tricode(opp) or "UNK"
        opp_name = str(opp.get("teamName") or opp.get("teamTricode") or opp_abbr)
        location = "Home" if pacers_home else "Away"

        is_key = False
        why = ""
        opp_slot = lottery_slots.get(opp_abbr)
        opp_wins = team_wins.get(opp_abbr)
        if opp_slot is not None and pacers_slot is not None and pacers_wins is not None and opp_wins is not None:
            if abs(opp_wins - pacers_wins) <= 5:
                is_key = True
                why = f"{opp_name} is within 5 wins of Indiana ({opp_wins} vs {pacers_wins}). Current lottery slot comparison: {opp_slot} vs {pacers_slot}."

        candidates.append(
            {
                "date_obj": game_dt,
                "date": _format_display_date(game_dt, "%a, %b %-d, %Y"),
                "opponent": opp_name,
                "location": location,
                "isKey": is_key,
                "why": why,
            }
        )

    candidates.sort(key=lambda x: x["date_obj"])
    out: List[dict] = []
    seen = set()
    for g in candidates:
        k = (g["date"], g["opponent"])
        if k in seen:
            continue
        seen.add(k)
        out.append({k2: g[k2] for k2 in ["date", "opponent", "location", "isKey", "why"]})
        if len(out) == 10:
            break
    return out


# ---------- NBA "What You Missed" ----------
def _try_parse_dt(value: object) -> Optional[datetime]:
    if not value:
        return None
    s = str(value)
    s = s.replace("Z", "+00:00")
    for fmt in [None, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"]:
        try:
            if fmt is None:
                return datetime.fromisoformat(s)
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _to_local_display_dt(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(LOCAL_DISPLAY_TZ)


def _format_display_date(dt: datetime, fmt: str) -> str:
    return _to_local_display_dt(dt).strftime(fmt)


def _try_parse_game_date(value: object) -> Optional[datetime]:
    dt = _try_parse_dt(value)
    if dt is not None:
        return dt
    s = str(value or "").strip()
    if not s:
        return None
    for fmt in ["%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y"]:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _parse_game_date_label(label: str) -> Optional[datetime]:
    s = str(label or "").strip()
    if not s:
        return None
    # Example: "Sun, Feb 22, 2026"
    m = re.match(r"^[A-Za-z]{3},\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})$", s)
    if not m:
        return None
    month_map = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
    }
    mon = month_map.get(m.group(1))
    if mon is None:
        return None
    day = int(m.group(2))
    year = int(m.group(3))
    return datetime(year, mon, day, tzinfo=timezone.utc)


def normalize_upcoming_games(games: List[dict]) -> List[dict]:
    now_local = datetime.now(LOCAL_DISPLAY_TZ)
    out: List[Tuple[datetime, dict]] = []
    for g in games:
        if not isinstance(g, dict):
            continue
        dt = _parse_game_date_label(str(g.get("date") or ""))
        if dt is None:
            continue
        # Keep today/future only.
        if dt.date() < now_local.date():
            continue
        g2 = dict(g)
        if "isKey" not in g2:
            why = str(g2.get("why") or "")
            g2["isKey"] = bool(re.search(r"within\s+5\s+wins", why, flags=re.IGNORECASE))
        out.append((dt, g2))
    out.sort(key=lambda x: x[0])
    cleaned = []
    seen = set()
    for _, g in out:
        key = (g.get("date"), g.get("opponent"))
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(g)
        if len(cleaned) == 10:
            break
    return cleaned


def _game_date_utc(game: dict) -> Optional[datetime]:
    for key in ["gameDateTimeUTC", "gameDateUTC", "gameDateEst", "gameDate"]:
        dt = _try_parse_dt(game.get(key))
        if dt:
            return dt.astimezone(timezone.utc)
    return None


def _is_final_game(game: dict) -> bool:
    status_num = game.get("gameStatus")
    try:
        if int(status_num) == 3:
            return True
    except (TypeError, ValueError):
        pass
    status_text = str(game.get("gameStatusText") or "").lower()
    return "final" in status_text


def _get_team_tricode(team: dict) -> str:
    return str(team.get("teamTricode") or team.get("teamCode") or "").upper()


def _flatten_nba_games(schedule_payload: dict) -> List[dict]:
    league = schedule_payload.get("leagueSchedule", {})
    dates = league.get("gameDates", [])
    games: List[dict] = []
    for day in dates:
        day_games = day.get("games", []) if isinstance(day, dict) else []
        for game in day_games:
            if isinstance(game, dict):
                games.append(game)
    return games


def _find_latest_completed_pacers_game(schedule_payload: dict) -> Optional[dict]:
    now = datetime.now(timezone.utc)
    candidates: List[Tuple[datetime, dict]] = []
    backup_candidates: List[Tuple[datetime, dict]] = []

    for game in _flatten_nba_games(schedule_payload):
        home = game.get("homeTeam", {})
        away = game.get("awayTeam", {})
        if _get_team_tricode(home) != PACERS_ABBR and _get_team_tricode(away) != PACERS_ABBR:
            continue
        game_dt = _game_date_utc(game)
        if not game_dt or game_dt >= now:
            continue
        backup_candidates.append((game_dt, game))
        if not _is_final_game(game):
            continue
        candidates.append((game_dt, game))

    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]
    if backup_candidates:
        backup_candidates.sort(key=lambda x: x[0], reverse=True)
        return backup_candidates[0][1]
    return None


def _as_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _is_number(value: object) -> bool:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return False
    return n == n


def _minutes_to_seconds(value: object) -> float:
    s = str(value or "")
    iso = re.match(r"^PT(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$", s)
    if iso:
        mins = int(iso.group(1) or 0)
        secs = float(iso.group(2) or 0.0)
        return mins * 60 + secs
    if ":" in s:
        parts = s.split(":")
        if len(parts) == 2:
            try:
                return int(parts[0]) * 60 + int(parts[1])
            except ValueError:
                return 0.0
    return _as_float(s) * 60


def _format_minutes(value: object) -> str:
    total = _minutes_to_seconds(value)
    mins = int(total // 60)
    secs = int(round(total - mins * 60))
    if secs == 60:
        mins += 1
        secs = 0
    return f"{mins}:{secs:02d}"


def _nba_headshot(person_id: object) -> str:
    pid = str(person_id or "").strip()
    if not pid.isdigit():
        return ""
    return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"


def _norm_name(value: object) -> str:
    return re.sub(r"[^a-z]", "", str(value or "").lower())


def _find_row_for_target(rows: List[dict], target: str) -> Optional[dict]:
    tnorm = _norm_name(target)
    if not tnorm:
        return None
    for r in rows:
        if not isinstance(r, dict):
            continue
        if _norm_name(r.get("name")) == tnorm:
            return r
    # Fallback: last-name + first initial matching.
    parts = [p for p in re.sub(r"[^A-Za-z ]", " ", target).split() if p]
    if not parts:
        return None
    first = parts[0].lower()
    last = parts[-1].lower()
    first_initial = first[:1]
    for r in rows:
        if not isinstance(r, dict):
            continue
        name = str(r.get("name") or "")
        clean = re.sub(r"[^A-Za-z ]", " ", name).lower()
        toks = [x for x in clean.split() if x]
        if not toks:
            continue
        if last not in toks:
            continue
        if toks[0].startswith(first_initial):
            return r
    return None


def extract_latest_pacers_highlight_video() -> str:
    playlist_id = str(_youtube_cfg().get("uploads_playlist_id") or "").strip()
    if not playlist_id:
        return ""
    return f"https://www.youtube.com/embed?listType=playlist&list={playlist_id}"


def fetch_youtube_data_api_latest() -> Tuple[Optional[dict], Optional[str]]:
    if not bool(_source_dict().get("youtube_data_api")):
        return None, "youtube_data_api_disabled"
    api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    if not api_key:
        return None, "missing_api_key"
    channel_id = str(_youtube_cfg().get("channel_id") or "").strip()
    if not channel_id:
        return None, "missing_channel_id"
    url = (
        "https://www.googleapis.com/youtube/v3/search"
        f"?part=snippet&channelId={channel_id}&maxResults=1&order=date&type=video&key={api_key}"
    )
    payload = fetch_json(url)
    items = payload.get("items", []) if isinstance(payload, dict) else []
    if not items or not isinstance(items[0], dict):
        return None, "no_data_yet"
    item = items[0]
    vid = str(item.get("id", {}).get("videoId") or "").strip()
    snippet = item.get("snippet", {}) if isinstance(item.get("snippet"), dict) else {}
    title = str(snippet.get("title") or "").strip()
    published = str(snippet.get("publishedAt") or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", vid):
        return None, "no_data_yet"
    return {"videoId": vid, "title": title, "publishedAt": published}, None


def _fetch_json_no_stale(url: str) -> Tuple[Optional[dict], dict]:
    res = fetch_url(url, allow_stale_cache=False)
    meta = {
        "url": url,
        "source": (res or {}).get("source") if isinstance(res, dict) else None,
        "http_status": (res or {}).get("status") if isinstance(res, dict) else None,
    }
    if not res or res.get("body") is None:
        meta["error"] = str((res or {}).get("error") if isinstance(res, dict) else "fetch_failed")
        return None, meta
    try:
        payload = json.loads(res["body"].decode("utf-8"))
        return payload if isinstance(payload, dict) else None, meta
    except Exception as exc:
        meta["error"] = f"json_parse_failed:{exc}"
        return None, meta


def _score_display(score: object) -> str:
    if isinstance(score, dict):
        display = str(score.get("displayValue") or "").strip()
        if display:
            return display
        value = score.get("value")
        if value is not None:
            return str(value)
        return "-"
    if score is None:
        return "-"
    s = str(score).strip()
    return s or "-"


def _extract_schedule_events(payload: dict) -> List[dict]:
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("events"), list):
        return [e for e in payload.get("events", []) if isinstance(e, dict)]
    schedule = payload.get("schedule", {})
    if isinstance(schedule, dict) and isinstance(schedule.get("events"), list):
        return [e for e in schedule.get("events", []) if isinstance(e, dict)]
    team = payload.get("team", {})
    if isinstance(team, dict) and isinstance(team.get("events"), list):
        return [e for e in team.get("events", []) if isinstance(e, dict)]
    return []


def _is_pacers_event(event: dict) -> bool:
    competitions = event.get("competitions", [])
    if not isinstance(competitions, list) or not competitions or not isinstance(competitions[0], dict):
        return False
    competitors = competitions[0].get("competitors", [])
    if not isinstance(competitors, list):
        return False
    for comp in competitors:
        if not isinstance(comp, dict):
            continue
        team = comp.get("team", {}) if isinstance(comp.get("team"), dict) else {}
        if str(team.get("id") or "").strip() == PACERS_TEAM_ID_STR:
            return True
        if str(team.get("abbreviation") or "").upper() == PACERS_ABBR:
            return True
    return False


def _is_pacers_game_in_progress(schedule_payload: dict) -> bool:
    now_utc = datetime.now(timezone.utc)
    for event in _extract_schedule_events(schedule_payload if isinstance(schedule_payload, dict) else {}):
        if not _is_pacers_event(event):
            continue
        competitions = event.get("competitions", [])
        comp0 = competitions[0] if isinstance(competitions, list) and competitions and isinstance(competitions[0], dict) else {}
        event_type = event.get("status", {}).get("type", {}) if isinstance(event.get("status"), dict) else {}
        comp_type = comp0.get("status", {}).get("type", {}) if isinstance(comp0.get("status"), dict) else {}
        event_completed = event_type.get("completed")
        comp_completed = comp_type.get("completed")
        if event_completed is True or comp_completed is True:
            continue

        state = str(comp_type.get("state") or event_type.get("state") or "").strip().lower()
        name = str(comp_type.get("name") or event_type.get("name") or "").strip().lower()
        detail = str(comp_type.get("detail") or event_type.get("detail") or comp0.get("status", {}).get("displayClock") or "").strip().lower()
        signal_text = f"{state} {name} {detail}"
        if "final" in signal_text or "postponed" in signal_text or "canceled" in signal_text:
            continue
        if state == "pre" or "scheduled" in signal_text:
            continue
        if (
            state == "in"
            or "in progress" in signal_text
            or "halftime" in signal_text
            or "q1" in signal_text
            or "q2" in signal_text
            or "q3" in signal_text
            or "q4" in signal_text
            or "ot" in signal_text
            or "live" in signal_text
        ):
            return True

        dt = _try_parse_dt(event.get("date"))
        if dt is not None:
            dt_utc = dt.astimezone(timezone.utc)
            # Fallback heuristic for feeds that lag status updates.
            if dt_utc <= now_utc <= dt_utc + timedelta(hours=6):
                return True
    return False


def _is_completed_espn_event(event: dict) -> bool:
    competitions = event.get("competitions", [])
    if isinstance(competitions, list) and competitions and isinstance(competitions[0], dict):
        completed = (
            competitions[0]
            .get("status", {})
            .get("type", {})
            .get("completed")
        )
        if completed is True:
            return True
    completed = event.get("status", {}).get("type", {}).get("completed")
    return completed is True


def _event_datetime(event: dict) -> datetime:
    dt = _try_parse_dt(event.get("date"))
    if dt is not None:
        return dt.astimezone(timezone.utc)
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def _first_completed_event(events: List[dict]) -> Optional[dict]:
    completed = [e for e in events if _is_completed_espn_event(e)]
    if not completed:
        return None
    for event in completed:
        event_id = str(event.get("id") or "").strip()
        raw_date = event.get("date")
        sort_dt = _event_datetime(event)
        print(
            f"FINAL Pacers event candidate: id={event_id!r}, date_field={raw_date!r}, sort_key_utc={sort_dt.isoformat()}",
            flush=True,
        )
    completed.sort(key=_event_datetime, reverse=True)
    latest = completed[0]
    latest_id = str(latest.get("id") or "").strip()
    latest_raw_date = latest.get("date")
    latest_sort_dt = _event_datetime(latest)
    print(
        f"Selected latest FINAL Pacers event: id={latest_id!r}, date_field={latest_raw_date!r}, sort_key_utc={latest_sort_dt.isoformat()}",
        flush=True,
    )
    return latest


def _latest_completed_pacers_event_from_scoreboard(days_back: int = 14) -> Optional[dict]:
    now_local = datetime.now(LOCAL_DISPLAY_TZ)
    candidates: List[Tuple[datetime, dict]] = []
    seen_ids = set()
    for delta in range(max(0, days_back) + 1):
        day = (now_local - timedelta(days=delta)).strftime("%Y%m%d")
        url = ESPN_SCOREBOARD_URL_TEMPLATE.format(yyyymmdd=day)
        res = fetch_url(url, use_fresh_cache=False)
        if not res or res.get("body") is None:
            continue
        try:
            payload = json.loads(res["body"].decode("utf-8"))
        except Exception:
            continue
        for event in _extract_schedule_events(payload if isinstance(payload, dict) else {}):
            if not _is_pacers_event(event):
                continue
            if not _is_completed_espn_event(event):
                continue
            event_id = str(event.get("id") or "").strip()
            if event_id in seen_ids:
                continue
            seen_ids.add(event_id)
            sort_dt = _event_datetime(event)
            print(
                f"FINAL Pacers scoreboard candidate: id={event_id!r}, date_field={event.get('date')!r}, "
                f"sort_key_utc={sort_dt.isoformat()}, scoreboard_date={day}",
                flush=True,
            )
            candidates.append((sort_dt, event))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    latest = candidates[0][1]
    latest_id = str(latest.get("id") or "").strip()
    latest_sort = _event_datetime(latest)
    print(
        f"Selected latest FINAL Pacers event from scoreboard: id={latest_id!r}, "
        f"date_field={latest.get('date')!r}, sort_key_utc={latest_sort.isoformat()}",
        flush=True,
    )
    return latest


def _latest_completed_pacers_event(schedule_payload: Optional[dict], days_back: int = 14) -> Optional[dict]:
    schedule_latest: Optional[dict] = None
    if isinstance(schedule_payload, dict):
        schedule_events = [
            e
            for e in _extract_schedule_events(schedule_payload)
            if _is_pacers_event(e) and _is_completed_espn_event(e)
        ]
        schedule_latest = _first_completed_event(schedule_events)
    scoreboard_latest = _latest_completed_pacers_event_from_scoreboard(days_back=days_back)
    if schedule_latest and scoreboard_latest:
        return (
            schedule_latest
            if _event_datetime(schedule_latest) >= _event_datetime(scoreboard_latest)
            else scoreboard_latest
        )
    return schedule_latest or scoreboard_latest


def _espn_headshot_url(athlete_id: str) -> str:
    return f"https://a.espncdn.com/i/headshots/nba/players/full/{athlete_id}.png"


def _athlete_stat_map(stat_block: dict, athlete_row: dict) -> Dict[str, str]:
    labels = stat_block.get("labels")
    names = stat_block.get("names")
    keys = stat_block.get("keys")
    headers: List[str] = []
    if isinstance(labels, list) and labels:
        headers = [str(x or "").strip() for x in labels]
    elif isinstance(names, list) and names:
        headers = [str(x or "").strip() for x in names]
    elif isinstance(keys, list) and keys:
        headers = [str(x or "").strip() for x in keys]
    raw_stats = athlete_row.get("stats")
    out: Dict[str, str] = {}
    if isinstance(raw_stats, list):
        for idx, value in enumerate(raw_stats):
            if idx < len(headers):
                out[headers[idx]] = str(value)
    elif isinstance(raw_stats, dict):
        for k, v in raw_stats.items():
            out[str(k)] = str(v)
    return out


def _boxscore_rows(summary_payload: dict) -> List[dict]:
    rows: List[dict] = []
    boxscore = summary_payload.get("boxscore", {})
    players_blocks = boxscore.get("players", []) if isinstance(boxscore, dict) else []
    for team_block in players_blocks:
        if not isinstance(team_block, dict):
            continue
        team = team_block.get("team", {}) if isinstance(team_block.get("team"), dict) else {}
        team_abbr = str(team.get("abbreviation") or "").upper()
        team_name = str(team.get("displayName") or team.get("name") or team_abbr)
        stat_blocks = team_block.get("statistics", []) if isinstance(team_block.get("statistics"), list) else []
        for stat_block in stat_blocks:
            if not isinstance(stat_block, dict):
                continue
            athletes = stat_block.get("athletes", []) if isinstance(stat_block.get("athletes"), list) else []
            for athlete in athletes:
                if not isinstance(athlete, dict):
                    continue
                athlete_obj = athlete.get("athlete", {}) if isinstance(athlete.get("athlete"), dict) else {}
                athlete_id = str(
                    athlete_obj.get("id")
                    or athlete.get("athleteId")
                    or athlete.get("playerId")
                    or ""
                ).strip()
                if not athlete_id:
                    continue
                stat_map = _athlete_stat_map(stat_block, athlete)
                rows.append(
                    {
                        "athleteId": athlete_id,
                        "name": str(athlete_obj.get("displayName") or athlete_obj.get("shortName") or athlete.get("displayName") or ""),
                        "teamAbbr": team_abbr,
                        "teamName": team_name,
                        "stats": stat_map,
                    }
                )
    return rows


def _parse_float(value: object) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return -1e9


def _leader_entry(rows: List[dict], stat_label: str, source_labels: List[str]) -> Optional[dict]:
    best: Optional[dict] = None
    best_val = -1e9
    for row in rows:
        stats = row.get("stats", {})
        if not isinstance(stats, dict):
            continue
        value = None
        for key in source_labels:
            if key in stats and str(stats.get(key)).strip() != "":
                value = stats.get(key)
                break
        if value is None:
            continue
        val_num = _parse_float(value)
        if val_num > best_val:
            best_val = val_num
            best = row
    if not best:
        return None
    stats = best.get("stats", {})
    out_val = "-"
    for key in source_labels:
        if key in stats:
            out_val = str(stats.get(key))
            break
    athlete_id = str(best.get("athleteId") or "").strip()
    return {
        "stat": stat_label,
        "value": out_val,
        "player": best.get("name") or "Unknown",
        "athleteId": athlete_id,
        "headshot": _espn_headshot_url(athlete_id),
    }


def _event_opponent_and_scoreline(event: dict) -> Tuple[str, str, str, str]:
    competitions = event.get("competitions", [])
    if not isinstance(competitions, list) or not competitions or not isinstance(competitions[0], dict):
        return "-", "-", "-", "Loss"
    competitors = competitions[0].get("competitors", [])
    pacers = None
    opponent = None
    for comp in competitors:
        if not isinstance(comp, dict):
            continue
        team = comp.get("team", {}) if isinstance(comp.get("team"), dict) else {}
        abbr = str(team.get("abbreviation") or "").upper()
        if abbr == PACERS_ABBR:
            pacers = comp
        else:
            opponent = comp
    if not pacers or not opponent:
        return "-", "-", "-", "Loss"
    opponent_name = str((opponent.get("team", {}) or {}).get("displayName") or (opponent.get("team", {}) or {}).get("name") or "-")
    pacers_score = _score_display(pacers.get("score"))
    opp_score = _score_display(opponent.get("score"))
    try:
        outcome = "Win" if float(pacers_score) > float(opp_score) else "Loss"
    except Exception:
        outcome = "Loss"
    return opponent_name, pacers_score, opp_score, outcome


def extract_what_you_missed(espn_schedule_payload: Optional[dict] = None) -> dict:
    completed_event = _latest_completed_pacers_event(espn_schedule_payload, days_back=14)
    if not completed_event:
        return {"summary": "Latest completed game not available.", "leaders": [], "_error": "no_completed_game_found"}

    event_id = str(completed_event.get("id") or "").strip()
    if not event_id:
        return {"summary": "Latest completed game not available.", "leaders": [], "_error": "missing_event_id"}
    summary_url = ESPN_SUMMARY_URL_TEMPLATE.format(event_id=event_id)
    summary_payload, summary_meta = _fetch_json_no_stale(summary_url)
    if not isinstance(summary_payload, dict):
        return {"summary": "Latest completed game not available.", "leaders": [], "_error": summary_meta.get("error") or "boxscore_fetch_failed"}

    all_rows = _boxscore_rows(summary_payload)
    pacers_rows = [r for r in all_rows if str(r.get("teamAbbr") or "").upper() == PACERS_ABBR]
    leaders: List[dict] = []
    pts = _leader_entry(pacers_rows, "PTS", ["PTS", "Points"])
    reb = _leader_entry(pacers_rows, "REB", ["REB", "Rebounds"])
    ast = _leader_entry(pacers_rows, "AST", ["AST", "Assists"])
    mins = _leader_entry(pacers_rows, "MIN", ["MIN", "Minutes"])
    for item in [pts, reb, ast, mins]:
        if isinstance(item, dict):
            leaders.append(item)

    game_dt = _event_datetime(completed_event)
    opp, pacers_score, opp_score, outcome = _event_opponent_and_scoreline(completed_event)
    summary = f"{_format_display_date(game_dt, '%B %-d, %Y')} vs {opp}: {outcome} ({pacers_score}-{opp_score})."
    if not leaders:
        return {"summary": summary, "leaders": [], "_error": "leader_extraction_failed"}
    return {"summary": summary, "leaders": leaders}


# ---------- Build payload ----------
def build_output(standings_payload: dict, espn_schedule_payload: dict, mock_override: Dict[int, Dict[str, str]]) -> dict:
    lottery_rows = parse_lottery_standings(standings_payload)
    all_team_wins = parse_all_team_wins(standings_payload)
    distributions = compute_lottery_distributions()
    mock_map = dict(mock_override or {})
    mock_source_url = f"manual:{MOCK_OVERRIDE_FILE}" if mock_map else "manual:none"
    if len(lottery_rows) != 14:
        log_event("warn", "draft_lottery_team_count_invalid", count=len(lottery_rows))

    lottery_standings = []
    team_to_slot = {}
    team_to_wins: Dict[str, int] = {}
    for i, row in enumerate(lottery_rows, start=1):
        slot = int(row.get("slot", i))
        odds = ODDS_BY_SLOT.get(slot, {"top4": 0.0, "first": 0.0})
        lottery_standings.append(
            {
                "slot": slot,
                "team": row["team"],
                "record": row["record"],
                "top4": float(odds["top4"]),
                "first": float(odds["first"]),
                "mockPlayerName": mock_map.get(slot, {}).get("name") if slot in mock_map else None,
                "mockPlayerUrl": mock_map.get(slot, {}).get("url") if slot in mock_map else None,
            }
        )
        team_to_slot[row["team"]] = slot
        team_to_wins[row["team"]] = row["wins"]

    pacers_slot = team_to_slot.get(PACERS_ABBR)
    pacers_pick_odds = []
    if pacers_slot is not None:
        slot_distribution = distributions[pacers_slot]
        pacers_pick_odds = [{"pick": pick, "pct": pct} for pick, pct in sorted(slot_distribution.items()) if pct > 0]
    pick_protection_impact = build_pick_protection_impact(lottery_standings, pacers_pick_odds, PICK_PROTECTION)

    what_you_missed = extract_what_you_missed(espn_schedule_payload if isinstance(espn_schedule_payload, dict) else None)
    what_you_missed["highlightVideoUrl"] = extract_latest_pacers_highlight_video()
    upcoming_games = build_upcoming_games(espn_schedule_payload, team_to_slot, all_team_wins)
    key_games = [g for g in upcoming_games if g.get("isKey")]

    return {
        "asOf": datetime.now().strftime("%B %-d, %Y"),
        "whatYouMissed": what_you_missed,
        "lotteryStandings": lottery_standings,
        "pacersPickOdds": pacers_pick_odds,
        "pickProtection": PICK_PROTECTION,
        "pickProtectionImpact": pick_protection_impact,
        "upcomingGames": upcoming_games,
        "keyGames": key_games,
        "meta": {
            "pacersSlot": pacers_slot,
            "updatedAtUtc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "sources": {
                "draftStandings": ESPN_STANDINGS_URL,
                "draftSchedule": ESPN_SCHEDULE_URL,
                "nbaSchedule": NBA_SCHEDULE_URL,
                "nbaBoxscoreTemplate": NBA_BOXSCORE_URL,
                "nbaPlayers": NBA_PLAYERS_URL,
                "mockSourceUsed": mock_source_url,
                "nbadraftProfilesFromNames": True,
                "mockOverrideFile": MOCK_OVERRIDE_FILE if mock_override else None,
            },
        },
    }


def _rebuild_draft_from_rows(
    lottery_rows: List[dict],
    mock_map: Dict[int, Dict[str, str]],
) -> Tuple[List[dict], List[dict], Dict[str, int]]:
    distributions = compute_lottery_distributions()
    lottery_standings: List[dict] = []
    team_to_slot: Dict[str, int] = {}
    all_team_wins: Dict[str, int] = {}
    for i, row in enumerate(lottery_rows, start=1):
        slot = int(row.get("slot", i))
        probs = distributions.get(slot, distributions[i])
        lottery_standings.append(
            {
                "slot": slot,
                "team": row["team"],
                "record": row["record"],
                "top4": round(sum(v for k, v in probs.items() if k <= 4), 1),
                "first": round(probs.get(1, 0.0), 1),
                "mockPlayerName": mock_map.get(slot, {}).get("name"),
                "mockPlayerUrl": mock_map.get(slot, {}).get("url"),
            }
        )
        team_to_slot[row["team"]] = slot
        all_team_wins[row["team"]] = int(row.get("wins", 0))

    pacers_pick_odds: List[dict] = []
    pacers_slot = team_to_slot.get(PACERS_ABBR)
    if pacers_slot is not None:
        slot_distribution = distributions[pacers_slot]
        pacers_pick_odds = [{"pick": pick, "pct": pct} for pick, pct in sorted(slot_distribution.items()) if pct > 0]
    return lottery_standings, pacers_pick_odds, all_team_wins


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh dashboard data.")
    parser.add_argument("--safe", action="store_true", help="Disable HTML/PDF scraping; use structured endpoints only.")
    parser.add_argument("--fast", action="store_true", help="Skip expensive fallbacks where possible.")
    parser.add_argument("--test-fail", choices=["standings"], default="", help="Force a section failure for reliability testing.")
    parser.add_argument("--test-invalid", choices=["standings"], default="", help="Force invalid parsed section data after fetch for validation fallback testing.")
    parser.add_argument("--test-429", choices=["espn"], default="", help="Deterministic offline simulation of one 429 retry cycle.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    init_runtime(args)
    started_at = time.time()
    out_file = Path(__file__).resolve().parents[1] / "data.json"
    print(f"Starting update_data.py (safe={SAFE_MODE}, fast={FAST_MODE})...", flush=True)
    log_event("info", "run_start", safe=SAFE_MODE, fast=FAST_MODE)
    if TEST_429_PROVIDER:
        test_url = f"test429://{TEST_429_PROVIDER}/scoreboard"
        res = fetch_url(test_url, allow_stale_cache=False)
        if not res or not res.get("ok"):
            msg = f"simulated 429 test failed for {TEST_429_PROVIDER}"
            RUN_ERRORS.append({"section": "scoreboard", "source": TEST_429_PROVIDER, "message": msg, "time_utc": utc_now_iso()})
            log_event("error", "test_429_failed", provider=TEST_429_PROVIDER, result=res)
        else:
            log_event("info", "test_429_success", provider=TEST_429_PROVIDER, status=res.get("status"))
    cache_probe_url = os.getenv("PACERS_CACHE_PROBE_URL", "").strip()
    if cache_probe_url:
        _ = fetch_url(cache_probe_url, allow_stale_cache=True)
        log_event("info", "cache_probe_complete", url=cache_probe_url)
    existing_payload: dict = {}
    if out_file.exists():
        try:
            existing_payload = json.loads(out_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing_payload = {}

    payload: dict = {}
    refresh_status = {"draft": "ok", "whatYouMissed": "ok"}
    espn_schedule: dict = {}
    mock_override = load_mock_override(Path(__file__).resolve().parents[1])
    if bool(_policy_dict().get("no_scraping")):
        if not str(_youtube_cfg().get("uploads_playlist_id") or "").strip():
            log_event("info", "section_skipped_policy", section="videos", reason="missing_uploads_playlist_id")
        else:
            log_event("info", "section_skipped_policy", section="videos", reason="youtube_official_playlist_embed_no_fetch")
    mock_source_url = f"manual:{MOCK_OVERRIDE_FILE}" if mock_override else "manual:none"

    standings_fetch_meta: Dict[str, object] = {}
    try:
        print("Fetching draft base data (compliant APIs/manual only)...", flush=True)
        standings, standings_fetch_meta = fetch_valid_standings_payload(ESPN_STANDINGS_URLS, use_fresh_cache=False)
        log_event(
            "info",
            "standings_url_used",
            url=standings_fetch_meta.get("url"),
            http_status=standings_fetch_meta.get("http_status"),
            fetch_source=standings_fetch_meta.get("source"),
            fetched_at_utc=standings_fetch_meta.get("fetched_at_utc"),
        )
        espn_schedule = fetch_json_any(ESPN_SCHEDULE_URLS, use_fresh_cache=False)
        payload = build_output(standings, espn_schedule, mock_override)
        if TEST_FAIL_SECTION == "standings":
            payload["lotteryStandings"] = []
            payload["pacersPickOdds"] = []
            refresh_status["draft"] = "error: forced test failure (standings)"
        if TEST_INVALID_SECTION == "standings":
            payload["lotteryStandings"] = []
            payload["pacersPickOdds"] = []
            msg = "validation failed: standings empty after successful fetch"
            RUN_ERRORS.append({"section": "standings", "source": "validation", "message": msg, "time_utc": utc_now_iso()})
            log_event("error", "validation_failed", section="standings", reason=msg)
        payload.setdefault("meta", {}).setdefault("sources", {})["mockSourceUsed"] = mock_source_url
        if standings_fetch_meta:
            payload.setdefault("meta", {}).setdefault("sources", {})["standingsFetch"] = standings_fetch_meta
        if payload.get("lotteryStandings"):
            refresh_status["draft"] = "ok:espn_api+odds_table+manual_override"
        else:
            refresh_status["draft"] = "error: espn_standings_missing"
        if not payload.get("upcomingGames"):
            payload["upcomingGames"] = existing_payload.get("upcomingGames") or DEFAULT_DRAFT["keyGames"]
        if not payload.get("keyGames"):
            payload["keyGames"] = [g for g in payload.get("upcomingGames", []) if isinstance(g, dict) and g.get("isKey")] or existing_payload.get("keyGames") or DEFAULT_DRAFT["keyGames"]
    except (HTTPError, URLError, TimeoutError) as exc:
        print(f"Warning: failed to refresh draft section from ESPN: {exc}", file=sys.stderr)
        refresh_status["draft"] = "error: espn_standings_missing"
        payload = existing_payload if isinstance(existing_payload, dict) else {}
        payload["upcomingGames"] = payload.get("upcomingGames") or existing_payload.get("upcomingGames") or DEFAULT_DRAFT["keyGames"]
        payload["keyGames"] = payload.get("keyGames") or [g for g in payload.get("upcomingGames", []) if isinstance(g, dict) and g.get("isKey")] or existing_payload.get("keyGames") or DEFAULT_DRAFT["keyGames"]
        payload.setdefault("meta", {}).setdefault("sources", {})["mockSourceUsed"] = mock_source_url
        prev_last_ok = (
            existing_payload.get("meta", {})
            .get("sections", {})
            .get("standings", {})
            .get("last_ok_at_utc")
            if isinstance(existing_payload, dict)
            else None
        )
        log_event("warn", "standings_lkg_used", reason=str(exc), last_ok_at_utc=prev_last_ok)

    # Always attempt NBA-driven sections even if draft fetch fails.
    pacers_live = _is_pacers_game_in_progress(espn_schedule if isinstance(espn_schedule, dict) else {})

    try:
        print("Refreshing section 1 (What You Missed)...", flush=True)
        payload["whatYouMissed"] = extract_what_you_missed(espn_schedule if isinstance(espn_schedule, dict) else None)
        payload["whatYouMissed"]["highlightVideoUrl"] = extract_latest_pacers_highlight_video()
        extraction_error = str(payload["whatYouMissed"].pop("_error", "") or "").strip()
        leaders = payload["whatYouMissed"].get("leaders", [])
        summary_text = str(payload["whatYouMissed"].get("summary") or "").lower()
        is_unavailable = ("not available" in summary_text) or ("unavailable" in summary_text)
        if not isinstance(leaders, list) or len(leaders) == 0:
            if pacers_live:
                refresh_status["whatYouMissed"] = "live:game_in_progress"
            else:
                refresh_status["whatYouMissed"] = f"error: {extraction_error or 'leader_extraction_failed'}"
            existing_missed = existing_payload.get("whatYouMissed", {})
            existing_leaders = existing_missed.get("leaders", []) if isinstance(existing_missed, dict) else []
            if isinstance(existing_leaders, list) and len(existing_leaders) > 0:
                # Keep fresh summary/opponent/date when available; only backfill missing leaders.
                payload["whatYouMissed"]["leaders"] = existing_leaders
                current_summary = str(payload["whatYouMissed"].get("summary") or "").strip()
                if not current_summary or "not available" in current_summary.lower():
                    payload["whatYouMissed"]["summary"] = existing_missed.get("summary") or current_summary
        elif is_unavailable:
            existing_missed = existing_payload.get("whatYouMissed", {})
            existing_leaders = existing_missed.get("leaders", []) if isinstance(existing_missed, dict) else []
            if isinstance(existing_leaders, list) and len(existing_leaders) > 0:
                payload["whatYouMissed"] = existing_missed
            refresh_status["whatYouMissed"] = "live:game_in_progress" if pacers_live else f"error: {extraction_error or 'leader_extraction_failed'}"
        else:
            refresh_status["whatYouMissed"] = "ok:espn_boxscore"
    except Exception as exc:  # pragma: no cover
        print(f"Warning: failed to refresh What You Missed: {exc}", file=sys.stderr)
        refresh_status["whatYouMissed"] = f"error: {exc}"
        payload["whatYouMissed"] = payload.get("whatYouMissed", DEFAULT_WHAT_YOU_MISSED)

    # Refresh upcoming games independently so this section still updates when standings fail.
    try:
        espn_schedule = fetch_json_any(ESPN_SCHEDULE_URLS, use_fresh_cache=False)
        fresh_games = build_upcoming_games(espn_schedule, {}, {})
        if fresh_games:
            payload["upcomingGames"] = fresh_games
    except Exception:
        pass

    if TEST_INVALID_SECTION == "standings":
        msg = "validation failed: standings empty after successful fetch"
        payload["lotteryStandings"] = []
        payload["pacersPickOdds"] = []
        RUN_ERRORS.append({"section": "standings", "source": "validation", "message": msg, "time_utc": utc_now_iso()})
        log_event("error", "validation_failed", section="standings", reason=msg)

    # Guarantee required keys so UI never crashes.
    payload.setdefault("lotteryStandings", existing_payload.get("lotteryStandings", []))
    manual_override_map = dict(mock_override or {})
    existing_mock_by_slot: Dict[int, Dict[str, object]] = {}
    existing_rows = existing_payload.get("lotteryStandings", []) if isinstance(existing_payload, dict) else []
    if isinstance(existing_rows, list):
        for prev_row in existing_rows:
            if not isinstance(prev_row, dict):
                continue
            try:
                prev_slot = int(prev_row.get("slot"))
            except (TypeError, ValueError):
                continue
            existing_mock_by_slot[prev_slot] = {
                "mockPlayerName": prev_row.get("mockPlayerName"),
                "mockPlayerUrl": prev_row.get("mockPlayerUrl"),
            }
    cleaned_rows: List[dict] = []
    for row in payload.get("lotteryStandings", []):
        if not isinstance(row, dict):
            continue
        out_row = dict(row)
        try:
            slot = int(out_row.get("slot"))
        except (TypeError, ValueError):
            slot = None
        if slot is not None:
            odds = ODDS_BY_SLOT.get(slot, {})
            if odds:
                out_row["top4"] = float(odds.get("top4", out_row.get("top4", 0.0)))
                out_row["first"] = float(odds.get("first", out_row.get("first", 0.0)))
        if slot is not None and slot in manual_override_map:
            out_row["mockPlayerName"] = manual_override_map[slot].get("name")
            out_row["mockPlayerUrl"] = manual_override_map[slot].get("url")
        else:
            prev_mock = existing_mock_by_slot.get(slot or -1, {})
            name_val = out_row.get("mockPlayerName")
            url_val = out_row.get("mockPlayerUrl")
            out_row["mockPlayerName"] = name_val if name_val not in (None, "") else prev_mock.get("mockPlayerName")
            out_row["mockPlayerUrl"] = url_val if url_val not in (None, "") else prev_mock.get("mockPlayerUrl")
        if is_blocked_mock_player(out_row.get("mockPlayerName")):
            out_row["mockPlayerName"] = ""
            out_row["mockPlayerUrl"] = ""
        cleaned_rows.append(out_row)
    payload["lotteryStandings"] = cleaned_rows
    payload.setdefault("pacersPickOdds", existing_payload.get("pacersPickOdds", []))
    payload.setdefault("pickProtection", existing_payload.get("pickProtection", PICK_PROTECTION))
    payload.setdefault("pickProtectionImpact", existing_payload.get("pickProtectionImpact", build_pick_protection_impact(payload.get("lotteryStandings", []), payload.get("pacersPickOdds", []), payload.get("pickProtection", PICK_PROTECTION))))
    payload.setdefault("keyGames", existing_payload.get("keyGames", []))
    payload.setdefault("upcomingGames", existing_payload.get("upcomingGames", []))
    if not payload.get("upcomingGames"):
        payload["upcomingGames"] = payload.get("keyGames") or DEFAULT_DRAFT["keyGames"]
    payload["upcomingGames"] = normalize_upcoming_games(payload.get("upcomingGames", []))
    if not payload.get("keyGames"):
        payload["keyGames"] = [g for g in payload.get("upcomingGames", []) if isinstance(g, dict) and g.get("isKey")] or DEFAULT_DRAFT["keyGames"]
    payload["keyGames"] = [g for g in payload.get("upcomingGames", []) if isinstance(g, dict) and g.get("isKey")]
    payload.setdefault("whatYouMissed", DEFAULT_WHAT_YOU_MISSED)
    players_section_status = "disabled"
    players_section_source = "none"
    players_section_error: Optional[str] = None
    if isinstance(payload.get("whatYouMissed"), dict):
        payload["whatYouMissed"].setdefault("highlightVideoUrl", extract_latest_pacers_highlight_video())
    has_playlist_embed = bool(payload.get("whatYouMissed", {}).get("highlightVideoUrl"))
    videos_status = "ok" if has_playlist_embed else "disabled"
    videos_source = "youtube_embed_official"
    videos_reason: Optional[str] = None if has_playlist_embed else "missing_uploads_playlist_id"
    if bool(_source_dict().get("youtube_data_api")):
        try:
            yt_latest, yt_reason = fetch_youtube_data_api_latest()
        except Exception as exc:
            yt_latest, yt_reason = None, f"api_error:{exc}"
        if yt_latest and isinstance(yt_latest, dict):
            vid = str(yt_latest.get("videoId") or "").strip()
            if vid:
                payload.setdefault("whatYouMissed", {})
                payload["whatYouMissed"]["highlightVideoUrl"] = f"https://www.youtube.com/embed/{vid}"
                videos_status = "ok"
                videos_source = "youtube_data_api"
                videos_reason = None
        else:
            videos_status = "disabled"
            videos_source = "youtube_data_api"
            videos_reason = "missing_api_key" if yt_reason == "missing_api_key" else "no_compliant_source"
    draft_status = str(refresh_status.get("draft", ""))
    wym_status = str(refresh_status.get("whatYouMissed", ""))
    stale = draft_status.startswith("error:") or wym_status.startswith("error:") or wym_status == "empty"
    if stale and isinstance(existing_payload, dict) and existing_payload.get("asOf"):
        payload["asOf"] = existing_payload.get("asOf")
    else:
        payload["asOf"] = datetime.now().strftime("%B %-d, %Y")
    payload.setdefault("meta", {})
    payload["meta"]["refreshStatus"] = refresh_status
    payload["meta"]["stale"] = stale

    # Build additive reliability metadata without breaking existing top-level keys.
    meta = payload.setdefault("meta", {})
    meta["schema_version"] = 1
    meta["generated_at_utc"] = utc_now_iso()
    existing_sections = {}
    if isinstance(existing_payload, dict):
        existing_meta = existing_payload.get("meta", {})
        if isinstance(existing_meta, dict) and isinstance(existing_meta.get("sections"), dict):
            existing_sections = existing_meta.get("sections", {})

    def _section_entry(name: str, status: str, source: str, error: Optional[str] = None, reason: Optional[str] = None) -> dict:
        prev = existing_sections.get(name, {}) if isinstance(existing_sections, dict) else {}
        now = utc_now_iso()
        last_ok = prev.get("last_ok_at_utc")
        if status == "ok":
            last_ok = now
        if status not in {"ok", "stale", "disabled"}:
            status = "stale"
        if status == "stale" and not last_ok:
            status = "disabled"
            reason = reason or "no_data_yet"
        entry = {
            "status": status,
            "source": source,
            "fetched_at_utc": now,
            "last_ok_at_utc": last_ok,
        }
        if error:
            entry["error"] = error
        if reason:
            entry["reason"] = reason
        if status == "disabled":
            entry["reason"] = reason or "no_compliant_source"
        return entry

    draft_status = str(refresh_status.get("draft", ""))
    wym_status = str(refresh_status.get("whatYouMissed", ""))
    standings_status = "ok" if payload.get("lotteryStandings") else "stale"
    if TEST_FAIL_SECTION == "standings":
        standings_status = "stale"
    draft_ok = isinstance(payload.get("lotteryStandings"), list) and len(payload.get("lotteryStandings", [])) > 0
    draft_reason = None if draft_ok else "espn_standings_missing"

    boxscore_error = None
    wym_live = wym_status.startswith("live:")
    if wym_status.startswith("error:"):
        boxscore_error = wym_status.split("error:", 1)[1].strip()
    meta["sections"] = {
        "standings": _section_entry("standings", standings_status, "espn_api"),
        "schedule": _section_entry("schedule", "ok" if payload.get("upcomingGames") else "stale", "espn_api"),
        "scoreboard": _section_entry("scoreboard", "ok" if isinstance(payload.get("whatYouMissed"), dict) else "stale", "espn_api"),
        "boxscores": _section_entry(
            "boxscores",
            "ok" if isinstance(payload.get("whatYouMissed", {}).get("leaders"), list) and payload.get("whatYouMissed", {}).get("leaders") else "stale",
            "espn_api",
            boxscore_error,
        ),
        "players": _section_entry("players", players_section_status, players_section_source, players_section_error),
        "draft": _section_entry("draft", "ok" if draft_ok else "disabled", "espn_api+odds_table+manual_override", None, draft_reason),
        "videos": _section_entry("videos", videos_status, videos_source, None, videos_reason),
        "whatYouMissed": _section_entry(
            "whatYouMissed",
            "ok" if (wym_live or (not wym_status.startswith("error") and wym_status != "empty")) else "stale",
            "espn_api",
            wym_status.split("error:", 1)[1].strip() if wym_status.startswith("error:") else (wym_status if wym_status.startswith("error") else None),
            "game_in_progress" if wym_live else None,
        ),
        "pickProtection": _section_entry("pickProtection", "ok" if payload.get("pickProtection") else "stale", "static"),
    }
    # Draft: disabled only when ESPN standings are unavailable.
    if meta["sections"]["draft"]["status"] != "ok":
        meta["sections"]["draft"]["status"] = "disabled"
        meta["sections"]["draft"]["reason"] = meta["sections"]["draft"].get("reason") or "espn_standings_missing"

    # Enforce policy reason visibility for compliance.
    if bool(_policy_dict().get("require_compliant_sources")):
        for s_name, s_meta in meta["sections"].items():
            if isinstance(s_meta, dict) and s_meta.get("status") == "disabled" and not s_meta.get("reason"):
                s_meta["reason"] = "no_compliant_source"

    # Ensure requirements are present in output metadata for front-end notices.
    meta.setdefault("requirements", _requirements_dict())
    meta["errors"] = RUN_ERRORS

    # Preserve last-known-good sections on failure states.
    def _preserve_if_bad(section_name: str, keys: List[str]) -> None:
        sec = meta["sections"].get(section_name, {})
        if not isinstance(sec, dict):
            return
        if sec.get("status") == "disabled":
            return
        if sec.get("status") == "ok":
            return
        if not isinstance(existing_payload, dict):
            return
        for k in keys:
            if k in existing_payload:
                payload[k] = existing_payload[k]
        sec["status"] = "stale"
        sec["source"] = f"{sec.get('source')}+lkg"
        log_event("warn", "preserve_last_known_good", section=section_name, keys=keys, source=sec["source"])

    _preserve_if_bad("standings", ["lotteryStandings", "pacersPickOdds", "pickProtection"])
    _preserve_if_bad("schedule", ["upcomingGames", "keyGames"])
    _preserve_if_bad("whatYouMissed", ["whatYouMissed"])
    _preserve_if_bad("videos", ["whatYouMissed"])
    # Enforce manual override semantics even when standings are served from last-known-good:
    # apply overrides by slot, otherwise preserve existing per-slot mock values.
    manual_override_map = dict(mock_override or {})
    existing_mock_by_slot: Dict[int, Dict[str, object]] = {}
    existing_rows = existing_payload.get("lotteryStandings", []) if isinstance(existing_payload, dict) else []
    if isinstance(existing_rows, list):
        for prev_row in existing_rows:
            if not isinstance(prev_row, dict):
                continue
            try:
                prev_slot = int(prev_row.get("slot"))
            except (TypeError, ValueError):
                continue
            existing_mock_by_slot[prev_slot] = {
                "mockPlayerName": prev_row.get("mockPlayerName"),
                "mockPlayerUrl": prev_row.get("mockPlayerUrl"),
            }
    normalized_rows: List[dict] = []
    for row in payload.get("lotteryStandings", []):
        if not isinstance(row, dict):
            continue
        r = dict(row)
        try:
            slot = int(r.get("slot"))
        except (TypeError, ValueError):
            slot = None
        if slot is not None:
            odds = ODDS_BY_SLOT.get(slot, {})
            if odds:
                r["top4"] = float(odds.get("top4", r.get("top4", 0.0)))
                r["first"] = float(odds.get("first", r.get("first", 0.0)))
        if slot is not None and slot in manual_override_map:
            r["mockPlayerName"] = manual_override_map[slot].get("name")
            r["mockPlayerUrl"] = manual_override_map[slot].get("url")
        else:
            prev_mock = existing_mock_by_slot.get(slot or -1, {})
            name_val = r.get("mockPlayerName")
            url_val = r.get("mockPlayerUrl")
            r["mockPlayerName"] = name_val if name_val not in (None, "") else prev_mock.get("mockPlayerName")
            r["mockPlayerUrl"] = url_val if url_val not in (None, "") else prev_mock.get("mockPlayerUrl")
        if is_blocked_mock_player(r.get("mockPlayerName")):
            r["mockPlayerName"] = ""
            r["mockPlayerUrl"] = ""
        normalized_rows.append(r)
    payload["lotteryStandings"] = normalized_rows
    draft_section = meta.get("sections", {}).get("draft", {})
    if isinstance(draft_section, dict):
        if isinstance(payload.get("lotteryStandings"), list) and payload.get("lotteryStandings"):
            draft_section["status"] = "ok"
            draft_section["reason"] = None
            draft_section["source"] = "espn_api+odds_table+manual_override"
            draft_section["last_ok_at_utc"] = utc_now_iso()
        else:
            draft_section["status"] = "disabled"
            draft_section["reason"] = draft_section.get("reason") or "espn_standings_missing"
    standings_meta = meta.get("sections", {}).get("standings", {})
    if isinstance(standings_meta, dict) and standings_meta.get("status") in {"stale", "error"}:
        prev_last_ok = standings_meta.get("last_ok_at_utc")
        if prev_last_ok:
            standings_meta["error"] = standings_meta.get("error") or f"using_last_known_good(last_ok_at_utc={prev_last_ok})"
        if not isinstance(payload.get("lotteryStandings"), list) or not payload.get("lotteryStandings"):
            payload["lotteryStandings"] = (
                existing_payload.get("lotteryStandings")
                if isinstance(existing_payload, dict) and isinstance(existing_payload.get("lotteryStandings"), list) and existing_payload.get("lotteryStandings")
                else DEFAULT_DRAFT["lotteryStandings"]
            )
            if not payload.get("pacersPickOdds"):
                payload["pacersPickOdds"] = (
                    existing_payload.get("pacersPickOdds")
                    if isinstance(existing_payload, dict) and isinstance(existing_payload.get("pacersPickOdds"), list) and existing_payload.get("pacersPickOdds")
                    else DEFAULT_DRAFT["pacersPickOdds"]
                )
            log_event("warn", "standings_non_empty_guard_applied", rows=len(payload.get("lotteryStandings", [])))

    # Keep legacy flags.
    stale = any(
        isinstance(v, dict) and v.get("status") in {"stale", "error", "disabled"}
        for v in meta.get("sections", {}).values()
    )
    payload["meta"]["stale"] = bool(stale)

    # Atomic write: temp file then replace.
    tmp_file = out_file.with_suffix(".json.tmp")
    log_event("info", "atomic_write_tmp_start", tmp_path=str(tmp_file))
    tmp_file.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    log_event("info", "atomic_write_tmp_written", tmp_path=str(tmp_file), bytes=tmp_file.stat().st_size if tmp_file.exists() else 0)
    os.replace(tmp_file, out_file)
    log_event("info", "atomic_write_replaced", tmp_path=str(tmp_file), dest_path=str(out_file))

    print("Status summary:", flush=True)
    for section_name, s in meta.get("sections", {}).items():
        if not isinstance(s, dict):
            continue
        line = f"- {section_name}: {s.get('status')} | {s.get('source')} | {s.get('fetched_at_utc')}"
        if s.get("error"):
            line += f" | error={s.get('error')}"
        print(line, flush=True)
        log_event("info", "section_status", section=section_name, **s)

    elapsed = time.time() - started_at
    print(f"Wrote {out_file}", flush=True)
    print(f"Finished in {elapsed:.1f}s", flush=True)
    log_event("info", "run_finish", elapsed_seconds=round(elapsed, 2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
