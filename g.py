#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
VK IPTV / M3U COLLECTOR — SKALA / DREG FULL BUILD
==================================================


Единый сборщик:


  1. VK public wall/group crawler.
  2. Извлечение постов и всех URL.
  3. Парсинг M3U/M3U8.
  4. Вложенные M3U.
  5. Публичные IPTV-источники.
  6. Поиск "IPTV Ru" в GitVerse.
  7. Поиск "IPTV Ru" в GitHub Gist.
  8. Сохранение происхождения каждой записи.
  9. БЕЗ дедупликации записей.
 10. Проверка рабочих потоков.
 11. Если поток не работает — поиск альтернатив по имени канала.
 12. Альтернативы проверяются ДО окончательного исключения.
 13. Формирование:
        combined.m3u
        combined_all_alternatives.m3u
        combined_working.m3u
        combined_archive.m3u
        combined_kz.m3u
        combined_tj.m3u
        combined_tm.m3u
        combined_uz.m3u
        combined_mn.m3u
 14. SKALA/DREG отчёты на русском.
 15. Диагностика JSONL.
 16. Отчёт по мультидорожкам.
 17. Определение HLS/DASH/live/VOD/catch-up/DVR признаков.
 18. Сохранение всех альтернатив.
 19. Не превращает обычные сайты/YouTube/GitHub в IPTV-потоки.
 20. Не записывает URL M3U как канал.
 21. Поддержка embedded M3U в VK-постах.


Зависимости:


    pip install requests beautifulsoup4


Опционально:


    ffprobe / ffmpeg


Если ffprobe установлен, код может дополнительно проверить media metadata.
Без ffprobe HTTP/HLS-проверка всё равно выполняется.


ВАЖНО:


M3U не является самим DVR/плеером. Возможности записи,
перемотки и pause зависят от сервера и IPTV-клиента.


"""


from __future__ import annotations


import argparse
import html
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
import threading

from concurrent.futures import ThreadPoolExecutor, as_completed


from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)
from urllib.parse import (
    parse_qsl,
    quote_plus,
    urlencode,
    urljoin,
    urlparse,
    urlunparse,
)


import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry




# ============================================================================
# VERSION
# ============================================================================


SKALA_VERSION = "4.0.1"
SKALA_NAME = "SKALA/DREG IPTV COLLECTOR"




# ============================================================================
# DEFAULT CONFIG
# ============================================================================


DEFAULT_URL = "https://vk.ru/club228871429"
DEFAULT_OUTPUT = "vk_iptv_output"


USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 12; K) "
    "AppleWebKit/537.36 "
    "(KHTML, like Gecko) "
    "Chrome/140.0.0.0 Mobile Safari/537.36 "
    "SKALA-DREG-IPTV-COLLECTOR/4.0"
)


REQUEST_TIMEOUT = (8, 20)
PLAYLIST_TIMEOUT = (8, 30)
STREAM_TIMEOUT = (5, 12)


MAX_HTML_BYTES = 30 * 1024 * 1024
MAX_PLAYLIST_BYTES = 80 * 1024 * 1024
MAX_MANIFEST_BYTES = 10 * 1024 * 1024


VK_DELAY = 0.7
PLAYLIST_DELAY = 0.15
STREAM_DELAY = 0.03
SEARCH_DELAY = 0.4


DEFAULT_MAX_PAGES = 1000
DEFAULT_MAX_PLAYLIST_DEPTH = 3


OFFSET_STEP = 20
EMPTY_PAGE_LIMIT = 5


STREAM_TEST_WORKERS = 12
ALTERNATIVE_TEST_WORKERS = 12


# ============================================================================
# 18+ FILTER — ADULT/EROTIC CHANNELS ARE JUNK
# ============================================================================

ADULT_FILTER_ENABLED = True

ADULT_STRONG_MARKERS = (
    "18+", "21+", "xxx", "porn", "porno", "pornhub",
    "xvideos", "xnxx", "xhamster", "redtube", "brazzers",
    "playboy", "hustler", "penthouse", "onlyfans",
    "erotic", "erotica", "эротик",
    "эротика", "эротический", "эротическое", "порно",
    "порн", "секс", "сексуаль", "для взрослых",
    "интим", "интимн",
)

ADULT_TOKEN_RE = re.compile(
    r"(?iu)(?:^|[\s._:/\-+()\[\]{}])"
    r"(?:18\+|21\+|xxx|porn|porno|erotic|эротик|порно|порн|"
    r"sex|секс|сексуаль|для\s+взросл|интим)"
    r"(?:$|[\s._:/\-+()\[\]{ }])"
)


def is_adult_content(*values: object) -> bool:
    """Жёсткий фильтр 18+: эротические/adult-каналы считаются мусором."""
    if not ADULT_FILTER_ENABLED:
        return False

    for value in values:
        text = str(value or "").strip().lower()
        if not text:
            continue

        normalized = re.sub(r"[\u00a0\t\r\n]+", " ", text)

        if ADULT_TOKEN_RE.search(normalized):
            return True

        compact = re.sub(r"[^a-zа-яё0-9+]+", " ", normalized)
        for marker in ADULT_STRONG_MARKERS:
            if marker in compact:
                return True

    return False




SEARCH_QUERY = "IPTV Ru"




# ============================================================================
# CONTROLLED OUTPUT STRUCTURE
# ============================================================================


SKALA_OUTPUT_FILES = [
    "combined.m3u",
    "combined_all_alternatives.m3u",
    "combined_working.m3u",
    "combined_archive.m3u",


    "combined_kz.m3u",
    "combined_tj.m3u",
    "combined_tm.m3u",
    "combined_uz.m3u",
    "combined_mn.m3u",


    "SKALA_DREG_DIAGNOSTICS.txt",
    "SKALA_DREG_WORKING.txt",
    "SKALA_DREG_FAILED.txt",
    "SKALA_DREG_ALTERNATIVES.txt",
    "SKALA_DREG_ARCHIVE.txt",
    "SKALA_DREG_MULTITRACK.txt",


    "diagnostics.jsonl",
    "alternatives.jsonl",


    "records.jsonl",
    "posts.jsonl",
    "posts_urls.txt",


    "links.jsonl",
    "playlists.jsonl",
    "playlists_found.jsonl",
    "playlists.txt",


    "streams.txt",
    "stats.json",
    "summary.txt",
    "errors.log",
]




# ============================================================================
# PUBLIC IPTV SOURCES
# ============================================================================


PUBLIC_INTERNET_SOURCES = [
    "https://IPTVRU2026/IPTVMIR/main/IPTV_MEGA_PLAYLIST.m3u",
    "https://Monoloshka/iptv/main/BeeTV.m3u",
    "https://Monoloshka/iptv/main/full-iptv.m3u",
    "https://Monoloshka/iptv/main/tv.m3u",


    "https://aidoseg/qazaqiptv/playlist.m3u8",


    "https://blackbirdstudiorus/IPTVPlay/main/IPTVPlay.m3u",
    "https://blackbirdstudiorus/IPTVPlay/main/KionPlus.m3u",


    "https://dearbulut/iptv/playlists/best.m3u",
    "https://dearbulut/iptv/playlists/category/documentary.m3u",
    "https://dearbulut/iptv/playlists/category/entertainment.m3u",
    "https://dearbulut/iptv/playlists/category/general.m3u",
    "https://dearbulut/iptv/playlists/category/kids.m3u",
    "https://dearbulut/iptv/playlists/category/movies.m3u",
    "https://dearbulut/iptv/playlists/category/music.m3u",
    "https://dearbulut/iptv/playlists/category/news.m3u",
    "https://dearbulut/iptv/playlists/category/sports.m3u",


    "https://dearbulut/iptv/playlists/country/by.m3u",
    "https://dearbulut/iptv/playlists/country/kg.m3u",
    "https://dearbulut/iptv/playlists/country/kz.m3u",
    "https://dearbulut/iptv/playlists/country/mn.m3u",
    "https://dearbulut/iptv/playlists/country/ru.m3u",
    "https://dearbulut/iptv/playlists/country/tj.m3u",
    "https://dearbulut/iptv/playlists/country/ua.m3u",
    "https://dearbulut/iptv/playlists/country/uz.m3u",


    "https://dearbulut/iptv/playlists/index.m3u",
    "https://dearbulut/iptv/playlists/language/rus.m3u",
    "https://dearbulut/iptv/playlists/online.m3u",


    "https://gitverse/api/repos/RUVIPIEN/IPTVMIR/raw/branch/main/IPTV_MEGA_PLAYLIST.m3u",


    "https://iptv-org/iptv/categories/documentary.m3u",
    "https://iptv-org/iptv/categories/entertainment.m3u",
    "https://iptv-org/iptv/categories/general.m3u",
    "https://iptv-org/iptv/categories/kids.m3u",
    "https://iptv-org/iptv/categories/movies.m3u",
    "https://iptv-org/iptv/categories/music.m3u",
    "https://iptv-org/iptv/categories/news.m3u",
    "https://iptv-org/iptv/categories/sports.m3u",


    "https://iptv-org/iptv/countries/am.m3u",
    "https://iptv-org/iptv/countries/az.m3u",
    "https://iptv-org/iptv/countries/by.m3u",
    "https://iptv-org/iptv/countries/ge.m3u",
    "https://iptv-org/iptv/countries/kg.m3u",
    "https://iptv-org/iptv/countries/kz.m3u",
    "https://iptv-org/iptv/countries/md.m3u",
    "https://iptv-org/iptv/countries/mn.m3u",
    "https://iptv-org/iptv/countries/ru.m3u",
    "https://iptv-org/iptv/countries/tj.m3u",
    "https://iptv-org/iptv/countries/tm.m3u",
    "https://iptv-org/iptv/countries/ua.m3u",
    "https://iptv-org/iptv/countries/uz.m3u",


    "https://iptv-org/iptv/index.category.m3u",
    "https://iptv-org/iptv/index.country.m3u",
    "https://iptv-org/iptv/index.language.m3u",
    "https://iptv-org/iptv/index.m3u",


    "https://iptv-org/iptv/languages/rus.m3u",
    "https://iptv-org/iptv/regions/cas.m3u",
    "https://iptv-org/iptv/regions/cis.m3u",


    "https://iptv.org.ua/iptv/avto-full.m3u",
    "https://iptv.org.ua/iptv/avto-full.m3u8",
    "https://iptv.org.ua/iptv/avto.m3u",
    "https://iptv.org.ua/iptv/avto.m3u8",
    "https://iptv.org.ua/iptv/avtomini.m3u",
    "https://iptv.org.ua/iptv/provayder.m3u",
    "https://iptv.org.ua/iptv/provayder.m3u8",
    "https://iptv.org.ua/iptv/tva1.m3u",
    "https://iptv.org.ua/iptv/tva2.m3u",
    "https://iptv.org.ua/iptv/tva3.m3u",
    "https://iptv.org.ua/iptv/tva4.m3u",
    "https://iptv.org.ua/iptv/tva5.m3u",


    "https://myplaylists/iptv/ru.m3u",
    "https://myplaylists/iptv/ua.m3u",


    "https://naggdd/iptv/cartoons.m3u",
    "https://naggdd/iptv/main/cartoons.m3u",
    "https://naggdd/iptv/main/music.m3u",
    "https://naggdd/iptv/main/ru.m3u",
    "https://naggdd/iptv/music.m3u",
    "https://naggdd/iptv/ru.m3u",


    "https://ngrch/iptv/cartoons.m3u",
    "https://ngrch/iptv/music.m3u",
    "https://ngrch/iptv/ru.m3u",


    "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8",


    "https://romaxa55/world_ip_tv/main/output/index.m3u",
    "https://romaxa55/world_ip_tv/output/index.m3u",


    "https://smart-iptv/kaz.m3u",
    "https://smart-iptv/russia.m3u",
    "https://smart-tv-iptv/russia.m3u",


    "https://smolnp/IPTVru/gh-pages/IPRadio.m3u",
    "https://smolnp/IPTVru/gh-pages/IPTVdonor.m3u",
    "https://smolnp/IPTVru/gh-pages/IPTVmir.m3u8",
    "https://smolnp/IPTVru/gh-pages/IPTVru.m3u",
    "https://smolnp/IPTVru/gh-pages/IPTVstable.m3u8",
    "https://smolnp/IPTVru/gh-pages/IPTVххх.m3u",
    "https://smolnp/IPTVru/gh-pages/KseniaTV.m3u",


    "https://tiny.one/qazaqiptv",
    "https://tiny.one/qazaqtv",


    "https://iptv.org.ua/iptv/tva2.m3u",
    "https://iptv.org.ua/iptv/tva3.m3u",
    "https://iptv.org.ua/iptv/tva4.m3u",
    "https://iptv.org.ua/iptv/tva5.m3u",
    "https://iptv.org.ua/iptv/avto.m3u8",
    "https://iptv.org.ua/iptv/avto-full.m3u8",
    "https://iptv.org.ua/iptv/provayder.m3u8",
]




# ============================================================================
# REGIONAL SOURCE PATTERNS
# ============================================================================


REGION_CODES = {
    "kz": "Казахстан",
    "tj": "Таджикистан",
    "tm": "Туркменистан",
    "uz": "Узбекистан",
    "mn": "Монголия",
}


REGION_HOST_MARKERS = {
    "kz": (
        ".kz",
        "kaz",
        "qazaq",
        "kazakh",
        "almaty",
        "astana",
        "karaganda",
    ),
    "tj": (
        ".tj",
        "tajik",
        "dushanbe",
        "khujand",
    ),
    "tm": (
        ".tm",
        "turkmen",
        "ashgabat",
    ),
    "uz": (
        ".uz",
        "uzbek",
        "tashkent",
        "samarkand",
    ),
    "mn": (
        ".mn",
        "mongol",
        "ulaanbaatar",
    ),
}




# ============================================================================
# NON STREAM HOSTS
# ============================================================================


NON_STREAM_HOST_MARKERS = (
    "vk.ru",
    "vk.com",
    "m.vk.com",
    "youtube.com",
    "youtu.be",
    "rutube.ru",
    "t.me",
    "telegram.me",
    "instagram.com",
    "facebook.com",
    "twitter.com",
    "x.com",
    "github.com",
    "gitlab.com",
    "gitverse.ru",
    "google.com",
    "googleusercontent.com",
    "yandex.ru",
)




# ============================================================================
# REGEX
# ============================================================================


WALL_RE = re.compile(
    r"(?:https?://[^/\s]+)?/(?:wall|w=wall)(-?\d+_\d+)",
    re.I,
)


WALL_ID_RE = re.compile(
    r"(?:wall(?:_|%5F)|w=wall(?:_|%5F))(-?\d+_\d+)",
    re.I,
)


URL_RE = re.compile(
    r"""(?ix)
    (?:
        https?://
        |
        //
    )
    [^\s<>"'\\]+
    """
)


ATTR_RE = re.compile(
    r"""(?is)
    ([a-zA-Z][a-zA-Z0-9_-]*)
    \s*=\s*
    (?:
        "([^"]*)"
        |
        '([^']*)'
    )
    """
)


M3U_EXTENSIONS = (".m3u", ".m3u8")


DIRECT_STREAM_EXTENSIONS = (
    ".m3u8",
    ".m3u",
    ".ts",
    ".m4s",
    ".aac",
    ".mp3",
    ".mp4",
    ".mkv",
    ".flv",
    ".webm",
    ".mpd",
)


DIRECT_STREAM_MARKERS = (
    "/hls/",
    "/hls?",
    "/live/",
    "/live?",
    "/stream/",
    "/stream?",
    "/playlist/",
    "/manifest",
    "/chunklist",
    "/master.",
    "format=m3u8",
    "type=m3u8",
    "output=m3u8",
)


CATCHUP_MARKERS = (
    "catchup",
    "timeshift",
    "timeshift=",
    "archive",
    "archive=",
    "dvr",
    "start=",
    "utc=",
    "from=",
    "to=",
)




# ============================================================================
# LOGGING
# ============================================================================


LOG = logging.getLogger("skala")




def setup_logging(output_dir: Path, verbose: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)


    LOG.setLevel(logging.DEBUG)
    LOG.handlers.clear()
    LOG.propagate = False


    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )


    file_handler = logging.FileHandler(
        output_dir / "errors.log",
        encoding="utf-8",
    )


    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)


    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(
        logging.DEBUG if verbose else logging.INFO
    )
    console_handler.setFormatter(formatter)


    LOG.addHandler(file_handler)
    LOG.addHandler(console_handler)




# ============================================================================
# DATA CLASSES
# ============================================================================


@dataclass
class Post:
    post_id: str
    url: str
    text: str
    html_fragment: str = ""
    page_url: str = ""
    discovered_by: str = ""




@dataclass
class Record:
    sequence: int
    name: str
    url: str
    source_type: str


    source_page: str = ""
    source_post: str = ""


    playlist_url: str = ""
    playlist_depth: int = 0


    extinf: str = ""


    tvg_id: str = ""
    tvg_name: str = ""
    tvg_logo: str = ""
    group_title: str = ""


    region: str = ""


    catchup: str = ""
    catchup_days: str = ""
    catchup_source: str = ""


    raw_text: str = ""


    # Проверка.
    working: bool = False
    status_code: int = 0
    content_type: str = ""
    final_url: str = ""


    diagnostic_reason: str = ""
    diagnostic_detail: str = ""


    # Media.
    protocol: str = ""
    is_live: bool = False
    is_vod: bool = False


    has_audio: bool = False
    has_video: bool = False
    audio_tracks: int = 0
    video_tracks: int = 0


    archive_supported: bool = False
    record_supported: bool = False
    rewind_supported: bool = False


    alternative_of: str = ""
    alternative_rank: int = 0




@dataclass
class StreamDiagnostics:
    url: str


    ok: bool = False


    http_status: int = 0
    content_type: str = ""
    final_url: str = ""


    protocol: str = ""
    status: str = ""


    reason_ru: str = ""
    detail_ru: str = ""


    bytes_read: int = 0
    response_time_ms: int = 0


    is_m3u: bool = False
    is_hls: bool = False
    is_dash: bool = False
    is_live: bool = False
    is_vod: bool = False


    has_audio: bool = False
    has_video: bool = False


    audio_tracks: int = 0
    video_tracks: int = 0


    archive_supported: bool = False
    record_supported: bool = False
    rewind_supported: bool = False


    codecs: List[str] = field(default_factory=list)


    exception: str = ""




@dataclass
class AlternativeCandidate:
    channel_name: str
    url: str


    source_type: str = ""
    source_page: str = ""
    source_post: str = ""


    tvg_id: str = ""
    tvg_name: str = ""
    tvg_logo: str = ""
    group_title: str = ""


    region: str = ""


    similarity: float = 0.0
    diagnostics: Optional[StreamDiagnostics] = None




@dataclass
class PlaylistEvent:
    url: str
    final_url: str = ""
    depth: int = 0
    status: str = ""
    records: int = 0
    nested: int = 0
    source_post: str = ""
    content_type: str = ""
    error: str = ""




@dataclass
class CollectorStats:
    pages_requested: int = 0
    pages_ok: int = 0
    pages_failed: int = 0


    posts_found: int = 0
    posts_processed: int = 0
    repeated_post_ids: int = 0


    urls_found_in_posts: int = 0
    all_links_found: int = 0


    direct_stream_urls: int = 0
    non_stream_links: int = 0


    playlist_urls_found: int = 0
    playlists_requested: int = 0
    playlists_ok: int = 0
    playlists_failed: int = 0
    playlists_not_m3u: int = 0


    playlist_records: int = 0
    nested_playlist_urls: int = 0


    total_records: int = 0
    adult_filtered: int = 0


    stream_tests: int = 0
    stream_working: int = 0
    stream_failed: int = 0


    alternatives_found: int = 0
    alternatives_working: int = 0


    regional_records: Dict[str, int] = field(
        default_factory=lambda: {
            "kz": 0,
            "tj": 0,
            "tm": 0,
            "uz": 0,
            "mn": 0,
        }
    )


    errors: int = 0


    repeated_playlist_urls: int = 0


    gitverse_searches: int = 0
    gitverse_playlists: int = 0


    gist_searches: int = 0
    gist_playlists: int = 0




# ============================================================================
# HTTP
# ============================================================================


def build_session() -> requests.Session:
    session = requests.Session()


    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
        respect_retry_after_header=True,
    )


    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=32,
        pool_maxsize=32,
    )


    session.mount("http://", adapter)
    session.mount("https://", adapter)


    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,*/*;q=0.8"
            ),
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )


    return session




# ============================================================================
# URL HELPERS
# ============================================================================


def clean_url(raw: str) -> str:
    value = html.unescape(str(raw or "")).strip()


    value = value.replace("&amp;", "&")
    value = value.replace("\\/", "/")


    value = value.strip("\"'<>")


    while value and value[-1] in ".,;:)]}>":
        value = value[:-1]


    while value.startswith("(") and value.endswith(")"):
        value = value[1:-1].strip()


    return value




def normalize_protocol_relative(url: 