#!/usr/bin/env python3
"""gdrive_watch 백그라운드 워커

플러그인(gdrive_watch.py)이 띄우는 독립 프로세스. BookOasis 요청 처리와 분리되어 상주하며
  Google Drive 변경 감지(Changes/Activity) → rclone VFS forget/refresh → BookOasis 부분 스캔(webhook)
을 수행하고, 모든 이벤트와 처리 결과를 state.db에 기록한다.

실행: python3 worker.py <DATA_DIR> [--daemon]
  DATA_DIR/runtime.json   플러그인이 써주는 실행 설정 (변경 시 자동 재적용)
  DATA_DIR/state.db       체크포인트, file_id→경로 상태, 이벤트/결과
  DATA_DIR/heartbeat.json 워커 상태 (플러그인 화면 표시용)
  DATA_DIR/stop.flag      있으면 종료
  DATA_DIR/wake.flag      있으면 즉시 수집

BookOasis Mate(AGPL-3.0)의 자체 변경 감지 설계를 참고해 다시 작성함.
"""
import base64
import hashlib
import json
import logging
import logging.handlers
import os
import posixpath
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

log = logging.getLogger("gdrive_watch")
STOP = False
SEEDERS = {}
CHANGES_PAGES = {}  # 한 번의 확인 안에서 (리모트, 드라이브, 페이지 토큰) → 변경 목록 페이지 (같은 드라이브 감시끼리 공유)  # 감시 폴더 이름 → 기존 파일 목록 수집 스레드/프로세스
IGNORE = []   # 무시할 경로 정규식 (설정에서 적용)
FILE_WAIT_INTERVAL = 20  # 파일이 마운트에 보일 때까지 다시 확인하는 간격(초)
DEFAULT_IGNORE_PATTERNS = [
    r"[/\\]\[업로드\]([/\\]|$)",                      # 업로드 중인 임시 폴더
    r"[/\\]\.(?!bookoasisignore$)[^/\\]+",           # 숨김 파일·폴더
    r"\.(part|partial|tmp|temp|crdownload|!qb|aria2)$",  # 받는 중인 임시 파일
]


def ignored(path, root=""):
    """무시 패턴은 감시 폴더 기준 상대 경로('/하위/파일')에 적용한다.
    (마운트 경로 자체에 '.'로 시작하는 폴더나 [업로드]가 있어도 모든 기록이 무시되지 않게)"""
    if not path:
        return False
    root = (root or "").rstrip("/")
    if root and (path == root or path.startswith(root + "/")):
        path = path[len(root):] or "/"
    return any(p.search(path) for p in IGNORE)


def compile_patterns(lines):
    out = []
    for line in lines or []:
        line = str(line).strip()
        if line and not line.startswith("#"):
            out.append(re.compile(line, re.IGNORECASE))
    return out


CRON_NAMES = {
    3: {n: i for i, n in enumerate(("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"), 1)},
    4: {n: i for i, n in enumerate(("sun", "mon", "tue", "wed", "thu", "fri", "sat"))},
}
CRON_LIMITS = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))
CRON_ALIASES = {"@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *", "@monthly": "0 0 1 * *",
                "@weekly": "0 0 * * 0", "@daily": "0 0 * * *", "@midnight": "0 0 * * *", "@hourly": "0 * * * *"}


def _cron_parts(field, index):
    """cron 한 필드를 (시작, 끝, 간격) 목록으로. 잘못된 값이면 ValueError."""
    low, high = CRON_LIMITS[index]
    names = CRON_NAMES.get(index, {})

    def num(text):
        text = text.strip().lower()
        if text in names:
            return names[text]
        if not text.isdigit():
            raise ValueError(f"'{text}'")
        value = int(text)
        if not low <= value <= high:
            raise ValueError(f"{value}는 {low}~{high} 범위 밖")
        return value

    parts = []
    for part in field.split(","):
        if not part:
            raise ValueError("빈 항목")
        step = 1
        if "/" in part:
            part, step_text = part.split("/", 1)
            if not step_text.isdigit() or int(step_text) < 1:
                raise ValueError(f"간격 '{step_text}'")
            step = int(step_text)
        if part == "*":
            start, end = low, high
        elif "-" in part:
            a, b = part.split("-", 1)
            start, end = num(a), num(b)
            if start > end:
                raise ValueError(f"범위 {part}")
        else:
            start = end = num(part)
            if step > 1:
                end = high
        parts.append((start, end, step))
    return parts


def _cron_field(field, value, index):
    return any(start <= value <= end and (value - start) % step == 0 for start, end, step in _cron_parts(field, index))


def cron_fields(cron):
    text = str(cron or "").strip()
    return CRON_ALIASES.get(text.lower(), text).split()


def validate_cron(cron):
    """표준 5필드 cron 검사. 문제가 없으면 '' , 있으면 이유."""
    fields = cron_fields(cron)
    if len(fields) != 5:
        return f"필드가 5개여야 합니다 (분 시 일 월 요일), 지금 {len(fields)}개"
    labels = ("분", "시", "일", "월", "요일")
    for index, field in enumerate(fields):
        try:
            _cron_parts(field, index)
        except ValueError as error:
            return f"{labels[index]} 필드 '{field}' 오류: {error}"
    return ""


def cron_matches(cron, when):
    """표준 5필드 cron(분 시 일 월 요일)이 이 시각(분 단위)에 실행되는지."""
    fields = cron_fields(cron)
    if len(fields) != 5:
        return False
    try:
        minute, hour, dom, month, dow = fields
        weekday = (when.weekday() + 1) % 7  # cron: 0=일요일
        dow_ok = _cron_field(dow, weekday, 4) or (weekday == 0 and _cron_field(dow, 7, 4))
        dom_ok = _cron_field(dom, when.day, 2)
        if dom != "*" and dow != "*":
            day_ok = dom_ok or dow_ok  # 둘 다 지정되면 cron은 OR로 본다
        else:
            day_ok = dom_ok and dow_ok
        return (_cron_field(minute, when.minute, 0) and _cron_field(hour, when.hour, 1)
                and _cron_field(month, when.month, 3) and day_ok)
    except ValueError:
        return False


def screen_event(event, root=""):
    """무시 패턴에 걸리는 쪽 경로를 빼서 이벤트를 다듬는다.
    [업로드] → 실제 폴더로 옮긴 경우는 '추가'로, 실제 폴더 → [업로드]는 '삭제'로 바뀐다. 둘 다 걸리면 None."""
    path, removed = event["path"], event["removed_path"]
    bad_new, bad_old = ignored(path, root), ignored(removed, root)
    if event["action"] == "delete":
        return None if bad_old else event
    if bad_new and (not removed or bad_old or removed == path):
        return None
    if bad_new:
        return dict(event, action="delete", path=removed, removed_path=removed)
    if removed and bad_old:
        return dict(event, action="create", removed_path="")
    return event

DRIVE_API = "https://www.googleapis.com/drive/v3"
ACTIVITY_API = "https://driveactivity.googleapis.com/v2/activity:query"
FOLDER_MIME = "application/vnd.google-apps.folder"
FILE_FIELDS = "id,name,mimeType,parents,trashed,modifiedTime,size,md5Checksum,driveId,shortcutDetails(targetId,targetMimeType)"
SHORTCUT_MIME = "application/vnd.google-apps.shortcut"
DEFAULT_EXTENSIONS = (
    ".zip .cbz .cbr .epub .pdf .txt .yaml .xml .json "
    ".mp3 .m4b .m4a .flac .aac .wav .ogg .opus .wma "
    ".mp4 .mkv .avi .webm .mov .m4v .ts .smi .srt .ass .vtt"
).split()


# ─────────────────────────── 경로 유틸 ───────────────────────────

def norm(path):
    path = str(path or "").replace("\\", "/").strip()
    return posixpath.normpath(path) if path.startswith("/") else ""


def under(path, root):
    root = root.rstrip("/")
    return not root or path == root or path.startswith(root + "/")


def prefix_upper(prefix):
    """'a/b/'로 시작하는 경로 범위의 상한 ('a/b0'). SQLite는 UTF-8 바이트순으로 비교하므로
    path >= 'a/b/' AND path < 'a/b0' 가 정확히 'a/b/' 하위 전체이며 ix_item_path 인덱스를 탄다."""
    return prefix[:-1] + "0"


# ── 폴더 비교: 폴더별 다시 읽는 간격 ──
# 폴더 규칙(감시 폴더 기준 상대 경로 → 간격). 그 폴더와 하위 전체에 적용되고, 더 깊은 규칙이 우선한다.
FOLDER_RULES = {
    "auto": ("자동", -1),            # 최근 변경 시점에 따라 (기본)
    "every": ("매 주기", 0),
    "1h": ("1시간", 3600),
    "6h": ("6시간", 6 * 3600),
    "12h": ("하루 2번", 12 * 3600),
    "1d": ("1일", 86400),
    "2d": ("2일", 2 * 86400),
    "7d": ("7일", 7 * 86400),
    "30d": ("30일", 30 * 86400),
    "off": ("읽지 않음", None),       # 이 폴더 아래는 다시 읽지 않음 (변경 감지 안 함)
}


def folder_rule(rules, rel):
    """rel(감시 폴더 기준 상대 경로, 루트는 '')에 적용되는 규칙 키와 규칙이 걸린 경로."""
    best = ("auto", None)
    for path, key in (rules or {}).items():
        path = str(path).strip("/")
        if key in FOLDER_RULES and under(rel, path) and (best[1] is None or len(path) > len(best[1])):
            best = (key, path)
    return best


# 다시 읽는 간격표: (마지막 변경 뒤 지난 기간 상한(초), 다시 읽는 간격(초)). 0=매 주기
POLL_TABLE = (
    (3600, 600),            # 1시간 안: 10분
    (6 * 3600, 1800),       # 1~6시간: 30분
    (86400, 3600),          # 6~24시간: 1시간
    (2 * 86400, 43200),     # 1~2일: 하루 2번
    (7 * 86400, 86400),     # 2~7일: 하루 1번
    (15 * 86400, 2 * 86400),  # 7~15일: 2일
    (30 * 86400, 7 * 86400),  # 16~30일: 7일
    (None, 30 * 86400),     # 31일 이상: 30일
)
# 분류 폴더(새 작품 폴더가 생기는 곳): 1일 안에 바뀌었으면 매 주기, 그 뒤는 일반 폴더와 같음
SKELETON_TABLE = ((86400, 0),) + POLL_TABLE[3:]


def poll_policy(cfg):
    """처리 옵션 → 간격 계산 기준. skeleton_depth: 이 단계까지는 분류 폴더로 본다."""
    try:
        depth = int(cfg.get("poll_skeleton_depth", 2) if cfg.get("poll_skeleton_depth") not in (None, "") else 2)
    except (TypeError, ValueError):
        depth = 2
    return {"skeleton_depth": min(5, max(0, depth)), "table": POLL_TABLE, "skeleton_table": SKELETON_TABLE}


def poll_interval(depth, last_change, now, policy, rule="auto"):
    """다시 읽는 간격(초). 0=매 주기, None=읽지 않음."""
    if rule != "auto" and rule in FOLDER_RULES:
        return FOLDER_RULES[rule][1]
    age = max(0.0, now - (last_change or 0))
    table = policy["skeleton_table"] if depth <= policy["skeleton_depth"] else policy["table"]
    for limit, interval in table:
        if limit is None or age <= limit:
            return interval
    return table[-1][1]


def human_interval(seconds):
    if seconds is None:
        return "읽지 않음"
    if not seconds:
        return "매번"
    if seconds == 43200:
        return "하루 2번"
    if seconds >= 86400:
        return f"{seconds // 86400}일"
    if seconds >= 3600:
        return f"{seconds // 3600}시간"
    return f"{seconds // 60}분"


WEBHOOK_HELP = ("알림 주소는 디스코드 웹훅(https://discord.com/api/webhooks/…) 또는 "
                "구글 앱스 스크립트 웹 앱(https://script.google.com/macros/s/…/exec?…) 형식이어야 합니다.")


def webhook_allowed(url):
    """알림을 보낼 수 있는 주소: 디스코드 웹훅, 또는 디스코드로 중계하는 구글 앱스 스크립트 웹 앱(쿼리 포함 가능)."""
    url = str(url or "").strip()
    if url.startswith(("https://discord.com/api/webhooks/", "https://discordapp.com/api/webhooks/")):
        return True
    return bool(re.match(r"^https://script\.google\.com/macros/s/[A-Za-z0-9_-]+/exec(\?.*)?$", url))


def utcnow():
    return datetime.now(timezone.utc)


def stamp(value):
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def parse_time(value):
    value = re.sub(r"(\.\d{6})\d+", r"\1", str(value or "").strip()).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


# ─────────────────────────── 상태 DB ───────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS cursor(root TEXT PRIMARY KEY, token TEXT, status TEXT, error TEXT, updated TEXT);
CREATE TABLE IF NOT EXISTS item(root TEXT, file_id TEXT, path TEXT, is_dir INTEGER, sig TEXT,
                                PRIMARY KEY(root, file_id));
CREATE INDEX IF NOT EXISTS ix_item_path ON item(root, path);
CREATE TABLE IF NOT EXISTS outside(root TEXT, file_id TEXT, PRIMARY KEY(root, file_id));
CREATE TABLE IF NOT EXISTS receipt(root TEXT, id TEXT, ts TEXT, PRIMARY KEY(root, id));
CREATE TABLE IF NOT EXISTS event(
    id INTEGER PRIMARY KEY AUTOINCREMENT, root TEXT, action TEXT, item_type TEXT,
    path TEXT, removed_path TEXT, created TEXT, ready_at REAL, attempts INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending', message TEXT DEFAULT '', result TEXT DEFAULT '', finished TEXT DEFAULT '');
CREATE INDEX IF NOT EXISTS ix_event_ready ON event(status, ready_at);
CREATE INDEX IF NOT EXISTS ix_event_created ON event(created);
CREATE INDEX IF NOT EXISTS ix_event_path ON event(root, path);
CREATE TABLE IF NOT EXISTS vfs_map(root TEXT PRIMARY KEY, rc TEXT, fs TEXT, remote TEXT, detected TEXT);
CREATE TABLE IF NOT EXISTS seedstate(root TEXT PRIMARY KEY, started TEXT);
CREATE TABLE IF NOT EXISTS links(parent TEXT, shortcut_id TEXT, target_id TEXT, path TEXT, drive TEXT, checked TEXT,
                                 PRIMARY KEY(parent, shortcut_id));
CREATE TABLE IF NOT EXISTS pollfolder(root TEXT, folder_id TEXT, path TEXT, depth INTEGER, last_change REAL, last_list REAL,
                                      PRIMARY KEY(root, folder_id));
CREATE INDEX IF NOT EXISTS ix_pollfolder_path ON pollfolder(root, path);
CREATE TABLE IF NOT EXISTS link_checked(parent TEXT, target_id TEXT, drive TEXT, PRIMARY KEY(parent, target_id));
CREATE TABLE IF NOT EXISTS root_stat(root TEXT PRIMARY KEY, checked TEXT, raw INTEGER, outside INTEGER, ext INTEGER,
                                     same INTEGER, events INTEGER, note TEXT, elapsed REAL);
"""


def open_db(path):
    db = sqlite3.connect(path, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    db.executescript(SCHEMA)
    try:  # v1.12.1: '선택 재시도'로 강제 처리할 때 전체 스캔 회피를 건너뛰는 표시
        db.execute("ALTER TABLE event ADD COLUMN force INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:  # v1.16.1: 폴더 자체의 Drive 수정 시각 (안쪽이 바뀌면 깨우기)
        db.execute("ALTER TABLE pollfolder ADD COLUMN folder_mtime REAL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    return db


class Store:
    def __init__(self, path):
        self.path = path
        self.db = open_db(path)

    def get_cursor(self, root):
        row = self.db.execute("SELECT token, status FROM cursor WHERE root=?", (root,)).fetchone()
        return (row["token"], row["status"]) if row else ("", "")

    def save_cursor(self, root, token, status="ready", error=""):
        with self.db:
            self.db.execute(
                "INSERT INTO cursor(root, token, status, error, updated) VALUES(?,?,?,?,?) "
                "ON CONFLICT(root) DO UPDATE SET token=excluded.token, status=excluded.status, "
                "error=excluded.error, updated=excluded.updated",
                (root, token, status, str(error)[:2000], datetime.now().isoformat(timespec="seconds")))

    def set_error(self, root, error, status="error"):
        token, _ = self.get_cursor(root)
        self.save_cursor(root, token, status, error)

    def get_item(self, root, file_id):
        row = self.db.execute("SELECT path, is_dir, sig FROM item WHERE root=? AND file_id=?",
                              (root, file_id)).fetchone()
        return {"path": row["path"], "is_dir": bool(row["is_dir"]), "sig": row["sig"] or ""} if row else None

    def _upsert(self, root, file_id, item):
        self.db.execute(
            "INSERT INTO item(root, file_id, path, is_dir, sig) VALUES(?,?,?,?,?) "
            "ON CONFLICT(root, file_id) DO UPDATE SET path=excluded.path, is_dir=excluded.is_dir, sig=excluded.sig",
            (root, file_id, item["path"], int(bool(item["is_dir"])), item.get("sig") or ""))

    def set_item(self, root, file_id, item):
        with self.db:
            self._upsert(root, file_id, item)

    def merge_items(self, root, rows):
        """수집한 목록을 기존 항목 위에 합친다. 수집 중에 변경 처리로 이미 갱신된 항목은 건드리지 않는다."""
        with self.db:
            self.db.executemany(
                "INSERT OR IGNORE INTO item(root, file_id, path, is_dir, sig) VALUES(?,?,?,?,?)",
                [(root, row[0], row[1], int(row[2]), row[3] if len(row) > 3 else "") for row in rows])

    def seed_pending(self, root):
        return self.db.execute("SELECT 1 FROM seedstate WHERE root=?", (root,)).fetchone() is not None

    def set_seed(self, root, pending):
        with self.db:
            if pending:
                self.db.execute("INSERT OR REPLACE INTO seedstate VALUES(?,?)",
                                (root, datetime.now().isoformat(timespec="seconds")))
            else:
                self.db.execute("DELETE FROM seedstate WHERE root=?", (root,))

    def replace_items(self, root, rows):
        with self.db:
            self.db.execute("DELETE FROM item WHERE root=?", (root,))
            self.db.executemany(
                "INSERT OR REPLACE INTO item(root, file_id, path, is_dir, sig) VALUES(?,?,?,?,?)",
                [(root, row[0], row[1], int(row[2]), row[3] if len(row) > 3 else "") for row in rows])

    def is_outside(self, root, file_id):
        return self.db.execute("SELECT 1 FROM outside WHERE root=? AND file_id=?", (root, file_id)).fetchone() is not None

    def set_outside(self, root, file_id, outside=True):
        with self.db:
            if outside:
                self.db.execute("INSERT OR IGNORE INTO outside VALUES(?,?)", (root, file_id))
            else:
                self.db.execute("DELETE FROM outside WHERE root=? AND file_id=?", (root, file_id))

    def has_receipt(self, root, receipt_id):
        return self.db.execute("SELECT 1 FROM receipt WHERE root=? AND id=?", (root, receipt_id)).fetchone() is not None

    def prune_receipts(self, root, before):
        with self.db:
            self.db.execute("DELETE FROM receipt WHERE root=? AND ts<?", (root, before))

    def record(self, root, file_id, prev, cur, event, buffer_seconds, receipt=None):
        """상태 갱신 + 이벤트 추가를 한 트랜잭션으로 (같은 페이지 재생 시 중복 이벤트 방지)."""
        old = (prev or {}).get("path") or ""
        new = (cur or {}).get("path") or ""
        with self.db:
            if prev and prev.get("is_dir") and old and old != new:
                prefix = old + "/"
                if new:
                    self.db.execute(
                        "UPDATE item SET path = ? || substr(path, ?) WHERE root=? AND path >= ? AND path < ?",
                        (new, len(old) + 1, root, prefix, prefix_upper(prefix)))
                else:
                    self.db.execute("DELETE FROM item WHERE root=? AND path >= ? AND path < ?",
                                    (root, prefix, prefix_upper(prefix)))
            if new:
                self._upsert(root, file_id, cur)
            else:
                self.db.execute("DELETE FROM item WHERE root=? AND file_id=?", (root, file_id))
            if event:
                self.db.execute(
                    "INSERT INTO event(root, action, item_type, path, removed_path, created, ready_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (root, event["action"], event["item_type"], event["path"], event["removed_path"],
                     datetime.now().isoformat(timespec="seconds"), time.time() + buffer_seconds))
            if receipt:
                self.db.execute("INSERT OR IGNORE INTO receipt VALUES(?,?,?)", (root, receipt[0], receipt[1]))

    def save_stat(self, root, stat, elapsed):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO root_stat VALUES(?,?,?,?,?,?,?,?,?)",
                            (root, datetime.now().isoformat(timespec="seconds"), stat["raw"], stat["outside"],
                             stat["ext"], stat["same"], stat["events"], stat["note"], round(elapsed, 1)))

    def get_vfs_map(self, root):
        row = self.db.execute("SELECT rc, fs, remote FROM vfs_map WHERE root=?", (root,)).fetchone()
        return dict(row) if row else None

    def save_vfs_map(self, root, rc, fs, remote):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO vfs_map VALUES(?,?,?,?,?)",
                            (root, rc, fs, remote, datetime.now().isoformat(timespec="seconds")))

    def claim(self, limit=500):
        rows = self.db.execute(
            "SELECT * FROM event WHERE status IN ('pending','waiting') AND ready_at<=? ORDER BY ready_at, id LIMIT ?",
            (time.time(), limit)).fetchall()
        return [dict(row) for row in rows]

    def finish(self, event, status, message, result, max_attempts, retry_in=None):
        now = datetime.now().isoformat(timespec="seconds")
        payload = json.dumps(result, ensure_ascii=False)
        with self.db:
            if status in ("done", "skipped", "timeout"):
                self.db.execute("UPDATE event SET status=?, message=?, result=?, finished=? WHERE id=?",
                                (status, message[:2000], payload, now, event["id"]))
                return status
            if status == "waiting":  # 파일이 아직 안 보임: 시도 횟수는 올리지 않고 잠시 뒤 다시
                self.db.execute("UPDATE event SET status='waiting', ready_at=?, message=?, result=? WHERE id=?",
                                (time.time() + (retry_in or FILE_WAIT_INTERVAL), message[:2000], payload, event["id"]))
                return status
            attempts = event["attempts"] + 1
            status = "failed" if attempts >= max_attempts else "pending"
            delay = min(1800, 60 * 2 ** (attempts - 1))
            self.db.execute(
                "UPDATE event SET status=?, attempts=?, ready_at=?, message=?, result=?, finished=? WHERE id=?",
                (status, attempts, time.time() + delay, message[:2000], payload,
                 now if status == "failed" else "", event["id"]))
            return status

    def cleanup(self, days):
        cutoff = (datetime.now() - timedelta(days=days)).isoformat(timespec="seconds")
        with self.db:
            self.db.execute("DELETE FROM event WHERE status IN ('done','skipped','timeout') AND created<?", (cutoff,))
            # 끝내 실패한 기록은 보관 기간의 두 배까지 남겨 두고 정리 (대기 중인 기록은 지우지 않음)
            old = (datetime.now() - timedelta(days=days * 2)).isoformat(timespec="seconds")
            self.db.execute("DELETE FROM event WHERE status='failed' AND created<?", (old,))


# ─────────────────────────── rclone / Drive 인증 ───────────────────────────

def explain_drive_error(code, detail, where=""):
    """Google API 오류를 무엇이 문제이고 어떻게 하면 되는지 한국어로 풀어 쓴다."""
    text = (detail or "").lower()
    target = {"changes": "변경 목록 조회", "changes/startPageToken": "변경 목록 시작점 조회",
              "files": "폴더 내용 조회", "file": "폴더·파일 정보 조회", "activity": "활동 기록 조회"}.get(where, "Drive 요청")
    if "pagetoken" in text or (code == 400 and where == "changes"):
        return f"{target}: 저장해 둔 체크포인트가 맞지 않습니다(감시 방식이 바뀌었거나 너무 오래됨). 새 시작점에서 다시 시작합니다."
    if "teamdrivemembershiprequired" in text or "membership" in text:
        return f"{target}: 이 계정은 해당 공유 드라이브의 멤버가 아니라 변경 목록을 받을 수 없습니다. 폴더 비교 방식으로 감시하세요."
    if code == 400 and "invalid" in text:
        what = "폴더 ID나 검색 조건" if where in ("files", "file") else "요청 값"
        return f"{target}: {what}이 올바르지 않습니다. 감시 폴더의 Drive 폴더 ID가 맞는지 [점검]으로 확인하세요."
    if code == 404 or "notfound" in text:
        return f"{target}: 폴더나 파일을 찾을 수 없습니다. 지워졌거나, 이 리모트 계정에 접근 권한이 없습니다."
    if code == 401 or "autherror" in text or "invalid credentials" in text:
        return f"{target}: 인증이 거부됐습니다. 토큰이 만료됐거나 무효입니다. [rclone 확인]에서 토큰 상태를 보세요."
    if "insufficient" in text and "scope" in text:
        return f"{target}: 토큰에 필요한 권한(scope)이 없습니다."
    if code == 403 and ("insufficientfilepermissions" in text or "forbidden" in text):
        return f"{target}: 접근 권한이 없습니다. 이 리모트 계정이 그 폴더를 볼 수 있는지 확인하세요."
    if code == 429 or "ratelimit" in text or "quota" in text:
        return f"{target}: Drive 호출 한도에 걸렸습니다. 잠시 뒤 자동으로 다시 시도합니다 (계속되면 [Drive 초당 요청 수]를 낮추세요)."
    if code in (500, 502, 503):
        return f"{target}: Google 쪽 일시 오류입니다. 자동으로 다시 시도합니다."
    return f"{target}: Google API 오류"


class DriveError(RuntimeError):
    def __init__(self, code, message, where=""):
        self.code = code
        self.detail = message
        self.where = where
        super().__init__(f"{explain_drive_error(code, message, where)} (Google API {code}: {message})")


class Rclone:
    """rclone.conf의 토큰을 '읽기만' 한다. 갱신은 `rclone about`을 실행해 rclone 자신에게 맡긴다."""

    def __init__(self, binary, config, timeout=60):
        self.binary, self.config, self.timeout = binary, config, timeout
        self._tokens = {}
        self._conf_path = config or ""
        self._conf_stat = "init"
        self.rc_sources = []  # 커스텀 인증 토큰을 빌려 올 rclone 마운트 RC 목록 ({"rc", "fs", "auth"})

    def config_path(self):
        """실제로 사용하는 rclone.conf 경로 (비어 있으면 rclone 기본 위치를 물어봄)."""
        if not self._conf_path:
            try:
                out = subprocess.run([self.binary, "config", "file"], capture_output=True, text=True, timeout=15).stdout
                lines = [l.strip() for l in out.splitlines() if l.strip()]
                self._conf_path = lines[-1] if lines else ""
            except (OSError, subprocess.SubprocessError):
                self._conf_path = ""
        return self._conf_path

    def config_changed(self):
        """rclone.conf 내용(mtime/크기/inode)이 바뀌었으면 토큰 캐시를 버리고 True."""
        path = self.config_path()
        try:
            info = os.stat(path)
            stat = (info.st_mtime_ns, info.st_size, info.st_ino)
        except OSError:
            stat = None
        if self._conf_stat == "init":
            self._conf_stat = stat
            return False
        if stat != self._conf_stat:
            self._conf_stat = stat
            self._tokens.clear()
            return True
        return False

    def command(self, *args):
        if self.config and not os.path.isfile(self.config):
            raise RuntimeError(f"rclone.conf 파일이 컨테이너 안에 없습니다: {self.config} "
                               "(실행 환경의 rclone.conf 경로 또는 도커 볼륨 마운트를 확인하세요)")
        return [self.binary] + (["--config", self.config] if self.config else []) + list(args)

    def run(self, *args, timeout=None):
        command = self.command(*args)
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout or self.timeout)
        if result.returncode != 0:
            raise RuntimeError(f"rclone {args[0]} 실패(코드 {result.returncode}): {result.stderr.strip()[-500:]}")
        return result.stdout

    def _read(self, remote):
        conf = json.loads(self.run("config", "dump")).get(remote) or {}
        if str(conf.get("type")).lower() != "drive":
            raise RuntimeError(f"{remote}는 drive 타입 리모트가 아닙니다.")
        token = json.loads(conf.get("token") or "{}")
        expiry = parse_time(token.get("expiry")) or utcnow() + timedelta(minutes=30)
        return conf, token, expiry

    @staticmethod
    def auth_mode(conf, token):
        """custom: 커스텀 인증(gds_endpoint 등) → 공유 rclone이 갱신한 값을 읽기만
        memory: 본인 client_id/secret이 있음 → 플러그인이 메모리에서 직접 갱신 (파일에 쓰지 않음)
        rclone: rclone 내장 client → rclone에게 갱신을 맡김 (rclone이 rclone.conf에 저장)"""
        if any("endpoint" in k.lower() for k in conf):
            return "custom"
        if conf.get("client_id") and conf.get("client_secret") and token.get("refresh_token"):
            return "memory"
        return "rclone"

    def _memory_refresh(self, remote, conf, token):
        form = urlencode({"client_id": conf["client_id"], "client_secret": conf["client_secret"],
                          "refresh_token": token["refresh_token"], "grant_type": "refresh_token"}).encode()
        url = conf.get("token_url") or "https://oauth2.googleapis.com/token"
        request = Request(url, data=form, headers={"Content-Type": "application/x-www-form-urlencoded"})
        try:
            with urlopen(request, timeout=30) as response:
                data = json.loads(response.read() or b"{}")
        except HTTPError as error:
            try:
                info = json.loads(error.read() or b"{}")
            except ValueError:
                info = {}
            reason = info.get("error_description") or info.get("error") or f"HTTP {error.code}"
            if info.get("error") == "invalid_grant":
                reason += " — refresh token이 무효입니다. rclone config reconnect로 다시 인증하세요."
            raise RuntimeError(f"{remote} 토큰 갱신 실패: {reason}") from None
        except (URLError, OSError) as error:
            raise RuntimeError(f"{remote} 토큰 갱신 실패(네트워크): {error}") from None
        expiry = utcnow() + timedelta(seconds=max(60, int(data.get("expires_in") or 3600) - 60))
        log.info("[%s] 토큰을 메모리에서 갱신했습니다 (rclone.conf에는 쓰지 않음, 만료 %s)",
                 remote, expiry.astimezone().strftime("%H:%M"))
        return data["access_token"], expiry

    _lock = threading.RLock()

    def token(self, remote, force=False):
        """rclone.conf는 FF·호스트 rclone과 공유한다는 전제로, 리모트 종류에 따라 토큰을 얻는다 (auth_mode 참고)."""
        with self._lock:
            return self._token(remote, force)

    def _token(self, remote, force=False):
        self.config_changed()
        cached = self._tokens.get(remote)
        if cached and not force and cached["expiry"] - utcnow() > timedelta(minutes=3):
            return cached
        conf, token, expiry = self._read(remote)
        mode = self.auth_mode(conf, token)
        access = token.get("access_token")
        fresh = access and expiry - utcnow() > timedelta(minutes=3)
        notes = []
        if mode == "memory" and (force or not fresh):
            access, expiry = self._memory_refresh(remote, conf, token)
        elif mode == "custom" and (force or not fresh):
            # 1) 이 리모트를 마운트해 쓰고 있는 rclone(RC)의 메모리에서 최신 토큰을 빌려 온다
            borrowed, notes = self._borrow_from_rc(remote)
            if borrowed:
                access, expiry = borrowed
            else:
                # 2) 이 인증 방식을 지원하는 rclone이면, rclone이 실제로 보내는 토큰을 요청 헤더에서 읽는다.
                #    (gds 포크처럼 토큰을 서버에서 받아 메모리에서만 쓰고 rclone.conf에 저장하지 않는 경우 대응)
                header, note = self._token_from_headers(remote)
                if header:
                    access, expiry = header, utcnow() + timedelta(minutes=45)
                else:
                    notes.append(note)
                    conf, token, expiry = self._read(remote)  # 혹시 저장하는 방식이면 파일에 새 값이 있음
                    access = token.get("access_token")
        elif mode == "rclone" and (force or not fresh):
            self.run("about", f"{remote}:", "--json")  # rclone이 토큰을 갱신해 rclone.conf에 저장
            conf, token, expiry = self._read(remote)
            access = token.get("access_token")
        if not access or expiry <= utcnow():
            where = self.config or "rclone 기본 설정 파일"
            if mode == "custom":
                custom = [k for k in conf if "endpoint" in k.lower()]
                raise RuntimeError(
                    f"{remote} 토큰이 만료된 상태입니다(만료 {token.get('expiry') or '알 수 없음'}, 설정 파일: {where}). "
                    f"커스텀 인증({', '.join(custom)}) 리모트입니다. 시도한 것: " + (" / ".join(notes) or "없음") +
                    ". 이 리모트를 마운트한 rclone의 RC에서 토큰을 읽을 수 있게 하거나(config/get 권한), "
                    "이 인증 방식을 지원하는 rclone 실행 파일을 지정하세요.")
            raise RuntimeError(f"{remote} 토큰이 만료된 상태입니다(만료 {token.get('expiry') or '알 수 없음'}, 설정 파일: {where}). "
                               "rclone이 이 리모트의 토큰을 갱신하지 못했습니다. rclone.conf 경로와 리모트의 인증 방식을 확인하세요.")
        self._tokens[remote] = {"access": access, "expiry": expiry, "mode": mode,
                                "team_drive": str(conf.get("team_drive") or "").strip()}
        return self._tokens[remote]

    def _token_from_headers(self, remote):
        """rclone about을 --dump auth로 실행해 Google API 요청의 Authorization 헤더에서 access token을 읽는다."""
        try:
            command = self.command("about", f"{remote}:", "--json", "--dump", "auth", "-vv")
            result = subprocess.run(command, capture_output=True, text=True, timeout=90)
        except (OSError, subprocess.SubprocessError, RuntimeError) as error:
            return None, f"rclone 실행 실패: {error}"
        found = re.findall(r"Authorization:\s*Bearer\s+([A-Za-z0-9._\-]+)", result.stderr)
        if result.returncode == 0 and found:
            log.info("[%s] rclone(%s)이 쓰는 토큰을 요청 헤더에서 가져왔습니다 (45분 사용, 파일에는 쓰지 않음)", remote, self.binary)
            return found[-1], ""
        tail = [l for l in result.stderr.splitlines() if "ERROR" in l or "Failed" in l or "CRITICAL" in l][-1:]
        if result.returncode != 0:
            return None, f"rclone({self.binary})으로 {remote}에 접속하지 못함 — 이 rclone이 이 인증 방식을 지원하지 않을 수 있음" + \
                (f": {tail[0][-160:]}" if tail else "")
        return None, "rclone 요청 헤더에서 토큰을 찾지 못함"

    def _borrow_from_rc(self, remote):
        """마운트 중인 rclone은 토큰을 스스로 갱신해 메모리에 들고 있다. RC의 config/get으로 그 값을 읽는다.
        (읽기만 하고 rclone.conf에는 쓰지 않는다)"""
        notes, want = [], f"{remote}:"
        seen, hosted = set(), False
        for source in self.rc_sources:
            rc = source["rc"]
            if rc in seen:
                continue
            seen.add(rc)
            rule = VfsRule({"local": "/", "rc": rc, "user": source.get("user"), "pass": source.get("pass")})
            try:
                fses = rule.call("vfs/list", {}, 15).get("vfses") or []
            except Exception as error:
                notes.append(f"{rc} 연결 실패")
                continue
            if want not in fses:
                continue
            hosted = True
            try:
                conf = rule.call("config/get", {"name": remote}, 15)
            except Exception as error:
                message = str(error)
                if "auth" in message.lower():
                    message = "RC에 인증이 설정돼 있지 않아 config/get을 쓸 수 없음 (--rc-user/--rc-pass 또는 --rc-no-auth 필요)"
                notes.append(f"{rc}: {message[:160]}")
                continue
            try:
                token = json.loads(conf.get("token") or "{}")
            except ValueError:
                token = {}
            expiry = parse_time(token.get("expiry"))
            if token.get("access_token") and expiry and expiry - utcnow() > timedelta(minutes=3):
                log.info("[%s] 토큰을 마운트 중인 rclone(%s)에서 가져왔습니다 (만료 %s, 파일에는 쓰지 않음)",
                         remote, rc, expiry.astimezone().strftime("%H:%M"))
                return (token["access_token"], expiry), notes
            notes.append(f"{rc}: 마운트의 토큰도 만료 상태")
        if not seen:
            notes.append("토큰을 빌려 올 RC 없음 (보관함 RC 주소나 VFS 규칙이 없음)")
        elif not hosted:
            notes.append(f"{want}를 마운트한 RC를 찾지 못함")
        return None, notes

    def env_for(self, remote):
        """rclone을 실행할 때 메모리에서 갱신한 토큰을 환경변수로 넘겨, rclone이 따로 갱신·저장하지 않게 한다."""
        info = self._tokens.get(remote)
        if not info or info.get("mode") not in ("memory", "custom") or not re.fullmatch(r"[A-Za-z0-9_]+", remote):
            return None
        conf, token, _ = self._read(remote)
        token = dict(token, access_token=info["access"], expiry=info["expiry"].isoformat())
        env = dict(os.environ)
        env[f"RCLONE_CONFIG_{remote.upper()}_TOKEN"] = json.dumps(token)
        return env


RATE_REASONS = ("ratelimitexceeded", "userratelimitexceeded", "rate limit", "quota exceeded", "backenderror")
DRIVE_RPS = 3.0  # 리모트당 Drive API 초당 요청 수 (설정에서 바꿈)


class RateLimiter:
    """리모트(=같은 토큰)별로 요청 간격을 맞춘다. 여러 스레드·감시 폴더가 같은 토큰을 써도 합쳐서 초당 DRIVE_RPS를 넘지 않는다."""
    _all = {}
    _guard = threading.Lock()

    def __init__(self):
        self.lock = threading.Lock()
        self.next_at = 0.0
        self.penalty_until = 0.0

    @classmethod
    def of(cls, remote):
        with cls._guard:
            return cls._all.setdefault(remote, cls())

    def wait(self):
        with self.lock:
            now = time.monotonic()
            start = max(now, self.next_at, self.penalty_until)
            self.next_at = start + 1.0 / max(0.2, DRIVE_RPS)
        delay = start - time.monotonic()
        if delay > 0:
            time.sleep(delay)

    def penalize(self, seconds):
        """호출 제한에 걸리면 이 리모트의 모든 요청을 잠시 멈춘다."""
        with self.lock:
            self.penalty_until = max(self.penalty_until, time.monotonic() + seconds)


class DriveApi:
    def __init__(self, rclone, remote, timeout=60):
        self.rclone, self.remote, self.timeout = rclone, remote, timeout
        self.limiter = RateLimiter.of(remote)

    @property
    def team_drive(self):
        return self.rclone.token(self.remote)["team_drive"]

    def call(self, url, body=None):
        auth_retry, backoff = False, 0
        while True:
            self.limiter.wait()
            access = self.rclone.token(self.remote, force=auth_retry)["access"]
            request = Request(url, data=None if body is None else json.dumps(body).encode(),
                              headers={"Authorization": f"Bearer {access}", "Content-Type": "application/json"})
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    return json.loads(response.read() or b"{}")
            except HTTPError as error:
                raw = error.read().decode("utf-8", "replace")
                try:
                    info = json.loads(raw).get("error") or {}
                    reason = ((info.get("errors") or [{}])[0]).get("reason") or info.get("status") or ""
                    detail = f"{info.get('message') or raw[:300]}" + (f" ({reason})" if reason else "")
                except (ValueError, AttributeError):
                    detail = raw[:300]
                if error.code == 401 and not auth_retry:
                    auth_retry = True
                    continue
                limited = error.code in (429, 500, 502, 503) or \
                    (error.code == 403 and any(r in detail.lower() for r in RATE_REASONS))
                if limited and backoff < 6 and not STOP:
                    # 호출 제한: 2, 4, 8, 16, 32, 64초 + 무작위로 쉬었다가 재시도 (같은 리모트의 다른 요청도 같이 쉼)
                    wait = 2 ** (backoff + 1) + (hash((url, time.time())) % 1000) / 1000
                    backoff += 1
                    self.limiter.penalize(wait)
                    log.info("[%s] Drive 호출 제한(%s) → %.0f초 쉬고 다시 시도 (%d/6)", self.remote, reason or error.code, wait, backoff)
                    continue
                raise DriveError(429 if limited else error.code, detail, self._where(url)) from None

    @staticmethod
    def _where(url):
        if "driveactivity" in url:
            return "activity"
        path = url.split("/drive/v3/", 1)[-1].split("?", 1)[0]
        if path.startswith("files/"):
            return "file"
        return path

    def get(self, path, **params):
        params.setdefault("supportsAllDrives", "true")
        return self.call(f"{DRIVE_API}/{path}?{urlencode(params)}")

    def file(self, file_id):
        try:
            return self.get(f"files/{file_id}", fields=FILE_FIELDS)
        except DriveError as error:
            if error.code == 404:
                return None
            raise


# ─────────────────────────── 변경 감지 ───────────────────────────

def signature(data):
    keys = ("modifiedTime", "size", "md5Checksum")
    return json.dumps({k: data.get(k) for k in keys}, sort_keys=True) if data.get("modifiedTime") else ""


def build_event(prev, cur):
    old = (prev or {}).get("path") or ""
    new = (cur or {}).get("path") or ""
    item_type = "directory" if (cur or prev or {}).get("is_dir") else "file"
    if old and not new:
        return {"action": "delete", "item_type": item_type, "path": old, "removed_path": old}
    if new and not old:
        return {"action": "create", "item_type": item_type, "path": new, "removed_path": ""}
    if old != new:
        return {"action": "rename", "item_type": item_type, "path": new, "removed_path": old}
    if not new or item_type == "directory" or (cur.get("sig") and prev.get("sig") == cur["sig"]):
        return None
    return {"action": "edit", "item_type": item_type, "path": new, "removed_path": ""}


class Watcher:
    def __init__(self, cfg, store, rclone, extensions, buffer_seconds, api_timeout):
        self.cfg = dict(cfg)
        self.name = cfg["name"]
        self.source_remote = str(cfg["source_remote"]).rstrip(":")
        self.root_id = cfg["root_id"]
        self.local_root = norm(cfg["local_root"])
        self.seed = cfg.get("seed", True)
        self.store, self.rclone = store, rclone
        self.api = DriveApi(rclone, self.source_remote, api_timeout)
        self.extensions = extensions
        self.buffer_seconds = buffer_seconds
        self.retry_at = 0.0
        self.failures = 0
        self.root_checked = False
        self.drive_id = ""
        self.user_feed = False  # 공유 드라이브 멤버가 아니면 계정 전체 변경 목록으로 감시
        self.verbose = False
        self.reset_stat()
        if not self.local_root:
            raise ValueError(f"{self.name}: local_root는 절대 경로여야 합니다.")

    def reset_stat(self):
        self.stat = {"raw": 0, "outside": 0, "ext": 0, "same": 0, "events": 0, "note": "", "seed": ""}

    def trace(self, message, *args):
        """자세한 로그를 켜면 변경 하나하나를 어떻게 처리했는지 남긴다."""
        if self.verbose:
            log.info("[%s] " + message, self.name, *args)

    def summary(self):
        st = self.stat
        if st["note"]:
            return f"{self.name}: {st['note']}"
        if not st["raw"]:
            return f"{self.name}: 변경 없음"
        if not st["events"] and st["outside"] == st["raw"]:
            return f"{self.name}: 변경 없음 (같은 드라이브의 다른 폴더 변경 {st['outside']}건 건너뜀)"
        parts = [f"기록 {st['events']}"]
        for key, label in (("outside", "범위 밖"), ("ext", "확장자 제외"), ("same", "변화 없음")):
            if st[key]:
                parts.append(f"{label} {st[key]}")
        return f"{self.name}: Drive 변경 {st['raw']} → " + ", ".join(parts)

    def check_root(self):
        """감시 폴더 ID가 바로가기면 실제 폴더 ID로 바꾼다 (하위 항목의 parents는 실제 폴더를 가리키므로)."""
        if self.root_checked:
            return
        data = self.api.file(self.root_id)
        if data is None:
            raise RuntimeError(f"감시 폴더 {self.root_id}를 찾을 수 없습니다. 폴더 ID와 {self.source_remote} 계정의 접근 권한을 확인하세요.")
        if data.get("mimeType") == SHORTCUT_MIME:
            detail = data.get("shortcutDetails") or {}
            if detail.get("targetMimeType") != FOLDER_MIME:
                raise RuntimeError("감시 폴더 ID가 폴더가 아닌 파일을 가리키는 바로가기입니다.")
            log.info("[%s] 감시 폴더가 바로가기라서 실제 폴더 %s를 감시합니다.", self.name, detail["targetId"])
            self.root_id = detail["targetId"]
            data = self.api.file(self.root_id) or {}
        elif data.get("mimeType") != FOLDER_MIME:
            raise RuntimeError("감시 폴더 ID가 폴더가 아닙니다.")
        # 변경 목록은 리모트의 기본 드라이브(team_drive)가 아니라 감시 폴더가 실제로 있는 드라이브 기준으로 받는다.
        # 토큰은 계정 단위라 같은 토큰으로 다른 공유 드라이브의 변경도 조회할 수 있다 (그 드라이브에 접근 권한만 있으면 됨).
        self.drive_id = "" if self.user_feed else (data.get("driveId") or "")
        self.root_checked = True

    @staticmethod
    def shortcut_target(data):
        """폴더를 가리키는 바로가기면 대상 폴더 ID."""
        if data.get("mimeType") != SHORTCUT_MIME:
            return ""
        detail = data.get("shortcutDetails") or {}
        return detail.get("targetId", "") if detail.get("targetMimeType") == FOLDER_MIME else ""

    def discover_links(self, force=False):
        """감시 폴더 안의 폴더 바로가기 중 대상이 '다른 드라이브'에 있는 것을 찾아 links에 기록한다.
        같은 드라이브의 대상은 이 폴더의 변경 목록에 함께 들어오지만, 다른 드라이브의 변경은 들어오지 않아
        그 대상 폴더를 따로 감시해야 한다 (Worker가 links로 하위 감시를 만든다).
        새로 생긴 바로가기만 Drive에 한 번씩 물어보고, 결과는 기억한다. 6시간마다 또는 바로가기가 바뀔 때 다시 맞춘다."""
        if not force and time.monotonic() < getattr(self, "next_link_check", 0):
            return
        self.next_link_check = time.monotonic() + 6 * 3600
        rows = self.store.db.execute(
            "SELECT file_id, path, sig FROM item WHERE root=? AND is_dir=1 AND sig LIKE 'shortcut:%'", (self.name,)).fetchall()
        current = {}
        for row in rows:
            target = (row["sig"] or "").split(":", 1)[1]
            if target:
                current[row["file_id"]] = (target, row["path"])
        known = {r["target_id"]: r["drive"] for r in
                 self.store.db.execute("SELECT target_id, drive FROM link_checked WHERE parent=?", (self.name,))}
        my_drive = getattr(self, "drive_id", "") or ""
        wanted = {}
        for shortcut_id, (target, path) in current.items():
            if target not in known:
                try:
                    data = self.api.file(target) or {}
                except Exception as error:
                    log.warning("[%s] 바로가기 대상 확인 실패 (%s): %s", self.name, path, error)
                    continue
                known[target] = data.get("driveId") or ""
                with self.store.db:
                    self.store.db.execute("INSERT OR REPLACE INTO link_checked VALUES(?,?,?)", (self.name, target, known[target]))
            if known[target] != my_drive:
                wanted[shortcut_id] = (target, path, known[target])
        with self.store.db:
            existing = {r["shortcut_id"]: (r["target_id"], r["path"]) for r in
                        self.store.db.execute("SELECT shortcut_id, target_id, path FROM links WHERE parent=?", (self.name,))}
            for shortcut_id in set(existing) - set(wanted):
                self.store.db.execute("DELETE FROM links WHERE parent=? AND shortcut_id=?", (self.name, shortcut_id))
            for shortcut_id, (target, path, drive) in wanted.items():
                if existing.get(shortcut_id) != (target, path):
                    self.store.db.execute("INSERT OR REPLACE INTO links VALUES(?,?,?,?,?,?)",
                                          (self.name, shortcut_id, target, path, drive, datetime.now().isoformat(timespec="seconds")))
                    log.info("[%s] 다른 드라이브를 가리키는 바로가기 발견 → 따로 감시: %s", self.name, path)

    def map_shortcut(self, shortcut_id, target_id, path):
        """감시 폴더 안의 폴더 바로가기: 대상 폴더 ID를 바로가기 경로로 등록해,
        대상 폴더 안에서 생긴 변경(parents=대상 폴더)이 바로가기 경로로 해석되게 한다."""
        self.store.set_item(self.name, target_id, {"path": path, "is_dir": True, "sig": f"target:{shortcut_id}"})
        self.store.set_outside(self.name, target_id, False)
        self.next_link_check = 0  # 다음 확인 때 바로가기 대상 드라이브를 맞춘다

    def relevant(self, event):
        if event["item_type"] == "directory" or self.extensions is None:
            return True
        return any(posixpath.splitext(p)[1].lower() in self.extensions or posixpath.basename(p) == ".bookoasisignore"
                   for p in (event["path"], event["removed_path"]) if p)

    def resolve(self, data, depth=0):
        """Drive 항목 → 로컬 경로. 감시 폴더 밖이면 ''. 부모 경로는 item/outside 테이블로 캐시."""
        name = str(data.get("name") or "").replace("/", "／")  # rclone 드라이브 인코딩과 동일
        parents = data.get("parents") or []
        if not name or name in (".", "..") or not parents or depth > 64:
            return ""
        parent_id = parents[0]
        if parent_id == self.root_id:
            base = self.local_root
        else:
            parent = self.store.get_item(self.name, parent_id)
            if parent:
                base = parent["path"]
            elif self.store.is_outside(self.name, parent_id):
                return ""
            else:
                parent_data = self.api.file(parent_id)
                base = self.resolve(parent_data, depth + 1) if parent_data else ""
                if not base:
                    self.store.set_outside(self.name, parent_id)
                    return ""
                self.store.set_item(self.name, parent_id, {"path": base, "is_dir": True, "sig": ""})
        return posixpath.join(base, name)

    def begin(self, token):
        """첫 체크포인트 저장. 스냅샷을 쓰면 'seeding' 상태로 두고 백그라운드에서 수집한다.
        수집하는 동안 이 폴더의 변경은 Drive 쪽에 쌓여 있다가, 끝나면 이어서 처리된다(다른 폴더는 계속 감시)."""
        self.stat["note"] = "감시 시작 (이 시점 이후의 변경부터 감지)"
        if self.seed:
            # 목록 수집은 백그라운드에서, 변경 감지는 바로 시작한다
            self.store.save_cursor(self.name, token)
            self.store.set_seed(self.name, True)
            self.launch_seed()
        else:
            self.store.save_cursor(self.name, token)
            log.info("[%s] 감시 시작 (기존 파일 목록 수집 안 함)", self.name)

    def seeding(self, status):
        """기존 파일 목록을 수집 중이면 진행 상황을 표시한다. 변경 감지는 막지 않는다 (항상 False 반환).
        워커가 재시작돼 수집이 끊겼으면 다시 시작한다."""
        if status == "seeding":  # 이전 버전 형식
            self.store.save_cursor(self.name, self.store.get_cursor(self.name)[0])
            self.store.set_seed(self.name, True)
        if not self.store.seed_pending(self.name):
            return False
        info = SEEDERS.get(self.name)
        if not info or not info["thread"].is_alive():
            self.launch_seed()
            info = SEEDERS.get(self.name)
        minutes = int((time.time() - info["started"]) / 60)
        self.stat["seed"] = (f"기존 목록 수집 중 {info.get('count', 0):,}개 · 폴더 {info.get('folders', 0):,}개 · {minutes}분")
        if time.time() - info.get("logged", 0) >= 300:
            info["logged"] = time.time()
            if minutes:
                log.info("[%s] 기존 파일 목록 수집 중… %d개, %d분 경과", self.name, info.get("count", 0), minutes)
        return False

    def launch_seed(self):
        """기존 파일 목록을 Drive API로 직접 모은다 (폴더 50개씩 묶어 조회).
        rclone을 거치지 않으므로 gds 포크처럼 루트를 서버에서 정하는 리모트에서도 감시 폴더 기준 경로가 정확하고,
        진행 상황(모은 개수)을 화면에 보여 줄 수 있다."""
        info = SEEDERS.get(self.name)
        if info and info["thread"].is_alive():
            return
        info = {"proc": None, "started": time.time(), "count": 0, "folders": 0}
        name, local_root, store_path, root_id = self.name, self.local_root, self.store.path, self.root_id
        api, drive_id = self.api, getattr(self, "drive_id", "")
        scope = {"corpora": "drive", "driveId": drive_id} if drive_id else {"corpora": "allDrives"}

        def run():
            store = Store(store_path)
            try:
                log.info("[%s] 기존 파일 목록 수집 시작 (Drive API, 폴더가 크면 오래 걸림)", name)
                rows, queue, seen = [], [(root_id, local_root)], {root_id}
                while queue and not STOP:
                    batch, queue = queue[:50], queue[50:]
                    parents = dict(batch)
                    q = "(" + " or ".join(f"'{fid}' in parents" for fid in parents) + ") and trashed = false"
                    page = None
                    while not STOP:
                        params = dict(scope, q=q, pageSize=1000, includeItemsFromAllDrives="true",
                                      fields="nextPageToken,files(id,name,mimeType,parents,shortcutDetails(targetId,targetMimeType))")
                        if page:
                            params["pageToken"] = page
                        data = api.get("files", **params)
                        for f in data.get("files") or []:
                            parent = next((p for p in f.get("parents") or [] if p in parents), None)
                            if not parent or not f.get("name"):
                                continue
                            path = posixpath.join(parents[parent], f["name"].replace("/", "／"))
                            mime = f.get("mimeType")
                            target = (f.get("shortcutDetails") or {})
                            if mime == SHORTCUT_MIME and target.get("targetMimeType") == FOLDER_MIME:
                                rows.append((f["id"], path, True, f"shortcut:{target['targetId']}"))
                                rows.append((target["targetId"], path, True, f"target:{f['id']}"))
                                if target["targetId"] not in seen:
                                    seen.add(target["targetId"])
                                    queue.append((target["targetId"], path))
                            elif mime == FOLDER_MIME:
                                rows.append((f["id"], path, True))
                                if f["id"] not in seen:
                                    seen.add(f["id"])
                                    queue.append((f["id"], path))
                            elif mime != SHORTCUT_MIME:
                                rows.append((f["id"], path, False))
                        info["count"] = len(rows)
                        page = data.get("nextPageToken")
                        if not page:
                            break
                    info["folders"] += len(batch)
                if STOP:
                    return
                store.merge_items(name, rows)
                store.set_seed(name, False)
                log.info("[%s] 기존 파일 %d개(폴더 %d개) 수집 완료 (%.0f분 소요)",
                         name, len(rows), info["folders"], (time.time() - info["started"]) / 60)
            except Exception as error:
                if STOP:
                    return
                store.set_seed(name, False)
                log.error("[%s] 기존 파일 목록 수집 실패, 목록 없이 계속 감시합니다: %s", name, error)
            finally:
                store.db.close()

        info["thread"] = threading.Thread(target=run, name=f"seed-{name}", daemon=True)
        SEEDERS[name] = info
        info["thread"].start()

    def emit(self, file_id, prev, cur, receipt=None, forced=None):
        event = build_event(prev, cur) or forced
        path = (cur or prev or {}).get("path", "")
        if event is None:
            self.stat["same"] += 1
            self.trace("변화 없음(경로·내용 그대로, 메타데이터만 바뀜): %s", path)
        elif not self.relevant(event):
            self.stat["ext"] += 1
            self.trace("확장자 제외: %s", event["path"] or event["removed_path"])
            event = None
        else:
            screened = screen_event(event, self.local_root)
            if screened is None:
                self.stat["ext"] += 1
                self.trace("무시 패턴: %s", event["path"] or event["removed_path"])
            else:
                self.stat["events"] += 1
            event = screened
        self.store.record(self.name, file_id, prev, cur, event, self.buffer_seconds, receipt)
        if event:
            log.info("[%s] %s %s %s%s", self.name, event["action"], event["item_type"], event["path"],
                     f" (← {event['removed_path']})" if event["removed_path"] not in ("", event["path"]) else "")
        return int(event is not None)


class ChangesWatcher(Watcher):
    mode = "changes"
    CATCHUP_SECONDS = 90

    def poll(self):
        token, status = self.store.get_cursor(self.name)
        if status == "blocked":
            return 0
        self.check_root()
        self.seeding(status)
        drive_params = {"driveId": self.drive_id} if self.drive_id else {}
        # 체크포인트는 어느 변경 목록(드라이브)의 것인지 함께 저장한다: "드라이브ID|토큰"
        saved_drive, _, raw = token.rpartition("|") if "|" in token else (None, "", token)
        if saved_drive is None:  # 이전 버전 형식: 리모트의 team_drive 기준이었음
            saved_drive = self.api.team_drive
        if not token:
            # 토큰을 먼저 받고 스냅샷 → 그 사이 변경은 스냅샷 뒤에 재생되어 흡수된다
            start = self.api.get("changes/startPageToken", **drive_params)["startPageToken"]
            self.begin(f"{self.drive_id}|{start}")
            return 0
        if saved_drive != self.drive_id:
            start = self.api.get("changes/startPageToken", **drive_params)["startPageToken"]
            self.store.save_cursor(self.name, f"{self.drive_id}|{start}")
            log.info("[%s] 변경 목록 기준을 %s로 바꿨습니다 (이 시점 이후 변경부터 감지)", self.name,
                     f"공유 드라이브 {self.drive_id}" if self.drive_id else "계정 전체")
            self.stat["note"] = "변경 목록 기준 전환 (이후 변경부터 감지)"
            return 0
        token = raw
        prefix = f"{self.drive_id}|"
        if not token.isdigit():
            # 다른 방식(폴더 비교·Activity)에서 쓰던 체크포인트가 남은 경우: 새 시작점에서 다시 시작
            start = self.api.get("changes/startPageToken", **drive_params)["startPageToken"]
            self.store.save_cursor(self.name, prefix + start)
            self.stat["note"] = "감시 방식이 바뀌어 새 시작점에서 다시 시작 (이후 변경부터 감지)"
            log.info("[%s] %s", self.name, self.stat["note"])
            return 0
        accepted = 0
        started = time.monotonic()
        self.behind = False
        while token and not STOP:
            key = (self.source_remote, self.drive_id, token)
            try:
                data = CHANGES_PAGES.get(key)
                if data is None:
                    data = self.api.get(
                        "changes", pageToken=token, pageSize=1000, includeRemoved="true",
                        includeItemsFromAllDrives="true", **drive_params,
                        fields=f"nextPageToken,newStartPageToken,changes(fileId,removed,file({FILE_FIELDS}))")
                    # 같은 드라이브를 감시하는 다른 폴더도 이번 확인에서 이 페이지를 그대로 쓴다 (Drive 호출 1번으로)
                    CHANGES_PAGES[key] = data
                else:
                    self.stat["shared"] = self.stat.get("shared", 0) + 1
            except DriveError as error:
                if error.code != 400:
                    raise
                # 체크포인트가 맞지 않음(만료·형식 불일치): 오류로 멈추지 않고 새 시작점에서 다시 시작
                start = self.api.get("changes/startPageToken", **drive_params)["startPageToken"]
                self.store.save_cursor(self.name, prefix + start)
                self.stat["note"] = "체크포인트가 맞지 않아 새 시작점에서 다시 시작 (이후 변경부터 감지)"
                log.warning("[%s] %s: %s", self.name, self.stat["note"], error.detail)
                return accepted
            for change in data.get("changes") or []:
                accepted += self.handle(change)
            if data.get("nextPageToken"):
                token = data["nextPageToken"]
                self.store.save_cursor(self.name, prefix + token)
                if time.monotonic() - started > self.CATCHUP_SECONDS:
                    # 밀린 변경이 많을 때 한 번에 다 따라잡지 않는다: 그동안 쌓인 기록 처리(VFS·스캔)가 밀리지 않도록
                    self.behind = True
                    self.stat["note"] = "밀린 변경을 따라잡는 중 (곧 이어서 확인)"
                    break
                continue
            self.store.save_cursor(self.name, prefix + (data.get("newStartPageToken") or token))
            break
        return accepted

    def handle(self, change):
        self.stat["raw"] += 1
        file_id = change.get("fileId") or ""
        data = change.get("file") or {}
        prev = self.store.get_item(self.name, file_id)
        cur = None
        target = self.shortcut_target(data)
        if prev and prev.get("sig", "").startswith("shortcut:") and (change.get("removed") or data.get("trashed")):
            # 폴더 바로가기가 지워지면 대상 폴더 등록도 해제
            target_id = prev["sig"].split(":", 1)[1]
            mapped = self.store.get_item(self.name, target_id)
            if mapped and mapped["path"] == prev["path"]:
                self.store.record(self.name, target_id, mapped, None, None, 0)
        if not change.get("removed") and not data.get("trashed"):
            path = self.resolve(data)
            is_dir = data.get("mimeType") == FOLDER_MIME or bool(target)
            if path and target:
                old_target = self.store.get_item(self.name, target)
                if old_target and old_target["path"] != path:
                    self.store.record(self.name, target, old_target, None, None, 0)  # 이동된 바로가기의 옛 하위 경로 정리
                self.map_shortcut(file_id, target, path)
            if path:
                cur = {"path": path, "is_dir": is_dir, "sig": f"shortcut:{target}" if target else signature(data)}
                if is_dir:
                    self.store.set_outside(self.name, file_id, False)
            elif is_dir and not prev:
                self.store.set_outside(self.name, file_id)
        if prev is None and cur is None and data.get("trashed"):
            # 목록에 없던 파일이 휴지통으로 가도, 휴지통 항목에 남은 부모 정보로 이전 경로를 복원한다
            old = self.resolve(data)
            if old:
                prev = {"path": old, "is_dir": data.get("mimeType") == FOLDER_MIME, "sig": ""}
        if prev is None and cur is None:
            # 같은 드라이브의 다른 폴더 변경: 드라이브 변경 목록은 드라이브 전체 단위라 정상적으로 섞여 온다.
            # 하나하나 로그에 남기지 않고 요약의 '범위 밖' 개수로만 센다.
            self.stat["outside"] += 1
            return 0
        return self.emit(file_id, prev, cur)


class ActivityWatcher(Watcher):
    """Drive Activity API: 감시 폴더 하위 활동만 조회. rclone scope에 drive.activity.readonly 필요."""
    mode = "activity"
    KINDS = ("delete", "move", "rename", "restore", "create", "edit")

    def __init__(self, cfg, *args):
        super().__init__(cfg, *args)
        self.delay = timedelta(seconds=int(cfg.get("activity_delay", 60)))

    def poll(self):
        token, status = self.store.get_cursor(self.name)
        if status == "blocked":
            return 0
        self.check_root()
        self.seeding(status)
        if not token:
            now = stamp(utcnow())
            self.begin(json.dumps({"start": now, "floor": now}))
            return 0
        try:
            cursor = json.loads(token)
            start = parse_time(cursor["start"])
        except (ValueError, TypeError, KeyError):
            # 다른 방식(Changes·폴더 비교)에서 쓰던 체크포인트가 남은 경우: 지금부터 새로 시작 (추적 목록은 그대로)
            now = stamp(utcnow())
            self.store.save_cursor(self.name, json.dumps({"start": now, "floor": now}))
            self.stat["note"] = "감시 방식이 바뀌어 새 시작점에서 다시 시작 (이후 변경부터 감지)"
            log.info("[%s] %s", self.name, self.stat["note"])
            return 0
        end = parse_time(cursor["end"]) if cursor.get("end") else utcnow() - self.delay
        accepted = 0
        while end > start and not STOP:
            body = {"ancestorName": f"items/{self.root_id}", "pageSize": 100,
                    "consolidationStrategy": {"none": {}},
                    "filter": f'time >= "{stamp(start)}" AND time < "{stamp(end)}"'}
            if cursor.get("page"):
                body["pageToken"] = cursor["page"]
            data = self.api.call(ACTIVITY_API, body)
            for activity in data.get("activities") or []:
                accepted += self.handle(activity)
            if data.get("nextPageToken"):
                cursor = dict(cursor, end=stamp(end), page=data["nextPageToken"])
                self.store.save_cursor(self.name, json.dumps(cursor))
                continue
            # 늦게 공개되는 활동 대비 5분 겹쳐서 다음 구간 시작 (중복은 receipt로 제거)
            new_start = max(parse_time(cursor["floor"]), end - timedelta(minutes=5))
            self.store.prune_receipts(self.name, stamp(new_start))
            self.store.save_cursor(self.name, json.dumps({"start": stamp(new_start), "floor": cursor["floor"]}))
            break
        return accepted

    def handle(self, activity):
        accepted = 0
        for action in activity.get("actions") or []:
            detail = action.get("detail") or {}
            kind = next((k for k in self.KINDS if k in detail), None)
            if not kind:
                continue
            targets = [action["target"]] if action.get("target") else activity.get("targets") or []
            when = (action.get("timestamp") or (action.get("timeRange") or {}).get("endTime")
                    or activity.get("timestamp") or (activity.get("timeRange") or {}).get("endTime"))
            for target in targets:
                item = target.get("driveItem")
                if not item or not when:
                    continue
                file_id = str(item.get("name") or "").removeprefix("items/")
                self.stat["raw"] += 1
                receipt_id = hashlib.sha256(json.dumps([file_id, when, detail], sort_keys=True).encode()).hexdigest()
                if self.store.has_receipt(self.name, receipt_id):
                    continue
                accepted += self.apply(file_id, kind, detail, item, (receipt_id, stamp(parse_time(when))))
        return accepted

    def apply(self, file_id, kind, detail, item, receipt):
        data = self.api.file(file_id)
        is_dir = "driveFolder" in item or (data or {}).get("mimeType") == FOLDER_MIME
        prev = self.store.get_item(self.name, file_id)
        old = (prev or {}).get("path") or ""
        cur = None
        if data and not data.get("trashed"):
            path = self.resolve(data)
            cur = {"path": path, "is_dir": is_dir, "sig": signature(data)} if path else None
        elif data and not old:
            old = self.resolve(data)  # 휴지통 항목은 부모 정보가 남아 있음
        if not prev and cur and kind == "rename":
            title = (detail["rename"] or {}).get("oldTitle")
            if title:
                old = posixpath.join(posixpath.dirname(cur["path"]), title.replace("/", "／"))
        if not prev and kind == "move":
            removed = (detail["move"] or {}).get("removedParents") or []
            parent_id = str(((removed[0] if removed else {}).get("driveItem") or {}).get("name") or "").removeprefix("items/")
            if parent_id:
                old = self.resolve({"name": item.get("title") or (data or {}).get("name"), "parents": [parent_id]})
        if not prev and old:
            prev = {"path": old, "is_dir": is_dir, "sig": ""}
        if prev is None and cur is None:
            self.stat["outside"] += 1
            log.warning("[%s] 경로 확인 불가 활동 무시: %s %s (%s)", self.name, kind, item.get("title"), file_id)
            self.store.record(self.name, file_id, None, None, None, 0, receipt)
            return 0
        forced = None
        if kind in ("edit", "create") and cur and not is_dir and prev and prev["path"] == cur["path"]:
            forced = {"action": "edit", "item_type": "file", "path": cur["path"], "removed_path": ""}
        return self.emit(file_id, prev, cur, receipt, forced)


# ─────────────────────────── Drive 폴더 비교 ───────────────────────────
# 변경 목록을 쓸 수 없는 경우(공유 드라이브 멤버가 아니라 폴더만 공유받은 계정 등)를 위한 방식.
# 정해진 주기마다 감시 폴더 트리를 Drive API로 훑어(폴더 50개씩 묶어 여러 개를 동시에 조회) 이전 목록과 비교한다.
# 훑는 작업은 감시 폴더마다 별도 스레드에서 돌아서, 크고 느린 폴더가 다른 폴더의 감시를 막지 않는다.

SWEEP_LOCKS = {}  # 리모트 → 폴더 비교 잠금
POLL_FIELDS = "nextPageToken,files(id,name,mimeType,parents,size,md5Checksum,modifiedTime,shortcutDetails(targetId,targetMimeType))"


class DrivePollWatcher(Watcher):
    mode = "drivepoll"

    def __init__(self, cfg, store, rclone, extensions, buffer_seconds, api_timeout):
        super().__init__(cfg, store, rclone, extensions, buffer_seconds, api_timeout)
        self.interval = max(120, int(cfg.get("drive_interval") or 600))
        self.workers = 4  # 동시 조회 수 (처리 옵션에서 설정)
        self.thread = None
        self.next_sweep = 0.0
        self.progress = {"folders": 0, "items": 0, "started": 0.0}
        self.last = ""

    def poll(self):
        """주 루프에서는 결과만 확인하고, 실제 훑기는 백그라운드 스레드가 한다."""
        token, status = self.store.get_cursor(self.name)
        if status == "blocked":
            self.stat["note"] = "확인 필요로 중지 (처음부터를 누르면 기준 목록을 다시 수집)"
            return 0
        if self.thread and self.thread.is_alive():
            p = self.progress
            if p.get("queued"):
                self.stat["note"] = "폴더 비교 대기 · 같은 리모트의 다른 폴더 비교가 끝나면 시작"
            else:
                self.stat["note"] = (f"폴더 비교 중 · {p['items']:,}개 / 폴더 {p['folders']:,}개 · "
                                     f"{int(time.time() - p['started'])}초 경과")
            return 0
        if self.last:
            self.stat["note"], self.last = self.last, ""
            return 0
        if time.monotonic() < self.next_sweep:
            left = int(self.next_sweep - time.monotonic())
            self.stat["note"] = f"폴더 비교 · 다음 비교 {left // 60}분 {left % 60}초 후"
            return 0
        self.check_root()
        self.thread = threading.Thread(target=self._sweep, args=(token,), daemon=True, name=f"drivepoll-{self.name}")
        self.progress = {"folders": 0, "items": 0, "started": time.time()}
        self.thread.start()
        self.stat["note"] = "폴더 비교 시작"
        return 0

    # ── 폴더 목록 조회 ──
    def _list(self, folders):
        """folders: [(폴더ID, 그 폴더의 로컬 경로)] → {폴더ID: [자식 dict]} (폴더 50개씩 묶어 동시 조회)"""
        from concurrent.futures import ThreadPoolExecutor
        scope = {"corpora": "allDrives"}  # 폴더만 공유받은 공유 드라이브도 조회되도록 부모 폴더 조건으로 찾는다
        result = {fid: [] for fid, _ in folders}

        def fetch(batch):
            parents = dict(batch)
            q = "(" + " or ".join(f"'{fid}' in parents" for fid in parents) + ") and trashed = false"
            out, page = [], None
            while not STOP:
                params = dict(scope, q=q, pageSize=1000, includeItemsFromAllDrives="true", fields=POLL_FIELDS)
                if page:
                    params["pageToken"] = page
                try:
                    data = self.api.get("files", **params)
                except DriveError as error:
                    if error.code != 400:
                        raise
                    if len(batch) > 1:  # 묶음 안의 문제 있는 폴더를 찾아 그것만 건너뛴다
                        half = len(batch) // 2
                        return fetch(batch[:half]) + fetch(batch[half:])
                    log.warning("[%s] 폴더를 조회할 수 없어 이번 비교에서 제외: %s (%s)", self.name, batch[0][1], error.detail)
                    self.skipped[batch[0][0]] = batch[0][1]
                    return []
                for f in data.get("files") or []:
                    parent = next((p for p in f.get("parents") or [] if p in parents), None)
                    if not parent or not f.get("name"):
                        continue
                    path = posixpath.join(parents[parent], f["name"].replace("/", "／"))
                    mime = f.get("mimeType")
                    target = f.get("shortcutDetails") or {}
                    child = {"id": f["id"], "path": path, "mtime": parse_time(f.get("modifiedTime")), "follow": None}
                    if mime == SHORTCUT_MIME:
                        if target.get("targetMimeType") != FOLDER_MIME:
                            continue
                        child.update(is_dir=True, sig=f"shortcut:{target['targetId']}", follow=target["targetId"])
                    elif mime == FOLDER_MIME:
                        child.update(is_dir=True, sig="", follow=f["id"])
                    else:
                        child.update(is_dir=False, sig=f"{f.get('size', '')}:{f.get('md5Checksum') or f.get('modifiedTime', '')}")
                    out.append((parent, child))
                page = data.get("nextPageToken")
                if not page:
                    break
            return out

        if not hasattr(self, "skipped"):
            self.skipped = {}
        batches = [folders[i:i + 50] for i in range(0, len(folders), 50)]
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            for batch, rows in zip(batches, pool.map(fetch, batches)):
                self.progress["folders"] += len(batch)
                for parent, child in rows:
                    result[parent].append(child)
                    self.progress["items"] += 1
        return result

    def _crawl_all(self, start):
        """start: [(폴더ID, 경로, 깊이)] 아래 트리 전체 → (항목 {id: (path, is_dir, sig)}, 폴더 {폴더ID: (path, depth, 최근 변경)})"""
        items, folders, seen, own = {}, {}, set(), {}
        level = [s for s in start if s[0] not in seen]
        seen.update(s[0] for s in level)
        while level and not STOP:
            listed = self._list([(fid, path) for fid, path, _ in level])
            depth_of = {fid: depth for fid, _, depth in level}
            nxt = []
            for fid, path, depth in level:
                if fid in getattr(self, "skipped", {}):
                    folders[fid] = (path, depth, time.time(), 0)  # 조회 실패한 폴더도 목록에 남겨 다음 비교 때 다시 읽는다
                    continue
                newest = 0.0
                for c in listed.get(fid, []):
                    items[c["id"]] = (c["path"], c["is_dir"], c["sig"])
                    if c["mtime"]:
                        newest = max(newest, c["mtime"].timestamp())
                    if c["follow"] and c["follow"] not in seen:
                        seen.add(c["follow"])
                        nxt.append((c["follow"], c["path"], depth_of[fid] + 1))
                        own[c["follow"]] = c["mtime"].timestamp() if c["mtime"] and c["sig"] == "" else 0
                folders[fid] = (path, depth, newest, own.get(fid, 0))
            level = nxt
        return items, folders

    # ── 온도별 주기 ──
    SKELETON_DEPTH = 2      # 감시 폴더에서 이 단계까지의 분류 폴더는 매 주기 확인 (새 작품 폴더가 생기는 곳)
    MAX_FOLDERS_PER_SWEEP = 3000

    def set_tiers(self, cfg):
        self.policy = poll_policy(cfg)

    def _rule(self, path):
        rel = path[len(self.local_root):].strip("/") if under(path, self.local_root) else path.strip("/")
        return folder_rule(self.cfg.get("folder_rules"), rel)[0]

    def _interval(self, depth, last_change, now, path=""):
        if not hasattr(self, "policy"):
            self.set_tiers({})
        return poll_interval(depth, last_change, now, self.policy, self._rule(path) if path else "auto")

    def _save_folders(self, store, folders, now, spread=False):
        import random
        rows = []
        for fid, info in folders.items():
            path, depth, last_change = info[:3]
            folder_mtime = info[3] if len(info) > 3 else 0
            interval = self._interval(depth, last_change, now, path)
            last_list = now - random.uniform(0, interval) if spread and interval else now
            rows.append((self.name, fid, path, depth, last_change, last_list, folder_mtime))
        with store.db:
            store.db.executemany("INSERT OR REPLACE INTO pollfolder(root, folder_id, path, depth, last_change, last_list, "
                                 "folder_mtime) VALUES(?,?,?,?,?,?,?)", rows)

    # ── 한 번의 비교 ──
    def _sweep(self, token):
        store = Store(self.store.path)
        started = time.monotonic()
        lock = SWEEP_LOCKS.setdefault(self.source_remote, threading.Lock())
        try:
            self.progress["queued"] = True
            with lock:  # 같은 리모트(같은 토큰)를 쓰는 폴더 비교는 하나씩 차례로 (호출 제한 방지)
                self.progress["queued"] = False
                self.progress["started"] = time.time()
                has_folders = store.db.execute("SELECT 1 FROM pollfolder WHERE root=? LIMIT 1", (self.name,)).fetchone()
                if not token.startswith("drivepoll") or not has_folders:
                    self._full_sweep(store, token)
                else:
                    self._partial_sweep(store, token)
            store.save_cursor(self.name, f"drivepoll:{int(time.time())}") if not self.last.startswith("확인 필요") else None
        except DriveError as error:
            if error.code == 429:
                self.last = "Drive 호출 제한으로 이번 비교를 건너뜀 (다음 주기에 다시 시도)"
                log.warning("[%s] %s: %s", self.name, self.last, error)
            else:
                store.set_error(self.name, str(error))
                self.last = f"오류: {error}"
                log.error("[%s] 폴더 비교 실패: %s", self.name, error)
        except Exception as error:
            store.set_error(self.name, str(error))
            self.last = f"오류: {error}"
            log.error("[%s] 폴더 비교 실패: %s", self.name, error)
        finally:
            self.next_sweep = time.monotonic() + self.interval
            store.db.close()

    def _full_sweep(self, store, token):
        """처음(기준 목록 만들기) 또는 나눠 훑기용 폴더 정보가 없을 때: 트리 전체를 한 번 훑는다."""
        now = time.time()
        self.skipped = {}
        items, folders = self._crawl_all([(self.root_id, self.local_root, 0)])
        if STOP:
            return
        if self.root_id in self.skipped:
            raise RuntimeError("감시 폴더 자체를 조회할 수 없습니다. 폴더 ID와 이 리모트 계정의 접근 권한을 [점검]으로 확인하세요.")
        old = {row["file_id"]: (row["path"], bool(row["is_dir"]), row["sig"] or "")
               for row in store.db.execute("SELECT file_id, path, is_dir, sig FROM item WHERE root=?", (self.name,))}
        if not token.startswith("drivepoll"):
            store.replace_items(self.name, [(fid, v[0], v[1], v[2]) for fid, v in items.items()])
            with store.db:
                store.db.execute("DELETE FROM pollfolder WHERE root=?", (self.name,))
            self._save_folders(store, folders, now, spread=True)
            self._retry_skipped(store)
            self.last = f"폴더 비교 기준 목록 수집 완료 ({len(items):,}개, 폴더 {len(folders):,}개 · 이후 변경부터 감지)"
            log.info("[%s] %s", self.name, self.last)
            return
        # 이전 버전(전체 비교)의 목록이 있으면 비교해서 변경을 기록하고, 나눠 훑기용 폴더 정보를 만든다
        rows = self._diff_full(store, old, items)
        if rows is None:
            return
        with store.db:
            store.db.execute("DELETE FROM pollfolder WHERE root=?", (self.name,))
        self._save_folders(store, folders, now, spread=True)
        self._retry_skipped(store)
        self.last = f"폴더 비교 {len(items):,}개 (전체) · 변경 {self.stat['raw']} → 기록 {rows}"

    def _retry_skipped(self, store):
        if getattr(self, "skipped", None):
            with store.db:
                store.db.executemany("UPDATE pollfolder SET last_list=0 WHERE root=? AND folder_id=?",
                                     [(self.name, fid) for fid in self.skipped])

    def _diff_full(self, store, old, current):
        # 조회에 실패한 폴더 아래는 '사라짐'으로 보지 않는다 (일시적인 조회 실패로 대량 삭제 판정이 나는 것 방지)
        skipped = list(getattr(self, "skipped", {}).values())
        removed = [fid for fid in old if fid not in current
                   and not any(under(old[fid][0], p) for p in skipped)]
        files_removed = sum(1 for fid in removed if not old[fid][1])
        total = max(1, sum(1 for v in old.values() if not v[1]))
        if not current and old or files_removed >= 100 and files_removed >= total * 0.2:
            store.save_cursor(self.name, "drivepoll:hold", "blocked",
                              f"한 번에 파일 {files_removed:,}개가 사라진 것으로 보여(전체의 {files_removed * 100 // total}%) 반영을 멈췄습니다. "
                              "권한이나 공유가 바뀌었는지 확인 후 '처음부터'를 누르세요.")
            self.last = "확인 필요로 중지 (대량 삭제 의심)"
            return None
        events, upserts = [], []
        removed_dirs = {old[fid][0] for fid in removed if old[fid][1]}
        for fid in removed:
            path, is_dir, _ = old[fid]
            if not any(under(path, d) and path != d for d in removed_dirs):
                events.append(build_event({"path": path, "is_dir": is_dir, "sig": ""}, None))
        created_dirs = {v[0] for fid, v in current.items() if fid not in old and v[1]}
        for fid, (path, is_dir, sig) in current.items():
            prev = old.get(fid)
            if prev == (path, is_dir, sig):
                continue
            upserts.append((fid, path, is_dir, sig))
            if prev is None and any(under(path, d) and path != d for d in created_dirs):
                continue
            ev = build_event({"path": prev[0], "is_dir": prev[1], "sig": prev[2]} if prev else None,
                             {"path": path, "is_dir": is_dir, "sig": sig})
            if ev:
                events.append(ev)
        return self._commit(store, events, removed, upserts)

    def _commit(self, store, events, removed, upserts, prefix_moves=()):
        self.reset_stat()
        self.stat["raw"] = len(events)
        rows = []
        for ev in events:
            if not self.relevant(ev):
                self.stat["ext"] += 1
                continue
            screened = screen_event(ev, self.local_root)
            if screened is None:
                self.stat["ext"] += 1
                continue
            rows.append(screened)
            log.info("[%s] %s %s %s", self.name, screened["action"], screened["item_type"], screened["path"])
        now = datetime.now().isoformat(timespec="seconds")
        with store.db:
            for old_path, new_path in prefix_moves:  # 폴더 이름 변경·이동: 하위 경로 일괄 치환
                store.db.execute("UPDATE pollfolder SET path=? WHERE root=? AND path=?", (new_path, self.name, old_path))
                pre = old_path + "/"
                for table, col in (("item", "path"), ("pollfolder", "path")):  # pollfolder는 ix_pollfolder_path
                    store.db.execute(f"UPDATE {table} SET {col} = ? || substr({col}, ?) WHERE root=? AND {col} >= ? AND {col} < ?",
                                     (new_path, len(old_path) + 1, self.name, pre, prefix_upper(pre)))
            store.db.executemany("DELETE FROM item WHERE root=? AND file_id=?", [(self.name, fid) for fid in removed])
            store.db.executemany(
                "INSERT OR REPLACE INTO item(root, file_id, path, is_dir, sig) VALUES(?,?,?,?,?)",
                [(self.name, fid, path, int(is_dir), sig) for fid, path, is_dir, sig in upserts])
            store.db.executemany(
                "INSERT INTO event(root, action, item_type, path, removed_path, created, ready_at) VALUES(?,?,?,?,?,?,?)",
                [(self.name, e["action"], e["item_type"], e["path"], e["removed_path"], now,
                  time.time() + self.buffer_seconds) for e in rows])
        self.stat["events"] = len(rows)
        return len(rows)

    def _partial_sweep(self, store, token):
        """온도별로 '지금 다시 볼 차례'인 폴더만 훑어 그 폴더의 바로 아래 항목을 비교한다."""
        now = time.time()
        folders = {r["folder_id"]: dict(r) for r in store.db.execute(
            "SELECT folder_id, path, depth, last_change, last_list, folder_mtime FROM pollfolder WHERE root=?", (self.name,))}
        intervals = {fid: self._interval(f["depth"], f["last_change"], now, f["path"]) for fid, f in folders.items()}
        due = [f for fid, f in folders.items()
               if intervals[fid] is not None and now - f["last_list"] >= intervals[fid] - 30]
        due.sort(key=lambda f: f["last_list"])
        due = due[:self.MAX_FOLDERS_PER_SWEEP]
        due.sort(key=lambda f: f["depth"])  # 상위 폴더부터: 이름이 바뀐 폴더를 먼저 알아야 하위 경로를 맞출 수 있다
        self.skipped = {}
        listed = self._list([(f["folder_id"], f["path"]) for f in due])
        if STOP:
            return
        seen_now = {c["id"] for kids in listed.values() for c in kids}

        def lookup(fid):
            row = store.db.execute("SELECT path, is_dir, sig FROM item WHERE root=? AND file_id=?", (self.name, fid)).fetchone()
            return (row["path"], bool(row["is_dir"]), row["sig"] or "") if row else None

        events, removed, upserts, moves, new_roots, touched = [], [], [], [], [], {}
        removed_dirs, wake = [], {}

        def remap(path):
            """이번 비교에서 이름이 바뀐(이동한) 상위 폴더가 있으면 새 경로로 바꾼다."""
            for old_path, new_path in moves:
                if path == old_path or path.startswith(old_path + "/"):
                    return new_path + path[len(old_path):]
            return path

        for f in due:
            prefix = f["path"].rstrip("/") + "/"
            old = {r["file_id"]: (remap(r["path"]), bool(r["is_dir"]), r["sig"] or "") for r in store.db.execute(
                "SELECT file_id, path, is_dir, sig FROM item WHERE root=? AND path >= ? AND path < ? "
                "AND instr(substr(path, ?), '/')=0", (self.name, prefix, prefix_upper(prefix), len(prefix) + 1))}
            cur = {c["id"]: dict(c, path=remap(c["path"])) for c in listed.get(f["folder_id"], [])}
            if f["folder_id"] in self.skipped:
                touched[f["folder_id"]] = "skip"  # 조회 실패: 다음 비교 때 다시
                continue
            if not cur and len(old) >= 20:
                # 항목이 많던 폴더가 갑자기 비어 보이면 일시적인 조회 문제로 보고 이번에는 건너뛴다
                log.warning("[%s] %s: 항목 %d개가 한꺼번에 사라져 보여 이번 비교에서 제외", self.name, f["path"], len(old))
                touched[f["folder_id"]] = "skip"
                continue
            changed = False
            for fid, (path, is_dir, _) in old.items():
                if fid in cur or fid in seen_now:
                    continue  # 그대로이거나, 이번에 훑은 다른 폴더로 옮겨 감 (그쪽에서 이동으로 처리)
                removed.append(fid)
                events.append(build_event({"path": path, "is_dir": is_dir, "sig": ""}, None))
                if is_dir:
                    removed_dirs.append(path)
                changed = True
            for fid, c in cur.items():
                if c["is_dir"] and c["follow"] in folders and c["sig"] == "" and c["mtime"]:
                    # 깨우기: 폴더 자체의 수정 시각이 바뀌었으면(안쪽 변경일 수 있음) 다음 비교 때 바로 읽는다
                    folder_time = c["mtime"].timestamp()
                    if folder_time > (folders[c["follow"]].get("folder_mtime") or 0) + 1:
                        wake[c["follow"]] = folder_time
                now_v = (c["path"], c["is_dir"], c["sig"])
                prev = old.get(fid) or lookup(fid)
                if prev and prev is not old.get(fid):
                    prev = (remap(prev[0]),) + prev[1:]
                if prev == now_v:
                    continue
                changed = True
                upserts.append((fid,) + now_v)
                ev = build_event({"path": prev[0], "is_dir": prev[1], "sig": prev[2]} if prev else None,
                                 {"path": c["path"], "is_dir": c["is_dir"], "sig": c["sig"]})
                if ev:
                    events.append(ev)
                if c["is_dir"] and c["follow"]:
                    if prev is None:
                        new_roots.append((c["follow"], c["path"], f["depth"] + 1))  # 새 폴더: 안쪽까지 목록만 만든다
                    elif prev[0] != c["path"]:
                        moves.append((prev[0], c["path"]))
                        touched[c["follow"]] = (c["path"], f["depth"] + 1)
            touched[f["folder_id"]] = None if not changed else "changed"
        # 사라진 폴더는 하위 항목과 폴더 정보도 정리
        for path in removed_dirs:
            pre = path + "/"
            removed += [r["file_id"] for r in store.db.execute(
                "SELECT file_id FROM item WHERE root=? AND path >= ? AND path < ?", (self.name, pre, prefix_upper(pre)))]
        new_items, new_folders = self._crawl_all(new_roots) if new_roots else ({}, {})
        upserts += [(fid,) + v for fid, v in new_items.items()]
        self._commit(store, events, removed, upserts, moves)
        with store.db:
            for path in removed_dirs:
                store.db.execute("DELETE FROM pollfolder WHERE root=? AND (path=? OR (path >= ? AND path < ?))",
                                 (self.name, path, path + "/", prefix_upper(path + "/")))
            for f in due:
                if touched.get(f["folder_id"]) == "skip":
                    continue
                if touched.get(f["folder_id"]) == "changed":
                    store.db.execute("UPDATE pollfolder SET last_list=?, last_change=? WHERE root=? AND folder_id=?",
                                     (now, now, self.name, f["folder_id"]))
                else:
                    store.db.execute("UPDATE pollfolder SET last_list=? WHERE root=? AND folder_id=?",
                                     (now, self.name, f["folder_id"]))
        self._save_folders(store, {fid: (v[0], v[1], now, v[3]) for fid, v in new_folders.items()}, now)
        woken = 0
        with store.db:
            for fid, folder_time in wake.items():
                if not (folders[fid].get("folder_mtime") or 0):  # 처음 기록하는 경우는 기준값만 저장
                    store.db.execute("UPDATE pollfolder SET folder_mtime=? WHERE root=? AND folder_id=?", (folder_time, self.name, fid))
                else:
                    woken += 1
                    store.db.execute("UPDATE pollfolder SET folder_mtime=?, last_list=0, last_change=? WHERE root=? AND folder_id=?",
                                     (folder_time, now, self.name, fid))
        counts = {}
        for value in intervals.values():
            counts[value] = counts.get(value, 0) + 1
        order = sorted(counts, key=lambda v: float("inf") if v is None else v)
        tiers = " / ".join(f"{human_interval(v)} {counts[v]:,}" for v in order)
        self.last = (f"폴더 비교 (나눠 훑기) · 이번 {len(due):,}/{len(folders):,}개 폴더 · 변경 {self.stat['raw']} → 기록 {self.stat['events']}"
                     + (f" · 깨움 {woken}" if woken else "") + (f" · 조회 실패 {len(self.skipped)}(다음에 다시)" if self.skipped else "")
                     + f" · {tiers}")


# ─────────────────────────── 로컬 폴더 감시 ───────────────────────────
# Google Drive가 아닌 폴더(로컬 디스크, NAS 공유 등)를 파일 목록 비교로 감시한다.
#  - 실시간(inotify): 로컬 파일시스템에서 변경 알림을 받아 바뀐 폴더만 다시 비교 (+1시간마다 전체 대조)
#  - 주기 비교: 정해진 주기마다 전체 목록을 이전 목록과 비교 (NAS·네트워크 공유·FUSE 마운트용)
# BookOasis Mate(AGPL-3.0)의 local_folder_watch 설계(기준 목록, 대량 삭제 보호, 루트 식별)를 참고함.

LOCAL_FS = {"ext2", "ext3", "ext4", "xfs", "btrfs", "zfs", "tmpfs", "overlay", "f2fs", "jfs", "reiserfs"}
LOCAL_IGNORE = ("@eaDir", "#recycle", "#snapshot", "$RECYCLE.BIN", "System Volume Information", "lost+found")
LOCAL_MAX_ENTRIES = 500000


def filesystem_type(path):
    best = (0, "unknown")
    try:
        with open("/proc/mounts", encoding="utf-8") as stream:
            for line in stream:
                fields = line.split()
                if len(fields) < 3:
                    continue
                mount = fields[1].replace("\\040", " ")
                if path == mount or path.startswith(mount.rstrip("/") + "/"):
                    best = max(best, (len(mount), fields[2]))
    except OSError:
        pass
    return best[1]


class Inotify:
    """외부 라이브러리 없이 ctypes로 쓰는 Linux inotify (폴더별 watch, 비재귀)."""
    IN_CLOSE_WRITE, IN_MOVED_FROM, IN_MOVED_TO, IN_CREATE, IN_DELETE = 0x8, 0x40, 0x80, 0x100, 0x200
    IN_DELETE_SELF, IN_MOVE_SELF, IN_Q_OVERFLOW, IN_IGNORED, IN_ONLYDIR = 0x400, 0x800, 0x4000, 0x8000, 0x01000000
    MASK = IN_CLOSE_WRITE | IN_MOVED_FROM | IN_MOVED_TO | IN_CREATE | IN_DELETE | IN_DELETE_SELF | IN_MOVE_SELF

    def __init__(self, callback):
        import ctypes
        import ctypes.util
        self.ctypes = ctypes
        self.libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
        self.fd = self.libc.inotify_init1(0x80000)  # IN_CLOEXEC
        if self.fd < 0:
            raise OSError(ctypes.get_errno(), "inotify를 사용할 수 없습니다")
        self.callback = callback
        self.wds, self.paths = {}, {}
        self.closed = False
        self.thread = threading.Thread(target=self._loop, daemon=True, name="inotify")
        self.thread.start()

    def add(self, path):
        if path in self.paths:
            return
        wd = self.libc.inotify_add_watch(self.fd, path.encode("utf-8", "surrogateescape"), self.MASK | self.IN_ONLYDIR)
        if wd < 0:
            err = self.ctypes.get_errno()
            if err in (2, 20):  # 이미 사라진 폴더
                return
            raise OSError(err, os.strerror(err))
        self.wds[wd], self.paths[path] = path, wd

    def remove_tree(self, path):
        for sub in [p for p in self.paths if p == path or p.startswith(path + "/")]:
            wd = self.paths.pop(sub)
            self.wds.pop(wd, None)
            self.libc.inotify_rm_watch(self.fd, wd)

    def _loop(self):
        import select
        import struct
        while not self.closed and not STOP:
            try:
                ready, _, _ = select.select([self.fd], [], [], 1.0)
                if not ready:
                    continue
                data = os.read(self.fd, 65536)
            except OSError:
                break
            offset = 0
            while offset + 16 <= len(data):
                wd, mask, _cookie, size = struct.unpack_from("iIII", data, offset)
                name = data[offset + 16:offset + 16 + size].split(b"\0", 1)[0].decode("utf-8", "surrogateescape")
                offset += 16 + size
                if mask & self.IN_Q_OVERFLOW:
                    self.callback(None)
                    continue
                if mask & self.IN_IGNORED:
                    path = self.wds.pop(wd, None)
                    if path:
                        self.paths.pop(path, None)
                    continue
                directory = self.wds.get(wd)
                if directory:
                    # 감시 중인 폴더 자체가 지워지거나 옮겨졌으면 그 상위 폴더를 다시 비교한다
                    self.callback(posixpath.dirname(directory) if mask & (self.IN_DELETE_SELF | self.IN_MOVE_SELF)
                                  else directory)

    def close(self):
        self.closed = True
        try:
            os.close(self.fd)
        except OSError:
            pass


class LocalWatcher(Watcher):
    mode = "local"

    def __init__(self, cfg, store_path, extensions, buffer_seconds):
        self.name = cfg["name"]
        self.local_root = norm(cfg["local_root"]).rstrip("/")
        if not self.local_root or self.local_root == "/":
            raise ValueError(f"{self.name}: 감시할 로컬 경로(절대 경로)를 입력하세요.")
        self.detect = cfg.get("local_detect") or "auto"
        self.interval = max(30, int(cfg.get("local_interval") or 300))
        self.store_path = store_path
        self.extensions = extensions
        self.buffer_seconds = buffer_seconds
        self.verbose = False
        self.retry_at = 0.0
        self.failures = 0
        self.reset_stat()
        self.method = ""
        self.last = "시작 대기"
        self.lock = threading.Lock()
        self.dirty, self.full_pending, self.changed, self.first = set(), True, 0.0, 0.0
        self.next_full = 0.0
        self.snapshot = None  # 상대 경로 → (is_dir, size, mtime_ns)
        self.children = {}    # 상위 상대 경로 → {이름}
        self.identity = None
        self.notify = None
        self.stopped = False
        self.thread = threading.Thread(target=self._run, daemon=True, name=f"local-{self.name}")
        self.thread.start()

    # ── 공통 ──
    def included(self, rel, is_dir):
        if ignored("/" + rel if rel else ""):
            return False
        parts = rel.split("/")
        for part in parts[:-1] if not is_dir else parts:
            if part in LOCAL_IGNORE or (part.startswith(".") and part != ".bookoasisignore"):
                return False
        name = parts[-1]
        if not is_dir and (name in LOCAL_IGNORE or (name.startswith(".") and name != ".bookoasisignore")):
            return False
        return is_dir or self.extensions is None or name == ".bookoasisignore" or \
            posixpath.splitext(name)[1].lower() in self.extensions

    def abs(self, rel):
        return posixpath.join(self.local_root, rel) if rel else self.local_root

    def mark(self, directory):
        with self.lock:
            if directory is None:
                self.full_pending = True
            else:
                rel = posixpath.relpath(directory, self.local_root) if directory != self.local_root else ""
                if not rel.startswith(".."):
                    self.dirty.add("" if rel == "." else rel)
            now = time.monotonic()
            self.changed, self.first = now, self.first or now

    def stop(self):
        self.stopped = True
        if self.notify:
            self.notify.close()

    # ── 스레드 ──
    def _run(self):
        store = Store(self.store_path)
        try:
            self._load(store)
            while not STOP and not self.stopped:
                try:
                    self._tick(store)
                except Exception as error:
                    self.last = f"오류: {error}"
                    self.stat["note"] = self.last
                    store.set_error(self.name, str(error))
                    log.error("[%s] 로컬 감시 오류: %s", self.name, error)
                    with self.lock:
                        self.full_pending = True
                    self.next_full = time.monotonic() + self.interval
                    time.sleep(5)
                time.sleep(1)
        finally:
            if self.notify:
                self.notify.close()
            store.db.close()

    def _load(self, store):
        token, status = store.get_cursor(self.name)
        if token:
            try:
                self.identity = tuple(json.loads(token).get("identity") or ()) or None
            except ValueError:
                self.identity = None
            rows = store.db.execute("SELECT file_id, is_dir, sig FROM item WHERE root=?", (self.name,)).fetchall()
            self.snapshot = {}
            for row in rows:
                size, _, mtime = (row["sig"] or "0:0").partition(":")
                self.snapshot[row["file_id"]] = (bool(row["is_dir"]), int(size or 0), int(mtime or 0))
            self._index()
        fstype = filesystem_type(self.local_root)
        want = self.detect if self.detect in ("inotify", "polling") else ("inotify" if fstype in LOCAL_FS else "polling")
        self.method = want
        self.fstype = fstype
        log.info("[%s] 로컬 폴더 감시: %s (파일시스템 %s, %s)", self.name, self.local_root, fstype,
                 "실시간 감지" if want == "inotify" else f"{self.interval}초마다 비교")

    def _index(self):
        self.children = {}
        for rel in self.snapshot:
            self.children.setdefault(posixpath.dirname(rel), set()).add(posixpath.basename(rel))

    def _start_notify(self):
        if self.method != "inotify" or self.notify or self.snapshot is None:
            return
        try:
            self.notify = Inotify(self.mark)
            self.notify.add(self.local_root)
            for rel, (is_dir, _, _) in self.snapshot.items():
                if is_dir:
                    self.notify.add(self.abs(rel))
        except OSError as error:
            if self.notify:
                self.notify.close()
            self.notify = None
            reason = "inotify 감시 개수 한도 초과 (fs.inotify.max_user_watches)" if error.errno == 28 else str(error)
            log.warning("[%s] 실시간 감지를 쓸 수 없어 %d초마다 비교로 전환: %s", self.name, self.interval, reason)
            self.method = "polling"

    def _tick(self, store):
        now = time.monotonic()
        token, status = store.get_cursor(self.name)
        if status == "blocked":
            self.stat["note"] = self.last = "확인 필요로 중지 (처음부터를 누르면 기준을 다시 수집)"
            return
        if not token and self.snapshot is not None:
            # '처음부터'로 초기화됨
            self.snapshot, self.identity = None, None
            if self.notify:
                self.notify.close()
                self.notify = None
        with self.lock:
            dirty_due = bool(self.dirty) and (now - self.changed >= 3 or now - self.first >= 30)
            full_due = self.snapshot is None or self.full_pending or now >= self.next_full
            if not (dirty_due or full_due):
                return
            scopes = set() if full_due else set(self.dirty)
            self.dirty, self.changed, self.first = set(), 0.0, 0.0
            self.full_pending = False
        started = time.monotonic()
        self.reset_stat()
        if full_due:
            self._full(store)
            self.next_full = time.monotonic() + (3600 if self.method == "inotify" and self.notify else self.interval)
        else:
            self._partial(store, scopes)
        self._start_notify()
        self.last = self.summary()
        try:
            store.save_stat(self.name, self.stat, time.monotonic() - started)
        except sqlite3.Error:
            pass

    # ── 목록 비교 ──
    def _identity(self):
        if os.path.islink(self.local_root) or not os.path.isdir(self.local_root):
            raise OSError(f"{self.local_root}에 접근할 수 없습니다. 마운트와 권한을 확인하세요.")
        info = os.stat(self.local_root)
        return (info.st_dev, info.st_ino)

    def _walk(self, rel, out):
        stack = [rel]
        while stack:
            current = stack.pop()
            try:
                entries = list(os.scandir(self.abs(current)))
            except (FileNotFoundError, NotADirectoryError, PermissionError):
                continue
            for entry in entries:
                if entry.is_symlink():
                    continue
                is_dir = entry.is_dir(follow_symlinks=False)
                child = posixpath.join(current, entry.name) if current else entry.name
                if not is_dir and not entry.is_file(follow_symlinks=False):
                    continue
                if not self.included(child, is_dir):
                    continue
                info = entry.stat(follow_symlinks=False)
                out[child] = (is_dir, 0 if is_dir else info.st_size, 0 if is_dir else info.st_mtime_ns)
                if len(out) > LOCAL_MAX_ENTRIES:
                    raise ValueError(f"항목이 {LOCAL_MAX_ENTRIES:,}개를 넘습니다. 감시 폴더를 더 작게 나눠 주세요.")
                if is_dir:
                    stack.append(child)
        return out

    def _full(self, store):
        identity = self._identity()
        if self.identity and self.identity != identity:
            raise OSError("감시 폴더의 식별 정보가 바뀌었습니다(다른 디스크가 마운트됨). 확인 후 '처음부터'를 누르세요.")
        current = self._walk("", {})
        if self.snapshot is None:
            self._commit(store, current, set(), current, [], identity, first=True)
            self.stat["note"] = f"기준 목록 수집 완료 ({len(current):,}개, 이후 변경부터 감지)"
            log.info("[%s] %s", self.name, self.stat["note"])
            return
        self._diff(store, dict(self.snapshot), current, identity, "")

    def _partial(self, store, scopes):
        identity = self._identity()
        old, current = {}, {}
        for scope in sorted(scopes, key=lambda s: s.count("/")):
            names_old = self.children.get(scope, set())
            for name in names_old:
                rel = posixpath.join(scope, name) if scope else name
                old[rel] = self.snapshot[rel]
            try:
                entries = list(os.scandir(self.abs(scope)))
            except (FileNotFoundError, NotADirectoryError):
                entries = []
            for entry in entries:
                if entry.is_symlink():
                    continue
                is_dir = entry.is_dir(follow_symlinks=False)
                rel = posixpath.join(scope, entry.name) if scope else entry.name
                if (not is_dir and not entry.is_file(follow_symlinks=False)) or not self.included(rel, is_dir):
                    continue
                info = entry.stat(follow_symlinks=False)
                current[rel] = (is_dir, 0 if is_dir else info.st_size, 0 if is_dir else info.st_mtime_ns)
                if is_dir and rel not in self.snapshot:
                    self._walk(rel, current)  # 새로 생긴 폴더는 안쪽까지
        self._diff(store, old, current, identity, "partial")

    def _diff(self, store, old, current, identity, kind):
        removed = set(old) - set(current)
        # 폴더 전체가 사라지면 그 아래 기준 항목도 함께 지운다 (이벤트는 맨 위 폴더 하나만)
        removed_all = set(removed)
        for rel in removed:
            if old[rel][0]:
                prefix = rel + "/"
                removed_all.update(k for k in self.snapshot if k.startswith(prefix))
        files_removed = sum(1 for r in removed_all if not self.snapshot.get(r, (True,))[0])
        total = max(1, sum(1 for v in self.snapshot.values() if not v[0]))
        if removed_all and (not current and kind != "partial" or files_removed >= 100 and files_removed >= total * 0.2):
            store.save_cursor(self.name, json.dumps({"identity": list(identity)}), "blocked",
                              f"한 번에 파일 {files_removed:,}개가 사라졌습니다(전체의 {files_removed * 100 // total}%). "
                              "마운트가 빠졌거나 대량 삭제일 수 있어 반영을 멈췄습니다. 확인 후 '처음부터'를 누르세요.")
            log.warning("[%s] 대량 삭제 의심으로 반영 보류 (파일 %d개)", self.name, files_removed)
            return
        events = []
        def has_ancestor(rel, pool):
            parent = posixpath.dirname(rel)
            while parent:
                if parent in pool:
                    return True
                parent = posixpath.dirname(parent)
            return False

        top_removed = [r for r in removed if not has_ancestor(r, removed)]
        for rel in sorted(top_removed):
            events.append(("delete", old[rel][0], rel))
        created = [r for r in current if r not in old]
        created_dirs = {r for r in created if current[r][0]}
        top_created = [r for r in created if not has_ancestor(r, created_dirs)]
        for rel in sorted(top_created):
            events.append(("create", current[rel][0], rel))
        for rel, sig in current.items():
            prev = old.get(rel)
            if prev is None:
                continue
            if prev[0] != sig[0]:
                events.append(("delete", prev[0], rel))
                events.append(("create", sig[0], rel))
            elif not sig[0] and prev != sig:
                events.append(("edit", False, rel))
        if len(events) > 10000:
            raise ValueError(f"한 번에 {len(events):,}건이 바뀌었습니다. 보관함 전체 스캔 후 '처음부터'를 누르세요.")
        self.stat["raw"] = len(events)
        self._commit(store, current, removed_all, {r: current[r] for r in current if old.get(r) != current[r]},
                     events, identity)

    def _commit(self, store, current, removed_all, upserts, events, identity, first=False):
        rows = []
        for action, is_dir, rel in events:
            path = self.abs(rel)
            ev = {"action": action, "item_type": "directory" if is_dir else "file",
                  "path": path, "removed_path": path if action == "delete" else ""}
            if not self.relevant(ev):
                self.stat["ext"] += 1
                continue
            self.stat["events"] += 1
            rows.append(ev)
            log.info("[%s] %s %s %s", self.name, action, ev["item_type"], path)
        now = datetime.now().isoformat(timespec="seconds")
        with store.db:
            if first:
                store.db.execute("DELETE FROM item WHERE root=?", (self.name,))
            store.db.executemany("DELETE FROM item WHERE root=? AND file_id=?", [(self.name, r) for r in removed_all])
            store.db.executemany(
                "INSERT OR REPLACE INTO item(root, file_id, path, is_dir, sig) VALUES(?,?,?,?,?)",
                [(self.name, rel, self.abs(rel), int(v[0]), f"{v[1]}:{v[2]}") for rel, v in upserts.items()])
            store.db.executemany(
                "INSERT INTO event(root, action, item_type, path, removed_path, created, ready_at) VALUES(?,?,?,?,?,?,?)",
                [(self.name, e["action"], e["item_type"], e["path"], e["removed_path"], now,
                  time.time() + self.buffer_seconds) for e in rows])
            store.db.execute(
                "INSERT INTO cursor(root, token, status, error, updated) VALUES(?,?,?,?,?) "
                "ON CONFLICT(root) DO UPDATE SET token=excluded.token, status=excluded.status, error='', updated=excluded.updated",
                (self.name, json.dumps({"identity": list(identity), "method": self.method}), "ready", "", now))
        if first:
            self.snapshot = dict(current)
        else:
            for rel in removed_all:
                self.snapshot.pop(rel, None)
            self.snapshot.update(upserts)
        self.identity = identity
        self._index()
        if self.notify:
            for rel in removed_all:
                if rel in self.notify.paths:
                    self.notify.remove_tree(self.abs(rel))
            for rel, v in upserts.items():
                if v[0]:
                    try:
                        self.notify.add(self.abs(rel))
                    except OSError as error:
                        log.warning("[%s] 새 폴더 실시간 감시 추가 실패(%s) → 1시간마다 전체 대조로 보완", self.name, error)

    def summary(self):
        mode = "실시간" if self.method == "inotify" and self.notify else f"{self.interval}초 주기"
        if self.stat["note"]:
            return f"{self.name}: {self.stat['note']}"
        if not self.stat["raw"]:
            return f"{self.name}: 로컬 {mode} · 변경 없음"
        text = f"{self.name}: 로컬 {mode} · 변경 {self.stat['raw']} → 기록 {self.stat['events']}"
        return text + (f", 확장자 제외 {self.stat['ext']}" if self.stat["ext"] else "")


# ─────────────────────────── rclone VFS ───────────────────────────

NOT_FOUND = ("directory not found", "file does not exist", "no such file or directory", "not found")


class VfsRule:
    def __init__(self, cfg):
        self.local = norm(cfg["local"]).rstrip("/") or "/"
        self.remote = str(cfg.get("remote") or "").strip("/")
        self.rc = str(cfg["rc"]).rstrip("/")
        if "://" not in self.rc:
            self.rc = "http://" + self.rc
        # gd-poller처럼 http://user:pass@host:5572 형식도 받는다
        from urllib.parse import unquote, urlsplit, urlunsplit
        parts = urlsplit(self.rc)
        if parts.username:
            cfg = dict(cfg, user=cfg.get("user") or unquote(parts.username), **{"pass": cfg.get("pass") or unquote(parts.password or "")})
            host = parts.netloc.rsplit("@", 1)[-1]  # IPv6 주소의 [ ]와 포트를 그대로 유지
            self.rc = urlunsplit((parts.scheme, host, parts.path, "", ""))
        self.fs = str(cfg.get("fs") or "").strip()
        if self.fs and not self.fs.endswith(":"):
            self.fs += ":"
        self.auth = None
        if cfg.get("user"):
            raw = f"{cfg['user']}:{cfg.get('pass', '')}".encode()
            self.auth = "Basic " + base64.b64encode(raw).decode()

    def to_remote(self, path):
        rel = path[len(self.local.rstrip("/")):].strip("/")
        return "/".join(part for part in (self.remote, rel) if part)

    def call(self, command, params, timeout):
        data = dict(params)
        if self.fs:
            data["fs"] = self.fs
        request = Request(f"{self.rc}/{command}", data=urlencode(data).encode(),
                          headers={"Content-Type": "application/x-www-form-urlencoded"})
        if self.auth:
            request.add_header("Authorization", self.auth)
        try:
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read() or b"{}")
        except HTTPError as error:
            raise RuntimeError(f"rclone rc {command} HTTP {error.code}: {error.read().decode('utf-8', 'replace')[:300]}") from None
        except URLError as error:
            raise RuntimeError(f"rclone rc 연결 실패({self.rc}): {error.reason}") from None


class Vfs:
    """VFS 규칙: 직접 지정한 규칙이 있으면 그것을, 없으면 보관함의 rclone_rc_url로 자동 규칙을 만든다.
    자동 규칙의 '마운트 안 경로'는 RC에 vfs/refresh를 시험 삼아 보내 찾아내고 state.db에 기억한다."""

    def __init__(self, rules, timeout=60, auto_libraries=None, store=None):
        self.rules = [VfsRule(r) for r in rules or []]
        self.timeout = timeout
        self.auto = sorted({(norm(root).rstrip("/") or "/", rc) for root, rc in auto_libraries or [] if root and rc},
                           key=lambda item: len(item[0]), reverse=True)
        self.store = store
        self._cache = {}

    def rules_for(self, path):
        return self.resolve(path)[0]

    def resolve(self, path):
        """반환 (규칙 목록, 오류 메시지)"""
        manual = [r for r in self.rules if under(path, r.local)]  # 여러 마운트에 fan-out
        if manual:
            return manual, None
        match = next(((root, rc) for root, rc in self.auto if under(path, root)), None)
        if not match:
            return [], None
        try:
            return [self.auto_rule(*match)], None
        except Exception as error:
            return [], str(error)

    def auto_rule(self, root, rc, redetect=False):
        key = (root, rc)
        if key in self._cache and not redetect:
            return self._cache[key]
        saved = self.store.get_vfs_map(root) if self.store and not redetect else None
        if saved and saved["rc"] == rc:
            found = {"local": root, "rc": rc, "fs": saved["fs"], "remote": saved["remote"]}
        else:
            found = self.detect(root, rc)
            if self.store:
                self.store.save_vfs_map(root, rc, found["fs"], found["remote"])
            log.info("VFS 경로 감지: %s → %s %s%s", root, rc, found["fs"], found["remote"])
        self._cache[key] = VfsRule(found)
        return self._cache[key]

    def detect(self, root, rc):
        probe = VfsRule({"local": root, "rc": rc})
        try:
            fses = probe.call("vfs/list", {}, self.timeout).get("vfses") or [""]
        except RuntimeError as error:
            if "연결 실패" in str(error):
                raise
            fses = [""]
        parts = [p for p in root.split("/") if p]
        for fs in fses:
            for index in range(len(parts)):  # 긴 경로부터: mnt/gds2/GDRIVE/책 → gds2/GDRIVE/책 → GDRIVE/책 …
                candidate = "/".join(parts[index:])
                rule = VfsRule({"local": root, "rc": rc, "fs": fs, "remote": candidate})
                result = rule.call("vfs/refresh", {"dir": candidate, "recursive": "false"}, self.timeout).get("result") or {}
                if result.get(candidate) == "OK":
                    return {"local": root, "rc": rc, "fs": fs, "remote": candidate}
        raise RuntimeError(f"{rc} 마운트에서 {root}에 해당하는 폴더를 찾지 못했습니다. 보관함의 RC 주소가 맞는지 확인하세요.")

    def forget(self, rule, items):
        """items: [(path, is_dir)] → 한 요청으로 dir/dir2…, file/file2… 묶어서 전송."""
        params, counters = {}, {"dir": 0, "file": 0}
        for path, is_dir in items:
            key = "dir" if is_dir else "file"
            counters[key] += 1
            params[key if counters[key] == 1 else f"{key}{counters[key]}"] = rule.to_remote(path)
        rule.call("vfs/forget", params, self.timeout)

    def refresh(self, rule, paths):
        """반환: {path: 오류메시지 또는 None}. 없는 폴더는 규칙 루트 안에서 상위로 올라가며 재시도."""
        pairs = [(p, rule.to_remote(p)) for p in paths]
        params = {"recursive": "false"}
        for index, (_, remote) in enumerate(pairs, 1):
            params["dir" if index == 1 else f"dir{index}"] = remote
        result = rule.call("vfs/refresh", params, self.timeout).get("result") or {}
        errors = {}
        for original, remote in pairs:
            path = original
            message = result.get(remote, "OK")
            while message != "OK" and any(w in str(message).lower() for w in NOT_FOUND):
                parent = posixpath.dirname(path.rstrip("/")) or "/"
                if parent == path or not under(parent, rule.local):
                    break
                path = parent
                message = (rule.call("vfs/refresh", {"dir": rule.to_remote(path), "recursive": "false"},
                                     self.timeout).get("result") or {}).get(rule.to_remote(path), "OK")
            errors[original] = None if message == "OK" else str(message)
        return errors


def rc_sources(vfs_rules, libraries):
    """토큰을 빌려 올 수 있는 rclone RC 후보: 직접 지정한 VFS 규칙 + 보관함에 설정된 RC 주소."""
    out = []
    for rule in vfs_rules or []:
        if rule.get("rc"):
            out.append({"rc": rule["rc"], "user": rule.get("user"), "pass": rule.get("pass")})
    for lib in libraries or []:
        if lib.get("rclone_rc_url"):
            out.append({"rc": lib["rclone_rc_url"]})
    return out


# ─────────────────────────── BookOasis 스캔 ───────────────────────────

class BookOasis:
    """보관함 목록은 플러그인이 runtime.json에 넣어준 값을 사용하고, 스캔은 /api/webhook/scan(path)로 요청."""

    def __init__(self, cfg, libraries, vfs_rules, store=None):
        self.url = str(cfg.get("bookoasis_url") or "http://127.0.0.1:5930").rstrip("/")
        self.token = cfg.get("webhook_token") or os.environ.get("WEBHOOK_TOKEN", "")
        self.timeout = int(cfg.get("scan_timeout", 300))
        self.file_wait = max(0, int(cfg.get("file_wait_minutes", 10)))  # 0이면 확인하지 않음
        self.guard = max(0, int(cfg.get("full_scan_guard_minutes", 10)))  # 전체 스캔 시간대 회피(분), 0이면 끔
        self.crons = {(lib["db_type"], int(lib["id"])): lib.get("cron_schedule") or ""
                      for lib in libraries or [] if lib.get("id") is not None}
        self.libraries = []
        for lib in libraries or []:
            for root in lib.get("roots") or []:
                root = norm(root).rstrip("/") or "/"
                if root:
                    self.libraries.append({"root": root, "db_type": lib["db_type"], "id": int(lib["id"]),
                                           "name": lib.get("name") or f"보관함 {lib['id']}"})
        self.libraries.sort(key=lambda l: len(l["root"]), reverse=True)
        auto = [(root, lib.get("rclone_rc_url")) for lib in libraries or [] for root in lib.get("roots") or []]
        self.vfs = Vfs(vfs_rules, int(cfg.get("rc_timeout", 60)), auto, store)

    def library_for(self, path):
        return next((lib for lib in self.libraries if under(path, lib["root"])), None)

    def full_scan_until(self, lib):
        """그 보관함의 전체 스캔(cron)이 최근 guard분 안에 시작됐으면, 회피가 끝나는 시각."""
        cron = self.crons.get((lib["db_type"], lib["id"])) if lib else ""
        if not self.guard or not cron:
            return None
        now = datetime.now().replace(second=0, microsecond=0)
        # 2시간보다 자주 도는 일정은 회피하지 않는다 (회피 시간대가 겹쳐 부분 스캔이 계속 밀리는 것 방지)
        fires = sum(1 for back in range(0, 360, 1) if cron_matches(cron, now - timedelta(minutes=back)))
        if fires > 3:
            return None
        for back in range(self.guard + 1):
            started = now - timedelta(minutes=back)
            if cron_matches(cron, started):
                return started + timedelta(minutes=self.guard)
        return None

    def _post(self, lib, rel):
        form = {"token": self.token, "library_id": lib["id"], "type": lib["db_type"], "force": "0"}
        if rel:
            form["path"] = rel
        request = Request(f"{self.url}/api/webhook/scan", data=urlencode(form).encode(),
                          headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"})
        try:
            with urlopen(request, timeout=self.timeout) as response:
                return response.status, json.loads(response.read() or b"{}")
        except HTTPError as error:
            try:
                payload = json.loads(error.read() or b"{}")
            except ValueError:
                payload = {}
            return error.code, payload
        except URLError as error:
            return 0, {"error": f"BookOasis 연결 실패: {error.reason}"}

    def scan(self, directory, removed):
        """반환 (ok, message, library) / 보관함 밖이면 (None, ...)"""
        lib = self.library_for(directory)
        if not lib:
            return None, "보관함 밖 경로", None
        if not self.token:
            return False, "WEBHOOK_TOKEN이 없습니다. 플러그인 설정 또는 BookOasis .env를 확인하세요.", lib
        rel = directory[len(lib["root"]):].strip("/")
        while True:
            code, payload = self._post(lib, rel)
            message = str(payload.get("message") or payload.get("error") or f"HTTP {code}")
            if code == 200 and payload.get("success"):
                return True, f"/{rel}" if rel else "(보관함 전체)", lib
            if code == 401:
                return False, (f"BookOasis가 스캔 요청을 거부했습니다(토큰 불일치: {message}). "
                               "실행 환경의 WEBHOOK_TOKEN을 BookOasis .env 값과 맞추거나 비워 두세요."), lib
            not_found = code == 404 or "찾을 수 없" in message or "not found" in message.lower()
            if removed and rel and not_found:
                rel = posixpath.dirname(rel)  # 삭제된 폴더면 상위로 (루트면 보관함 스캔)
                continue
            return False, message, lib

    def process(self, events):
        """events → {event_id: {"status", "message", "vfs": [...], "scans": [...]}}"""
        results = {ev["id"]: {"ok": True, "vfs": [], "scans": [], "messages": []} for ev in events}

        # 1) VFS: 이전 경로 forget → 부모 디렉터리 refresh (비재귀), 마운트별로 묶어서 전송
        forgets, refreshes, owners = {}, {}, {}
        # 같은 폴더에서 파일이 여러 개 지워지면 파일마다 forget하지 않고 그 폴더를 한 번 forget
        deleted_in = {}
        for ev in events:
            if ev["action"] == "delete" and ev["item_type"] == "file" and ev["removed_path"]:
                deleted_in.setdefault(posixpath.dirname(ev["removed_path"]), []).append(ev["id"])
        collapse = {d for d, ids in deleted_in.items() if len(ids) > 1}
        for ev in events:
            if ev.get("root") in getattr(self, "skip_vfs", ()):
                continue
            is_dir = ev["item_type"] == "directory"
            path, removed = ev["path"], ev["removed_path"]
            ops = []
            if ev["action"] == "delete" and not is_dir and posixpath.dirname(removed) in collapse:
                ops.append(("forget", posixpath.dirname(removed), True))
            elif ev["action"] in ("rename", "move", "delete") and removed:
                ops.append(("forget", removed, is_dir))
            for p in (removed, path):
                if p:
                    ops.append(("refresh", posixpath.dirname(p) or "/", True))
            for op, p, d in ops:
                rules, err = self.vfs.resolve(p)
                if err:
                    results[ev["id"]]["vfs"].append({"op": op, "path": p, "rc": "", "ok": False, "msg": err})
                    results[ev["id"]]["ok"] = False
                    results[ev["id"]]["messages"].append(f"VFS 경로 감지 실패: {err}")
                for rule in rules:
                    (forgets if op == "forget" else refreshes).setdefault(rule, {})[p] = d
                    owners.setdefault((op, id(rule), p), set()).add(ev["id"])
        for op, bucket in (("forget", forgets), ("refresh", refreshes)):
            for rule, items in bucket.items():
                try:
                    if op == "forget":
                        self.vfs.forget(rule, list(items.items()))
                        outcome = {p: None for p in items}
                    else:
                        outcome = self.vfs.refresh(rule, list(items))
                except Exception as error:
                    outcome = {p: str(error) for p in items}
                for p, err in outcome.items():
                    for event_id in owners.get((op, id(rule), p), ()):
                        entry = {"op": op, "path": p, "rc": rule.rc, "ok": err is None, "msg": err or ""}
                        results[event_id]["vfs"].append(entry)
                        if err:
                            results[event_id]["ok"] = False
                            results[event_id]["messages"].append(f"VFS {op} 실패: {err}")

        # 2) 마운트에 실제로 보이는지 확인 (plex_mate 방식): 추가·수정은 보일 때까지, 삭제는 사라질 때까지 기다린다.
        #    rclone 목록 반영이 늦을 때 빈 폴더를 스캔하고 '반영됨'으로 끝나는 것을 막는다.
        if self.file_wait:
            now = datetime.now()
            for ev in events:
                r = results[ev["id"]]
                if not r["ok"] or ev.get("root") in getattr(self, "skip_vfs", ()):
                    continue
                target = ev["removed_path"] if ev["action"] == "delete" else ev["path"]
                if not target or not self.library_for(target):
                    continue
                want = ev["action"] != "delete"
                if os.path.exists(target) == want:
                    continue
                try:
                    waited = (now - datetime.fromisoformat(ev["created"])).total_seconds()
                except (TypeError, ValueError):
                    waited = 0
                state = "보이지" if want else "사라지지"
                if waited >= self.file_wait * 60:
                    r["timeout"] = True
                    r["messages"].append(f"{self.file_wait}분 동안 마운트에서 파일이 {state} 않았습니다. rclone 마운트 상태를 확인하세요.")
                else:
                    r["waiting"] = True
                    r["messages"].append(f"마운트에 아직 {state} 않음 · {int(waited)}초 대기 중 (최대 {self.file_wait}분)")

        # 3) 스캔할 디렉터리 (VFS 실패·파일 대기 이벤트는 보류) → 상위 폴더로 합치기
        wanted, own = {}, {}
        for ev in events:
            r = results[ev["id"]]
            if not r["ok"] or r.get("waiting") or r.get("timeout"):
                continue
            dirs = []
            if ev["action"] != "delete" and ev["path"]:
                dirs.append((ev["path"] if ev["item_type"] == "directory" else posixpath.dirname(ev["path"]), False))
            if ev["removed_path"] and (ev["action"] == "delete" or ev["removed_path"] != ev["path"]):
                dirs.append((posixpath.dirname(ev["removed_path"]), True))
            own[ev["id"]] = {d for d, _ in dirs}
            for d, is_removed in dirs:
                entry = wanted.setdefault(d, {"events": set(), "removed": False})
                entry["events"].add(ev["id"])
                entry["removed"] |= is_removed
        kept = []
        for d in sorted(wanted, key=lambda v: (v.count("/"), v)):
            parent = next((k for k in kept if under(d, k) and self.library_for(k) == self.library_for(d)), None)
            if parent:
                wanted[parent]["events"] |= wanted[d]["events"]
                wanted[parent]["removed"] |= wanted[d]["removed"]
            else:
                kept.append(d)

        # 4) 스캔 요청
        for d in kept:
            if STOP:
                break
            forced = any(ev.get("force") for ev in events if ev["id"] in wanted[d]["events"])
            until = None if forced else self.full_scan_until(self.library_for(d))
            if until:
                wait = max(60, int((until - datetime.now()).total_seconds()))
                for event_id in wanted[d]["events"]:
                    results[event_id]["deferred"] = wait
                    results[event_id]["messages"].append(
                        f"보관함 전체 스캔 시간대라 부분 스캔을 {until.strftime('%H:%M')}까지 미룸")
                log.info("전체 스캔 시간대 회피: %s → %s 이후 스캔", d, until.strftime("%H:%M"))
                continue
            try:
                ok, message, lib = self.scan(d, wanted[d]["removed"])
            except Exception as error:
                ok, message, lib = False, f"{type(error).__name__}: {error}", None
            label = f"{lib['db_type']}#{lib['id']} {lib['name']}" if lib else ""
            if ok is not None:
                log.info("스캔 %s %s [%s] %s", "OK" if ok else "실패", d, label, message)
            for event_id in wanted[d]["events"]:
                results[event_id]["scans"].append({"dir": d, "library": label, "ok": ok, "msg": message,
                                                   "merged": d not in own.get(event_id, ())})
                if ok is False:
                    results[event_id]["ok"] = False
                    if "토큰 불일치" in message:
                        results[event_id]["terminal"] = True
                    results[event_id]["messages"].append(f"스캔 실패 {d}: {message}")

        final = {}
        for ev in events:
            r = results[ev["id"]]
            if r.get("timeout"):
                status = "timeout"
            elif r.get("waiting") or (r.get("deferred") and r["ok"] and not r["scans"]):
                status = "waiting"
            elif not r["ok"]:
                status = "failed"
            elif r["scans"] and all(s["ok"] is None for s in r["scans"]):
                status = "skipped"
            else:
                status = "done"
            if STOP and not r["scans"] and r["ok"]:
                status = "failed"
                r["messages"].append("워커 중지로 처리 미완료")
            message = "; ".join(r["messages"]) or ("보관함 밖 경로" if status == "skipped" else "")
            final[ev["id"]] = {"status": status, "message": message, "vfs": r["vfs"], "scans": r["scans"],
                               "terminal": bool(r.get("terminal")), "retry_in": r.get("deferred")}
        return final


# ─────────────────────────── 디스코드 알림 ───────────────────────────

class Notifier:
    """처리 결과를 디스코드 웹훅으로 알린다. 감지 시점이 아니라 '반영됨/실패'가 정해진 뒤, 처리 묶음마다 한 번."""
    ICON = {"create": "➕", "edit": "✏️", "rename": "↪️", "move": "↪️", "delete": "🗑️"}

    def __init__(self, cfg):
        self.url = str(cfg.get("discord_webhook") or "").strip()
        self.on_done = bool(cfg.get("notify_done", True))
        self.on_failed = bool(cfg.get("notify_failed", True))
        self.on_error = bool(cfg.get("notify_error", True))
        self.sent_errors = {}

    @property
    def enabled(self):
        return self.url.startswith("https://")

    def post(self, embeds):
        if not self.enabled or not embeds:
            return None
        body = json.dumps({"username": "드라이브 변경 감시", "embeds": embeds[:10]}).encode()
        request = Request(self.url, data=body, headers={"Content-Type": "application/json", "User-Agent": "gdrive_watch"})
        try:
            with urlopen(request, timeout=15) as response:
                return response.status
        except HTTPError as error:
            log.warning("디스코드 알림 실패: HTTP %s", error.code)
            return error.code
        except (URLError, OSError) as error:
            log.warning("디스코드 알림 실패: %s", error)
            return 0

    @staticmethod
    def _line(ev, extra=""):
        path = ev["path"] or ev["removed_path"]
        name = posixpath.basename(path.rstrip("/")) or path
        parent = posixpath.basename(posixpath.dirname(path.rstrip("/")))
        icon = Notifier.ICON.get(ev["action"], "•")
        return f"{icon} **{name}**{'/' if ev['item_type'] == 'directory' else ''} · {parent}{extra}"[:300]

    def results(self, events, results):
        if not self.enabled:
            return
        done = [ev for ev in events if results[ev["id"]]["status"] == "done"]
        failed = [ev for ev in events if results[ev["id"]]["status"] in ("failed", "timeout")]
        embeds = []
        if self.on_done and done:
            libs = sorted({s["library"] for ev in done for s in results[ev["id"]]["scans"] if s.get("ok") and s.get("library")})
            lines = [self._line(ev) for ev in done[:15]] + ([f"… 외 {len(done) - 15}건"] if len(done) > 15 else [])
            embeds.append({"title": f"반영됨 {len(done)}건", "color": 5763719, "description": "\n".join(lines)[:3900],
                           "footer": {"text": ", ".join(libs)[:200]} if libs else None})
        if self.on_failed and failed:
            lines = [self._line(ev, f"\n　└ {results[ev['id']]['message'][:160]}") for ev in failed[:10]]
            lines += [f"… 외 {len(failed) - 10}건"] if len(failed) > 10 else []
            embeds.append({"title": f"실패 {len(failed)}건 (재시도 예정이거나 확인 필요)", "color": 15548997,
                           "description": "\n".join(lines)[:3900]})
        for embed in embeds:
            if embed.get("footer") is None:
                embed.pop("footer", None)
        self.post(embeds)

    def root_error(self, name, message):
        """감시 폴더 오류는 같은 내용이면 한 번만 알린다."""
        if not self.enabled or not self.on_error:
            return
        key = message[:120]
        if self.sent_errors.get(name) == key:
            return
        self.sent_errors[name] = key
        self.post([{"title": f"감시 오류: {name}", "color": 16776960, "description": message[:1500]}])

    def root_ok(self, name):
        if self.sent_errors.pop(name, None) and self.enabled and self.on_error:
            self.post([{"title": f"감시 복구: {name}", "color": 3447003, "description": "정상적으로 다시 확인하고 있습니다."}])


# ─────────────────────────── 워커 본체 ───────────────────────────

class Worker:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.runtime_path = os.path.join(data_dir, "runtime.json")
        self.store = Store(os.path.join(data_dir, "state.db"))
        self.runtime_mtime = 0
        self.watchers, self.target, self.rclone, self.locals = [], None, None, []
        self.notifier = Notifier({})
        self.cfg = {}
        self.state = {"pid": os.getpid(), "started": datetime.now().isoformat(timespec="seconds"),
                      "activity": "시작 중", "last_poll": "", "last_process": "", "error": ""}

    def heartbeat(self):
        self.state["ts"] = time.time()
        path = os.path.join(self.data_dir, "heartbeat.json")
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(self.state, handle, ensure_ascii=False)
        os.replace(tmp, path)

    def activity(self, text):
        self.state["activity"] = text

    # 감시기 구성에 영향을 주는 설정. 이 밖의 값(보관함 목록·cron·VFS·알림·스캔 설정)만 바뀌면
    # 감시기는 그대로 두고 처리 대상(BookOasis)과 알림만 새로 만든다.
    WATCH_KEYS = ("roots", "rclone_path", "rclone_config", "rclone_timeout", "extensions", "buffer_seconds",
                  "api_timeout", "drive_workers", "drive_rps", "ignore_patterns", "verbose_log",
                  "poll_skeleton_depth")

    @staticmethod
    def _watch_view(cfg, key):
        """비교용 값. 폴더별 주기 규칙(folder_rules)은 감시기를 다시 만들지 않고 바로 바꿔 끼우므로 뺀다."""
        value = cfg.get(key)
        if key == "roots":
            value = [{k: v for k, v in r.items() if k != "folder_rules"} for r in value or []]
        return value

    def load(self):
        mtime = os.path.getmtime(self.runtime_path)
        if mtime == self.runtime_mtime:
            return False
        first = not self.runtime_mtime
        self.runtime_mtime = mtime
        with open(self.runtime_path, encoding="utf-8") as handle:
            cfg = json.load(handle)
        old_cfg, self.cfg = self.cfg, cfg
        try:
            apply_log_retention(self.data_dir, cfg.get("log_keep_days", 3))
        except Exception as error:
            log.warning("로그 정리 실패: %s", error)
        if not first and self.rclone and all(self._watch_view(old_cfg, k) == self._watch_view(cfg, k) for k in self.WATCH_KEYS):
            rules = {r.get("name"): r.get("folder_rules") or {} for r in cfg.get("roots") or []}
            for watcher in self.watchers:
                if watcher.name in rules:
                    watcher.cfg["folder_rules"] = rules[watcher.name]
            self.notifier = Notifier(cfg)
            self.rclone.rc_sources = rc_sources(cfg.get("vfs"), cfg.get("libraries"))
            skip_vfs = self.target.skip_vfs if self.target else set()
            self.target = BookOasis(cfg, cfg.get("libraries"), cfg.get("vfs"), self.store)
            self.target.skip_vfs = skip_vfs
            log.info("설정 적용(감시기 유지): 보관함 경로 %d개, VFS 규칙 %d개",
                     len(self.target.libraries), len(self.target.vfs.rules))
            return False
        rclone = Rclone(cfg.get("rclone_path") or "rclone", cfg.get("rclone_config") or "",
                        int(cfg.get("rclone_timeout", 60)))
        ext = str(cfg.get("extensions") or "").strip()
        if ext.lower() == "all":
            extensions = None
        elif ext:
            extensions = tuple(e if e.startswith(".") else f".{e}"
                               for e in re.split(r"[\s,]+", ext.lower()) if e)
        else:
            extensions = tuple(DEFAULT_EXTENSIONS)
        global IGNORE, DRIVE_RPS
        DRIVE_RPS = min(50.0, max(0.5, float(cfg.get("drive_rps", 3) or 3)))
        patterns = cfg.get("ignore_patterns")
        try:
            IGNORE = compile_patterns(DEFAULT_IGNORE_PATTERNS if patterns is None else patterns)
        except re.error as error:
            log.error("무시 패턴 오류, 기본값 사용: %s", error)
            IGNORE = compile_patterns(DEFAULT_IGNORE_PATTERNS)
        self.notifier = Notifier(cfg)
        buffer_seconds = int(cfg.get("buffer_seconds", 60))
        for old in getattr(self, "locals", []):
            old.stop()
        watchers, locals_ = [], []
        for root in cfg.get("roots") or []:
            if not root.get("enabled", True):
                continue
            if root.get("mode") == "local":
                try:
                    local = LocalWatcher(root, self.store.path, extensions, buffer_seconds)
                    local.verbose = bool(cfg.get("verbose_log"))
                    locals_.append(local)
                except Exception as error:
                    log.error("[%s] 로컬 감시 설정 오류: %s", root.get("name"), error)
                continue
            try:
                fallback = self.fallbacks().get(root.get("name"))
                activity = root.get("mode") == "activity" and fallback != "changes"
                drivepoll = root.get("mode") == "drivepoll" or fallback in ("drivepoll", "userfeed")
                cls = DrivePollWatcher if drivepoll else ActivityWatcher if activity else ChangesWatcher
                watcher = cls(root, self.store, rclone, extensions, buffer_seconds, int(cfg.get("api_timeout", 60)))
                watcher.user_feed = fallback == "userfeed"
                if isinstance(watcher, DrivePollWatcher):
                    watcher.workers = min(16, max(1, int(cfg.get("drive_workers", 4) or 4)))
                    watcher.set_tiers(cfg)
                watcher.verbose = bool(cfg.get("verbose_log"))
                watchers.append(watcher)
            except Exception as error:
                log.error("[%s] 감시 설정 오류: %s", root.get("name"), error)
        # 진행 중인 폴더 비교 스레드는 새 감시기에 넘겨, 끝나기 전에는 같은 폴더를 중복으로 훑지 않게 한다
        previous = {w.name: w for w in self.watchers}
        for watcher in watchers:
            old = previous.get(watcher.name)
            if isinstance(watcher, DrivePollWatcher) and isinstance(old, DrivePollWatcher):
                watcher.thread = old.thread
                if old.cfg == watcher.cfg:
                    watcher.next_sweep = old.next_sweep  # 같은 폴더 설정이면 다음 비교 시각도 그대로
        self.watchers = watchers
        self.locals = locals_
        self.rclone = rclone
        rclone.config_changed()  # 기준 상태 기록
        log.info("rclone 설정 파일: %s", rclone.config_path() or "(확인 실패)")
        rclone.rc_sources = rc_sources(cfg.get("vfs"), cfg.get("libraries"))
        self.target = BookOasis(cfg, cfg.get("libraries"), cfg.get("vfs"), self.store)
        self.target.skip_vfs = {w.name for w in locals_}  # 로컬 폴더는 이미 파일을 보고 감지했으므로 VFS 새로고침 불필요
        log.info("설정 적용: Drive 감시 %d개, 로컬 감시 %d개, 보관함 경로 %d개, VFS 규칙 %d개",
                 len(self.watchers), len(locals_), len(self.target.libraries), len(self.target.vfs.rules))
        return not first

    def fallbacks(self):
        try:
            with open(os.path.join(self.data_dir, "fallback.json"), encoding="utf-8") as handle:
                return json.load(handle) or {}
        except (OSError, ValueError):
            return {}

    @staticmethod
    def scope_missing(error):
        text = str(error).lower()
        return any(k in text for k in ("insufficient", "scope", "has not been used", "is disabled", "access_token_scope"))

    def set_fallback(self, name, value):
        data = self.fallbacks()
        data[name] = value
        path = os.path.join(self.data_dir, "fallback.json")
        with open(path + ".tmp", "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False)
        os.replace(path + ".tmp", path)

    def fall_back(self, watcher):
        """Activity → Changes 자동 전환. 전환 사실은 fallback.json에 남겨 재시작 후에도 유지한다."""
        self.set_fallback(watcher.name, "changes")
        with self.store.db:  # Activity 체크포인트는 Changes에서 쓸 수 없으므로 비운다 (추적 목록은 그대로 사용)
            self.store.db.execute("DELETE FROM cursor WHERE root=?", (watcher.name,))
        cfg = dict(watcher.cfg, mode="changes", seed=False)
        replacement = ChangesWatcher(cfg, self.store, watcher.rclone, watcher.extensions, watcher.buffer_seconds,
                                     watcher.api.timeout)
        replacement.verbose = watcher.verbose
        self.watchers[self.watchers.index(watcher)] = replacement
        message = (f"[{watcher.name}] 토큰에 Drive Activity 권한(drive.activity.readonly)이 없어 Changes 방식으로 "
                   "자동 전환했습니다. 이 시점 이후의 변경부터 감지합니다.")
        log.warning(message)
        self.notifier.root_error(watcher.name, message)

    def sync_links(self):
        """바로가기 대상이 다른 드라이브에 있으면 그 대상 폴더를 하위 감시로 따로 둔다 (이름: 상위 › 폴더)."""
        parents = {w.name: w for w in self.watchers if not getattr(w, "parent", None)}
        wanted = {}
        for row in self.store.db.execute("SELECT * FROM links"):
            parent = parents.get(row["parent"])
            if not parent or isinstance(parent, DrivePollWatcher):
                continue  # 폴더 비교는 바로가기 대상까지 직접 훑으므로 따로 둘 필요 없음
            name = f"{row['parent']} › {posixpath.basename(row['path'])}"
            wanted[name] = (parent, row)
        for watcher in [w for w in self.watchers if getattr(w, "parent", None) and w.name not in wanted]:
            self.watchers.remove(watcher)
            log.info("[%s] 바로가기 하위 감시 종료", watcher.name)
        present = {w.name for w in self.watchers}
        for name, (parent, row) in wanted.items():
            if name in present:
                continue
            cfg = {"name": name, "mode": "changes", "source_remote": parent.source_remote, "root_id": row["target_id"],
                   "local_root": row["path"], "seed": True, "enabled": True}
            fallback = self.fallbacks().get(name)
            cls = DrivePollWatcher if fallback in ("drivepoll", "userfeed") else ChangesWatcher
            child = cls(cfg, self.store, parent.rclone, parent.extensions, parent.buffer_seconds, parent.api.timeout)
            child.verbose, child.parent = parent.verbose, parent.name
            if isinstance(child, DrivePollWatcher):
                child.workers = min(16, max(1, int(self.cfg.get("drive_workers", 4) or 4)))
                child.set_tiers(self.cfg)
            self.watchers.append(child)
            log.info("[%s] 바로가기 하위 감시 시작 (대상 폴더 %s, 드라이브 %s)", name, row["target_id"], row["drive"] or "내 드라이브")

    def collect(self):
        started = time.monotonic()
        CHANGES_PAGES.clear()
        lines = []
        for watcher in list(self.watchers):
            if isinstance(watcher, (ChangesWatcher, ActivityWatcher)) and watcher.root_checked \
                    and not getattr(watcher, "parent", None):
                try:
                    watcher.discover_links()
                except Exception as error:
                    log.warning("[%s] 바로가기 확인 실패: %s", watcher.name, error)
        self.sync_links()
        for watcher in self.watchers:
            if STOP:
                break
            watcher.reset_stat()
            if time.monotonic() < watcher.retry_at:
                lines.append(f"{watcher.name}: 호출 제한으로 대기 중 ({int(watcher.retry_at - time.monotonic())}초 남음)")
                continue
            row = self.store.db.execute("SELECT status, error FROM cursor WHERE root=?", (watcher.name,)).fetchone()
            if row and row["status"] == "blocked" and "membership" in (row["error"] or "").lower():
                self.store.set_error(watcher.name, row["error"], "error")  # 이전 버전에서 멈춘 폴더: 자동 전환으로 다시 시도
            self.activity(f"[{watcher.name}] 변경 확인 중")
            one = time.monotonic()
            try:
                watcher.poll()
                watcher.failures = 0
                lines.append(watcher.summary())
            except DriveError as error:
                if error.code == 429 or "rateLimitExceeded" in str(error):
                    watcher.failures = min(watcher.failures + 1, 5)
                    delay = min(900, 60 * 2 ** (watcher.failures - 1))
                    watcher.retry_at = time.monotonic() + delay
                    watcher.stat["note"] = f"호출 제한, {delay}초 후 재시도"
                    self.store.set_error(watcher.name, watcher.stat["note"])
                elif error.code == 403 and isinstance(watcher, ChangesWatcher) and "membership" in str(error).lower() \
                        and not watcher.user_feed:
                    # 공유 드라이브 멤버가 아니라 폴더만 공유받은 계정: 변경 목록 대신 폴더 비교로 감시한다
                    # (계정 전체 변경 목록은 관계없는 변경이 수십만 건이라 쓸 수 없음)
                    self.set_fallback(watcher.name, "drivepoll")
                    replacement = DrivePollWatcher(dict(watcher.cfg, mode="drivepoll"), self.store, watcher.rclone,
                                                   watcher.extensions, watcher.buffer_seconds, watcher.api.timeout)
                    replacement.verbose = watcher.verbose
                    replacement.workers = min(16, max(1, int(self.cfg.get("drive_workers", 4) or 4)))
                    replacement.set_tiers(self.cfg)
                    replacement.parent = getattr(watcher, "parent", None)
                    self.watchers[self.watchers.index(watcher)] = replacement
                    message = (f"[{watcher.name}] 이 계정은 공유 드라이브 멤버가 아니라 변경 목록을 받을 수 없어, "
                               "폴더 비교 방식으로 전환했습니다.")
                    log.warning(message)
                    self.notifier.root_error(watcher.name, message)
                    lines.append(f"{watcher.name}: 공유 드라이브 멤버 아님 → 폴더 비교로 전환")
                    continue
                elif error.code == 403 and isinstance(watcher, ActivityWatcher) and self.scope_missing(error):
                    # 토큰에 drive.activity.readonly가 없으면 Activity를 쓸 수 없다 → 같은 폴더를 Changes로 감시
                    self.fall_back(watcher)
                    lines.append(f"{watcher.name}: Activity 권한 없음 → Changes로 전환")
                    continue
                elif error.code == 403:
                    watcher.stat["note"] = "권한 오류로 감시 중지 (처음부터 누르면 재시도)"
                    self.store.set_error(watcher.name, str(error), "blocked")
                else:
                    watcher.stat["note"] = f"오류: {error}"
                    self.store.set_error(watcher.name, str(error))
                log.error("[%s] %s", watcher.name, error)
                lines.append(f"{watcher.name}: {watcher.stat['note'][:120]}")
            except Exception as error:
                watcher.stat["note"] = f"오류: {error}"
                self.store.set_error(watcher.name, str(error))
                log.exception("[%s] 확인 실패: %s", watcher.name, error)
                lines.append(f"{watcher.name}: 오류 - {str(error)[:160]}")
            if watcher.stat.get("seed") and not watcher.stat.get("note"):
                counts = watcher.summary().split(": ", 1)[-1]
                watcher.stat["note"] = f"{counts} · {watcher.stat['seed']}"
                lines[-1] = f"{watcher.name}: {watcher.stat['note']}"
            note = watcher.stat.get("note") or ""
            if note.startswith(("오류", "권한 오류")):
                self.notifier.root_error(watcher.name, note)
            elif not note.startswith("호출 제한"):
                self.notifier.root_ok(watcher.name)
            try:
                self.store.save_stat(watcher.name, watcher.stat, time.monotonic() - one)
            except sqlite3.Error:
                pass
            if time.monotonic() - one > 20:
                self.process()  # 오래 걸린 감시기 뒤에는 그동안 처리할 차례가 된 기록을 먼저 처리
        lines += [local.last if local.last.startswith(local.name) else f"{local.name}: {local.last}" for local in self.locals]
        for local in self.locals:
            if local.last.startswith(("오류", "확인 필요")) or ": 오류" in local.last or "확인 필요" in local.last:
                self.notifier.root_error(local.name, local.last)
            elif local.snapshot is not None:
                self.notifier.root_ok(local.name)
        self.state["last_poll"] = datetime.now().isoformat(timespec="seconds")
        row = self.store.db.execute("SELECT COUNT(*), MIN(ready_at) FROM event WHERE status IN ('pending','waiting')").fetchone()
        if row and row[0]:
            wait = max(0, int(row[1] - time.time()))
            lines.append(f"처리 대기 {row[0]}건" + (f" (다음 처리 {wait}초 후)" if wait else " (곧 처리)"))
        if lines:
            log.info("확인 완료 (%.1f초) | %s", time.monotonic() - started, " | ".join(lines))

    def process(self):
        events = self.store.claim()
        if not events or not self.target:
            return 0
        self.activity(f"이벤트 {len(events)}건 처리 중")
        log.info("이벤트 %d건 처리 시작", len(events))
        try:
            results = self.target.process(events)
        except Exception as error:
            log.exception("이벤트 처리 실패")
            results = {ev["id"]: {"status": "failed", "message": str(error), "vfs": [], "scans": []} for ev in events}
        max_attempts = int(self.cfg.get("max_attempts", 5))
        for ev in events:
            results.setdefault(ev["id"], {"status": "failed", "message": "결과 없음", "vfs": [], "scans": []})
        try:
            self.notifier.results(events, results)
        except Exception as error:
            log.warning("알림 처리 오류: %s", error)
        for ev in events:
            r = results[ev["id"]]
            status = self.store.finish(ev, r["status"], r["message"], {"vfs": r["vfs"], "scans": r["scans"]},
                                       1 if r.get("terminal") else max_attempts, r.get("retry_in"))
            if r["status"] in ("failed", "timeout"):
                log.warning("이벤트 #%d %s → %s: %s", ev["id"], ev["path"] or ev["removed_path"], status, r["message"])
            elif r["status"] == "waiting" and not ev["message"]:
                log.info("이벤트 #%d %s: 마운트에 파일이 보일 때까지 대기", ev["id"], ev["path"] or ev["removed_path"])
        self.state["last_process"] = datetime.now().isoformat(timespec="seconds")
        return len(events)

    def run(self):
        stop_flag = os.path.join(self.data_dir, "stop.flag")
        wake_flag = os.path.join(self.data_dir, "wake.flag")
        next_poll, next_cleanup = 0.0, 0.0

        def beat():
            while not STOP:
                try:
                    self.heartbeat()
                except OSError:
                    pass
                time.sleep(3)
        threading.Thread(target=beat, daemon=True).start()

        global STOP
        while not STOP:
            if os.path.exists(stop_flag):
                break
            try:
                if self.load():
                    next_poll = 0.0  # 설정이 바뀌면 바로 다시 확인
                self.state["error"] = ""
            except Exception as error:
                self.state["error"] = f"설정 로드 실패: {error}"
                log.error(self.state["error"])
            now = time.monotonic()
            if os.path.exists(wake_flag):
                try:
                    os.remove(wake_flag)
                except OSError:
                    pass
                next_poll = 0.0
                for watcher in self.watchers:  # '지금 확인'·'바로 읽기': 폴더 비교도 다음 확인 때 바로 돌린다
                    if isinstance(watcher, DrivePollWatcher):
                        watcher.next_sweep = 0.0
            if self.rclone and self.rclone.config_changed():
                log.info("rclone.conf 변경 감지 → 토큰을 다시 읽고 즉시 확인합니다.")
                for watcher in self.watchers:
                    watcher.retry_at = 0.0
                    if self.store.get_cursor(watcher.name)[1] == "blocked":
                        self.store.set_error(watcher.name, "rclone.conf 변경으로 재시도")
                next_poll = 0.0
            if now >= next_poll:
                self.collect()
                interval = max(15, int(self.cfg.get("poll_seconds", 60)))
                if any(getattr(w, "behind", False) for w in self.watchers):
                    interval = 5  # 밀린 변경을 따라잡는 중: 기록을 처리한 뒤 곧바로 이어서 확인
                next_poll = time.monotonic() + interval
                self.state["next_poll"] = (datetime.now() + timedelta(seconds=interval)).isoformat(timespec="seconds")
            self.process()
            if now >= next_cleanup:
                self.store.cleanup(int(self.cfg.get("keep_days", 30)))
                try:
                    apply_log_retention(self.data_dir, self.cfg.get("log_keep_days", 3))
                except Exception:
                    pass
                next_cleanup = now + 86400
            self.activity("대기 중")
            for _ in range(4):
                if STOP or os.path.exists(stop_flag) or os.path.exists(wake_flag):
                    break
                time.sleep(0.5)
        STOP = True
        for local in self.locals:
            local.stop()
        for info in SEEDERS.values():
            proc = info.get("proc")
            if proc and proc.poll() is None:
                proc.terminate()  # 수집 중이던 rclone lsjson도 같이 종료 (다음 시작 때 다시 수집)
        self.activity("종료")
        try:
            self.heartbeat()
            os.remove(stop_flag)
        except OSError:
            pass
        log.info("워커 종료")


def apply_log_retention(data_dir, days):
    """worker.log 보관 기간 적용: 날짜별로 나뉜 지난 로그는 days일치만 남기고 지운다 (이전 버전의 크기별 로그 포함)."""
    days = max(1, int(days or 3))
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.handlers.TimedRotatingFileHandler):
            handler.backupCount = days
    cutoff = time.time() - days * 86400
    for name in os.listdir(data_dir):
        if name.startswith(("worker.log.", "worker.out.")) or name in ("worker.log.1", "worker.log.2"):
            full = os.path.join(data_dir, name)
            try:
                if os.path.getmtime(full) < cutoff or name in ("worker.log.1", "worker.log.2"):
                    os.remove(full)
            except OSError:
                pass
    out = os.path.join(data_dir, "worker.out")  # 표준 출력(예외 흔적)은 커지면 비운다
    try:
        if os.path.getsize(out) > 5 * 1024 * 1024:
            open(out, "w").close()
    except OSError:
        pass


def acquire_lock(path):
    handle = open(path, "a+")
    try:
        import fcntl
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except ImportError:
        try:
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except (ImportError, OSError):
            handle.close()
            return None
    except OSError:
        handle.close()
        return None
    return handle


def main():
    global STOP
    if len(sys.argv) < 2:
        print("usage: worker.py <DATA_DIR> [--daemon]")
        return 2
    data_dir = os.path.abspath(sys.argv[1])
    os.makedirs(data_dir, exist_ok=True)
    if "--daemon" in sys.argv and hasattr(os, "fork"):
        if os.fork() > 0:  # 부모(플러그인이 기다리는 런처)는 즉시 종료 → 좀비 방지
            return 0
        os.setsid()
    lock = acquire_lock(os.path.join(data_dir, "worker.lock"))
    if lock is None:
        return 0  # 이미 실행 중
    # 하루 단위로 나눠 저장하고, 처리 옵션의 '로그 보관(일)'만큼만 남긴다 (Worker.load에서 적용)
    handler = logging.handlers.TimedRotatingFileHandler(os.path.join(data_dir, "worker.log"), when="midnight",
                                                        backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
    logging.basicConfig(level=logging.INFO, handlers=[handler])

    def stop(*_):
        global STOP
        STOP = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    log.info("워커 시작 pid=%d", os.getpid())
    try:
        Worker(data_dir).run()
    except Exception:
        log.exception("워커 비정상 종료")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
