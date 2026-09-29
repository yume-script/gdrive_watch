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
SEEDERS = {}  # 감시 폴더 이름 → 기존 파일 목록 수집 스레드/프로세스

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
CREATE TABLE IF NOT EXISTS vfs_map(root TEXT PRIMARY KEY, rc TEXT, fs TEXT, remote TEXT, detected TEXT);
CREATE TABLE IF NOT EXISTS root_stat(root TEXT PRIMARY KEY, checked TEXT, raw INTEGER, outside INTEGER, ext INTEGER,
                                     same INTEGER, events INTEGER, note TEXT, elapsed REAL);
"""


def open_db(path):
    db = sqlite3.connect(path, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    db.executescript(SCHEMA)
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
                        "UPDATE item SET path = ? || substr(path, ?) WHERE root=? AND substr(path, 1, ?)=?",
                        (new, len(old) + 1, root, len(prefix), prefix))
                else:
                    self.db.execute("DELETE FROM item WHERE root=? AND substr(path, 1, ?)=?",
                                    (root, len(prefix), prefix))
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
            "SELECT * FROM event WHERE status='pending' AND ready_at<=? ORDER BY ready_at, id LIMIT ?",
            (time.time(), limit)).fetchall()
        return [dict(row) for row in rows]

    def finish(self, event, status, message, result, max_attempts):
        now = datetime.now().isoformat(timespec="seconds")
        payload = json.dumps(result, ensure_ascii=False)
        with self.db:
            if status in ("done", "skipped"):
                self.db.execute("UPDATE event SET status=?, message=?, result=?, finished=? WHERE id=?",
                                (status, message[:2000], payload, now, event["id"]))
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
            self.db.execute("DELETE FROM event WHERE status IN ('done','skipped') AND created<?", (cutoff,))


# ─────────────────────────── rclone / Drive 인증 ───────────────────────────

class DriveError(RuntimeError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(f"Google API {code}: {message}")


class Rclone:
    """rclone.conf의 토큰을 '읽기만' 한다. 갱신은 `rclone about`을 실행해 rclone 자신에게 맡긴다."""

    def __init__(self, binary, config, timeout=60):
        self.binary, self.config, self.timeout = binary, config, timeout
        self._tokens = {}
        self._conf_path = config or ""
        self._conf_stat = "init"

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

    def token(self, remote, force=False):
        """rclone.conf에서 토큰을 읽는다. rclone.conf를 FF·호스트 rclone과 공유하는 환경을 전제로,
        아직 유효하면 그대로 쓰고(파일에 쓰지 않음), 만료가 가까울 때만 rclone에게 갱신을 맡긴다.
        커스텀 인증 리모트(gds_endpoint 등)는 갱신을 시도하지 않고, 공유 중인 다른 rclone이 갱신해 저장한 토큰을 읽는다."""
        self.config_changed()
        cached = self._tokens.get(remote)
        if cached and not force and cached["expiry"] - utcnow() > timedelta(minutes=3):
            return cached
        conf, token, expiry = self._read(remote)
        custom = [k for k in conf if "endpoint" in k.lower()]
        fresh = token.get("access_token") and expiry - utcnow() > timedelta(minutes=3)
        if not custom and (force or not fresh):
            self.run("about", f"{remote}:", "--json")  # rclone이 토큰을 갱신해 rclone.conf에 저장
            conf, token, expiry = self._read(remote)
        if not token.get("access_token") or expiry <= utcnow():
            where = self.config or "rclone 기본 설정 파일"
            if custom:
                raise RuntimeError(
                    f"{remote} 토큰이 만료된 상태입니다(만료 {token.get('expiry') or '알 수 없음'}, 설정 파일: {where}). "
                    f"커스텀 인증({', '.join(custom)}) 리모트라 이 플러그인은 갱신하지 않고, 이 rclone.conf를 함께 쓰는 "
                    "FF·호스트 rclone이 갱신해 저장하기를 기다립니다(파일이 바뀌면 바로 다시 시도). 계속 이 상태면 "
                    "그쪽에서 이 리모트를 쓰고 있는지 확인하세요.")
            raise RuntimeError(f"{remote} 토큰이 만료된 상태입니다(만료 {token.get('expiry') or '알 수 없음'}, 설정 파일: {where}). "
                               "rclone이 이 리모트의 토큰을 갱신하지 못했습니다. rclone.conf 경로와 리모트의 인증 방식을 확인하세요.")
        self._tokens[remote] = {"access": token["access_token"], "expiry": expiry,
                                "team_drive": str(conf.get("team_drive") or "").strip()}
        return self._tokens[remote]


class DriveApi:
    def __init__(self, rclone, remote, timeout=60):
        self.rclone, self.remote, self.timeout = rclone, remote, timeout

    @property
    def team_drive(self):
        return self.rclone.token(self.remote)["team_drive"]

    def call(self, url, body=None):
        for attempt in (0, 1):
            access = self.rclone.token(self.remote, force=attempt == 1)["access"]
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
                if error.code == 401 and attempt == 0:
                    continue
                raise DriveError(error.code, detail) from None

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
        self.verbose = False
        self.reset_stat()
        if not self.local_root:
            raise ValueError(f"{self.name}: local_root는 절대 경로여야 합니다.")

    def reset_stat(self):
        self.stat = {"raw": 0, "outside": 0, "ext": 0, "same": 0, "events": 0, "note": ""}

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
        team_drive = self.api.team_drive
        # team_drive 리모트는 driveId로 그 공유 드라이브의 변경만 받는다. (내 드라이브 리모트는
        # includeItemsFromAllDrives로 참여 중인 공유 드라이브 변경까지 받으므로 제한 없음)
        if self.mode == "changes" and team_drive and (data.get("driveId") or "") != team_drive:
            where = f"공유 드라이브 {data['driveId']}" if data.get("driveId") else "내 드라이브"
            mine = f"공유 드라이브 {team_drive}" if team_drive else "내 드라이브"
            raise RuntimeError(f"감시 폴더(실제 위치: {where})가 리모트 {self.source_remote}의 드라이브({mine})와 달라 "
                               "Changes로 변경을 받을 수 없습니다. 그 드라이브를 가리키는 리모트를 지정하거나 Activity 방식을 쓰세요.")
        self.root_checked = True

    @staticmethod
    def shortcut_target(data):
        """폴더를 가리키는 바로가기면 대상 폴더 ID."""
        if data.get("mimeType") != SHORTCUT_MIME:
            return ""
        detail = data.get("shortcutDetails") or {}
        return detail.get("targetId", "") if detail.get("targetMimeType") == FOLDER_MIME else ""

    def map_shortcut(self, shortcut_id, target_id, path):
        """감시 폴더 안의 폴더 바로가기: 대상 폴더 ID를 바로가기 경로로 등록해,
        대상 폴더 안에서 생긴 변경(parents=대상 폴더)이 바로가기 경로로 해석되게 한다."""
        self.store.set_item(self.name, target_id, {"path": path, "is_dir": True, "sig": f"target:{shortcut_id}"})
        self.store.set_outside(self.name, target_id, False)

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
            self.store.save_cursor(self.name, token, "seeding")
            self.launch_seed()
        else:
            self.store.save_cursor(self.name, token)
            log.info("[%s] 감시 시작 (기존 파일 목록 수집 안 함)", self.name)

    def seeding(self, status):
        """seeding 상태면 True (워커가 재시작돼 수집이 끊겼으면 다시 시작)."""
        if status != "seeding":
            return False
        info = SEEDERS.get(self.name)
        if not info or not info["thread"].is_alive():
            self.launch_seed()
            info = SEEDERS.get(self.name)
        minutes = int((time.time() - info["started"]) / 60)
        self.stat["note"] = f"기존 파일 목록 수집 중 ({minutes}분 경과, 이 폴더의 변경은 끝난 뒤 처리)"
        if time.time() - info.get("logged", 0) >= 300:
            info["logged"] = time.time()
            if minutes:
                log.info("[%s] 기존 파일 목록 수집 중… %d분 경과", self.name, minutes)
        return True

    def launch_seed(self):
        info = SEEDERS.get(self.name)
        if info and info["thread"].is_alive():
            return
        info = {"proc": None, "started": time.time()}
        command = self.rclone.command("lsjson", "-R", "--fast-list", "--no-mimetype", "--no-modtime",
                                      "--drive-root-folder-id", self.root_id, f"{self.source_remote}:")
        name, local_root, store_path = self.name, self.local_root, self.store.path

        def run():
            store = Store(store_path)
            try:
                log.info("[%s] 기존 파일 목록 수집 시작 (rclone lsjson, 폴더가 크면 오래 걸림)", name)
                proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                info["proc"] = proc
                out, err = proc.communicate(timeout=6 * 3600)
                if STOP:
                    return
                if proc.returncode != 0:
                    raise RuntimeError(f"rclone lsjson 실패(코드 {proc.returncode}): {err.strip()[-400:]}")
                rows = []
                for e in json.loads(out or "[]"):
                    if not e.get("ID"):
                        continue
                    path = posixpath.join(local_root, e["Path"])
                    ids = str(e["ID"]).split("\t")  # rclone은 바로가기를 '대상ID<TAB>바로가기ID'로 표기
                    if len(ids) == 2 and e.get("IsDir"):
                        rows.append((ids[1], path, True, f"shortcut:{ids[0]}"))
                        rows.append((ids[0], path, True, f"target:{ids[1]}"))
                    else:
                        rows.append((ids[0], path, e.get("IsDir", False)))
                store.replace_items(name, rows)
                token, _ = store.get_cursor(name)
                store.save_cursor(name, token, "ready")
                log.info("[%s] 기존 파일 %d개 수집 완료 (%.0f분 소요) → 쌓인 변경부터 처리합니다",
                         name, len(rows), (time.time() - info["started"]) / 60)
            except Exception as error:
                if STOP:
                    return
                token, _ = store.get_cursor(name)
                store.save_cursor(name, token, "error",
                                  f"기존 파일 목록 수집 실패, 목록 없이 감시를 계속합니다: {error}")
                log.error("[%s] 기존 파일 목록 수집 실패, 목록 없이 계속: %s", name, error)
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
            self.stat["events"] += 1
        self.store.record(self.name, file_id, prev, cur, event, self.buffer_seconds, receipt)
        if event:
            log.info("[%s] %s %s %s%s", self.name, event["action"], event["item_type"], event["path"],
                     f" (← {event['removed_path']})" if event["removed_path"] not in ("", event["path"]) else "")
        return int(event is not None)


class ChangesWatcher(Watcher):
    mode = "changes"

    def poll(self):
        token, status = self.store.get_cursor(self.name)
        if status == "blocked" or self.seeding(status):
            return 0
        self.check_root()
        drive_params = {"driveId": self.api.team_drive} if self.api.team_drive else {}
        if not token:
            # 토큰을 먼저 받고 스냅샷 → 그 사이 변경은 스냅샷 뒤에 재생되어 흡수된다
            self.begin(self.api.get("changes/startPageToken", **drive_params)["startPageToken"])
            return 0
        accepted = 0
        while token and not STOP:
            data = self.api.get(
                "changes", pageToken=token, pageSize=1000, includeRemoved="true",
                includeItemsFromAllDrives="true", **drive_params,
                fields=f"nextPageToken,newStartPageToken,changes(fileId,removed,file({FILE_FIELDS}))")
            for change in data.get("changes") or []:
                accepted += self.handle(change)
            if data.get("nextPageToken"):
                token = data["nextPageToken"]
                self.store.save_cursor(self.name, token)
                continue
            self.store.save_cursor(self.name, data.get("newStartPageToken") or token)
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
            self.stat["outside"] += 1
            self.trace("범위 밖: %s (%s)", data.get("name") or "(삭제된 항목)", file_id)
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
        if status == "blocked" or self.seeding(status):
            return 0
        self.check_root()
        if not token:
            now = stamp(utcnow())
            self.begin(json.dumps({"start": now, "floor": now}))
            return 0
        cursor = json.loads(token)
        start = parse_time(cursor["start"])
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
                    self.callback(directory if mask & (self.IN_DELETE_SELF | self.IN_MOVE_SELF) else directory)

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


# ─────────────────────────── BookOasis 스캔 ───────────────────────────

class BookOasis:
    """보관함 목록은 플러그인이 runtime.json에 넣어준 값을 사용하고, 스캔은 /api/webhook/scan(path)로 요청."""

    def __init__(self, cfg, libraries, vfs_rules, store=None):
        self.url = str(cfg.get("bookoasis_url") or "http://127.0.0.1:5930").rstrip("/")
        self.token = cfg.get("webhook_token") or os.environ.get("WEBHOOK_TOKEN", "")
        self.timeout = int(cfg.get("scan_timeout", 300))
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
        for ev in events:
            if ev.get("root") in getattr(self, "skip_vfs", ()):
                continue
            is_dir = ev["item_type"] == "directory"
            path, removed = ev["path"], ev["removed_path"]
            ops = []
            if ev["action"] in ("rename", "move", "delete") and removed:
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

        # 2) 스캔할 디렉터리 (VFS 실패 이벤트는 보류) → 상위 폴더로 합치기
        wanted = {}
        for ev in events:
            if not results[ev["id"]]["ok"]:
                continue
            dirs = []
            if ev["action"] != "delete" and ev["path"]:
                dirs.append((ev["path"] if ev["item_type"] == "directory" else posixpath.dirname(ev["path"]), False))
            if ev["removed_path"] and (ev["action"] == "delete" or ev["removed_path"] != ev["path"]):
                dirs.append((posixpath.dirname(ev["removed_path"]), True))
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

        # 3) 스캔 요청
        for d in kept:
            if STOP:
                break
            try:
                ok, message, lib = self.scan(d, wanted[d]["removed"])
            except Exception as error:
                ok, message, lib = False, f"{type(error).__name__}: {error}", None
            label = f"{lib['db_type']}#{lib['id']} {lib['name']}" if lib else ""
            if ok is not None:
                log.info("스캔 %s %s [%s] %s", "OK" if ok else "실패", d, label, message)
            for event_id in wanted[d]["events"]:
                results[event_id]["scans"].append({"dir": d, "library": label, "ok": ok, "msg": message})
                if ok is False:
                    results[event_id]["ok"] = False
                    if "토큰 불일치" in message:
                        results[event_id]["terminal"] = True
                    results[event_id]["messages"].append(f"스캔 실패 {d}: {message}")

        final = {}
        for ev in events:
            r = results[ev["id"]]
            if not r["ok"]:
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
                               "terminal": bool(r.get("terminal"))}
        return final


# ─────────────────────────── 워커 본체 ───────────────────────────

class Worker:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.runtime_path = os.path.join(data_dir, "runtime.json")
        self.store = Store(os.path.join(data_dir, "state.db"))
        self.runtime_mtime = 0
        self.watchers, self.target, self.rclone, self.locals = [], None, None, []
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

    def load(self):
        mtime = os.path.getmtime(self.runtime_path)
        if mtime == self.runtime_mtime:
            return False
        first = not self.runtime_mtime
        self.runtime_mtime = mtime
        with open(self.runtime_path, encoding="utf-8") as handle:
            cfg = json.load(handle)
        self.cfg = cfg
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
                cls = ActivityWatcher if root.get("mode") == "activity" else ChangesWatcher
                watcher = cls(root, self.store, rclone, extensions, buffer_seconds, int(cfg.get("api_timeout", 60)))
                watcher.verbose = bool(cfg.get("verbose_log"))
                watchers.append(watcher)
            except Exception as error:
                log.error("[%s] 감시 설정 오류: %s", root.get("name"), error)
        self.watchers = watchers
        self.locals = locals_
        self.rclone = rclone
        rclone.config_changed()  # 기준 상태 기록
        log.info("rclone 설정 파일: %s", rclone.config_path() or "(확인 실패)")
        self.target = BookOasis(cfg, cfg.get("libraries"), cfg.get("vfs"), self.store)
        self.target.skip_vfs = {w.name for w in locals_}  # 로컬 폴더는 이미 파일을 보고 감지했으므로 VFS 새로고침 불필요
        log.info("설정 적용: Drive 감시 %d개, 로컬 감시 %d개, 보관함 경로 %d개, VFS 규칙 %d개",
                 len(self.watchers), len(locals_), len(self.target.libraries), len(self.target.vfs.rules))
        return not first

    def collect(self):
        started = time.monotonic()
        lines = []
        for watcher in self.watchers:
            if STOP:
                break
            watcher.reset_stat()
            if time.monotonic() < watcher.retry_at:
                lines.append(f"{watcher.name}: 호출 제한으로 대기 중 ({int(watcher.retry_at - time.monotonic())}초 남음)")
                continue
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
            try:
                self.store.save_stat(watcher.name, watcher.stat, time.monotonic() - one)
            except sqlite3.Error:
                pass
        lines += [local.last if local.last.startswith(local.name) else f"{local.name}: {local.last}" for local in self.locals]
        self.state["last_poll"] = datetime.now().isoformat(timespec="seconds")
        row = self.store.db.execute("SELECT COUNT(*), MIN(ready_at) FROM event WHERE status='pending'").fetchone()
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
            r = results.get(ev["id"]) or {"status": "failed", "message": "결과 없음", "vfs": [], "scans": []}
            status = self.store.finish(ev, r["status"], r["message"], {"vfs": r["vfs"], "scans": r["scans"]},
                                       1 if r.get("terminal") else max_attempts)
            if r["status"] == "failed":
                log.warning("이벤트 #%d %s → %s: %s", ev["id"], ev["path"] or ev["removed_path"], status, r["message"])
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
                next_poll = time.monotonic() + interval
                self.state["next_poll"] = (datetime.now() + timedelta(seconds=interval)).isoformat(timespec="seconds")
            self.process()
            if now >= next_cleanup:
                self.store.cleanup(int(self.cfg.get("keep_days", 30)))
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
    handler = logging.handlers.RotatingFileHandler(os.path.join(data_dir, "worker.log"),
                                                   maxBytes=2 * 1024 * 1024, backupCount=2, encoding="utf-8")
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
