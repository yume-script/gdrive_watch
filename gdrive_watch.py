# -*- coding: utf-8 -*-
"""드라이브 변경 감시 (gdrive_watch)

Google Drive 변경(Changes/Activity)을 감지해 rclone VFS를 갱신하고 BookOasis에 부분 스캔을 요청한다.
실제 감시는 별도 워커 프로세스(worker.py)가 수행하고, 이 모듈은
  - 워커 실행/중지/자동 시작
  - 설정(runtime.json) 생성: 플러그인 설정 + 감시 설정 + 보관함 목록(DB)
  - 카테고리 탭 화면용 RPC (현황, 이벤트 기록, 로그, 감시 설정)
를 담당한다.

필요 조건: .env 에 ALLOW_PLUGIN_SUBPROCESS=true
"""
import hashlib
import importlib.util
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time

from plugins.metadata.base import BaseMetadataProvider

PLUGIN_ID = "gdrive_watch"
HERE = os.path.dirname(os.path.abspath(__file__))
WORKER_PATH = os.path.join(HERE, "worker.py")
DATA_DIR = os.path.abspath(os.path.join("plugins", "data", PLUGIN_ID))
DB_TYPES = ("general", "adult", "audiobook", "video")
LIBRARY_REFRESH_SECONDS = 60


def _path(name):
    return os.path.join(DATA_DIR, name)


def _read_json(name, default=None):
    try:
        with open(_path(name), encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return default


def _write_json(name, data):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = _path(name) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    os.replace(tmp, _path(name))


def _worker_alive():
    """락 파일을 잡아보면 워커 실행 여부를 정확히 알 수 있다 (posix). 그 외에는 heartbeat로 판단."""
    lock_path = _path("worker.lock")
    if not os.path.exists(lock_path):
        return False
    try:
        import fcntl
        with open(lock_path, "a+") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return True
            fcntl.flock(handle, fcntl.LOCK_UN)
            return False
    except ImportError:
        beat = _read_json("heartbeat.json", {}) or {}
        return time.time() - float(beat.get("ts") or 0) < 20 and beat.get("activity") != "종료"


def _start_worker():
    if _worker_alive():
        return False
    if not os.path.exists(_path("runtime.json")):
        raise RuntimeError("실행 설정이 아직 없습니다. 감시 설정을 먼저 저장하세요.")
    os.makedirs(DATA_DIR, exist_ok=True)
    try:
        os.remove(_path("stop.flag"))
    except OSError:
        pass
    out = open(_path("worker.out"), "ab")
    kwargs = {"stdin": subprocess.DEVNULL, "stdout": out, "stderr": out, "cwd": DATA_DIR, "close_fds": True}
    if os.name == "nt":
        kwargs["creationflags"] = 0x00000200 | 0x00000008  # NEW_PROCESS_GROUP | DETACHED_PROCESS
        subprocess.Popen([sys.executable, WORKER_PATH, DATA_DIR], **kwargs)
    else:
        # --daemon: 워커가 fork 후 런처는 즉시 종료 → BookOasis 프로세스에 좀비가 남지 않음
        launcher = subprocess.Popen([sys.executable, WORKER_PATH, DATA_DIR, "--daemon"],
                                    start_new_session=True, **kwargs)
        launcher.wait(timeout=15)
    out.close()
    return True


def _stop_worker(disable=True):
    os.makedirs(DATA_DIR, exist_ok=True)
    open(_path("stop.flag"), "w").close()
    if disable:
        open(_path("disabled.flag"), "w").close()  # 자동 시작 억제 (사용자가 직접 중지)
    beat = _read_json("heartbeat.json", {}) or {}
    pid = int(beat.get("pid") or 0)
    if pid and os.name != "nt":
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass
    for _ in range(20):
        if not _worker_alive():
            return True
        time.sleep(0.5)
    return False


def _autostart():
    """모듈 로드 시(서버 시작 포함) 자동 시작. 너무 자주 확인하지 않도록 30초 간격 제한."""
    try:
        runtime = _read_json("runtime.json")
        if not runtime or not runtime.get("auto_start") or not runtime.get("roots"):
            return
        stamp = _path("autostart.check")
        if os.path.exists(stamp) and time.time() - os.path.getmtime(stamp) < 30:
            return
        open(stamp, "w").close()
        if os.path.exists(_path("disabled.flag")):
            return  # 사용자가 직접 중지한 상태
        _start_worker()
    except Exception:
        pass


_autostart()


def _file_bind_mounted(path):
    """rclone.conf가 도커에 '파일 단위'로 바인드 마운트됐는지 확인 (/proc/self/mountinfo).
    파일 단위 마운트는 호스트에서 파일을 새로 써서 교체(rename)하면 컨테이너가 옛 내용을 계속 본다."""
    if not path:
        return False
    try:
        real = os.path.realpath(path)
        with open("/proc/self/mountinfo", encoding="utf-8") as handle:
            for line in handle:
                parts = line.split()
                if len(parts) > 4 and parts[4].replace("\\040", " ") in (path, real):
                    return True
    except OSError:
        pass
    return False


def _verify_token(base_url, token):
    """/api/webhook/plugins/status 는 WEBHOOK_TOKEN으로 인증하는 읽기 전용 API라 스캔 없이 토큰만 확인할 수 있다."""
    from urllib.error import HTTPError, URLError
    from urllib.parse import quote
    from urllib.request import Request, urlopen
    if not token:
        return False, "토큰이 없습니다."
    request = Request(f"{base_url.rstrip('/')}/api/webhook/plugins/status?token={quote(token)}",
                      headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=10) as response:
            return (response.status == 200), "토큰 일치"
    except HTTPError as error:
        return False, "토큰 불일치 (BookOasis가 401 반환)" if error.code == 401 else f"HTTP {error.code}"
    except URLError as error:
        return False, f"BookOasis 연결 실패 ({base_url}): {error.reason}"


def _plugin_version():
    try:
        with open(os.path.join(HERE, "VERSION"), encoding="utf-8") as handle:
            return str(json.load(handle).get("plugin version") or "")
    except (OSError, ValueError):
        return ""


def _load_worker_module():
    spec = importlib.util.spec_from_file_location("gdrive_watch_worker", WORKER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _db():
    os.makedirs(DATA_DIR, exist_ok=True)
    db = sqlite3.connect(_path("state.db"), timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
    db.executescript(_load_worker_module().SCHEMA)
    try:
        db.execute("ALTER TABLE event ADD COLUMN force INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    return db


DEFAULT_WATCH = {
    "roots": [],
    "vfs": [],
    "poll_seconds": 60,
    "buffer_seconds": 60,
    "max_attempts": 5,
    "keep_days": 30,
    "extensions": "",
    "verbose_log": False,
    "file_wait_minutes": 10,
    "full_scan_guard_minutes": 10,
    "ignore_patterns": None,  # None이면 워커 기본값
    "discord_webhook": "",
    "notify_done": True,
    "notify_failed": True,
    "notify_error": True,
}


class GDriveWatchProvider(BaseMetadataProvider):
    id = "gdrive_watch"
    name = "드라이브 변경 감시"
    is_searchable = False
    admin_only = True

    config_schema = [
        {"key": "RCLONE_PATH", "label": "rclone 실행 파일 경로", "type": "text", "default": "rclone"},
        {"key": "RCLONE_CONFIG", "label": "rclone.conf 경로 (비우면 rclone 기본값)", "type": "text", "default": ""},
        {"key": "BOOKOASIS_URL", "label": "BookOasis 내부 주소 (스캔 요청용)", "type": "text",
         "default": "http://127.0.0.1:5930"},
        {"key": "WEBHOOK_TOKEN", "label": "WEBHOOK_TOKEN (비우면 .env 값 사용)", "type": "password", "default": ""},
        {"key": "AUTO_START", "label": "BookOasis 시작 시 감시 자동 시작", "type": "checkbox", "default": True},
    ]

    category_tab = {
        "title": "드라이브 변경 감시",
        "icon": "fa-solid fa-arrows-rotate",
        "order": 95,
        "sessions": "all",
    }

    # ── 필수 계약 ──
    def search(self, db_type, query):
        return {"success": True, "items": []}

    def apply(self, db_type, book_id, item_data):
        return False, "감시 전용 플러그인입니다."

    def get_context_menu_items(self, db_type, context):
        return []  # 도서 우클릭 메뉴에는 노출하지 않음 (RPC 전용)

    # ── 설정/런타임 ──
    ENV_KEYS = ("RCLONE_PATH", "RCLONE_CONFIG", "BOOKOASIS_URL", "WEBHOOK_TOKEN", "AUTO_START")

    def _raw_env(self):
        """플러그인 설정(환경설정 화면) 위에 카테고리 탭에서 저장한 값을 덮어쓴다."""
        cfg = dict(self.get_plugin_config("general", default={}) or {})
        env = (_read_json("watch.json", {}) or {}).get("env") or {}
        for key in self.ENV_KEYS:
            if key in env and (env[key] not in ("", None) or key in ("RCLONE_CONFIG", "WEBHOOK_TOKEN")):
                cfg[key] = env[key]
        return cfg

    def _settings(self):
        cfg = self._raw_env()
        auto = cfg.get("AUTO_START", True)
        return {
            "rclone_path": str(cfg.get("RCLONE_PATH") or "rclone").strip(),
            "rclone_config": str(cfg.get("RCLONE_CONFIG") or "").strip(),
            "bookoasis_url": str(cfg.get("BOOKOASIS_URL") or "http://127.0.0.1:5930").strip(),
            "webhook_token": str(cfg.get("WEBHOOK_TOKEN") or "").strip(),
            "auto_start": str(auto).lower() not in ("false", "0", "off", ""),
        }

    def _rclone_cmd(self, settings=None):
        settings = settings or self._settings()
        return [settings["rclone_path"]] + (["--config", settings["rclone_config"]] if settings["rclone_config"] else [])

    def _watch(self):
        data = dict(DEFAULT_WATCH)
        data.update(_read_json("watch.json", {}) or {})
        return data

    def _libraries(self, force=False):
        cached = _read_json("libraries.json")
        if cached and not force and time.time() - float(cached.get("ts") or 0) < LIBRARY_REFRESH_SECONDS:
            return cached["items"]
        items = []
        for db_type in DB_TYPES:
            try:
                gateway = self.get_db_gateway(db_type)
                rows = None
                for columns in ("id, name, physical_path, rclone_rc_url, cron_schedule",
                                "id, name, physical_path, rclone_rc_url", "id, name, physical_path"):
                    try:
                        rows = gateway.fetch_all(f"SELECT {columns} FROM libraries") or []
                        break
                    except Exception:
                        continue
                if rows is None:
                    continue
            except Exception:
                continue
            for row in rows:
                row = dict(row)
                roots = [line.strip() for line in str(row.get("physical_path") or "").splitlines() if line.strip()]
                if roots:
                    items.append({"db_type": db_type, "id": int(row["id"]), "name": row.get("name") or "",
                                  "roots": roots, "rclone_rc_url": row.get("rclone_rc_url") or "",
                                  "cron_schedule": row.get("cron_schedule") or ""})
        _write_json("libraries.json", {"ts": time.time(), "items": items})
        return items

    def _sync_runtime(self, force_libraries=False):
        """플러그인 설정 + 감시 설정 + 보관함 목록 → runtime.json (내용이 바뀐 경우에만 기록)."""
        runtime = dict(self._watch())
        runtime.update(self._settings())
        runtime["libraries"] = self._libraries(force=force_libraries)
        current = _read_json("runtime.json")
        if current != runtime:
            _write_json("runtime.json", runtime)
        return runtime

    @staticmethod
    def _clear_disabled():
        try:
            os.remove(_path("disabled.flag"))
        except OSError:
            pass

    # ── RPC 진입점 ──
    def run_context_menu_action(self, db_type, action_id, context):
        try:
            from flask import session
            if session.get("role") != "admin":
                return {"success": False, "error": "관리자만 사용할 수 있습니다."}
        except Exception:
            pass
        handler = getattr(self, f"_rpc_{action_id}", None)
        if not handler:
            return {"success": False, "error": f"알 수 없는 동작: {action_id}"}
        try:
            return handler(context or {})
        except Exception as error:
            return {"success": False, "error": f"{type(error).__name__}: {error}"}

    def _rpc_status(self, ctx):
        runtime = self._sync_runtime()
        alive = _worker_alive()
        if not alive and runtime.get("auto_start") and runtime.get("roots") and not os.path.exists(_path("disabled.flag")):
            alive = _start_worker() or _worker_alive()
        beat = _read_json("heartbeat.json", {}) or {}
        db = _db()
        counts = {row["status"]: row["n"] for row in db.execute("SELECT status, COUNT(*) n FROM event GROUP BY status")}
        today = time.strftime("%Y-%m-%d")
        today_counts = {row["status"]: row["n"] for row in db.execute(
            "SELECT status, COUNT(*) n FROM event WHERE created >= ? GROUP BY status", (today,))}
        roots = []
        for root in runtime.get("roots") or []:
            name = root.get("name")
            cur = db.execute("SELECT status, error, updated FROM cursor WHERE root=?", (name,)).fetchone()
            items = db.execute("SELECT COUNT(*) FROM item WHERE root=?", (name,)).fetchone()[0]
            last = db.execute("SELECT created FROM event WHERE root=? ORDER BY id DESC LIMIT 1", (name,)).fetchone()
            stat = db.execute("SELECT * FROM root_stat WHERE root=?", (name,)).fetchone()
            roots.append({
                "name": name, "mode": root.get("mode", "changes"), "enabled": root.get("enabled", True),
                "local_detect": root.get("local_detect", ""),
                "fallback": (_read_json("fallback.json", {}) or {}).get(name, ""),
                "local_root": root.get("local_root"), "status": cur["status"] if cur else "",
                "error": cur["error"] if cur else "", "updated": cur["updated"] if cur else "",
                "items": items, "last_event": last["created"] if last else "",
                "stat": dict(stat) if stat else None,
            })
        oldest = db.execute("SELECT MIN(created) FROM event WHERE status IN ('pending','waiting')").fetchone()[0]
        token_fail = db.execute("SELECT COUNT(*) FROM event WHERE status IN ('failed','pending') AND "
                                "(message LIKE '%토큰 불일치%' OR message LIKE '%Invalid webhook token%')").fetchone()[0]
        db.close()
        warnings = []
        if token_fail:
            warnings.append(f"스캔 요청 {token_fail}건이 WEBHOOK_TOKEN 불일치로 실패했습니다. [감시 설정 > 실행 환경]에서 "
                            "[토큰 확인]을 눌러 보고, 맞춘 뒤 [실패 전부 재시도]를 누르세요.")
        if not runtime.get("roots"):
            warnings.append("감시 루트가 없습니다. [감시 설정] 탭에서 추가하세요.")
        if oldest and not alive:
            warnings.append("워커가 멈춰 있어 대기 중인 기록이 처리되지 않습니다. [시작]을 누르세요.")
        if not (runtime.get("webhook_token") or os.environ.get("WEBHOOK_TOKEN")):
            warnings.append("WEBHOOK_TOKEN이 없어 스캔 요청을 보낼 수 없습니다. 플러그인 설정 또는 .env를 확인하세요.")
        if not runtime.get("vfs") and not any(l.get("rclone_rc_url") for l in runtime.get("libraries") or []):
            warnings.append("RC 주소가 설정된 보관함이 없어 rclone VFS 새로고침 없이 스캔만 요청합니다.")
        if not runtime.get("rclone_config"):
            warnings.append("rclone.conf 경로가 비어 있어 rclone 기본 위치의 설정 파일을 사용합니다. [감시 설정 > 실행 환경]에서 확인하세요.")
        elif _file_bind_mounted(runtime["rclone_config"]):
            warnings.append("rclone.conf가 파일 단위로 마운트되어 있어 호스트에서 바꾼 내용이 컨테이너에 반영되지 않을 수 있습니다. "
                            "rclone.conf가 들어 있는 디렉터리째 마운트하세요.")
        elif not os.path.exists(runtime["rclone_config"]):
            warnings.append(f"rclone.conf를 찾을 수 없습니다: {runtime['rclone_config']}")
        return {"success": True, "worker": {"alive": alive, "pid": beat.get("pid"), "started": beat.get("started"),
                                             "activity": beat.get("activity") if alive else "중지됨",
                                             "last_poll": beat.get("last_poll"), "next_poll": beat.get("next_poll"),
                                             "last_process": beat.get("last_process"),
                                             "error": beat.get("error"), "stopped_by_user": os.path.exists(_path("disabled.flag"))},
                "counts": counts, "today": today_counts, "roots": roots, "warnings": warnings,
                "libraries": len(runtime.get("libraries") or []), "auto_start": runtime.get("auto_start"),
                "version": _plugin_version()}

    def _rpc_start(self, ctx):
        self._clear_disabled()
        self._sync_runtime(force_libraries=True)
        started = _start_worker()
        return {"success": True, "message": "감시를 시작했습니다." if started else "이미 실행 중입니다."}

    def _rpc_stop(self, ctx):
        ok = _stop_worker()
        return {"success": True, "message": "감시를 중지했습니다." if ok else "중지 요청을 보냈습니다. (진행 중인 작업이 끝나면 종료)"}

    def _rpc_restart(self, ctx):
        _stop_worker(disable=False)
        self._clear_disabled()
        self._sync_runtime(force_libraries=True)
        _start_worker()
        return {"success": True, "message": "감시를 다시 시작했습니다."}

    def _rpc_poll_now(self, ctx):
        self._sync_runtime(force_libraries=True)
        open(_path("wake.flag"), "w").close()
        return {"success": True, "message": "즉시 확인을 요청했습니다."}

    def _rpc_events(self, ctx):
        page = max(1, int(ctx.get("page") or 1))
        size = min(200, max(10, int(ctx.get("size") or 50)))
        where, params = [], []
        groups = {"pending": ("pending", "waiting"), "failed": ("failed", "timeout")}
        if ctx.get("status"):
            wanted = groups.get(ctx["status"], (ctx["status"],))
            where.append("status IN (" + ",".join("?" * len(wanted)) + ")")
            params += list(wanted)
        if ctx.get("root"):
            where.append("root=?")
            params.append(ctx["root"])
        if ctx.get("q"):
            where.append("(path LIKE ? OR removed_path LIKE ? OR message LIKE ?)")
            params += [f"%{ctx['q']}%"] * 3
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        db = _db()
        total = db.execute(f"SELECT COUNT(*) FROM event {clause}", params).fetchone()[0]
        rows = db.execute(f"SELECT * FROM event {clause} ORDER BY id DESC LIMIT ? OFFSET ?",
                          params + [size, (page - 1) * size]).fetchall()
        db.close()
        items = []
        for row in rows:
            item = dict(row)
            try:
                item["result"] = json.loads(item.get("result") or "{}")
            except ValueError:
                item["result"] = {}
            items.append(item)
        return {"success": True, "items": items, "total": total, "page": page, "size": size}

    def _rpc_retry(self, ctx):
        db = _db()
        with db:
            if ctx.get("all_failed"):
                n = db.execute("UPDATE event SET status='pending', attempts=0, ready_at=0, force=1, created=? "
                               "WHERE status IN ('failed','timeout')", (time.strftime("%Y-%m-%dT%H:%M:%S"),)).rowcount
            else:
                ids = [int(i) for i in ctx.get("ids") or []]
                marks = ",".join("?" * len(ids)) or "NULL"
                n = db.execute(f"UPDATE event SET status='pending', attempts=0, ready_at=0, force=1, created=? WHERE id IN ({marks})",
                               [time.strftime("%Y-%m-%dT%H:%M:%S")] + ids).rowcount
        db.close()
        open(_path("wake.flag"), "w").close()
        return {"success": True, "message": f"{n}건을 재시도 대기열에 넣었습니다."}

    def _rpc_delete(self, ctx):
        db = _db()
        with db:
            if ctx.get("clear") == "done":
                n = db.execute("DELETE FROM event WHERE status IN ('done','skipped','timeout')").rowcount
            else:
                ids = [int(i) for i in ctx.get("ids") or []]
                marks = ",".join("?" * len(ids)) or "NULL"
                n = db.execute(f"DELETE FROM event WHERE id IN ({marks})", ids).rowcount
        db.close()
        return {"success": True, "message": f"{n}건을 삭제했습니다."}

    def _rpc_manual(self, ctx):
        """경로 직접 반영: 감시와 같은 처리(VFS 새로고침 → 파일 확인 → 스캔)를 태운다."""
        worker = _load_worker_module()
        path = worker.norm(str(ctx.get("path") or "").strip())
        action = "delete" if ctx.get("action") == "delete" else "create"
        if not path or path == "/":
            return {"success": False, "error": "컨테이너 기준 절대 경로를 입력하세요."}
        exists = os.path.exists(path)
        if action == "create" and not exists:
            note = " (아직 마운트에 보이지 않아 보일 때까지 기다립니다)"
        elif action == "delete" and exists:
            note = " (아직 마운트에 남아 있어 사라질 때까지 기다립니다)"
        else:
            note = ""
        is_dir = os.path.isdir(path) if exists else (not os.path.splitext(path)[1])
        libs = [lib for lib in self._libraries() for r in lib["roots"] if worker.under(path, r.rstrip("/"))]
        db = _db()
        with db:
            db.execute("INSERT INTO event(root, action, item_type, path, removed_path, created, ready_at) VALUES(?,?,?,?,?,?,?)",
                       ("수동 요청", action, "directory" if is_dir else "file", path, path if action == "delete" else "",
                        time.strftime("%Y-%m-%dT%H:%M:%S"), 0))
        db.close()
        open(_path("wake.flag"), "w").close()
        where = "" if libs else " 이 경로에 걸친 보관함이 없어 스캔은 건너뛰게 됩니다."
        return {"success": True, "message": ("추가" if action == "create" else "삭제") + f"로 반영을 요청했습니다{note}.{where}"}

    def _rpc_reset_root(self, ctx):
        name = str(ctx.get("name") or "")
        if not name:
            return {"success": False, "error": "루트 이름이 없습니다."}
        db = _db()
        with db:
            for table in ("cursor", "item", "outside", "receipt", "seedstate"):
                db.execute(f"DELETE FROM {table} WHERE root=?", (name,))
        db.close()
        open(_path("wake.flag"), "w").close()
        return {"success": True, "message": f"[{name}] 체크포인트를 초기화했습니다. 다음 수집 때 처음부터 다시 시작합니다."}

    def _rpc_log(self, ctx):
        lines = min(1000, max(20, int(ctx.get("lines") or 200)))
        text = ""
        for name in ("worker.log", "worker.out"):
            try:
                with open(_path(name), "rb") as handle:
                    handle.seek(0, 2)
                    size = handle.tell()
                    handle.seek(max(0, size - 256 * 1024))
                    chunk = handle.read().decode("utf-8", "replace").splitlines()[-lines:]
                    if chunk:
                        text += f"── {name} ──\n" + "\n".join(chunk) + "\n"
            except OSError:
                continue
        return {"success": True, "text": text or "(로그 없음)"}

    def _drop_legacy_rules(self):
        """v1.1.x의 'RC로 규칙 추가'가 만든 규칙(보관함 루트=마운트 루트로 가정, 하위 경로 없음)은
        실제 마운트 구조와 달라 잘못된 경로로 refresh하므로 제거한다. 이제 같은 일을 자동 감지가 한다."""
        stored = _read_json("watch.json")
        if not stored or not stored.get("vfs"):
            return 0
        auto = {(root.rstrip("/"), lib.get("rclone_rc_url", "").rstrip("/"))
                for lib in self._libraries() for root in lib["roots"] if lib.get("rclone_rc_url")}
        keep = [r for r in stored["vfs"]
                if r.get("remote") or (r.get("local", "").rstrip("/"), r.get("rc", "").rstrip("/")) not in auto]
        removed = len(stored["vfs"]) - len(keep)
        if removed:
            stored["vfs"] = keep
            _write_json("watch.json", stored)
        return removed

    def _vfs_engine(self):
        worker = _load_worker_module()
        runtime = self._sync_runtime()
        store = worker.Store(_path("state.db"))
        target = worker.BookOasis(runtime, runtime.get("libraries"), runtime.get("vfs"), store)
        return worker, store, target

    def _rpc_vfs_map(self, ctx):
        db = _db()
        saved = {row["root"]: dict(row) for row in db.execute("SELECT * FROM vfs_map")}
        db.close()
        rows = []
        for lib in self._libraries():
            for root in lib["roots"]:
                rc = lib.get("rclone_rc_url") or ""
                m = saved.get(root.rstrip("/"))
                rows.append({"db_type": lib["db_type"], "id": lib["id"], "name": lib["name"], "root": root, "rc": rc,
                             "fs": m["fs"] if m and m["rc"] == rc else "", "remote": m["remote"] if m and m["rc"] == rc else None,
                             "detected": m["detected"] if m and m["rc"] == rc else ""})
        return {"success": True, "items": rows}

    def _rpc_detect_vfs(self, ctx):
        worker, store, target = self._vfs_engine()
        wanted = ctx.get("root")
        done, failed = 0, []
        for root, rc in target.vfs.auto:
            if wanted and root != worker.norm(wanted).rstrip("/"):
                continue
            try:
                target.vfs.auto_rule(root, rc, redetect=True)
                done += 1
            except Exception as error:
                failed.append(f"{root}: {error}")
        store.db.close()
        if failed and not done:
            return {"success": False, "error": failed[0] + (f" 외 {len(failed) - 1}건" if len(failed) > 1 else "")}
        return {"success": True, "message": f"{done}개 경로를 감지했습니다." + (f" 실패 {len(failed)}건: {failed[0]}" if failed else ""),
                "failed": failed}

    def _rpc_get_watch(self, ctx):
        removed = self._drop_legacy_rules()
        settings = self._settings()
        watch = self._watch()
        watch.pop("env", None)
        defaults = list(_load_worker_module().DEFAULT_IGNORE_PATTERNS)
        if watch.get("ignore_patterns") is None:
            watch["ignore_patterns"] = defaults
        return {"success": True, "watch": watch, "libraries": self._libraries(force=True), "legacy_removed": removed,
                "settings": {"rclone_path": settings["rclone_path"], "rclone_config": settings["rclone_config"],
                             "bookoasis_url": settings["bookoasis_url"], "auto_start": settings["auto_start"],
                             "token_set": bool(settings["webhook_token"]),
                             "env_token": bool(os.environ.get("WEBHOOK_TOKEN"))},
                "default_ignore": defaults}

    def _rpc_save_env(self, ctx):
        env = ctx.get("env") or {}
        clean = {
            "RCLONE_PATH": str(env.get("rclone_path") or "rclone").strip(),
            "RCLONE_CONFIG": str(env.get("rclone_config") or "").strip(),
            "BOOKOASIS_URL": str(env.get("bookoasis_url") or "http://127.0.0.1:5930").strip().rstrip("/"),
            "AUTO_START": bool(env.get("auto_start", True)),
        }
        if clean["RCLONE_CONFIG"] and not os.path.isfile(clean["RCLONE_CONFIG"]):
            return {"success": False, "error": f"rclone.conf 파일을 찾을 수 없습니다: {clean['RCLONE_CONFIG']}"}
        watch = _read_json("watch.json", {}) or dict(DEFAULT_WATCH)
        stored = watch.get("env") or {}
        if "webhook_token" in env:  # 빈 값으로 보내면 .env 값 사용으로 되돌림
            clean["WEBHOOK_TOKEN"] = str(env.get("webhook_token") or "").strip()
        elif "WEBHOOK_TOKEN" in stored:
            clean["WEBHOOK_TOKEN"] = stored["WEBHOOK_TOKEN"]
        watch["env"] = clean
        _write_json("watch.json", watch)
        # 환경설정 화면의 플러그인 설정과도 맞춰 둔다 (지원되는 경우)
        try:
            gateway = self.get_db_gateway("general")
            current = dict(gateway.get_plugin_config(self.id) or {})
            current.update({k: v for k, v in clean.items() if k != "WEBHOOK_TOKEN" or v})
            gateway.set_plugin_config(self.id, current)
        except Exception:
            pass
        self._sync_runtime()
        return {"success": True, "message": "실행 환경을 저장했습니다. 실행 중인 워커에 자동 반영됩니다."}

    def _rpc_save_watch(self, ctx):
        watch = ctx.get("watch") or {}
        names = set()
        roots = []
        for index, root in enumerate(watch.get("roots") or [], 1):
            name = str(root.get("name") or "").strip()
            if not name or name in names:
                return {"success": False, "error": f"{index}번 루트: 이름이 비었거나 중복입니다."}
            names.add(name)
            local_root = str(root.get("local_root") or "").strip().rstrip("/")
            if not local_root.startswith("/"):
                return {"success": False, "error": f"[{name}] 로컬 경로는 절대 경로여야 합니다."}
            if root.get("mode") == "local":
                roots.append({"name": name, "mode": "local", "local_root": local_root,
                              "local_detect": root.get("local_detect") if root.get("local_detect") in ("inotify", "polling") else "auto",
                              "local_interval": max(30, int(root.get("local_interval") or 300)),
                              "enabled": bool(root.get("enabled", True))})
                continue
            if not str(root.get("root_id") or "").strip() or not str(root.get("source_remote") or "").strip():
                return {"success": False, "error": f"[{name}] 리모트와 폴더 ID를 입력하세요."}
            roots.append({"name": name, "mode": root.get("mode") if root.get("mode") in ("activity", "drivepoll") else "changes",
                          "drive_interval": max(120, int(root.get("drive_interval") or 600)),
                          "source_remote": str(root["source_remote"]).strip().rstrip(":"),
                          "root_id": str(root["root_id"]).strip(), "local_root": local_root,
                          "seed": bool(root.get("seed", True)), "enabled": bool(root.get("enabled", True)),
                          "activity_delay": int(root.get("activity_delay") or 60)})
        for i, left in enumerate(roots):
            for right in roots[i + 1:]:
                l, r = left["local_root"], right["local_root"]
                if (left["mode"] == "local" or right["mode"] == "local") and (l == r or l.startswith(r + "/") or r.startswith(l + "/")):
                    return {"success": False, "error": f"[{left['name']}]와 [{right['name']}]의 로컬 경로가 겹칩니다. "
                                                       "같은 폴더를 두 방식으로 감시하면 스캔이 중복됩니다."}
        import re
        raw = watch.get("ignore_patterns")
        patterns = [l.strip() for l in (raw.splitlines() if isinstance(raw, str) else raw or []) if l.strip()]
        for line in patterns:
            if line.startswith("#"):
                continue
            try:
                re.compile(line)
            except re.error as error:
                return {"success": False, "error": f"무시 패턴 오류: {line} ({error})"}
        webhook = str(watch.get("discord_webhook") or "").strip()
        if webhook and not webhook.startswith("https://discord.com/api/webhooks/") and not webhook.startswith("https://discordapp.com/api/webhooks/"):
            return {"success": False, "error": "디스코드 웹훅 주소는 https://discord.com/api/webhooks/… 형식이어야 합니다."}
        vfs = []
        for index, rule in enumerate(watch.get("vfs") or [], 1):
            local, rc = str(rule.get("local") or "").strip(), str(rule.get("rc") or "").strip()
            if not local and not rc:
                continue
            if not local.startswith("/") or not rc:
                return {"success": False, "error": f"VFS {index}번 규칙: 마운트 경로(절대 경로)와 RC 주소를 입력하세요."}
            vfs.append({"local": local, "rc": rc, "remote": str(rule.get("remote") or "").strip().strip("/"),
                        "fs": str(rule.get("fs") or "").strip(), "user": str(rule.get("user") or "").strip(),
                        "pass": str(rule.get("pass") or "")})
        data = {
            "roots": roots, "vfs": vfs,
            "poll_seconds": max(15, int(watch.get("poll_seconds") or 60)),
            "buffer_seconds": max(0, int(watch.get("buffer_seconds") or 0)),
            "max_attempts": max(1, int(watch.get("max_attempts") or 5)),
            "keep_days": max(1, int(watch.get("keep_days") or 30)),
            "extensions": str(watch.get("extensions") or "").strip(),
            "verbose_log": bool(watch.get("verbose_log")),
            "file_wait_minutes": max(0, int(watch.get("file_wait_minutes", 10) or 0)),
            "full_scan_guard_minutes": max(0, int(watch.get("full_scan_guard_minutes", 10) or 0)),
            "ignore_patterns": patterns,
            "discord_webhook": webhook,
            "notify_done": bool(watch.get("notify_done", True)),
            "notify_failed": bool(watch.get("notify_failed", True)),
            "notify_error": bool(watch.get("notify_error", True)),
        }
        # 사용자가 방식을 다시 저장하면 자동 전환 기록은 지운다 (Activity로 다시 시도)
        fallback = _read_json("fallback.json", {}) or {}
        old_modes = {r.get("name"): r.get("mode") for r in (_read_json("watch.json", {}) or {}).get("roots") or []}
        for r in roots:
            if r["name"] in fallback and old_modes.get(r["name"]) != r["mode"]:
                fallback.pop(r["name"], None)
        _write_json("fallback.json", fallback)
        data["env"] = (_read_json("watch.json", {}) or {}).get("env") or {}
        _write_json("watch.json", data)
        self._sync_runtime(force_libraries=True)
        return {"success": True, "message": "저장했습니다. 실행 중인 워커에 자동 반영됩니다."}

    def _rpc_remotes(self, ctx):
        settings = self._settings()
        if ctx.get("env"):  # 저장 전 값으로 확인
            env = ctx["env"]
            settings = dict(settings, rclone_path=str(env.get("rclone_path") or "rclone").strip(),
                            rclone_config=str(env.get("rclone_config") or "").strip())
        base = self._rclone_cmd(settings)

        def run(*args):
            return subprocess.run(base + list(args), capture_output=True, text=True, timeout=30)

        try:
            version = run("version")
        except FileNotFoundError:
            return {"success": False, "error": f"rclone 실행 파일을 찾을 수 없습니다: {settings['rclone_path']}"}
        config_file = run("config", "file")
        path_line = [l for l in (config_file.stdout or "").splitlines() if l.strip()][-1:] or [""]
        dump = run("config", "dump")
        if dump.returncode != 0:
            return {"success": False, "error": dump.stderr.strip()[-400:] or "rclone config dump 실패"}
        remotes = []
        for name, conf in sorted((json.loads(dump.stdout or "{}") or {}).items()):
            if not isinstance(conf, dict):
                continue
            item = {"name": name, "type": conf.get("type", "")}
            if item["type"] == "drive":
                try:
                    tok = json.loads(conf.get("token") or "{}")
                except ValueError:
                    tok = {}
                item["expiry"] = tok.get("expiry", "")
                item["scope_conf"] = conf.get("scope", "") or "drive (기본)"
                item["auth_mode"] = _load_worker_module().Rclone.auth_mode(conf, tok)
                item["granted"] = self._granted_scopes(tok)
                item["custom_auth"] = any(k for k in conf if "endpoint" in k.lower())
                item["team_drive"] = bool(conf.get("team_drive"))
            elif item["type"] == "union":
                item["upstreams"] = conf.get("upstreams", "")
            remotes.append(item)
        conf_path = path_line[0].strip()
        modified = ""
        try:
            modified = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(conf_path)))
        except OSError:
            pass
        return {"success": True, "remotes": remotes, "version": (version.stdout or "").splitlines()[:1],
                "config_file": conf_path, "using_default": not settings["rclone_config"],
                "modified": modified, "file_mount": _file_bind_mounted(conf_path)}

    def _probe_api(self, root):
        """감시 폴더 설정으로 Drive API와 변경 목록 기준(드라이브/계정 전체)을 준비한다."""
        worker = _load_worker_module()
        settings = self._settings()
        rclone = worker.Rclone(settings["rclone_path"], settings["rclone_config"], 60)
        rclone.rc_sources = worker.rc_sources(self._watch().get("vfs"), self._libraries())
        remote = str(root.get("source_remote") or "").strip().rstrip(":")
        api = worker.DriveApi(rclone, remote, 30)
        data = api.file(str(root.get("root_id") or "").strip()) or {}
        if data.get("mimeType") == worker.SHORTCUT_MIME:
            data = api.file((data.get("shortcutDetails") or {}).get("targetId", "")) or {}
        if not data:
            raise RuntimeError("감시 폴더를 찾을 수 없습니다.")
        fallback = (_read_json("fallback.json", {}) or {}).get(root.get("name"))
        drive_id = "" if fallback == "userfeed" else (data.get("driveId") or "")
        return worker, api, data, drive_id

    def _rpc_feed_probe_start(self, ctx):
        """변경 목록 시험 1단계: 지금 시점의 체크포인트를 받아 둔다."""
        root = ctx.get("root") or {}
        try:
            worker, api, data, drive_id = self._probe_api(root)
            params = {"driveId": drive_id} if drive_id else {}
            token = api.get("changes/startPageToken", **params)["startPageToken"]
        except Exception as error:
            return {"success": False, "error": str(error)}
        probes = _read_json("probe.json", {}) or {}
        probes[root.get("name") or "_"] = {"token": token, "drive": drive_id, "ts": time.time()}
        _write_json("probe.json", probes)
        where = f"공유 드라이브 {drive_id}" if drive_id else "계정 전체"
        return {"success": True, "message": f"시험 시작 ({where} 변경 목록). 이제 감시 폴더 안에 파일을 하나 올리고, "
                                            "1~2분 뒤 [결과 확인]을 누르세요."}

    def _rpc_feed_probe_check(self, ctx):
        """변경 목록 시험 2단계: 시작 이후 변경 목록에 무엇이 들어왔는지 그대로 보여 준다 (감시 상태는 건드리지 않음)."""
        root = ctx.get("root") or {}
        probe = (_read_json("probe.json", {}) or {}).get(root.get("name") or "_")
        if not probe:
            return {"success": False, "error": "먼저 [변경 목록 시험]을 눌러 시작하세요."}
        try:
            worker, api, data, drive_id = self._probe_api(root)
            params = {"driveId": probe["drive"]} if probe["drive"] else {}
            token, changes = probe["token"], []
            while token and len(changes) < 2000:
                page = api.get("changes", pageToken=token, pageSize=1000, includeRemoved="true",
                               includeItemsFromAllDrives="true", **params,
                               fields="nextPageToken,newStartPageToken,changes(fileId,removed,file(id,name,mimeType,parents,trashed,driveId))")
                changes += page.get("changes") or []
                token = page.get("nextPageToken")
        except Exception as error:
            return {"success": False, "error": str(error)}
        root_id = data.get("id")
        cache = {}

        def inside(file_data, depth=0):
            parents = file_data.get("parents") or []
            if not parents or depth > 30:
                return False
            if root_id in parents:
                return True
            pid = parents[0]
            if pid not in cache:
                cache[pid] = api.file(pid) or {}
            return inside(cache[pid], depth + 1) if cache[pid] else False

        rows = []
        for change in changes[-200:]:
            f = change.get("file") or {}
            try:
                hit = inside(f) if f else None
            except Exception:
                hit = None
            rows.append({"name": f.get("name") or change.get("fileId"), "removed": bool(change.get("removed") or f.get("trashed")),
                         "inside": hit, "drive": f.get("driveId") or ""})
        found = [r for r in rows if r["inside"]]
        minutes = int((time.time() - probe["ts"]) / 60)
        if found:
            verdict = f"변경 목록에 감시 폴더 안 항목이 {len(found)}건 들어왔습니다. 변경 목록은 정상이고, 반영이 안 된다면 그 뒤 단계(확장자·무시 패턴·보관함) 문제입니다."
        elif rows:
            verdict = (f"{minutes}분 동안 변경 {len(changes)}건이 들어왔지만 감시 폴더 안 항목은 없습니다. "
                       "올린 파일이 이 목록에 없다면 이 계정의 변경 목록으로는 그 폴더의 변경을 받을 수 없습니다.")
        else:
            verdict = (f"{minutes}분 동안 변경 목록이 비어 있습니다. 파일을 올렸는데도 비어 있다면 "
                       "이 계정의 변경 목록으로는 그 폴더의 변경을 받을 수 없습니다.")
        return {"success": True, "verdict": verdict, "total": len(changes), "rows": rows[-30:],
                "feed": probe["drive"] or "계정 전체"}

    def _rpc_check_root(self, ctx):
        """감시 폴더 설정 점검 (저장 전 값으로도 가능): 토큰 → Drive 폴더 → 드라이브 일치 → 로컬 경로 → 보관함/VFS."""
        root = ctx.get("root") or {}
        worker = _load_worker_module()
        settings = self._settings()
        checks = []

        def add(label, ok, msg):
            checks.append({"label": label, "ok": ok, "msg": msg})

        remote = str(root.get("source_remote") or "").strip().rstrip(":")
        folder_id = str(root.get("root_id") or "").strip()
        local_root = worker.norm(str(root.get("local_root") or "").strip())
        if root.get("mode") == "local":
            return self._check_local(worker, settings, local_root, root, add, checks)
        mode = "activity" if root.get("mode") == "activity" else "changes"
        if not remote or not folder_id or not local_root:
            return {"success": True, "checks": [{"label": "입력", "ok": False, "msg": "리모트, 폴더 ID, 로컬 경로(절대 경로)를 모두 입력하세요."}]}

        rclone = worker.Rclone(settings["rclone_path"], settings["rclone_config"], 60)
        rclone.rc_sources = worker.rc_sources(self._watch().get("vfs"), self._libraries())
        api = worker.DriveApi(rclone, remote, 30)
        # 1) 토큰
        try:
            token = rclone.token(remote)
            where = f"공유 드라이브 {token['team_drive']}" if token["team_drive"] else "내 드라이브"
            how = {"memory": "플러그인이 메모리에서 갱신 · rclone.conf에 쓰지 않음", "custom": "커스텀 인증 · 공유 rclone이 갱신한 값을 읽음",
                   "rclone": "rclone이 갱신해 rclone.conf에 저장"}.get(token.get("mode"), "")
            add("rclone 인증", True, f"{remote}: 토큰 정상 (만료 {token['expiry'].astimezone().strftime('%m-%d %H:%M')}, {where}) · {how}")
        except Exception as error:
            add("rclone 인증", False, str(error))
            return {"success": True, "checks": checks}
        # 2) Drive 폴더
        try:
            data = api.file(folder_id)
        except Exception as error:
            add("Drive 폴더", False, str(error))
            return {"success": True, "checks": checks}
        if data is None:
            add("Drive 폴더", False, f"폴더 ID {folder_id}를 찾을 수 없거나 {remote} 계정에 권한이 없습니다.")
            return {"success": True, "checks": checks}
        real_id = folder_id
        if data.get("mimeType") == worker.SHORTCUT_MIME:
            detail = data.get("shortcutDetails") or {}
            if detail.get("targetMimeType") != worker.FOLDER_MIME:
                add("Drive 폴더", False, "폴더가 아닌 파일을 가리키는 바로가기입니다.")
                return {"success": True, "checks": checks}
            real_id = detail["targetId"]
            target = api.file(real_id)
            if target is None:
                add("Drive 폴더", False, f"바로가기 '{data.get('name')}'의 대상 폴더에 접근할 수 없습니다.")
                return {"success": True, "checks": checks}
            add("Drive 폴더", True, f"바로가기 '{data.get('name')}' → 실제 폴더 '{target.get('name')}'를 감시합니다.")
            data = target
        elif data.get("mimeType") != worker.FOLDER_MIME:
            add("Drive 폴더", False, f"'{data.get('name')}'는 폴더가 아닙니다.")
            return {"success": True, "checks": checks}
        else:
            add("Drive 폴더", True, f"'{data.get('name')}' 폴더 확인")
        # 3) 변경을 받을 수 있는지
        drive_id = data.get("driveId") or ""
        if root.get("mode") == "drivepoll":
            try:
                q = f"'{real_id}' in parents and trashed = false"
                listed = api.get("files", q=q, pageSize=10, corpora="allDrives", includeItemsFromAllDrives="true",
                                 fields="files(id)").get("files") or []
                add("변경 수신", True, f"폴더 비교 사용 가능 · {int(root.get('drive_interval') or 600)}초마다 폴더 트리를 훑어 비교합니다 "
                                     f"(바로 아래 항목 {len(listed)}개 이상 확인).")
            except Exception as error:
                add("변경 수신", False, f"폴더 내용을 읽을 수 없습니다: {error}")
        elif mode == "changes":
            try:
                params = {"driveId": drive_id} if drive_id else {}
                api.get("changes/startPageToken", **params)
                note = ""
                if token["team_drive"] and drive_id and drive_id != token["team_drive"]:
                    note = f" (리모트 기본 드라이브 {token['team_drive']}와 달라도 같은 토큰으로 조회 가능)"
                add("변경 수신", True, "Changes 사용 가능 · " + (f"공유 드라이브 {drive_id}의 변경만 받음{note}" if drive_id
                                                           else "내 드라이브: 계정 전체 변경 중 이 폴더 것만 골라냄"))
            except Exception as error:
                if "membership" in str(error).lower():
                    try:
                        api.get("changes/startPageToken")
                        add("변경 수신", None, "이 계정은 공유 드라이브 멤버가 아니라 폴더만 공유받았습니다. 계정 전체 변경 목록으로 "
                                            "감시합니다(자동 전환). 공유받은 폴더의 변경이 이 목록에 오는지는 실제 변경으로 확인해야 합니다.")
                    except Exception as inner:
                        add("변경 수신", False, f"변경 목록을 받을 수 없습니다: {inner}")
                else:
                    add("변경 수신", False, f"이 드라이브의 변경 목록을 받을 수 없습니다: {error}")
        else:
            try:
                now = worker.stamp(worker.utcnow())
                api.call(worker.ACTIVITY_API, {"ancestorName": f"items/{real_id}", "pageSize": 1, "filter": f'time >= "{now}"'})
                add("변경 수신", True, "Activity 사용 가능")
            except Exception as error:
                add("변경 수신", False, f"Activity 조회 실패 (rclone scope에 drive.activity.readonly가 필요): {error}")
        # 4) 로컬 경로가 이 Drive 폴더와 같은 곳인지
        if not os.path.isdir(local_root):
            add("로컬 경로", False, f"컨테이너 안에 {local_root} 폴더가 없습니다.")
        else:
            try:
                q = f"'{real_id}' in parents and trashed = false"
                params = {"q": q, "pageSize": 30, "fields": "files(name)", "includeItemsFromAllDrives": "true"}
                params.update({"corpora": "drive", "driveId": drive_id} if drive_id else {"corpora": "allDrives"})
                names = [f["name"].replace("/", "／") for f in api.get("files", **params).get("files") or []]
                local = set(os.listdir(local_root))
                if not names:
                    add("로컬 경로", None, f"Drive 폴더가 비어 있어 비교할 수 없습니다. 로컬 항목 {len(local)}개")
                else:
                    hit = sum(1 for n in names if n in local)
                    ok = hit >= max(1, int(len(names) * 0.8))
                    add("로컬 경로", ok if hit else False,
                        f"Drive 폴더의 항목 {len(names)}개 중 {hit}개가 {local_root}에 보입니다."
                        + ("" if ok else " 로컬 경로가 이 Drive 폴더가 아니거나, 마운트 캐시가 오래됐을 수 있습니다."))
            except Exception as error:
                add("로컬 경로", None, f"폴더는 있지만 내용 비교 실패: {error}")
        # 5) 보관함 / VFS
        libs = [(lib, r.rstrip("/")) for lib in self._libraries() for r in lib["roots"]
                if worker.under(r.rstrip("/"), local_root) or worker.under(local_root, r.rstrip("/"))]
        if not libs:
            add("BookOasis 보관함", False, "이 경로에 걸친 보관함이 없어 변경을 감지해도 스캔하지 않습니다.")
        else:
            no_rc = [r for lib, r in libs if not lib.get("rclone_rc_url")]
            add("BookOasis 보관함", True, f"보관함 경로 {len(libs)}개가 이 폴더에 걸쳐 있습니다."
                + (f" 그중 {len(no_rc)}개는 RC 주소가 없어 VFS 새로고침을 하지 않습니다." if no_rc else ""))
        token, source = self._token_source(settings)
        ok, msg = _verify_token(settings["bookoasis_url"], token)
        add("스캔 요청", ok, f"{msg} · 사용 중인 토큰: {source}")
        return {"success": True, "checks": checks}

    def _check_local(self, worker, settings, local_root, root, add, checks):
        if not local_root or local_root == "/":
            add("입력", False, "감시할 폴더의 절대 경로를 입력하세요.")
            return {"success": True, "checks": checks}
        if not os.path.isdir(local_root):
            add("폴더", False, f"컨테이너 안에 {local_root} 폴더가 없습니다.")
            return {"success": True, "checks": checks}
        try:
            count = sum(1 for _ in os.scandir(local_root))
            add("폴더", True, f"{local_root} 확인 (바로 아래 항목 {count}개)")
        except OSError as error:
            add("폴더", False, f"폴더를 읽을 수 없습니다: {error}")
            return {"success": True, "checks": checks}
        fstype = worker.filesystem_type(local_root)
        local = fstype in worker.LOCAL_FS
        detect = root.get("local_detect") or "auto"
        if detect == "inotify" and not local:
            add("감지 방식", False, f"파일시스템이 {fstype}라서 실시간 감지가 동작하지 않습니다. 다른 기기·rclone이 바꾼 내용은 알림이 오지 않으니 '주기 비교'를 쓰세요.")
        elif detect == "polling" or (detect == "auto" and not local):
            note = " rclone 마운트라면 마운트의 디렉터리 캐시가 갱신돼야 변경이 보입니다(--dir-cache-time / --poll-interval)." if fstype.startswith("fuse") else ""
            add("감지 방식", True, f"파일시스템 {fstype} → {int(root.get('local_interval') or 300)}초마다 전체 목록 비교.{note}")
        else:
            try:
                with open("/proc/sys/fs/inotify/max_user_watches") as handle:
                    limit = int(handle.read().strip())
                add("감지 방식", True, f"파일시스템 {fstype} → 실시간 감지 (폴더 하나당 감시 1개, 한도 {limit:,}개. 넘으면 주기 비교로 자동 전환)")
            except OSError:
                add("감지 방식", True, f"파일시스템 {fstype} → 실시간 감지")
        libs = [(lib, r.rstrip("/")) for lib in self._libraries() for r in lib["roots"]
                if worker.under(r.rstrip("/"), local_root) or worker.under(local_root, r.rstrip("/"))]
        add("BookOasis 보관함", bool(libs), f"보관함 경로 {len(libs)}개가 이 폴더에 걸쳐 있습니다." if libs
            else "이 경로에 걸친 보관함이 없어 변경을 감지해도 스캔하지 않습니다.")
        token, source = self._token_source(settings)
        ok, msg = _verify_token(settings["bookoasis_url"], token)
        add("스캔 요청", ok, f"{msg} · 사용 중인 토큰: {source}")
        return {"success": True, "checks": checks}

    @staticmethod
    def _token_source(settings):
        if settings.get("webhook_token"):
            return settings["webhook_token"], "플러그인 설정에 입력한 값"
        if os.environ.get("WEBHOOK_TOKEN"):
            return os.environ["WEBHOOK_TOKEN"], "BookOasis .env의 WEBHOOK_TOKEN"
        return "", "없음"

    def _rpc_check_token(self, ctx):
        settings = self._settings()
        env = ctx.get("env") or {}
        if env.get("webhook_token"):
            settings = dict(settings, webhook_token=env["webhook_token"])
        if env.get("bookoasis_url"):
            settings = dict(settings, bookoasis_url=env["bookoasis_url"])
        token, source = self._token_source(settings)
        ok, msg = _verify_token(settings["bookoasis_url"], token)
        return {"success": True, "ok": ok, "message": f"{msg} · 사용 중인 토큰: {source}"}

    def _rpc_refresh_token(self, ctx):
        """rclone.conf의 리모트 토큰을 지금 갱신한다.

        실제 rclone.conf는 건드리지 않고 임시 복사본에서만 토큰 만료 시각을 과거로 바꿔 rclone을 실행한다.
        rclone이 새 토큰을 받아 오면 실제 파일에서는 그 리모트의 'token =' 한 줄만 교체한다.
        (rclone config update는 백엔드 설정 과정을 다시 거치면서 scope·team_drive 같은 값을 바꿀 수 있어 쓰지 않음)
        갱신에 실패하면 실제 파일은 그대로다."""
        import re
        import shutil
        import tempfile
        remote = str(ctx.get("remote") or "").strip().rstrip(":")
        if not remote:
            return {"success": False, "error": "리모트를 지정하세요."}
        settings = self._settings()
        conf_path = settings["rclone_config"]
        if not conf_path:
            out = subprocess.run([settings["rclone_path"], "config", "file"], capture_output=True, text=True, timeout=15).stdout
            lines = [l.strip() for l in out.splitlines() if l.strip()]
            conf_path = lines[-1] if lines else ""
        if not conf_path or not os.path.isfile(conf_path):
            return {"success": False, "error": f"rclone.conf를 찾을 수 없습니다: {conf_path or '(알 수 없음)'}"}

        section_re = re.compile(r"^\[(.+?)\]\s*$")
        token_re = re.compile(r"^(\s*token\s*=\s*)(.*?)\s*$")

        def find_token(text):
            """(줄 목록, token 줄 번호, 앞부분, 값) — 해당 리모트 섹션 안에서만 찾는다."""
            lines = text.splitlines(keepends=True)
            inside = False
            for index, line in enumerate(lines):
                head = section_re.match(line.strip())
                if head:
                    inside = head.group(1) == remote
                    continue
                if inside:
                    m = token_re.match(line.rstrip("\r\n"))
                    if m:
                        return lines, index, m.group(1), m.group(2)
            return lines, -1, "", ""

        def eol(line):
            return "\r\n" if line.endswith("\r\n") else "\n"

        with open(conf_path, encoding="utf-8") as handle:
            original = handle.read()
        lines, index, prefix, value = find_token(original)
        if index < 0:
            return {"success": False, "error": f"rclone.conf의 [{remote}] 섹션에 token이 없습니다."}
        try:
            token = json.loads(value)
        except ValueError:
            return {"success": False, "error": f"[{remote}]의 token 값을 읽을 수 없습니다."}
        if not token.get("refresh_token"):
            return {"success": False, "error": f"[{remote}]에는 refresh_token이 없어 갱신할 수 없습니다."}
        custom = [k for k in re.findall(r"^\s*([\w-]*endpoint[\w-]*)\s*=", original, re.M | re.I)]
        before = token.get("expiry", "")

        workdir = tempfile.mkdtemp(prefix="gdw-token-")
        try:
            expired = dict(token, expiry="2000-01-01T00:00:00Z")
            temp_lines = list(lines)
            temp_lines[index] = prefix + json.dumps(expired, separators=(",", ":")) + eol(lines[index])
            temp_conf = os.path.join(workdir, "rclone.conf")
            with open(temp_conf, "w", encoding="utf-8") as handle:
                handle.write("".join(temp_lines))
            about = subprocess.run([settings["rclone_path"], "--config", temp_conf, "about", f"{remote}:", "--json"],
                                   capture_output=True, text=True, timeout=90)
            with open(temp_conf, encoding="utf-8") as handle:
                _, t_index, _, t_value = find_token(handle.read())
            try:
                fresh = json.loads(t_value) if t_index >= 0 else {}
            except ValueError:
                fresh = {}
            after = fresh.get("expiry", "")
            if about.returncode != 0 or not after or after.startswith("2000-"):
                reason = about.stderr.strip()[-300:] or "rclone이 새 토큰을 저장하지 않았습니다."
                hint = (f" 이 리모트는 커스텀 인증({', '.join(custom)})이라 지금 rclone으로는 갱신할 수 없습니다."
                        if custom else "")
                return {"success": False, "error": f"갱신 실패 (rclone.conf는 바꾸지 않았습니다).{hint} {reason}"}
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

        # 실제 파일: 그 사이 바뀌었을 수 있으니 다시 읽고, 해당 리모트의 token 한 줄만 교체해 제자리에 쓴다
        with open(conf_path, encoding="utf-8") as handle:
            latest = handle.read()
        lines, index, prefix, _ = find_token(latest)
        if index < 0:
            return {"success": False, "error": "갱신 중 rclone.conf가 바뀌어 반영하지 못했습니다. 다시 시도하세요."}
        lines[index] = prefix + t_value + eol(lines[index])
        with open(conf_path, "r+", encoding="utf-8") as handle:  # 제자리 쓰기: 파일 단위 마운트에서도 동작
            handle.write("".join(lines))
            handle.truncate()
        return {"success": True, "expiry": after,
                "message": f"{remote} 토큰을 갱신했습니다. 새 만료 {after[:19].replace('T', ' ')} (이전 {before[:19].replace('T', ' ')}). "
                           "다른 설정(scope 등)은 그대로입니다."}

    @staticmethod
    def _granted_scopes(token):
        """토큰이 실제로 가진 권한을 Google tokeninfo로 확인 (rclone.conf의 scope 값과 다를 수 있음)."""
        from urllib.error import HTTPError, URLError
        from urllib.parse import quote
        from urllib.request import urlopen
        access = token.get("access_token")
        if not access:
            return "토큰 없음"
        try:
            with urlopen(f"https://oauth2.googleapis.com/tokeninfo?access_token={quote(access)}", timeout=5) as response:
                scopes = (json.loads(response.read() or b"{}").get("scope") or "").split()
            return ", ".join(sc.rsplit("/", 1)[-1] for sc in scopes) or "알 수 없음"
        except HTTPError:
            return "확인 불가 (토큰 만료 — 갱신 후 다시 확인)"
        except (URLError, OSError, ValueError):
            return "확인 불가 (네트워크)"

    def _rpc_test_token(self, ctx):
        """메모리 갱신 리모트: 실제로 새 토큰을 받아 보되 rclone.conf에는 쓰지 않는다."""
        remote = str(ctx.get("remote") or "").strip().rstrip(":")
        worker = _load_worker_module()
        settings = self._settings()
        rclone = worker.Rclone(settings["rclone_path"], settings["rclone_config"], 60)
        rclone.rc_sources = worker.rc_sources(self._watch().get("vfs"), self._libraries())
        try:
            conf, token, _ = rclone._read(remote)
            if rclone.auth_mode(conf, token) != "memory":
                return {"success": False, "error": "본인 client_id가 있는 리모트만 메모리 갱신을 시험할 수 있습니다."}
            _, expiry = rclone._memory_refresh(remote, conf, token)
        except Exception as error:
            return {"success": False, "error": str(error)}
        return {"success": True, "message": f"{remote}: 메모리에서 새 토큰을 받았습니다 (만료 {expiry.astimezone().strftime('%H:%M')}). "
                                            "rclone.conf는 바꾸지 않았습니다."}

    def _rpc_test_rc_token(self, ctx):
        """커스텀 인증 리모트: 마운트 중인 rclone RC에서 토큰을 읽어 올 수 있는지 확인 (파일에는 쓰지 않음)."""
        remote = str(ctx.get("remote") or "").strip().rstrip(":")
        worker = _load_worker_module()
        settings = self._settings()
        rclone = worker.Rclone(settings["rclone_path"], settings["rclone_config"], 60)
        rclone.rc_sources = worker.rc_sources(self._watch().get("vfs"), self._libraries())
        borrowed, notes = rclone._borrow_from_rc(remote)
        if borrowed:
            return {"success": True, "message": f"{remote}: 마운트 중인 rclone에서 유효한 토큰을 읽었습니다 "
                                                f"(만료 {borrowed[1].astimezone().strftime('%H:%M')}). 자동 갱신이 동작합니다."}
        header, note = rclone._token_from_headers(remote)
        if header:
            return {"success": True, "message": f"{remote}: rclone({settings['rclone_path']})이 쓰는 토큰을 가져왔습니다. "
                                                "자동 갱신이 동작합니다 (45분마다 다시 가져옴)."}
        return {"success": False, "error": f"{remote} 토큰을 가져오지 못했습니다: " + " / ".join(notes + [note])}

    def _rpc_test_discord(self, ctx):
        worker = _load_worker_module()
        url = str(ctx.get("url") or self._watch().get("discord_webhook") or "").strip()
        if not url:
            return {"success": False, "error": "웹훅 주소를 입력하세요."}
        notifier = worker.Notifier({"discord_webhook": url})
        status = notifier.post([{"title": "드라이브 변경 감시 · 알림 시험", "color": 3447003,
                                 "description": "이 채널로 처리 결과(반영됨·실패)와 감시 오류를 알립니다."}])
        if status in (200, 204):
            return {"success": True, "message": "디스코드로 시험 알림을 보냈습니다."}
        return {"success": False, "error": f"전송 실패 (HTTP {status})" if status else "전송 실패 (연결 오류)"}

    # ── 스캔 일정 (구 scan_scheduler) ──
    SCHEDULE_SCOPES = (("general", "일반 (general)"), ("adult", "성인 (adult)"),
                       ("audiobook", "오디오북 (audiobook)"), ("video", "비디오 (video)"))

    @staticmethod
    def _runs_often(cron):
        """하루 2번 이상 도는 일정인지 (분·시 필드에 여러 값/범위/간격이 있으면)."""
        fields = str(cron or "").split()
        return len(fields) >= 5 and any(ch in fields[0] + fields[1] for ch in ",-/*")

    def _rpc_schedules(self, ctx):
        worker = _load_worker_module()
        roots = [r for r in self._watch().get("roots") or [] if r.get("enabled", True)]
        items, errors = [], []
        for scope, label in self.SCHEDULE_SCOPES:
            try:
                gateway = self.get_db_gateway(scope)
                rows = None
                for columns in ("id, name, physical_path, cron_schedule, last_scanned_at, scan_status, is_remote, "
                                "vfs_refresh_before_scan, rclone_rc_url", "id, name, physical_path, cron_schedule"):
                    try:
                        rows = gateway.fetch_all(f"SELECT {columns} FROM libraries ORDER BY name") or []
                        break
                    except Exception as error:
                        last = error
                if rows is None:
                    raise last
            except Exception as error:
                errors.append(f"{scope}: {error}")
                continue
            for row in rows:
                row = dict(row)
                paths = [worker.norm(p.strip()).rstrip("/") for p in str(row.get("physical_path") or "").splitlines() if p.strip()]
                watch_roots = sorted({r["name"] for r in roots for p in paths if p and r.get("local_root") and
                                      (worker.under(p, r["local_root"].rstrip("/")) or worker.under(r["local_root"].rstrip("/"), p))})
                cron = row.get("cron_schedule") or ""
                items.append({
                    "scope": scope, "scope_label": label, "id": row.get("id"), "name": row.get("name") or "(이름 없음)",
                    "cron_schedule": cron, "last_scanned_at": row.get("last_scanned_at"),
                    "scan_status": row.get("scan_status") or "", "is_remote": bool(row.get("is_remote")),
                    "vfs_refresh_before_scan": bool(row.get("vfs_refresh_before_scan")),
                    "rclone_rc_url": row.get("rclone_rc_url") or "",
                    "watched": bool(watch_roots), "watch_roots": watch_roots,
                    "frequent": bool(watch_roots) and self._runs_often(cron),
                })
        if errors and not items:
            return {"success": False, "error": "; ".join(errors)}
        return {"success": True, "items": items, "errors": errors}

    def _rpc_update_cron(self, ctx):
        scope = str(ctx.get("scope") or "").strip()
        if scope not in {s for s, _ in self.SCHEDULE_SCOPES}:
            return {"success": False, "error": f"유효하지 않은 세션입니다: {scope}"}
        try:
            library_id = int(ctx.get("id"))
        except (TypeError, ValueError):
            return {"success": False, "error": "유효하지 않은 보관함 ID입니다."}
        cron = str(ctx.get("cron_schedule") or "").strip()
        if len(cron.split()) < 5:
            return {"success": False, "error": f"유효하지 않은 cron 표현식입니다: {cron}"}
        try:
            self.get_db_gateway(scope).execute("UPDATE libraries SET cron_schedule = %s WHERE id = %s", (cron, library_id))
        except Exception as error:
            return {"success": False, "error": f"저장 중 오류가 발생했습니다: {error}"}
        self._sync_runtime(force_libraries=True)  # 워커의 전체 스캔 회피 시간대에도 바로 반영
        return {"success": True, "message": f"스케줄이 저장되었습니다 ({cron})"}

    def _rpc_preview(self, ctx):
        path = str(ctx.get("path") or "").strip()
        runtime = self._sync_runtime()
        worker = _load_worker_module()
        path = worker.norm(path)
        if not path:
            return {"success": False, "error": "절대 경로를 입력하세요."}
        roots = [r["name"] for r in runtime.get("roots") or [] if worker.under(path, r["local_root"])]
        store = worker.Store(_path("state.db"))
        target = worker.BookOasis(runtime, runtime.get("libraries"), runtime.get("vfs"), store)
        lib = target.library_for(path)
        directory = worker.posixpath.dirname(path)
        found, err = target.vfs.resolve(directory)
        store.db.close()
        rules = [{"rc": r.rc, "fs": r.fs, "dir": r.to_remote(directory)} for r in found]
        if err:
            rules = [{"rc": "감지 실패", "fs": "", "dir": err}]
        return {"success": True, "roots": roots, "vfs": rules,
                "library": {"db_type": lib["db_type"], "id": lib["id"], "name": lib["name"], "root": lib["root"],
                            "path": directory[len(lib["root"]):].strip("/")} if lib else None}

    # 대시보드/홈 위젯은 제공하지 않음
    def get_dashboard_data(self, db_type, limit=10):
        return {"success": True, "items": []}
