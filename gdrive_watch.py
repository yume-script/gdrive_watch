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
                try:
                    rows = gateway.fetch_all("SELECT id, name, physical_path, rclone_rc_url FROM libraries") or []
                except Exception:
                    rows = gateway.fetch_all("SELECT id, name, physical_path FROM libraries") or []
            except Exception:
                continue
            for row in rows:
                row = dict(row)
                roots = [line.strip() for line in str(row.get("physical_path") or "").splitlines() if line.strip()]
                if roots:
                    items.append({"db_type": db_type, "id": int(row["id"]), "name": row.get("name") or "",
                                  "roots": roots, "rclone_rc_url": row.get("rclone_rc_url") or ""})
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
                "local_root": root.get("local_root"), "status": cur["status"] if cur else "",
                "error": cur["error"] if cur else "", "updated": cur["updated"] if cur else "",
                "items": items, "last_event": last["created"] if last else "",
                "stat": dict(stat) if stat else None,
            })
        token_fail = db.execute("SELECT COUNT(*) FROM event WHERE status IN ('failed','pending') AND "
                                "(message LIKE '%토큰 불일치%' OR message LIKE '%Invalid webhook token%')").fetchone()[0]
        db.close()
        warnings = []
        if token_fail:
            warnings.append(f"스캔 요청 {token_fail}건이 WEBHOOK_TOKEN 불일치로 실패했습니다. [감시 설정 > 실행 환경]에서 "
                            "[토큰 확인]을 눌러 보고, 맞춘 뒤 [실패 전부 재시도]를 누르세요.")
        if not runtime.get("roots"):
            warnings.append("감시 루트가 없습니다. [감시 설정] 탭에서 추가하세요.")
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
                "libraries": len(runtime.get("libraries") or []), "auto_start": runtime.get("auto_start")}

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
        if ctx.get("status"):
            where.append("status=?")
            params.append(ctx["status"])
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
            item.pop("ready_at", None)
            items.append(item)
        return {"success": True, "items": items, "total": total, "page": page, "size": size}

    def _rpc_retry(self, ctx):
        db = _db()
        with db:
            if ctx.get("all_failed"):
                n = db.execute("UPDATE event SET status='pending', attempts=0, ready_at=0 WHERE status='failed'").rowcount
            else:
                ids = [int(i) for i in ctx.get("ids") or []]
                marks = ",".join("?" * len(ids)) or "NULL"
                n = db.execute(f"UPDATE event SET status='pending', attempts=0, ready_at=0 WHERE id IN ({marks})",
                               ids).rowcount
        db.close()
        open(_path("wake.flag"), "w").close()
        return {"success": True, "message": f"{n}건을 재시도 대기열에 넣었습니다."}

    def _rpc_delete(self, ctx):
        db = _db()
        with db:
            if ctx.get("clear") == "done":
                n = db.execute("DELETE FROM event WHERE status IN ('done','skipped')").rowcount
            else:
                ids = [int(i) for i in ctx.get("ids") or []]
                marks = ",".join("?" * len(ids)) or "NULL"
                n = db.execute(f"DELETE FROM event WHERE id IN ({marks})", ids).rowcount
        db.close()
        return {"success": True, "message": f"{n}건을 삭제했습니다."}

    def _rpc_reset_root(self, ctx):
        name = str(ctx.get("name") or "")
        if not name:
            return {"success": False, "error": "루트 이름이 없습니다."}
        db = _db()
        with db:
            for table in ("cursor", "item", "outside", "receipt"):
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
        return {"success": True, "watch": watch, "libraries": self._libraries(force=True), "legacy_removed": removed,
                "settings": {"rclone_path": settings["rclone_path"], "rclone_config": settings["rclone_config"],
                             "bookoasis_url": settings["bookoasis_url"], "auto_start": settings["auto_start"],
                             "token_set": bool(settings["webhook_token"]),
                             "env_token": bool(os.environ.get("WEBHOOK_TOKEN"))}}

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
            roots.append({"name": name, "mode": root.get("mode") if root.get("mode") == "activity" else "changes",
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
        }
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
                    item["expiry"] = json.loads(conf.get("token") or "{}").get("expiry", "")
                except ValueError:
                    item["expiry"] = ""
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
        api = worker.DriveApi(rclone, remote, 30)
        # 1) 토큰
        try:
            token = rclone.token(remote)
            where = f"공유 드라이브 {token['team_drive']}" if token["team_drive"] else "내 드라이브"
            add("rclone 인증", True, f"{remote}: 토큰 정상 (만료 {token['expiry'].astimezone().strftime('%m-%d %H:%M')}, {where})")
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
        if mode == "changes":
            if token["team_drive"] and drive_id != token["team_drive"]:
                add("변경 수신", False, "리모트가 다른 공유 드라이브 전용이라 이 폴더의 변경을 받을 수 없습니다. 폴더가 있는 드라이브의 리모트를 쓰세요.")
            else:
                try:
                    params = {"driveId": token["team_drive"]} if token["team_drive"] else {}
                    api.get("changes/startPageToken", **params)
                    add("변경 수신", True, "Changes 사용 가능" + ("" if token["team_drive"] else " (내 드라이브 리모트: 드라이브 전체 변경 중 이 폴더 것만 골라냄)"))
                except Exception as error:
                    add("변경 수신", False, str(error))
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
