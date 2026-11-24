"""Guarda el estado del bot en un archivo JSON local mediante un hilo de E/S."""

import json
import os
import tempfile
import threading
import time
from collections import deque
from typing import Callable, Deque, List, Optional, Tuple


HEARTBEAT_S = float(os.getenv("STATE_HEARTBEAT_S", "40"))
CLOSE_RETRY_S = 12.0
DEBOUNCE_S = 0.3
BACKOFF_MAX_S = 30.0

Restore = Tuple[Optional[dict], List[dict], str]


class StateStore:
    """Recupera el archivo antes de operar y reemplaza cada versión en forma atómica."""

    def __init__(self, local_path: str, owner_id: str,
                 build_doc: Callable[[], Tuple[dict, List[dict]]], log: Callable[[str], None],
                 max_trades: int = 5000) -> None:
        self.local_path = local_path
        self.owner_id = owner_id
        self.build_doc = build_doc
        self.log = log
        self.boot_source = ""
        self.writes_enabled = False
        self.stopping = False
        self.last_write = 0.0
        self.last_error = ""
        self.writes = 0
        self.failures = 0
        self.last_doc: Optional[dict] = None

        self._lock = threading.Lock()
        self._io_lock = threading.Lock()
        self._wake = threading.Event()
        self._crit_ver = 0
        self._soft_ver = 0
        self._done_crit = 0
        self._done_soft = 0
        self._crit_since = 0.0
        self._trades: Deque[dict] = deque()
        self._recent_trades: Deque[dict] = deque(maxlen=max(1, max_trades))
        self._restore: Optional[Restore] = None
        self._restore_taken = False
        self._last_body: Optional[str] = None
        self._last_write_mono = 0.0
        self._next_try = 0.0
        self._backoff = 1.0
        self._stop = False
        self._thread: Optional[threading.Thread] = None

    def can_act(self) -> bool:
        return self.writes_enabled and not self.stopping

    def can_open(self) -> bool:
        return self.can_act()

    def status(self) -> dict:
        if self.stopping:
            mode = "apagando"
        else:
            mode = "local" if self.writes_enabled else "recuperando"
        age = round(time.time() - self.last_write, 1) if self.last_write else None
        with self._lock:
            pending = self._crit_ver != self._done_crit or bool(self._trades)
        return {
            "backend": "local",
            "mode": mode,
            "owner": self.owner_id,
            "holder": "",
            "restored_from": self.boot_source,
            "last_ok_ago_s": age,
            "last_write_ago_s": age,
            "writes": self.writes,
            "failures": self.failures,
            "pending": pending,
            "last_error": self.last_error,
        }

    def mark_dirty(self, critical: bool = True) -> None:
        with self._lock:
            if critical:
                if self._crit_ver == self._done_crit:
                    self._crit_since = time.monotonic()
                self._crit_ver += 1
            else:
                self._soft_ver += 1
        if critical:
            self._wake.set()

    def append_trade(self, rec: dict) -> None:
        """Incluye el cierre en el guardado que retira su posición abierta."""
        with self._lock:
            self._trades.append(rec)
        self.mark_dirty(critical=True)

    def peek_trades(self) -> List[dict]:
        with self._lock:
            return list(self._trades)

    def take_restore(self) -> Optional[Restore]:
        with self._lock:
            restore, self._restore = self._restore, None
            if restore is not None:
                self._restore_taken = True
            return restore

    def enable_writes(self) -> bool:
        with self._lock:
            if not self._restore_taken or self.stopping:
                return False
            self.writes_enabled = True
        self.mark_dirty(critical=True)
        return True

    def start(self) -> None:
        if self._thread is not None:
            return
        doc = self.read_local()
        rows = doc.get("recent_trades") if doc else None
        if isinstance(rows, list):
            self._recent_trades.extend(r for r in rows if isinstance(r, dict))
        self.boot_source = "copia local" if doc else "vacío (sin estado previo)"
        with self._lock:
            self._restore = (doc, list(self._recent_trades), self.boot_source)
        self._thread = threading.Thread(target=self._run, name="state-store", daemon=True)
        self._thread.start()

    def begin_stop(self) -> None:
        self.stopping = True

    def close(self) -> bool:
        self.begin_stop()
        deadline = time.monotonic() + CLOSE_RETRY_S
        with self._io_lock:
            try:
                while not self._stop and self.writes_enabled:
                    try:
                        return self._flush_locked(force=True)
                    except Exception as exc:
                        self._failed(exc)
                        if time.monotonic() + 1.0 >= deadline:
                            return False
                        time.sleep(1.0)
            finally:
                self._stop = True
                self._wake.set()
        return False

    def read_local(self) -> Optional[dict]:
        try:
            with open(self.local_path, encoding="utf-8") as fh:
                doc = json.load(fh)
            return doc if isinstance(doc, dict) else None
        except FileNotFoundError:
            return None
        except Exception as exc:
            self.log(f"[estado] copia local ilegible ({exc})")
            return None

    def _run(self) -> None:
        while not self._stop:
            wait = self._next_try - time.monotonic()
            self._wake.wait(timeout=min(1.0, wait) if wait > 0 else 1.0)
            self._wake.clear()
            if self._stop or time.monotonic() < self._next_try:
                continue
            with self._io_lock:
                try:
                    self._flush_locked()
                except Exception as exc:
                    self._failed(exc)

    def _failed(self, exc: Exception) -> None:
        self.failures += 1
        self.last_error = str(exc)
        self._next_try = time.monotonic() + self._backoff
        self.log(f"[estado] No se guardó la copia local ({exc}). Reintento en {self._backoff:.0f}s")
        self._backoff = min(self._backoff * 2, BACKOFF_MAX_S)

    def _flush_locked(self, force: bool = False) -> bool:
        if self._stop or not self.writes_enabled:
            return False
        now = time.monotonic()
        with self._lock:
            crit_ver, soft_ver = self._crit_ver, self._soft_ver
            crit = crit_ver != self._done_crit
            soft = soft_ver != self._done_soft
            crit_due = self._crit_since + DEBOUNCE_S
        if crit and not force and now < crit_due:
            self._next_try = crit_due
            return False
        if not (force or crit or (soft and now - self._last_write_mono >= HEARTBEAT_S)):
            return True

        doc, trades = self.build_doc()
        recent = self._recent_trades.copy()
        recent.extend(trades)
        doc = {**doc, "recent_trades": list(recent)}
        body = json.dumps(doc, ensure_ascii=False, separators=(",", ":"), default=str, sort_keys=True)
        if body != self._last_body:
            saved_at = time.time()
            doc.update(saved_at=saved_at, owner=self.owner_id)
            payload = json.dumps(doc, ensure_ascii=False, separators=(",", ":"), default=str)
            self._write_local(payload)
            self._recent_trades = recent
            self.last_doc = doc
            self.last_write = saved_at
            self._last_write_mono = now
            self._last_body = body
            self.writes += 1
        with self._lock:
            self._done_crit, self._done_soft = crit_ver, soft_ver
            for _ in trades:
                self._trades.popleft()
            if self._crit_ver != self._done_crit:
                self._crit_since = time.monotonic()
        self.last_error = ""
        self._backoff = 1.0
        self._next_try = 0.0
        return True

    def _write_local(self, payload: str) -> None:
        directory = os.path.dirname(os.path.abspath(self.local_path))
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=directory, delete=False) as fh:
            temp_path = fh.name
            try:
                fh.write(payload)
                fh.close()
                os.replace(temp_path, self.local_path)
            finally:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)


def make_owner_id() -> str:
    inst = os.getenv("RENDER_INSTANCE_ID") or os.uname().nodename
    return f"{inst}-{os.getpid()}-{int(time.time())}".replace("|", "-")
