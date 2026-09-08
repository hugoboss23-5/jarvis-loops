#!/usr/bin/env python3
"""
jarvis loops v0 — three connected loops on one Mac, sharing one memory (Marcos).

    HEARTBEAT  taps the OS at the source once a second and appends events
    TRIAGE     asks the fast model (gpt-5.3-codex-spark via `codex exec`) what a batch
               of events is: noise, a read-only reflex, or worth the big model
    LEARNING   reads decisions + what happened after, and moves the triage threshold

One process, three threads.  State is JSONL files in ./state/ only.

    python3 loops.py            run forever (launchd runs this)
    python3 loops.py --status   last 10 events, last 10 decisions, current threshold
    python3 loops.py --beats N  run N beats in the foreground, then exit (verification)

Reflex actions are limited by construction to reading files, running whitelisted
read-only shell commands, writing inside ./state/, and writing memory lines.  Nothing
here sends, deletes, buys, or changes a setting, and nothing ever asks Hugo a question.

Memory: every non-noise decision and every threshold change is written as a dated
line to ~/brain/jarvis-loops.md (Marcos ingests ~/brain) AND sent to the Marcos MCP
`remember` tool over HTTP when that endpoint answers.  ./state/memory.jsonl records
which path worked for each line.
"""
import argparse
import ctypes
import ctypes.util
import hashlib
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
STATE = ROOT / "state"
HOME = Path.home()

EVENTS_F = STATE / "events.jsonl"
DECISIONS_F = STATE / "decisions.jsonl"
THRESHOLD_F = STATE / "threshold.json"
LEARNING_F = STATE / "learning.jsonl"
REFLEXES_F = STATE / "reflexes.jsonl"
ESCALATIONS_F = STATE / "escalations.jsonl"
MEMORY_F = STATE / "memory.jsonl"
CURSOR_F = STATE / "cursor.json"
PID_F = STATE / "loops.pid"
LOG_F = STATE / "loops.log"
BRAIN_F = HOME / "brain" / "jarvis-loops.md"
SCHEMA_F = ROOT / "triage_schema.json"
MARCOS_URL_F = ROOT / "marcos.url"
CODEX_HISTORY = HOME / ".codex" / "history.jsonl"
WATCH_DIRS = [HOME / "projects", HOME / "brain"]

CFG = {
    # heartbeat
    "beat": 1.0,                 # seconds per beat (<= 1s required)
    "window_every": 1,           # poll the window title every N beats (Accessibility API, ~0.1 ms)
    "chatter_hits": 3,           # a path changed this many times within chatter_window ...
    "chatter_window": 60.0,      # ... seconds is a machine rewriting a status file: prefiltered as noise
    "chatter_quiet": 300.0,      # until it has been quiet this long
    "events_cap": 6000,          # ring buffer: trim when events.jsonl exceeds this many lines
    "events_keep": 4000,         # ... down to this many
    # triage (fast model)
    "fast_model": "gpt-5.3-codex-spark",
    "triage_gap": 45.0,          # seconds between fast-model calls while events keep coming
    "triage_batch_max": 30,      # events per fast-model call
    "triage_per_hour": 60,       # hard cap on fast-model calls per hour (plan quota, not money)
    "triage_timeout": 90,
    # escalation (big model)
    "big_model": "claude-fable-5-1",
    "escalate_gap": 900.0,       # at most one escalation per 15 minutes
    "escalate_per_day": 12,
    "escalate_timeout": 420,
    "escalate_budget_usd": 4.0,  # --max-budget-usd per escalation
    "escalate_max_turns": 6,
    # learning
    "learn_every_beats": 60,
    "threshold_init": 0.60,
    "threshold_min": 0.30,
    "threshold_max": 0.95,
    "step_noop": +0.05,          # an escalation that produced nothing: be pickier
    "step_action": -0.03,        # an escalation that produced an action: be a little looser
    "step_starved": -0.02,       # many held escalations and none tested for hours: loosen
    "starved_after": 5,
    "starved_hours": 6,
}

# --- redaction (same patterns kirkland's tailer uses) -----------------------------------
SECRETS = [
    (re.compile(r"\bsk-ant-[A-Za-z0-9_\-]{16,}"), "anthropic-key"),
    (re.compile(r"\bsk-[A-Za-z0-9_\-]{16,}"), "openai-style-key"),
    (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), "aws-access-key-id"),
    (re.compile(r"\bghp_[A-Za-z0-9]{20,}"), "github-token"),
    (re.compile(r"(?i)\b(api[_-]?key|secret|password|token)\b\s*[=:]\s*['\"]?([A-Za-z0-9_\-]{12,})['\"]?"),
     "labelled-secret"),
    (re.compile(r"\b[A-Za-z0-9_\-]{40,}\b"), "long-opaque-string"),
]


def redact(s, limit=300):
    if not isinstance(s, str):
        return s
    for pat, label in SECRETS:
        s = pat.sub("[REDACTED:%s]" % label, s)
    s = s.replace("\n", " ").replace("\r", " ")
    return s if len(s) <= limit else s[:limit] + "…[%d chars]" % len(s)


def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def local_date():
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def log(msg):
    line = "%s %s\n" % (now_iso(), msg)
    try:
        with open(LOG_F, "a") as f:
            f.write(line)
        if LOG_F.stat().st_size > 1_000_000:
            lines = LOG_F.read_text(errors="replace").splitlines()[-2000:]
            _atomic_write(LOG_F, "\n".join(lines) + "\n")
    except Exception:
        pass


def _atomic_write(path, text):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def read_jsonl(path, last=None):
    if not path.exists():
        return []
    out = []
    try:
        lines = path.read_text(errors="replace").splitlines()
        if last:
            lines = lines[-last:]
        for ln in lines:
            ln = ln.strip()
            if not ln:
                continue
            try:
                out.append(json.loads(ln))
            except Exception:
                continue
    except Exception:
        pass
    return out


class Jsonl:
    """Append-only JSONL file with an optional ring cap."""

    def __init__(self, path, cap=None, keep=None):
        self.path, self.cap, self.keep = path, cap, keep
        self.lock = threading.Lock()
        self.n = 0
        if path.exists():
            try:
                with open(path, "rb") as f:
                    self.n = sum(1 for _ in f)
            except Exception:
                self.n = 0

    def append(self, obj):
        line = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
        with self.lock:
            with open(self.path, "a") as f:
                f.write(line + "\n")
            self.n += 1
            if self.cap and self.n > self.cap:
                lines = self.path.read_text(errors="replace").splitlines()[-self.keep:]
                _atomic_write(self.path, "\n".join(lines) + "\n")
                self.n = len(lines)


# --- shared state ----------------------------------------------------------------------
class State:
    def __init__(self):
        STATE.mkdir(exist_ok=True)
        self.stop = threading.Event()
        self.events = Jsonl(EVENTS_F, CFG["events_cap"], CFG["events_keep"])
        self.decisions = Jsonl(DECISIONS_F, 6000, 4000)
        self.reflexes = Jsonl(REFLEXES_F, 2000, 1500)
        self.escalations = Jsonl(ESCALATIONS_F)
        self.learning = Jsonl(LEARNING_F)
        self.memory_log = Jsonl(MEMORY_F, 4000, 3000)
        self.recent = deque(maxlen=3000)     # in-memory copy of recent events for triage
        self.seq = 0                         # continue numbering across restarts
        for e in read_jsonl(EVENTS_F, last=1):
            self.seq = int(e.get("id", 0))
        self.beats = 0
        self.last_beat = 0.0
        self.lock = threading.Lock()
        self.threshold = load_threshold()
        self.tap_errors = {}                 # tap -> last time we logged an error
        self.fswatch_pid = None
        self.last_reflexes = deque(maxlen=3)
        self.last_decisions = deque(maxlen=5)
        self.escalation_times = deque(maxlen=64)
        self.triage_times = deque(maxlen=256)

    def emit(self, kind, **fields):
        with self.lock:
            self.seq += 1
            ev = {"id": self.seq, "ts": now_iso(), "t": round(time.time(), 3), "kind": kind}
            ev.update(fields)
            self.recent.append(ev)
        self.events.append(ev)
        return ev

    def events_since(self, seq):
        with self.lock:
            return [e for e in self.recent if e["id"] > seq]

    def tap_error(self, tap, err):
        last = self.tap_errors.get(tap, 0)
        if time.time() - last > 3600:       # once an hour per tap, so a dead tap stays visible
            self.tap_errors[tap] = time.time()
            self.emit("tap_error", tap=tap, error=redact(str(err), 400))
            log("tap_error %s: %s" % (tap, redact(str(err), 400)))


def load_threshold():
    if THRESHOLD_F.exists():
        try:
            return json.loads(THRESHOLD_F.read_text())
        except Exception:
            pass
    t = {"threshold": CFG["threshold_init"], "updated": now_iso(),
         "reason": "initial value", "adjustments": 0}
    _atomic_write(THRESHOLD_F, json.dumps(t, indent=2))
    return t


# --- memory (brain file + Marcos MCP) ---------------------------------------------------
class Memory:
    HEADER = (
        "---\n"
        "title: Jarvis loops — decision and threshold log\n"
        "type: log\n"
        "status: live\n"
        "confidence: measured\n"
        "updated: %s\n"
        "sources: [~/projects/jarvis-loops/state/decisions.jsonl, ~/projects/jarvis-loops/state/learning.jsonl]\n"
        "---\n"
        "# jarvis-loops\n\n"
        "One dated line per non-noise triage decision and per threshold change, appended by the\n"
        "daemon at `~/projects/jarvis-loops/loops.py`. The daemon writes it; nobody hand-edits it.\n"
        "Same lines are also sent to Marcos `remember` (source jarvis, session loops) when reachable.\n\n"
    )

    def __init__(self, st):
        self.st = st
        self.url = None
        if MARCOS_URL_F.exists():
            self.url = MARCOS_URL_F.read_text().strip() or None
        self.sid = None
        self.q = deque()
        self.cv = threading.Condition()
        self.last = None
        self.marcos_ok = None                # None = untested, True/False = last result
        self.thread = threading.Thread(target=self._worker, name="memory", daemon=True)
        self.thread.start()

    def write(self, text):
        text = redact(text, 600)
        line = "- %s %s" % (local_date(), text)
        brain_ok, err = True, None
        try:
            BRAIN_F.parent.mkdir(parents=True, exist_ok=True)
            if not BRAIN_F.exists():
                BRAIN_F.write_text(self.HEADER % datetime.now().strftime("%Y-%m-%d"))
            with open(BRAIN_F, "a") as f:
                f.write(line + "\n")
        except Exception as e:
            brain_ok, err = False, str(e)
        with self.cv:
            self.q.append((line, brain_ok, err))
            self.cv.notify()

    # -- Marcos streamable-HTTP MCP client, plain urllib ----------------------------------
    def _post(self, body, timeout=15):
        h = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        if self.sid:
            h["Mcp-Session-Id"] = self.sid
        req = urllib.request.Request(self.url, data=json.dumps(body).encode(), headers=h, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            sid = r.headers.get("mcp-session-id")
            raw = r.read().decode("utf-8", "replace")
        msgs = []
        for ln in raw.splitlines():
            if ln.startswith("data:"):
                try:
                    msgs.append(json.loads(ln[5:].strip()))
                except Exception:
                    pass
        if not msgs and raw.strip().startswith("{"):
            try:
                msgs.append(json.loads(raw))
            except Exception:
                pass
        return sid, msgs

    def _init(self):
        sid, msgs = self._post({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                                "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                                           "clientInfo": {"name": "jarvis-loops", "version": "0"}}})
        if not sid:
            raise RuntimeError("no mcp-session-id in initialize response")
        self.sid = sid
        try:
            self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})
        except Exception:
            pass

    def remember(self, text):
        if not self.url:
            return False, "no marcos.url"
        for attempt in (1, 2):
            try:
                if not self.sid:
                    self._init()
                _, msgs = self._post({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                                      "params": {"name": "remember",
                                                 "arguments": {"text": text, "source": "jarvis",
                                                               "session": "loops"}}})
                for m in msgs:
                    if "result" in m:
                        if m["result"].get("isError"):
                            return False, json.dumps(m["result"])[:300]
                        return True, json.dumps(m["result"].get("content", ""))[:200]
                    if "error" in m:
                        return False, json.dumps(m["error"])[:300]
                return False, "no result in response"
            except urllib.error.HTTPError as e:
                self.sid = None                      # session expired → re-initialize once
                if attempt == 2:
                    return False, "HTTP %s" % e.code
            except Exception as e:
                self.sid = None
                if attempt == 2:
                    return False, redact(str(e), 300)
        return False, "unreachable"

    def _worker(self):
        while True:
            with self.cv:
                while not self.q:
                    self.cv.wait()
                line, brain_ok, brain_err = self.q.popleft()
            t0 = time.time()
            ok, info = self.remember(line)
            self.marcos_ok = ok
            rec = {"ts": now_iso(), "text": line, "brain_file": brain_ok, "marcos": ok,
                   "marcos_info": info, "latency_s": round(time.time() - t0, 2)}
            if brain_err:
                rec["brain_error"] = brain_err
            self.last = rec
            self.st.memory_log.append(rec)


# --- heartbeat taps ---------------------------------------------------------------------
def run(cmd, timeout=3, input_text=None, cwd=None, env=None):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                       input=input_text, cwd=cwd, env=env)
    return p.returncode, p.stdout, p.stderr


def front_app(st):
    """(display name, pid) of the frontmost app via LaunchServices. ~20 ms, no permissions."""
    try:
        rc, out, err = run(["lsappinfo", "front"], 2)
        asn = out.strip()
        if not asn:
            return None, None
        rc, out, err = run(["lsappinfo", "info", "-only", "name", "-only", "pid", asn], 2)
        m = re.search(r'"LSDisplayName"="(.*)"', out)
        p = re.search(r'"pid"=(\d+)', out)
        return (m.group(1) if m else None), (int(p.group(1)) if p else None)
    except Exception as e:
        st.tap_error("front_app", e)
        return None, None


class AX:
    """Focused-window title through the Accessibility API, in-process (no System Events, no
    osascript — System Events was found wedged for every caller on this Mac, 2026-09-07).
    Needs Accessibility trust for the *responsible* process; when untrusted the call fails
    silently (no prompt) and the daemon records one tap_error per hour."""
    UTF8 = 0x08000100
    ERRORS = {-25211: "accessibility API disabled for this process (not trusted)",
              -25204: "cannot complete", -25212: "no value (no focused window)",
              -25205: "attribute unsupported", -25202: "invalid element"}

    def __init__(self):
        self.ok = False
        try:
            self.cf = ctypes.cdll.LoadLibrary(ctypes.util.find_library("CoreFoundation"))
            self.ax = ctypes.cdll.LoadLibrary(ctypes.util.find_library("ApplicationServices"))
            cf, ax = self.cf, self.ax
            cf.CFStringCreateWithCString.restype = ctypes.c_void_p
            cf.CFStringCreateWithCString.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32]
            cf.CFStringGetCString.restype = ctypes.c_bool
            cf.CFStringGetCString.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_long, ctypes.c_uint32]
            cf.CFRelease.argtypes = [ctypes.c_void_p]
            cf.CFGetTypeID.restype = ctypes.c_ulong
            cf.CFGetTypeID.argtypes = [ctypes.c_void_p]
            cf.CFStringGetTypeID.restype = ctypes.c_ulong
            ax.AXUIElementCreateApplication.restype = ctypes.c_void_p
            ax.AXUIElementCreateApplication.argtypes = [ctypes.c_int]
            ax.AXUIElementCopyAttributeValue.restype = ctypes.c_int
            ax.AXUIElementCopyAttributeValue.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
                                                         ctypes.POINTER(ctypes.c_void_p)]
            ax.AXIsProcessTrusted.restype = ctypes.c_bool
            self.k_focused = cf.CFStringCreateWithCString(None, b"AXFocusedWindow", self.UTF8)
            self.k_title = cf.CFStringCreateWithCString(None, b"AXTitle", self.UTF8)
            self.trusted = bool(ax.AXIsProcessTrusted())
            self.ok = True
        except Exception as e:
            self.error = str(e)

    def title(self, pid):
        if not self.ok or not pid:
            return None, getattr(self, "error", "AX not loaded")
        cf, ax = self.cf, self.ax
        app = ax.AXUIElementCreateApplication(pid)
        if not app:
            return None, "no AX element for pid %s" % pid
        win = ctypes.c_void_p()
        err = ax.AXUIElementCopyAttributeValue(app, self.k_focused, ctypes.byref(win))
        if err != 0 or not win:
            cf.CFRelease(app)
            return None, "AXFocusedWindow: %s" % self.ERRORS.get(err, "err %d" % err)
        t = ctypes.c_void_p()
        err = ax.AXUIElementCopyAttributeValue(win, self.k_title, ctypes.byref(t))
        out = None
        if err == 0 and t and cf.CFGetTypeID(t) == cf.CFStringGetTypeID():
            buf = ctypes.create_string_buffer(2048)
            if cf.CFStringGetCString(t, buf, 2048, self.UTF8):
                out = buf.value.decode("utf-8", "replace")
        for o in (t, win, app):
            if o:
                cf.CFRelease(o)
        return out, (None if out is not None else "AXTitle: %s" % self.ERRORS.get(err, "err %d" % err))


_AX = None


def front_window(st, pid):
    global _AX
    if _AX is None:
        _AX = AX()
        log("AX loaded ok=%s trusted=%s" % (_AX.ok, getattr(_AX, "trusted", None)))
    try:
        title, err = _AX.title(pid)
        if title is None and err and "no focused window" not in err:
            st.tap_error("front_window", err)
        return title
    except Exception as e:
        st.tap_error("front_window", e)
        return None


def clipboard_sig(st):
    try:
        p = subprocess.run(["pbpaste"], capture_output=True, timeout=3)
        data = p.stdout
        if not data:
            return None
        sha = hashlib.sha256(data).hexdigest()[:12]
        text = data.decode("utf-8", "replace")
        return {"sha": sha, "len": len(data), "preview": redact(text[:160], 160)}
    except Exception as e:
        st.tap_error("clipboard", e)
        return None


class CodexHistory:
    """Tail ~/.codex/history.jsonl from the current end; never replays old prompts."""

    def __init__(self):
        self.offset = None
        try:
            cur = json.loads(CURSOR_F.read_text()) if CURSOR_F.exists() else {}
            self.offset = cur.get("codex_history_offset")
        except Exception:
            self.offset = None
        size = CODEX_HISTORY.stat().st_size if CODEX_HISTORY.exists() else 0
        if self.offset is None or self.offset > size:
            self.offset = size

    def poll(self):
        out = []
        try:
            if not CODEX_HISTORY.exists():
                return out
            size = CODEX_HISTORY.stat().st_size
            if size < self.offset:
                self.offset = 0
            if size == self.offset:
                return out
            with open(CODEX_HISTORY, "rb") as f:
                f.seek(self.offset)
                chunk = f.read(size - self.offset)
            # only consume complete lines
            cut = chunk.rfind(b"\n")
            if cut < 0:
                return out
            self.offset += cut + 1
            for ln in chunk[:cut].splitlines():
                try:
                    j = json.loads(ln.decode("utf-8", "replace"))
                except Exception:
                    continue
                out.append({"session": str(j.get("session_id", ""))[:8],
                            "codex_ts": j.get("ts"),
                            "text": redact(str(j.get("text", "")), 400)})
            _atomic_write(CURSOR_F, json.dumps({"codex_history_offset": self.offset,
                                                "updated": now_iso()}))
        except Exception:
            pass
        return out


FS_FLAGS = {"Created", "Updated", "Removed", "Renamed", "OwnerModified", "AttributeModified",
            "MovedFrom", "MovedTo", "IsFile", "IsDir", "IsSymLink", "Link", "PlatformSpecific",
            "CloseWrite", "NoOp", "Overflow"}
FS_EXCLUDE = [r"/\.git/", r"/node_modules/", r"/__pycache__/", r"\.DS_Store$", r"/jarvis-loops/state/",
              # the daemon's own outputs: watching them made it examine its own memory lines in a loop
              r"/brain/jarvis-loops\.md$", r"/jarvis-loops/triage_schema\.json$",
              r"/kirkland/spool/", r"/\.wrangler/", r"/\.venv/", r"/venv/", r"\.pyc$", r"\.log$",
              r"\.tmp$", r"-journal$", r"-wal$", r"-shm$", r"\.lock$", r"\.pid$", r"/\.cache/",
              r"/\.next/", r"/dist/", r"/build/", r"\.sqlite$", r"\.db$"]
FS_EXCLUDE_RE = re.compile("|".join(FS_EXCLUDE))


class FSWatch:
    """fswatch (FSEvents) as a subprocess; null-separated `path flags` records."""

    def __init__(self, st):
        self.st = st
        self.q = deque()
        self.lock = threading.Lock()
        self.proc = None
        self.backoff = 2
        self.thread = threading.Thread(target=self._loop, name="fswatch", daemon=True)
        self.thread.start()

    def _start(self):
        cmd = ["fswatch", "-r", "-x", "-0", "--latency", "0.5"]
        for pat in FS_EXCLUDE:
            cmd += ["-e", pat]
        cmd += [str(d) for d in WATCH_DIRS if d.exists()]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.st.fswatch_pid = self.proc.pid
        log("fswatch started pid=%d" % self.proc.pid)

    def _loop(self):
        while not self.st.stop.is_set():
            try:
                self._start()
            except Exception as e:
                self.st.tap_error("fswatch", e)
                self.st.stop.wait(min(self.backoff, 60))
                self.backoff *= 2
                continue
            buf = b""
            while not self.st.stop.is_set():
                chunk = self.proc.stdout.read1(65536) if hasattr(self.proc.stdout, "read1") \
                    else self.proc.stdout.read(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\0" in buf:
                    rec, buf = buf.split(b"\0", 1)
                    self._parse(rec.decode("utf-8", "replace"))
            if self.st.stop.is_set():
                break
            err = self.proc.stderr.read().decode("utf-8", "replace")[-300:] if self.proc.stderr else ""
            self.st.tap_error("fswatch", "exited rc=%s %s" % (self.proc.poll(), err))
            self.st.stop.wait(min(self.backoff, 60))
            self.backoff = min(self.backoff * 2, 60)

    def _parse(self, rec):
        toks = rec.strip().split(" ")
        flags = []
        while toks and toks[-1] in FS_FLAGS:
            flags.append(toks.pop())
        path = " ".join(toks)
        if not path or FS_EXCLUDE_RE.search(path):
            return
        with self.lock:
            self.q.append((path, sorted(flags)))

    def drain(self):
        with self.lock:
            items, self.q = list(self.q), deque()
        merged = {}
        for path, flags in items:
            merged.setdefault(path, set()).update(flags)
        out = []
        for path, flags in merged.items():
            short = path.replace(str(HOME), "~")
            out.append({"path": short, "flags": sorted(f for f in flags if f not in ("IsFile", "IsDir")),
                        "is_dir": "IsDir" in flags})
        return out

    def kill(self):
        try:
            if self.proc and self.proc.poll() is None:
                self.proc.terminate()
        except Exception:
            pass


WINDOW_KEY_RE = re.compile(r"[\W\d_]+", re.UNICODE)


def heartbeat(st):
    fs = FSWatch(st)
    hist = CodexHistory()
    last_app = None
    last_win_key = cand_win_key = None
    last_clip = None
    beat = 0
    st.emit("daemon", msg="start", pid=os.getpid(), fast_model=CFG["fast_model"],
            big_model=CFG["big_model"], threshold=st.threshold["threshold"])
    while not st.stop.is_set():
        t0 = time.time()
        beat += 1
        app, pid = front_app(st)
        if app and app != last_app:
            st.emit("app", app=app, prev=last_app, pid=pid)
            last_app = app
        if beat % CFG["window_every"] == 0:
            win = front_window(st, pid)
            # Spinners, clocks and counters in titles (Claude Code's ◐◑, "3:21 left", 42%) flip every
            # second; compare titles with digits and symbols stripped, and require two stable polls.
            key = WINDOW_KEY_RE.sub("", win).strip().lower() if win is not None else None
            if key is not None and key != last_win_key:
                if key == cand_win_key:
                    st.emit("window", app=app, title=redact(win, 200))
                    last_win_key = key
                cand_win_key = key
        if time.time() - t0 > 2.0:
            st.tap_error("beat", "beat %d took %.1fs (taps are supposed to be sub-second)" % (beat, time.time() - t0))
        clip = clipboard_sig(st)
        if clip and (last_clip is None or clip["sha"] != last_clip):
            if last_clip is not None:
                st.emit("clipboard", **clip)
            last_clip = clip["sha"]
        for ev in fs.drain():
            st.emit("file", **ev)
        for ev in hist.poll():
            st.emit("codex", **ev)
        st.beats = beat
        st.last_beat = time.time()
        if beat % 30 == 0:
            try:
                _atomic_write(PID_F, json.dumps({"pid": os.getpid(), "beats": beat, "last_beat": now_iso(),
                                                 "fswatch_pid": st.fswatch_pid}))
            except Exception:
                pass
        st.stop.wait(max(0.0, CFG["beat"] - (time.time() - t0)))
    fs.kill()
    st.emit("daemon", msg="stop", pid=os.getpid(), beats=beat)


# --- triage (fast model) ----------------------------------------------------------------
NOISE_PATH_RE = re.compile(r"(\.tmp$|~$|/\.[^/]+$|\.swp$|\.part$|\.crdownload$|\.orig$|/tmp/)")

TRIAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "class": {"type": "string", "enum": ["noise", "reflex", "escalate"]},
        "score": {"type": "number"},
        "reason": {"type": "string"},
        "reflex_cmd": {"type": "string"},
    },
    "required": ["class", "score", "reason", "reflex_cmd"],
    "additionalProperties": False,
}

REFLEX_ALLOWED = {"ls", "cat", "head", "tail", "wc", "grep", "find", "stat", "du", "df", "ps",
                  "date", "uptime", "file", "which", "git", "mdfind", "lsappinfo", "sw_vers", "pmset"}
REFLEX_GIT_OK = {"status", "log", "diff", "show", "branch", "rev-parse", "ls-files"}
REFLEX_BAD_CHARS = re.compile(r"[;&|<>`$\n]")


def prefilter_noise(ev):
    """Obvious noise never reaches the model."""
    k = ev["kind"]
    if k in ("daemon", "tap_error"):
        return True
    if k == "file":
        p = ev.get("path", "")
        if NOISE_PATH_RE.search(p):
            return True
        if ev.get("is_dir") and ev.get("flags") == ["AttributeModified"]:
            return True
    return False


def summarize_event(ev):
    k = ev["kind"]
    t = ev["ts"][11:19]
    if k == "app":
        return "%s app -> %s (from %s)" % (t, ev.get("app"), ev.get("prev"))
    if k == "window":
        return "%s window [%s] %s" % (t, ev.get("app"), ev.get("title"))
    if k == "file":
        return "%s file %s %s" % (t, ",".join(ev.get("flags", [])), ev.get("path"))
    if k == "clipboard":
        return "%s clipboard %d bytes: %s" % (t, ev.get("len", 0), ev.get("preview"))
    if k == "codex":
        return "%s codex prompt (session %s): %s" % (t, ev.get("session"), ev.get("text"))
    return "%s %s %s" % (t, k, json.dumps({x: y for x, y in ev.items() if x not in ("id", "ts", "t", "kind")})[:200])


def fast_model_flags():
    return ["codex", "exec", "--ignore-user-config", "-m", CFG["fast_model"], "-s", "read-only",
            "--skip-git-repo-check", "--ephemeral", "--color", "never",
            "--disable", "apps", "--disable", "computer_use", "--disable", "browser_use",
            "--disable", "code_mode_host", "--disable", "skill_search", "--disable", "hooks",
            "--disable", "guardian_approval", "--disable", "skill_mcp_dependency_install",
            "-c", "features.skip_host_skill_discovery=true", "-c", 'web_search="disabled"',
            "-c", 'model_reasoning_effort="low"', "-C", str(STATE)]


def build_triage_prompt(st, batch):
    thr = st.threshold["threshold"]
    ctx = []
    for d in list(st.last_decisions)[-3:]:
        ctx.append("- %s class=%s score=%.2f action=%s: %s" % (d["ts"][11:19], d["class"], d["score"],
                                                              d["action"], d.get("reason", "")[:120]))
    rfx = []
    for r in list(st.last_reflexes):
        rfx.append("- `%s` -> %s" % (r["cmd"], r.get("output", "")[:300]))
    return (
        "You are the triage reflex of \"jarvis loops\", a background nervous system on Hugo's Mac. "
        "Hugo builds software with AI assistants; he never sees prompts from this system and must never be asked anything.\n"
        "You get a batch of OS events: active app, window title, file changes under ~/projects and ~/brain, "
        "clipboard changes, and prompts Hugo typed into Codex. Classify the BATCH as one of:\n"
        "- \"noise\": routine activity, nothing to do (most batches are noise).\n"
        "- \"reflex\": ONE read-only shell command would capture a useful fact for the record "
        "(e.g. look at the file that just changed). Allowed commands: ls, cat, head, tail, wc, grep, find, stat, du, df, "
        "git status|log|diff|show, ps, date, uptime, file, which, mdfind. Single command, no pipes, redirects or chaining; "
        "paths may use ~. Put it in reflex_cmd.\n"
        "- \"escalate\": a senior assistant should look now — Hugo asked Codex for something that looks stuck or failed, "
        "a project file shows an error or a deadline, an anomaly, or something he clearly needs. Escalation is expensive "
        "(a full Claude session); use it only when a person would want an assistant to act.\n"
        "score = how much this batch deserves attention, 0.0 (nothing) to 1.0 (urgent). Current escalation threshold: %.2f "
        "(escalations scoring below it are held, not run).\n"
        "Do NOT run commands yourself. Answer only with the JSON object.\n\n"
        "Recent decisions:\n%s\n\nRecent reflex outputs:\n%s\n\nEVENTS (%d):\n%s\n"
    ) % (thr, "\n".join(ctx) or "- none", "\n".join(rfx) or "- none", len(batch),
         "\n".join(summarize_event(e) for e in batch))


def ask_fast(st, batch):
    if not SCHEMA_F.exists():
        SCHEMA_F.write_text(json.dumps(TRIAGE_SCHEMA, indent=2))
    out_f = STATE / "triage.last.txt"
    try:
        out_f.unlink()
    except FileNotFoundError:
        pass
    prompt = build_triage_prompt(st, batch)
    cmd = fast_model_flags() + ["--output-schema", str(SCHEMA_F), "-o", str(out_f), "-"]
    t0 = time.time()
    try:
        p = subprocess.run(cmd, input=prompt, capture_output=True, text=True, timeout=CFG["triage_timeout"])
    except subprocess.TimeoutExpired:
        return None, round(time.time() - t0, 2), "timeout after %ss" % CFG["triage_timeout"]
    except Exception as e:
        return None, round(time.time() - t0, 2), str(e)
    lat = round(time.time() - t0, 2)
    raw = out_f.read_text().strip() if out_f.exists() else ""
    if not raw:
        tail = (p.stdout + "\n" + p.stderr).strip().splitlines()[-6:]
        return None, lat, "no output (rc=%s): %s" % (p.returncode, redact(" | ".join(tail), 500))
    try:
        j = json.loads(raw)
        j["score"] = max(0.0, min(1.0, float(j.get("score", 0))))
        if j.get("class") not in ("noise", "reflex", "escalate"):
            j["class"] = "noise"
        return j, lat, None
    except Exception as e:
        return None, lat, "bad json (%s): %s" % (e, redact(raw, 300))


def validate_reflex(cmd):
    if not cmd or REFLEX_BAD_CHARS.search(cmd):
        return None, "empty or contains a shell metacharacter"
    try:
        parts = shlex.split(cmd)
    except ValueError as e:
        return None, "unparsable: %s" % e
    if not parts or parts[0] not in REFLEX_ALLOWED:
        return None, "command not in read-only whitelist"
    if parts[0] == "git":
        rest, i = parts[1:], 0
        while i < len(rest) and rest[i].startswith("-"):      # skip global options: -C <dir>, --no-pager
            i += 2 if rest[i] in ("-C", "-c") else 1
        if i >= len(rest) or rest[i] not in REFLEX_GIT_OK:
            return None, "git subcommand not read-only"
    if parts[0] == "find" and any(a in ("-delete", "-exec", "-execdir", "-ok", "-okdir") for a in parts):
        return None, "find with side effects"
    parts = [os.path.expanduser(a) if a.startswith("~") else a for a in parts]
    return parts, None


def run_reflex(st, decision_id, cmd):
    parts, why = validate_reflex(cmd)
    rec = {"ts": now_iso(), "decision_id": decision_id, "cmd": cmd}
    if parts is None:
        rec.update({"ok": False, "rejected": why})
        st.reflexes.append(rec)
        return rec
    t0 = time.time()
    try:
        p = subprocess.run(parts, capture_output=True, text=True, timeout=15, cwd=str(HOME))
        out = redact((p.stdout or "") + (("\n[stderr] " + p.stderr) if p.stderr.strip() else ""), 4000)
        rec.update({"ok": p.returncode == 0, "rc": p.returncode, "output": out,
                    "latency_s": round(time.time() - t0, 2)})
    except subprocess.TimeoutExpired:
        rec.update({"ok": False, "error": "timeout 15s"})
    except Exception as e:
        rec.update({"ok": False, "error": str(e)})
    st.reflexes.append(rec)
    st.last_reflexes.append(rec)
    return rec


def escalation_allowed(st):
    """Rate limit read from disk, so a restart (or a forced escalation from the CLI) counts too."""
    now = time.time()
    times = list(st.escalation_times)
    for e in read_jsonl(ESCALATIONS_F, last=200):
        try:
            times.append(datetime.fromisoformat(e["ts"]).timestamp())
        except Exception:
            pass
    times.sort()
    if times and now - times[-1] < CFG["escalate_gap"]:
        return False, "last escalation %ds ago (< %ds)" % (now - times[-1], CFG["escalate_gap"])
    day = [t for t in times if now - t < 86400]
    if len(day) >= CFG["escalate_per_day"]:
        return False, "%d escalations in 24h (cap %d)" % (len(day), CFG["escalate_per_day"])
    return True, ""


def escalate(st, mem, decision, batch):
    """Big model, in its own thread. Read-only by instruction + tool removal; result is a note."""
    esc_id = "esc-%s" % datetime.now().strftime("%Y%m%d-%H%M%S")
    st.escalation_times.append(time.time())
    prompt = (
        "You are the escalation tier of \"jarvis loops\", a background nervous system on Hugo's Mac "
        "(project: ~/projects/jarvis-loops). The fast triage model flagged a batch of OS events as worth your attention "
        "(score %.2f, threshold %.2f). Reason given: %s\n\n"
        "Rules: you may read files and run read-only commands to understand what happened. You must NOT write, edit, "
        "send, delete, install, buy, or change any setting, and you must not ask Hugo anything. Keep it under 6 tool calls.\n"
        "Decide: is there something concrete an assistant should do for Hugo right now? If yes, verdict=\"action\" and write the "
        "note Hugo (or the next assistant) should see: what you observed, what to do, in plain English, no code. If nothing is needed, "
        "verdict=\"noop\" with a one-line summary.\n"
        "Reply with ONLY this JSON: {\"verdict\":\"action\"|\"noop\",\"summary\":\"one line\",\"note\":\"markdown or empty\"}\n\n"
        "EVENTS:\n%s\n"
    ) % (decision["score"], decision["threshold"], decision.get("reason", ""),
         "\n".join(summarize_event(e) for e in batch))
    cmd = ["claude", "-p", "--model", CFG["big_model"], "--dangerously-skip-permissions",
           "--output-format", "json", "--max-turns", str(CFG["escalate_max_turns"]),
           "--max-budget-usd", str(CFG["escalate_budget_usd"]),
           "--disallowedTools", "Write,Edit,NotebookEdit,Bash(rm *),Bash(git push*),Bash(sudo *)"]
    rec = {"id": esc_id, "ts": now_iso(), "decision_id": decision["id"], "model": CFG["big_model"],
           "score": decision["score"], "threshold": decision["threshold"], "events": [e["id"] for e in batch]}
    t0 = time.time()
    try:
        p = subprocess.run(cmd, input=prompt, capture_output=True, text=True,
                           timeout=CFG["escalate_timeout"], cwd=str(ROOT))
        rec["latency_s"] = round(time.time() - t0, 1)
        try:
            j = json.loads(p.stdout)
        except Exception:
            j = {}
        rec["cost_usd_est"] = j.get("total_cost_usd")
        rec["turns"] = j.get("num_turns")
        rec["session_id"] = j.get("session_id")
        result = j.get("result") or ""
        m = re.search(r"\{.*\}", result, re.S)
        verdict = {}
        if m:
            try:
                verdict = json.loads(m.group(0))
            except Exception:
                verdict = {}
        rec["verdict"] = verdict.get("verdict") if verdict.get("verdict") in ("action", "noop") else "unparsed"
        rec["summary"] = redact(str(verdict.get("summary") or result), 400)
        rec["note"] = redact(str(verdict.get("note") or ""), 1500)
        if p.returncode != 0 and not result:
            rec["error"] = redact((p.stderr or p.stdout)[-500:], 500)
            rec["verdict"] = "error"
    except subprocess.TimeoutExpired:
        rec.update({"latency_s": round(time.time() - t0, 1), "verdict": "error",
                    "error": "timeout after %ss" % CFG["escalate_timeout"]})
    except Exception as e:
        rec.update({"latency_s": round(time.time() - t0, 1), "verdict": "error", "error": str(e)})
    st.escalations.append(rec)
    log("escalation %s verdict=%s latency=%ss cost=%s" % (esc_id, rec.get("verdict"), rec.get("latency_s"),
                                                          rec.get("cost_usd_est")))
    mem.write("escalation %s (%s) verdict=%s cost≈$%s in %ss: %s%s" % (
        esc_id, CFG["big_model"], rec.get("verdict"), rec.get("cost_usd_est"), rec.get("latency_s"),
        rec.get("summary") or rec.get("error", ""), (" | NOTE: " + rec["note"]) if rec.get("note") else ""))


def record_decision(st, mem, **d):
    d.setdefault("ts", now_iso())
    d["id"] = "d-%d-%d" % (int(time.time()), st.decisions.n + 1)
    d["threshold"] = st.threshold["threshold"]
    st.decisions.append(d)
    if d.get("model") != "prefilter":
        st.last_decisions.append(d)
    if d["class"] not in ("noise",) and d.get("model") != "prefilter":
        mem.write("triage %s: class=%s score=%.2f threshold=%.2f action=%s (%s, %ss) — %s%s" % (
            d["id"], d["class"], d["score"], d["threshold"], d["action"], d.get("model"), d.get("latency_s"),
            d.get("reason", ""), (" | cmd: " + d["reflex_cmd"]) if d.get("reflex_cmd") else ""))
    return d


class Chatter:
    """A path rewritten every few seconds by a machine (status files, logs) is noise after the
    first few hits; it stays muted until it has been quiet for a while."""

    def __init__(self):
        self.hits = {}
        self.muted = {}

    def is_chatter(self, ev):
        if ev["kind"] != "file":
            return False
        p, now = ev.get("path"), ev["t"]
        if p in self.muted:
            if now - self.muted[p] < CFG["chatter_quiet"]:
                self.muted[p] = now
                return True
            del self.muted[p]
        h = self.hits.setdefault(p, deque(maxlen=CFG["chatter_hits"]))
        h.append(now)
        if len(h) == CFG["chatter_hits"] and now - h[0] <= CFG["chatter_window"]:
            self.muted[p] = now
            return True
        return False


def triage(st, mem):
    cursor = st.seq
    pending = []
    last_call = 0.0
    chatter = Chatter()
    while not st.stop.wait(1.0):
        new = st.events_since(cursor)
        if new:
            cursor = new[-1]["id"]
        noise = [e for e in new if prefilter_noise(e)]
        chat = [e for e in new if e not in noise and chatter.is_chatter(e)]
        if noise:
            record_decision(st, mem, events=[e["id"] for e in noise], n_events=len(noise), model="prefilter",
                            latency_s=0, **{"class": "noise"}, score=0.0, action="drop",
                            reason="obvious noise (temp/hidden files, daemon housekeeping)")
        if chat:
            record_decision(st, mem, events=[e["id"] for e in chat], n_events=len(chat), model="prefilter",
                            latency_s=0, **{"class": "noise"}, score=0.0, action="drop",
                            reason="chatter: %s rewritten >= %d times in %ds" % (
                                ", ".join(sorted({e["path"] for e in chat}))[:200], CFG["chatter_hits"],
                                CFG["chatter_window"]))
        pending += [e for e in new if e not in noise and e not in chat]
        if not pending:
            continue
        if time.time() - last_call < CFG["triage_gap"] and len(pending) < CFG["triage_batch_max"]:
            continue
        batch, pending = pending[:CFG["triage_batch_max"]], pending[CFG["triage_batch_max"]:]
        hour = [t for t in st.triage_times if time.time() - t < 3600]
        if len(hour) >= CFG["triage_per_hour"]:
            record_decision(st, mem, events=[e["id"] for e in batch], n_events=len(batch), model="cap",
                            latency_s=0, **{"class": "noise"}, score=0.0, action="dropped_rate_cap",
                            reason="fast-model hourly cap reached (%d/h)" % CFG["triage_per_hour"])
            continue
        last_call = time.time()
        st.triage_times.append(last_call)
        res, lat, err = ask_fast(st, batch)
        base = dict(events=[e["id"] for e in batch], n_events=len(batch), model=CFG["fast_model"], latency_s=lat)
        if res is None:
            record_decision(st, mem, **base, **{"class": "error"}, score=0.0, action="model_error", reason=err)
            log("triage error: %s" % err)
            continue
        cls, score, reason, cmd = res["class"], res["score"], res.get("reason", ""), res.get("reflex_cmd", "")
        thr = st.threshold["threshold"]
        if cls == "noise":
            record_decision(st, mem, **base, **{"class": cls}, score=score, action="none", reason=reason)
        elif cls == "reflex":
            d = record_decision(st, mem, **base, **{"class": cls}, score=score, action="reflex", reason=reason,
                                reflex_cmd=cmd)
            r = run_reflex(st, d["id"], cmd)
            mem.write("reflex for %s `%s` -> %s" % (d["id"], cmd, r.get("rejected") or r.get("error") or
                                                    (r.get("output", "")[:240] or "(no output)")))
        else:  # escalate
            if score >= thr:
                ok, why = escalation_allowed(st)
                if ok:
                    d = record_decision(st, mem, **base, **{"class": cls}, score=score, action="escalated",
                                        reason=reason)
                    threading.Thread(target=escalate, args=(st, mem, d, batch), name="escalate",
                                     daemon=True).start()
                else:
                    record_decision(st, mem, **base, **{"class": cls}, score=score, action="escalation_rate_limited",
                                    reason="%s | %s" % (why, reason))
            else:
                record_decision(st, mem, **base, **{"class": cls}, score=score, action="held_below_threshold",
                                reason=reason)


# --- learning ---------------------------------------------------------------------------
def set_threshold(st, mem, new, reason, evidence):
    old = st.threshold["threshold"]
    new = round(max(CFG["threshold_min"], min(CFG["threshold_max"], new)), 3)
    if new == old:
        return False
    st.threshold = {"threshold": new, "updated": now_iso(), "reason": reason,
                    "adjustments": st.threshold.get("adjustments", 0) + 1, "previous": old}
    _atomic_write(THRESHOLD_F, json.dumps(st.threshold, indent=2))
    st.learning.append({"ts": now_iso(), "old": old, "new": new, "reason": reason, "evidence": evidence})
    log("threshold %.3f -> %.3f: %s" % (old, new, reason))
    mem.write("threshold %.2f -> %.2f because %s" % (old, new, reason))
    return True


def learning(st, mem):
    evaluated = {r.get("evidence", {}).get("escalation") for r in read_jsonl(LEARNING_F)}
    evaluated.discard(None)
    last_starve_check = time.time()
    while not st.stop.wait(CFG["learn_every_beats"] * CFG["beat"]):
        # 1. every finished escalation teaches once: no-op -> pickier, action -> looser
        for e in read_jsonl(ESCALATIONS_F):
            if e["id"] in evaluated or e.get("verdict") not in ("action", "noop"):
                continue
            evaluated.add(e["id"])
            if e["verdict"] == "noop":
                set_threshold(st, mem, st.threshold["threshold"] + CFG["step_noop"],
                              "escalation %s (score %.2f) came back a no-op: raising the bar" % (e["id"], e["score"]),
                              {"escalation": e["id"], "verdict": "noop", "score": e["score"], "cost_usd_est": e.get("cost_usd_est")})
            else:
                set_threshold(st, mem, st.threshold["threshold"] + CFG["step_action"],
                              "escalation %s (score %.2f) produced an action: lowering the bar slightly" % (e["id"], e["score"]),
                              {"escalation": e["id"], "verdict": "action", "score": e["score"], "cost_usd_est": e.get("cost_usd_est")})
        # 2. starvation: many would-be escalations held and nothing tested for hours -> loosen a little
        if time.time() - last_starve_check >= 600:
            last_starve_check = time.time()
            cutoff = time.time() - CFG["starved_hours"] * 3600
            recent_esc = [t for t in st.escalation_times if t > cutoff]
            held = [d for d in read_jsonl(DECISIONS_F, last=2000)
                    if d.get("action") == "held_below_threshold" and d["ts"] >= datetime.fromtimestamp(cutoff).astimezone().isoformat()]
            if not recent_esc and len(held) >= CFG["starved_after"]:
                top = max(d["score"] for d in held)
                if st.threshold["threshold"] > top:
                    set_threshold(st, mem, st.threshold["threshold"] + CFG["step_starved"],
                                  "%d escalations held below threshold in %dh and none tested: loosening" % (len(held), CFG["starved_hours"]),
                                  {"held": len(held), "top_held_score": top, "escalation": None})


# --- status -----------------------------------------------------------------------------
def status():
    print("jarvis loops — status %s" % now_iso())
    pid = None
    try:
        pidinfo = json.loads(PID_F.read_text())
        pid = pidinfo.get("pid")
        alive = pid and os.path.exists("/proc/%d" % pid) if sys.platform != "darwin" else _pid_alive(pid)
        print("daemon: pid %s (%s), beats %s, last beat %s, fswatch pid %s" % (
            pid, "alive" if alive else "NOT RUNNING", pidinfo.get("beats"), pidinfo.get("last_beat"),
            pidinfo.get("fswatch_pid")))
    except Exception:
        print("daemon: no pid file (never started or not yet 30 beats in)")
    thr = load_threshold()
    print("threshold: %.2f (updated %s; adjustments %s) — %s" % (thr["threshold"], thr.get("updated"),
                                                                thr.get("adjustments", 0), thr.get("reason")))
    print("\nlast 10 events:")
    for e in read_jsonl(EVENTS_F, last=10):
        print("  #%-6s %s" % (e["id"], summarize_event(e)))
    print("\nlast 10 decisions:")
    for d in read_jsonl(DECISIONS_F, last=10):
        print("  %s %-8s score=%.2f thr=%.2f %-24s %s %ss n=%s  %s" % (
            d["ts"][11:19], d["class"], d["score"], d["threshold"], d["action"], d.get("model"),
            d.get("latency_s"), d.get("n_events"), (d.get("reason") or "")[:110]))
    esc = read_jsonl(ESCALATIONS_F)
    print("\nescalations: %d" % len(esc))
    for e in esc[-3:]:
        print("  %s %s verdict=%s cost≈$%s %ss: %s" % (e["ts"][11:19], e["id"], e.get("verdict"),
                                                        e.get("cost_usd_est"), e.get("latency_s"),
                                                        (e.get("summary") or e.get("error") or "")[:120]))
    lrn = read_jsonl(LEARNING_F)
    print("\nthreshold adjustments: %d" % len(lrn))
    for r in lrn[-3:]:
        print("  %s %.2f -> %.2f: %s" % (r["ts"][11:19], r["old"], r["new"], r["reason"][:120]))
    memlog = read_jsonl(MEMORY_F, last=1)
    if memlog:
        m = memlog[0]
        print("\nlast memory write: brain_file=%s marcos=%s (%s) %s" % (m.get("brain_file"), m.get("marcos"),
                                                                        m.get("marcos_info"), m["text"][:100]))


def escalate_last(n):
    """Operator-forced escalation of the last N real events (not noise, not chatter). Appends the same
    records the daemon would; the daemon's learning loop picks the verdict up from disk."""
    st = State()
    mem = Memory(st)
    # prefer the events the fast model actually saw (its batches already exclude noise and chatter)
    seen = []
    for d in read_jsonl(DECISIONS_F, last=400):
        if d.get("model") == CFG["fast_model"]:
            seen += d.get("events", [])
    by_id = {e["id"]: e for e in read_jsonl(EVENTS_F, last=1000)}
    ev = [by_id[i] for i in seen[-n:] if i in by_id]
    if not ev:
        ev = [e for e in read_jsonl(EVENTS_F, last=400) if not prefilter_noise(e)][-n:]
    if not ev:
        print("no events to escalate")
        return
    ok, why = escalation_allowed(st)
    if not ok:
        print("refusing: %s" % why)
        return
    d = record_decision(st, mem, events=[e["id"] for e in ev], n_events=len(ev), model="operator", latency_s=0,
                        **{"class": "escalate"}, score=1.0, action="escalated_forced_by_operator",
                        reason="operator ran `loops.py --escalate-last %d`" % n)
    print("escalating %d events (%s) to %s ..." % (len(ev), d["id"], CFG["big_model"]))
    escalate(st, mem, d, ev)
    for e in read_jsonl(ESCALATIONS_F, last=1):
        print(json.dumps(e, indent=2, ensure_ascii=False))
    for _ in range(40):
        if not mem.q:
            break
        time.sleep(0.25)


def _pid_alive(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


# --- main -------------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--beats", type=int, default=0, help="run N beats then exit (foreground verification)")
    ap.add_argument("--escalate-last", type=int, metavar="N", default=0,
                    help="operator switch: send the last N real non-noise events to the big model now, "
                         "recorded as a forced escalation (the running daemon learns from its verdict)")
    a = ap.parse_args()
    if a.status:
        status()
        return
    if a.escalate_last:
        escalate_last(a.escalate_last)
        return
    STATE.mkdir(exist_ok=True)
    # single instance
    try:
        other = json.loads(PID_F.read_text()).get("pid") if PID_F.exists() else None
        if other and other != os.getpid() and _pid_alive(other):
            print("another loops.py is running (pid %s); refusing to start a second one" % other)
            sys.exit(1)
    except (ValueError, OSError):
        pass
    _atomic_write(PID_F, json.dumps({"pid": os.getpid(), "beats": 0, "last_beat": now_iso(), "fswatch_pid": None}))
    st = State()
    mem = Memory(st)
    log("start pid=%d threshold=%.2f" % (os.getpid(), st.threshold["threshold"]))
    # one start line per hour at most, so a crash loop cannot spam the shared memory
    try:
        cur = json.loads(CURSOR_F.read_text()) if CURSOR_F.exists() else {}
    except Exception:
        cur = {}
    if time.time() - cur.get("last_start_memory", 0) > 3600:
        cur["last_start_memory"] = time.time()
        _atomic_write(CURSOR_F, json.dumps(cur))
        mem.write("jarvis-loops daemon started (pid %d, threshold %.2f, fast=%s, big=%s)" % (
            os.getpid(), st.threshold["threshold"], CFG["fast_model"], CFG["big_model"]))

    def on_signal(signum, frame):
        log("signal %s: stopping" % signum)
        st.stop.set()
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    threads = [threading.Thread(target=heartbeat, args=(st,), name="heartbeat", daemon=True),
               threading.Thread(target=triage, args=(st, mem), name="triage", daemon=True),
               threading.Thread(target=learning, args=(st, mem), name="learning", daemon=True)]
    for t in threads:
        t.start()
    try:
        while not st.stop.is_set():
            st.stop.wait(1.0)
            if a.beats and st.beats >= a.beats:
                st.stop.set()
            if not threads[0].is_alive():
                log("heartbeat thread died; exiting so launchd restarts us")
                st.stop.set()
    finally:
        for t in threads:
            t.join(timeout=5)
        # give the memory worker a moment to flush
        for _ in range(20):
            if not mem.q:
                break
            time.sleep(0.25)
        log("stop pid=%d beats=%d" % (os.getpid(), st.beats))
        try:
            PID_F.unlink()
        except Exception:
            pass


if __name__ == "__main__":
    main()
