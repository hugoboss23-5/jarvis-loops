# Jarvis loops v0 — build report (2026-09-07, Fable)

Everything below is measured on this Mac unless labelled otherwise. Times are local (EDT).

## What was built

`~/projects/jarvis-loops/loops.py` — one Python 3.9 process (system `/usr/bin/python3`), three threads, no server, no database. State is JSONL under `./state/`.

| loop | what it does every cycle | writes |
|---|---|---|
| HEARTBEAT (1 s) | front app + pid via LaunchServices (`lsappinfo`), focused-window title via the Accessibility API in-process, clipboard hash via `pbpaste`, file changes under `~/projects` and `~/brain` from an `fswatch` (FSEvents) child, new lines of `~/.codex/history.jsonl` by byte offset | `state/events.jsonl` (ring, 6000→4000 lines), `state/loops.pid`, `state/cursor.json` |
| TRIAGE (1 s read, model call at most every 45 s / 60 per hour) | prefilters obvious noise and "chatter" (a path rewritten ≥3× in 60 s) locally, batches the rest (≤30 events) and asks `codex exec -m gpt-5.3-codex-spark` for `{class, score, reason, reflex_cmd}` under a JSON schema; runs whitelisted read-only reflexes; escalates to `claude -p --model claude-fable-5-1 --dangerously-skip-permissions` when `score ≥ threshold` (≤1 per 15 min, ≤12 per day, `--max-budget-usd 4`, `--max-turns 6`, Write/Edit tools removed) | `state/decisions.jsonl` (every decision: score + threshold used), `state/reflexes.jsonl`, `state/escalations.jsonl` |
| LEARNING (every 60 beats) | reads escalations that finished since the last pass: a no-op verdict raises the threshold by 0.05, an action verdict lowers it by 0.03; every 10 min, if ≥5 would-be escalations were held below the threshold and none was tested in 6 h, lowers it by 0.02 (bounds 0.30–0.95) | `state/threshold.json`, `state/learning.jsonl` (each change with its reason and evidence) |

Memory: every non-noise decision, every reflex result, every escalation verdict and every threshold change goes to **both** `~/brain/jarvis-loops.md` (dated markdown lines, Marcos ingests `~/brain`) and the Marcos MCP `remember` tool over HTTP (source `jarvis`, session `loops`); `state/memory.jsonl` records which path worked for each line.

Reflex safety is by construction: a single command, first word in a whitelist of readers (`ls cat head tail wc grep find stat du df ps date uptime file which git(status|log|diff|show|branch|rev-parse|ls-files) mdfind lsappinfo sw_vers pmset`), no `; & | < > $ \``, `find` may not `-delete`/`-exec`, 15 s timeout. Nothing sends, deletes, buys or changes a setting, and nothing can ask Hugo anything.

## Commands

```
python3 loops.py                 # what launchd runs
python3 loops.py --status        # last 10 events, last 10 decisions, threshold, escalations
python3 loops.py --escalate-last N   # operator switch: force the big model on the last N real events
launchctl kickstart -k gui/$(id -u)/com.hugo.jarvis-loops   # restart after editing loops.py
launchctl bootout gui/$(id -u)/com.hugo.jarvis-loops        # stop it for good
```

## Proof — the running system (captured 2026-09-07 20:49:57 EDT)

```
$ launchctl list | grep jarvis
26365	0	com.hugo.jarvis-loops
```
Earlier in the same session the first launchd pid (82552) was checked at 3 s and at 68 s after `launchctl bootstrap`: same pid both times, 60 beats in that minute. The pid has changed since only because I restarted the job (`launchctl kickstart -k`) after each code patch; every restart came back within 10 s.

```
$ python3 loops.py --status
jarvis loops — status 2026-09-07T20:49:57-04:00
daemon: pid 26365 (alive), beats 30, last beat 2026-09-07T20:49:49-04:00, fswatch pid 26370
threshold: 0.65 (updated 2026-09-07T20:48:45-04:00; adjustments 1) — escalation esc-20260907-204809 (score 1.00) came back a no-op: raising the bar

last 10 events:
  #337    20:49:18 daemon {"msg": "start", "pid": 26365, "fast_model": "gpt-5.3-codex-spark", "big_model": "claude-fable-5-1", "threshold": 0.65}
  #338    20:49:18 app -> Claude (from None)
  #339    20:49:19 window [Claude] Claude
  #340    20:49:20 file Renamed ~/projects/ptt/ptt-status.json
  #341    20:49:27 file Renamed ~/projects/ptt/ptt-status.json
  #342    20:49:32 file Renamed ~/projects/ptt/ptt-status.json
  #343    20:49:36 file Renamed ~/projects/ptt/ptt-status.json
  #344    20:49:44 file Renamed ~/projects/ptt/ptt-status.json
  #345    20:49:48 file Renamed ~/projects/ptt/ptt-status.json
  #346    20:49:54 file Renamed ~/projects/ptt/ptt-status.json

last 10 decisions:
  20:49:13 reflex   score=0.56 thr=0.65 reflex                   gpt-5.3-codex-spark 4.29s n=2  A temp-to-final rename occurred for `loops.py`, indicating a runtime/auto-edit event in jarvis-loops state cod
  20:49:14 noise    score=0.00 thr=0.65 drop                     prefilter 0s n=1  chatter: ~/projects/ptt/ptt-status.json rewritten >= 3 times in 60s
  20:49:15 noise    score=0.00 thr=0.65 drop                     prefilter 0s n=1  chatter: ~/projects/ptt/ptt-status.json rewritten >= 3 times in 60s
  20:49:19 noise    score=0.00 thr=0.65 drop                     prefilter 0s n=1  obvious noise (temp/hidden files, daemon housekeeping)
  20:49:24 noise    score=0.05 thr=0.65 none                     gpt-5.3-codex-spark 5.11s n=1  Single event indicates app activation only; no file changes, clipboard signal, or prompt text indicating a pro
  20:49:33 noise    score=0.00 thr=0.65 drop                     prefilter 0s n=1  chatter: ~/projects/ptt/ptt-status.json rewritten >= 3 times in 60s
  20:49:37 noise    score=0.00 thr=0.65 drop                     prefilter 0s n=1  chatter: ~/projects/ptt/ptt-status.json rewritten >= 3 times in 60s
  20:49:45 noise    score=0.00 thr=0.65 drop                     prefilter 0s n=1  chatter: ~/projects/ptt/ptt-status.json rewritten >= 3 times in 60s
  20:49:49 noise    score=0.00 thr=0.65 drop                     prefilter 0s n=1  chatter: ~/projects/ptt/ptt-status.json rewritten >= 3 times in 60s
  20:49:55 noise    score=0.00 thr=0.65 drop                     prefilter 0s n=1  chatter: ~/projects/ptt/ptt-status.json rewritten >= 3 times in 60s

escalations: 1
  20:48:09 esc-20260907-204809 verdict=noop cost≈$0.7821175 26.9s: Manual escalation test; the renames are the push-to-talk daemon's normal status heartbeat (atomic rewrite every few seco

threshold adjustments: 1
  20:48:45 0.60 -> 0.65: escalation esc-20260907-204809 (score 1.00) came back a no-op: raising the bar

last memory write: brain_file=True marcos=True ([{"text": "{\n  \"added\": 1,\n  \"skipped\": 0\n}", "type": "text"}]) - 2026-09-07 20:49 triage d-1788828553-232: class=reflex score=0.56 threshold=0.65 action=reflex (gp
```

The events above are real: Hugo's Claude desktop app is frontmost, and the push-to-talk tool rewrites `~/projects/ptt/ptt-status.json` every 5 seconds (the chatter filter mutes it after the third hit, which is why the same path fills the event tail while never reaching the model).

### One real triage decision from gpt-5.3-codex-spark (verbatim line from `state/decisions.jsonl`)

```
{"events":[330,331],"n_events":2,"model":"gpt-5.3-codex-spark","latency_s":4.29,"class":"reflex","score":0.56,"action":"reflex","reason":"A temp-to-final rename occurred for `loops.py`, indicating a runtime/auto-edit event in jarvis-loops state code. No error signal or user-blocking prompt was included, so it is likely routine but worth a single read-only check for record.","reflex_cmd":"tail -n 120 ~/projects/jarvis-loops/loops.py","ts":"2026-09-07T20:49:13-04:00","id":"d-1788828553-232","threshold":0.65}
```
The very first one (20:24:17) was `class=noise score=0.02 latency 3.76 s` for "Terminal became active".

Across the build window: 26 fast-model calls, latency min / median / max = **2.91 / 5.48 / 13.79 s**, 0 errors, 16 noise + 10 reflex, 215 batches dropped locally by the prefilter (temp files, chatter) without a model call. 10 reflexes: 8 ran, 1 rejected by the whitelist (`git -C … status` before I taught the validator about `-C`), 1 ran and failed honestly (`git status` in a folder that is not a git repo — the model's mistake, recorded as such).

### One escalation and one threshold adjustment

No batch crossed the 0.60 bar on its own during the 25-minute watch (top real scores 0.55, 0.58, 0.59 — other assistants' activity in `~/projects/contract-watcher`). To exercise the path on real events I ran the operator switch once:

```
$ python3 loops.py --escalate-last 6
escalating 6 events (d-1788828489-221) to claude-fable-5-1 ...
{ "id": "esc-20260907-204809", "ts": "2026-09-07T20:48:09-04:00", "model": "claude-fable-5-1",
  "latency_s": 26.9, "cost_usd_est": 0.7821175, "turns": 3, "verdict": "noop",
  "summary": "Manual escalation test; the renames are the push-to-talk daemon's normal status heartbeat (atomic rewrite every few seconds), process alive 19h, mic live, last dictation at 20:13 worked, model 'cold' is the designed idle unload that reloads in ~1.5s on next press. Nothing to do." }
```
That escalation is recorded as `action=escalated_forced_by_operator`, not as a model decision. The running daemon's learning loop then adjusted on its real verdict, 36 seconds later:

```
state/learning.jsonl
{"ts":"2026-09-07T20:48:45-04:00","old":0.6,"new":0.65,"reason":"escalation esc-20260907-204809 (score 1.00) came back a no-op: raising the bar","evidence":{"escalation":"esc-20260907-204809","verdict":"noop","score":1.0,"cost_usd_est":0.7821175}}
```
`state/threshold.json` now reads 0.65 (previous 0.60, adjustments 1).

### Memory write path — both worked

Every one of the 22 memory lines written during the build landed in **both** places: `~/brain/jarvis-loops.md` (38 lines now) and Marcos `remember` over HTTPS (`{"added": 1, "skipped": 0}` each time, median 4.6 s per call). Independent check: Marcos `recall("jarvis-loops daemon started threshold")` from this Claude session returned the daemon's lines as ids 11872, 11876, 11877 with source `jarvis`, session `loops`. The endpoint URL (with its token) lives only in `marcos.url` (mode 600) inside the project; it is never logged.

Primary path = Marcos MCP; the brain file is written unconditionally as the on-disk record and the fallback. Note the duplication: Marcos also ingests `~/brain`, so each line can exist twice in Marcos (once via `remember`, once via ingest). If that bothers anyone, drop the brain-file write, not the MCP one.

## What failed, with the real errors

1. **"Verify the fast model answers in < 2 s" — it does not, via `codex exec`.** Best wall time measured 2.91 s, median 5.5 s. Breakdown from Codex's own log: process start 0.16 s, websocket open 0.7 s, then the model turn. Every call carries ~13 k input tokens of Codex's own tool and instruction scaffolding (18.9 k with Hugo's `~/.codex/config.toml` and `AGENTS.md` loaded, 12.9 k with `--ignore-user-config` and the apps/computer-use/browser/skills/hooks features disabled). `model_reasoning_effort="minimal"` is refused: `Unsupported value: 'minimal' is not supported with the 'gpt-5.3-codex-spark' model. Supported values are: 'low', 'medium', 'high', and 'xhigh'.` The daemon runs with `low`. Getting under 2 s would need the Responses API called directly with the ChatGPT token from `~/.codex/auth.json`, which I did not do (task said `codex exec`, and reusing that token outside Codex risks its refresh rotation and logging Hugo out).
2. **Window title via AppleScript hung System Events.** First call took 0.14 s; minutes later every `osascript … System Events` call on this Mac hung past 30 s, including one from another tool (a different script text was stuck in the same queue; `System Events` had been running 31 h). Error recorded: `Command '['osascript', '-e', …]' timed out after 4 seconds`, and the beat stretched to ~2.5 s. Replaced with the Accessibility API in-process (`AXUIElementCopyAttributeValue` via ctypes, ~0.1 ms). It reports `trusted=True` both from Terminal and under launchd. If Accessibility trust is ever revoked for python3 the daemon logs one `tap_error` per hour and keeps the app name only; it never prompts.
3. **Self-excitation loop.** The daemon's memory line → `~/brain/jarvis-loops.md` changed → fswatch event → the fast model ran `cat ~/brain/jarvis-loops.md` as a reflex → new memory line → … Three cycles happened (20:32–20:34) before I excluded the daemon's own outputs from the watch.
4. **Title spinners.** Claude Code's terminal title flips ◐/◑ every second; that produced 50 window events in 50 s. Titles are now compared with digits and symbols stripped and must be stable for two polls.
5. **`--escalate-last` first picked chatter events** (the ptt status renames) because the chatter filter lives in the triage thread. Fixed after the run: it now takes the events the fast model actually batched. The forced escalation above was still on real events.
6. **`--json` timeline run errored** with `The following tools cannot be used with reasoning.effort 'minimal': web_search.` — only a probe, led to disabling web search in the flags.

## What was not exercised (honest gaps)

- **Clipboard tap**: `pbpaste` runs every beat and hashes the content, but Hugo did not copy anything during the build window, so no `clipboard` event exists yet. I did not touch his clipboard to fake one.
- **Codex prompt tap**: no new line appeared in `~/.codex/history.jsonl` during the window. The tailer passed an offline test against a temp copy (starts at end of file, never replays old prompts, holds partial lines, saves its byte offset, redacts an API-key-shaped string).
- **Escalation from the model's own decision**: only the forced path ran. The code path is the same function, but the "score ≥ threshold, rate limit allows" branch in the triage thread has not fired on its own yet.
- **Confirmation**: all of the above is my own measurement. Nobody has done an adversarial pass.

## Costs and quotas (measured)

- Fast model: ~13 k input tokens per call on the ChatGPT plan, capped at 60 calls per hour, and zero calls while nothing happens. During active building it ran about once a minute (26 calls in 25 min).
- Big model: Claude subscription (Stripe), so `cost_usd_est` is the API-rate estimate Claude Code prints, not a charge. A trivial one-turn `claude -p` estimated $3.19 (system prompt + hooks); the real 3-turn escalation estimated $0.78. Hard caps: 1 per 15 min, 12 per day, `--max-budget-usd 4`, 6 turns.
- Daemon: 0.2 % CPU, 13 MB RSS; fswatch child ~0 %.

## Two house-rule conflicts, left for Hugo

- The brief allowed exactly one file outside the project (`~/brain/jarvis-loops.md`) plus the plist, so I did **not** write `~/brain/now.md` and did **not** run `brain-holes` (it overwrites `~/brain/KNOWN-HOLES.md`). The re-entry note for now.md is the "Next" section below; copy it there if wanted.
- `brew install fswatch` added one tool (1.3 MB). The brief named fswatch or watchdog as the FSEvents route; neither was installed, and the system Python has no PyObjC.

## Next

- Leave it running; look at `python3 loops.py --status` tomorrow and read `~/brain/jarvis-loops.md`. The first thing to judge is whether the reflexes the fast model picks are worth their lines.
- If the threshold climbs to 0.95 and nothing ever escalates, the starvation rule loosens it by 0.02 per 10 min once 5 would-be escalations are held for 6 h; if that is too slow, change `step_starved` at the top of `loops.py`.
- Two small cleanups worth doing when someone is in the file anyway: aggregate chatter decisions into one line per path per minute, and add a `--tail` command that follows events live.
