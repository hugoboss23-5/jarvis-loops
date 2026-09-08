# Jarvis loops v0 — three connected loops, running unattended

## Goal
Build and START a nervous system on this Mac made of three connected loops sharing one memory (Marcos):
1. HEARTBEAT — a daemon that never exits. Every beat it taps the OS at the source, not the screen: active app + window title, file changes under ~/projects and ~/brain (FSEvents via fswatch or watchdog), clipboard changes, new lines in ~/.codex/history.jsonl. Appends events to a rolling JSONL ring buffer (./state/events.jsonl, capped). Beat interval ≤ 1s; no screenshots unless a change is detected.
2. TRIAGE — reads new events since last beat and asks the FAST model whether this is (a) noise, (b) a reflex it can do itself in one shell command, or (c) worth escalating to the big model. Fast model = `codex exec -m gpt-5.3-codex-spark` (already logged in, ChatGPT auth; verify it answers in <2s). Escalation = `claude -p --model claude-fable-5-1 --dangerously-skip-permissions`. Every decision appended to ./state/decisions.jsonl with a numeric score and the threshold used.
3. LEARNING — every N beats, reads decisions + what happened after (did the escalation produce an action or a no-op?) and adjusts the triage threshold in ./state/threshold.json. Log each adjustment with the reason.
Memory: every non-noise decision and every threshold change is written to Marcos. Marcos ingests ~/brain and ~/.claude/projects — so write dated markdown lines to ~/brain/jarvis-loops.md (create it; that is the ONE file you may touch outside the project folder). Also try the Marcos MCP `remember` tool at https://marcos.tail85451d.ts.net/70b4bc58cc1b4ce0647df0d8648440ad/mcp (source "jarvis", session "loops"); use it if it works, fall back to the brain file if not, and say which in REPORT.md.

## What you may READ
/Users/hugovillalba/projects/kirkland (existing session-capture system — reuse its tailers if useful), ~/.codex/config.toml, ~/brain/CLAUDE.md, ~/.claude.json (do not print secrets).

## The ONE folder you may WRITE to
/Users/hugovillalba/projects/jarvis-loops/  (plus ~/brain/jarvis-loops.md and one launchd plist in ~/Library/LaunchAgents/com.hugo.jarvis-loops.plist)

## Three design rules
1. Plain Python 3, no web server, no database except JSONL files in ./state/. One process, three loops as threads or asyncio tasks. `python3 loops.py` runs it; `python3 loops.py --status` prints last 10 events, last 10 decisions, current threshold.
2. Reflex actions are limited to: reading files, running read-only shell commands, writing inside ./state/, and writing memory lines. Nothing that sends, deletes, buys, or changes settings. Hugo never gets an approval prompt.
3. Keep it alive: install the launchd plist with KeepAlive=true and RunAtLoad=true, `launchctl load` it, and confirm the pid survives 60 seconds.

## Done means
- REPORT.md shows: `launchctl list | grep jarvis` output with a pid, `python3 loops.py --status` output with real events from Hugo's Mac, one real triage decision from gpt-5.3-codex-spark with its latency, one threshold adjustment, and the memory write path that worked.
- Anything that failed is listed with the real error, not guessed around.

Follow Hugo's Trilateral Method: observe, one step, read back and verify.
