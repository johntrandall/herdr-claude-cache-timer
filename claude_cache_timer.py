#!/usr/bin/env python3
"""Show when each Claude agent's prompt cache expires, in Herdr's agents panel.

Claude Code caches the start of each request's prompt for 5 minutes or an
hour, and every request that uses the cache starts that time again. So a
conversation's cache expires one such time after its last request; until
then, carrying on with it is cheaper and faster. Claude's own file for the
conversation records each response's time and which cache time it wrote.

Herdr runs this script when an agent's status changes, and once at
startup. Each run looks at every agent. While an agent works it keeps
sending requests, each one starting its cache time again, but Herdr starts
no run for them; so while any Claude agent works, a watcher (see watch)
looks at the working agents' files every WATCH_SECONDS and brings their
tokens up to date. Each Claude agent gets

- `$cache`: `cache 14:52`, when its cache expires,
- `$cache_until`: the same time in seconds since the epoch, which
  Herdrsson's `format = "left"` shows as `42m left`, counting down, and
- `$cache_span`: the time of its last request and that time, `FROM/UNTIL`,
  which Herdrsson's `format = "bar"` shows as `▰▰▰▰▱▱`, the share of the
  cache time still left, and
- `$cache_tokens`: `70.0k`, how many tokens its last request's prompt had,
  counting those read from and written to the cache: the number Claude
  Code's status line gets as the context used. Most of it is in the cache,
  and the next request after the cache has expired writes it all again.
  After the conversation has been compacted, the status line shows no
  number until the next response, and neither does `$cache_tokens`.

`$cache` is reported to expire with the cache. `$cache_until`, `$cache_span`
and `$cache_tokens` stay, so the agents panel can show that the cache has
expired:
the bar is empty then, `▱▱▱▱▱▱`, which a rule can strike through. Herdr's
token lifetimes stop while the computer sleeps, so after a sleep `$cache`
can outlast the cache until the next run clears it; the bar goes by the
clock. Agents other than Claude get no tokens.

`$cache_bar` (johntrandall's fork): `▰▰▰▱▱▱ 14:52` on stock Herdr, which
knows no `format = "bar"`. It loses a segment each sixth of the cache time
and ends with the expiry time; it goes once the cache has expired. Stock
Herdr shows a token as reported and can't count down, so a ticker (see
ticker) rewrites the idle agents' bars at segment boundaries only, about
six reports per agent per cache time; the watcher keeps the working
agents' bars current as it does their other tokens.

Herdr starts no run when Claude switches conversation without a status
change (`/clear`, `/resume`), so the tokens keep showing the previous
conversation until the agent next works.

Nothing is kept between runs: each one works the times out again from
Claude's files, so a missed event is corrected by the next run, and a
server restart, which drops Herdr's tokens, loses nothing once the startup
run has been. While one run waits for its turn, further events' runs end
at once (see turn).

Usage:
  claude_cache_timer.py         bring every agent's tokens up to date
  claude_cache_timer.py watch   follow the working agents (started by runs)
"""
import fcntl
import glob
import hashlib
import json
import math
import os
import resource
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime

PLUGIN_ID = os.environ.get("HERDR_PLUGIN_ID", "bagonyi.claude-cache-timer")
SOURCE = f"plugin:{PLUGIN_ID}"
LABEL_TOKEN = "cache"
TIME_TOKEN = "cache_until"
SPAN_TOKEN = "cache_span"
SIZE_TOKEN = "cache_tokens"
BAR_TOKEN = "cache_bar"
TOKENS = (LABEL_TOKEN, TIME_TOKEN, SPAN_TOKEN, SIZE_TOKEN, BAR_TOKEN)
BAR_SEGMENTS = 6
# A bar is reported to live until shortly after its next boundary, so a dead
# ticker leaves no bar that overstates the time left.
BAR_GRACE_SECONDS = 60
# The ticker never sleeps longer than this: a sleeping Mac stops the clock
# its wait uses, and on waking it works the bars out again from the wall clock.
TICKER_MAX_SLEEP_SECONDS = 300
SCRIPT = os.path.abspath(__file__)
# Herdr sets it for its runs; a run by hand uses the current folder, and
# starts no watcher (see start_watcher).
HERDR_STATE_DIR = os.environ.get("HERDR_PLUGIN_STATE_DIR")
STATE_DIR = HERDR_STATE_DIR or "."
SESSION = os.environ.get("HERDR_SOCKET_PATH", "")
# Claude Code keeps each conversation in projects/<folder>/<session id>.jsonl.
CLAUDE_DIR = os.environ.get("CLAUDE_CONFIG_DIR") or os.path.expanduser("~/.claude")

CACHE_5M = 300
CACHE_1H = 3600
# Herdr may see an agent stop around when Claude writes its last response
# to the file, so a run first waits until the files of the agents that
# aren't working have been still this long, counting from the run's start,
# but no longer than SETTLE_MAX_SECONDS.
SETTLE_SECONDS = 1.0
SETTLE_MAX_SECONDS = 3.0
# How far back from a file's end to look for a cache write.
TAIL_MAX = 8 << 20
# The longest time Herdr keeps a token reported with a lifetime.
TTL_MAX_MS = 86_400_000
# How long a Herdr CLI call may take before the run gives up on it.
HERDR_TIMEOUT_SECONDS = 10
# How often the watcher looks at the working agents' files.
WATCH_SECONDS = 2.0
# How many passes in a row may fail to list the agents before the watcher
# ends, while the server's socket is still there.
WATCH_FAILURES_MAX = 5
# How big the watcher's log may grow before a new watcher starts it afresh.
LOG_MAX = 64 << 10


HERDR_CALLS = 0


def perf_log(line):
    """Cost measurements, written only while the state directory holds a
    perf.on file. Each line: kind, then fields."""
    if not os.path.exists(os.path.join(STATE_DIR, "perf.on")):
        return
    own, kids = resource.getrusage(resource.RUSAGE_SELF), resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu = own.ru_utime + own.ru_stime + kids.ru_utime + kids.ru_stime
    with open(os.path.join(STATE_DIR, "perf.log"), "a") as f:
        f.write(f"{line} t={time.time():.1f} pid={os.getpid()} cpu_total={cpu:.3f} calls_total={HERDR_CALLS}\n")


def herdr(*args):
    """Run a Herdr CLI command and return its parsed JSON result, if it
    prints one."""
    global HERDR_CALLS
    HERDR_CALLS += 1
    binary = os.environ.get("HERDR_BIN_PATH", "herdr")
    completed = subprocess.run(
        [binary, *args],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=True,
        timeout=HERDR_TIMEOUT_SECONDS,
    )
    return json.loads(completed.stdout)["result"] if completed.stdout.strip() else None


@contextmanager
def turn():
    """Wait for this run's turn, as Herdr may start several hooks at once,
    and yield whether to run.

    A run gives up while another one is already waiting for its turn: that
    one starts once the current run ends, and sees all that has happened by
    then. So a burst of events, as when a restarted server finds its agents
    again, leads to at most two runs at a time instead of one per event; the
    others end at once.
    """
    with open(state_file("waiting.lock"), "w") as waiting, open(
        state_file("running.lock"), "w"
    ) as running:
        try:
            fcntl.flock(waiting, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        fcntl.flock(running, fcntl.LOCK_EX)
        # The next run can wait behind this one now.
        fcntl.flock(waiting, fcntl.LOCK_UN)
        yield True


def state_file(name):
    """A file of this session's in the state directory. Every session shares
    the directory and a run sees only its own session, so the locks are kept
    per session."""
    os.makedirs(STATE_DIR, exist_ok=True)
    session = hashlib.sha256(SESSION.encode()).hexdigest()[:16]
    stem, extension = os.path.splitext(name)
    return os.path.join(STATE_DIR, f"{stem}-{session}{extension}")


def claude_session(agent):
    """The agent's Claude session id, which names its conversation's file,
    or None if it isn't a Claude agent Herdr knows the session of."""
    session = agent.get("agent_session") or {}
    name = os.path.basename(session.get("value") or "")
    if session.get("agent") != "claude" or session.get("kind") != "id" or not name:
        return None
    return name


def conversation_file(agent):
    """The file of the agent's Claude conversation, or None.

    A new conversation has no file until its first message.
    """
    name = claude_session(agent)
    if name is None:
        return None
    pattern = os.path.join(glob.escape(CLAUDE_DIR), "projects", "*", glob.escape(name) + ".jsonl")
    paths = glob.glob(pattern)
    return paths[0] if paths else None


def modified(path):
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0


def wait_until_still(paths):
    """Wait until none of the files has changed for SETTLE_SECONDS, counting
    the run's start as a change, or for SETTLE_MAX_SECONDS at most.

    Counting the start makes every run wait at least SETTLE_SECONDS, so a
    last response written just after Herdr saw the agent stop, following a
    quiet spell (as while Claude streams a long answer), is still seen.
    """
    if not paths:
        return
    start = time.time()
    give_up = start + SETTLE_MAX_SECONDS
    while True:
        newest = max(start, *(modified(path) for path in paths))
        wait = min(newest + SETTLE_SECONDS, give_up) - time.time()
        if wait <= 0:
            return
        time.sleep(wait)


def cache_window(path):
    """When the conversation's last request was sent and when its cache
    expires (or expired), in seconds, and how many tokens that request's
    prompt had (None if not recorded), or None if there's no response to go
    by."""
    try:
        sent, cache_time, prompt = last_request(path)
    except OSError:
        return None
    if sent is None:
        return None
    # No cache write in view: assume the shorter time rather than overstate it.
    return sent, sent + (cache_time or CACHE_5M), prompt


def last_request(path):
    """When the conversation's last request was sent, the cache time of its
    latest cache write and the size of the last request's prompt, read from
    the file's end, as the file can be many megabytes."""
    with open(path, "rb") as f:
        end = f.seek(0, os.SEEK_END)
        size = 1 << 18
        while True:
            start = max(0, end - size)
            f.seek(start)
            lines = f.read(end - start).splitlines()
            # The first line may start partway through an entry.
            sent, cache_time, prompt = scan(lines[1:] if start else lines)
            if (sent is not None and cache_time) or not start or size >= TAIL_MAX:
                return sent, cache_time, prompt
            size = min(size * 4, TAIL_MAX)


def scan(lines):
    """Go back through conversation entries for the last request's time, the
    cache time of the latest cache write and the last request's prompt
    size."""
    last_id = sent = cache_time = prompt = None
    compacted = False
    for line in reversed(lines):
        entry = parse_entry(line)
        if last_id is None and is_compaction(entry):
            compacted = True
            continue
        if not is_response(entry):
            continue
        message = entry["message"]
        # One response is written as several entries (thinking, text, tool
        # use); the earliest is closest to when its request was sent.
        if last_id is None:
            last_id = message["id"]
            # Compacted since: the prompt is smaller now, by how much the
            # file doesn't say until the next response.
            prompt = None if compacted else prompt_size(message["usage"])
        if message["id"] == last_id:
            sent = parse_time(entry.get("timestamp")) or sent
        elif cache_time:
            break
        if cache_time is None:
            cache_time = written_cache_time(message["usage"])
    return sent, cache_time, prompt


def parse_entry(line):
    try:
        entry = json.loads(line)
    except ValueError:
        return None
    return entry if isinstance(entry, dict) else None


def is_response(entry):
    """Whether the entry is a response in the conversation itself: not a
    subagent's, and not one Claude Code made up, such as an error."""
    if entry is None or entry.get("type") != "assistant" or entry.get("isSidechain"):
        return False
    message = entry.get("message")
    return (
        isinstance(message, dict)
        and isinstance(message.get("usage"), dict)
        and bool(message.get("id"))
        and message.get("model") != "<synthetic>"
    )


def is_compaction(entry):
    """Whether the entry marks where Claude Code compacted the conversation
    (`/compact`, or on its own when the context is full)."""
    return (
        entry is not None
        and entry.get("type") == "system"
        and entry.get("subtype") == "compact_boundary"
        and not entry.get("isSidechain")
    )


def written_cache_time(usage):
    """The cache time a response's request wrote with, if it wrote any."""
    creation = usage.get("cache_creation")
    if not isinstance(creation, dict):
        return None
    if creation.get("ephemeral_1h_input_tokens"):
        return CACHE_1H
    if creation.get("ephemeral_5m_input_tokens"):
        return CACHE_5M
    return None


def prompt_size(usage):
    """How many tokens a response's request sent, counting those read from
    and written to the cache, or None if the response doesn't say."""
    counts = [
        usage.get(name)
        for name in ("input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")
    ]
    if not all(isinstance(count, int) for count in counts):
        return None
    return sum(counts)


def short_count(count):
    """A token count as Claude Code's status line shows it: 812, 70.0k, 1.2M."""
    if count >= 1_000_000:
        return f"{count / 1_000_000:.1f}M"
    if count >= 1_000:
        return f"{count / 1_000:.1f}k"
    return str(count)


def parse_time(stamp):
    if not isinstance(stamp, str):
        return None
    try:
        return datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def report(agent, window):
    """Bring the agent's tokens up to date, reporting only what changed.

    `$cache_until`, `$cache_span` and `$cache_tokens` stay once the cache
    has expired, so the agents panel can show that; `$cache` is reported to
    expire with the cache. An agent without a conversation (whose window is
    None) has none of them.
    """
    pane_id, tokens = agent["pane_id"], agent.get("tokens") or {}
    if window is None:
        clear_tokens(pane_id, tokens, TOKENS)
        return
    sent, until, prompt = window
    # Whole seconds, which is all the tokens hold, so the label never
    # outlasts the time it shows.
    until = int(until)
    kept = {TIME_TOKEN: str(until), SPAN_TOKEN: f"{int(sent)}/{until}"}
    if prompt is not None:
        kept[SIZE_TOKEN] = short_count(prompt)
    if any(tokens.get(name) != value for name, value in kept.items()):
        set_tokens(pane_id, kept)
    if prompt is None:
        clear_tokens(pane_id, tokens, (SIZE_TOKEN,))
    now = time.time()
    if until <= now:
        clear_tokens(pane_id, tokens, (LABEL_TOKEN, BAR_TOKEN))
        return
    if tokens.get(BAR_TOKEN) != bar(sent, until, now):
        report_bar(pane_id, sent, until, now)
    label = datetime.fromtimestamp(until).strftime("cache %H:%M")
    # A new expiry in the same minute keeps the label but moves its end.
    if tokens.get(LABEL_TOKEN) != label or tokens.get(TIME_TOKEN) != str(until):
        ttl_ms = min(max(1, int((until - now) * 1000)), TTL_MAX_MS)
        set_tokens(pane_id, {LABEL_TOKEN: label}, "--ttl-ms", str(ttl_ms))


def bar(start, until, now):
    """`▰▰▰▱▱▱ 14:52`: the segments of the cache time still left, rounded up,
    so the last segment shows until the cache expires, then the expiry time."""
    left = segments_left(start, until, now)
    return "▰" * left + "▱" * (BAR_SEGMENTS - left) + datetime.fromtimestamp(until).strftime(" %H:%M")


def segments_left(start, until, now):
    span = max(until - start, 1)
    return min(BAR_SEGMENTS, max(1, math.ceil((until - now) * BAR_SEGMENTS / span)))


def next_boundary(start, until, now):
    """When the bar next loses a segment (the last one goes at expiry)."""
    span = max(until - start, 1)
    return until - (segments_left(start, until, now) - 1) * span / BAR_SEGMENTS


def report_bar(pane_id, start, until, now):
    """Report the bar alone, to live until just past its next boundary."""
    lifetime = min(next_boundary(start, until, now) + BAR_GRACE_SECONDS, until) - now
    ttl_ms = min(max(1, int(lifetime * 1000)), TTL_MAX_MS)
    set_tokens(pane_id, {BAR_TOKEN: bar(start, until, now)}, "--ttl-ms", str(ttl_ms))


def set_tokens(pane_id, values, *options):
    settings = [arg for name, value in values.items() for arg in ("--token", f"{name}={value}")]
    herdr("pane", "report-metadata", pane_id, "--source", SOURCE, *settings, *options)


def clear_tokens(pane_id, tokens, names):
    clear = [arg for name in names if name in tokens for arg in ("--clear-token", name)]
    if clear:
        herdr("pane", "report-metadata", pane_id, "--source", SOURCE, *clear)


def reconcile():
    agents = herdr("agent", "list")["agents"]
    files = {agent["pane_id"]: conversation_file(agent) for agent in agents}
    # A working agent's file keeps changing; the watcher follows it.
    wait_until_still(
        [
            files[agent["pane_id"]]
            for agent in agents
            if agent["agent_status"] != "working" and files[agent["pane_id"]]
        ]
    )
    failed = report_all(agents, files)
    if any(watched(agent) for agent in agents):
        start_watcher()
    # The idle agents whose bar the ticker must step down.
    now = time.time()
    windows = {}
    for agent in agents:
        path = files.get(agent["pane_id"])
        if path is None or agent["agent_status"] == "working":
            continue
        window = cache_window(path)
        if window and window[1] > now:
            windows[agent["pane_id"]] = [window[0], int(window[1])]
    write_windows(windows)
    if windows:
        kick_ticker()
    perf_log(f"run agents={len(agents)} warm_idle={len(windows)} failed={int(failed)}")
    return failed


def report_all(agents, files):
    """Bring the agents' tokens up to date from their files, and return
    whether any report failed."""
    failed = False
    for agent in agents:
        path = files.get(agent["pane_id"])
        try:
            report(agent, cache_window(path) if path else None)
        except subprocess.CalledProcessError as error:
            # The pane may have closed since the list; the others still count.
            print(f"{agent['pane_id']}: {error.stderr.strip() or error}", file=sys.stderr)
            failed = True
        except (subprocess.SubprocessError, OSError, ValueError, LookupError) as error:
            # A timeout, Herdr missing while it is reinstalled, or an answer
            # this doesn't understand.
            print(f"{agent['pane_id']}: {error}", file=sys.stderr)
            failed = True
    return failed


def watched(agent):
    """Whether the watcher follows the agent: a working Claude agent."""
    return agent["agent_status"] == "working" and claude_session(agent) is not None


def take(lock):
    """Take the lock if no one holds it, and return whether this did."""
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def start_watcher():
    """Start the watcher unless one is running, in its own session, so it
    outlives this run.

    Only Herdr's runs start one: a run by hand has no state directory of
    Herdr's, so its locks aren't those of Herdr's runs and their watcher.
    """
    if not HERDR_STATE_DIR:
        return
    with open(state_file("watching.lock"), "w") as lock:
        if not take(lock):
            return
    path = state_file("watching.log")
    # Added to, so a watcher's last words outlast the next one's start.
    try:
        mode = "w" if os.path.getsize(path) > LOG_MAX else "a"
    except OSError:
        mode = "w"
    with open(path, mode) as log_file:
        subprocess.Popen(
            [sys.executable, SCRIPT, "watch"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=log_file,
            start_new_session=True,
            close_fds=True,
        )


def watch():
    """Follow the working agents until none works, bringing their tokens up
    to date every WATCH_SECONDS.

    One watcher runs per session; a second one ends at once. Before it ends,
    the watcher lets go of its lock and looks once more, so an agent that
    starts working meanwhile is followed, by this watcher or by the one the
    run for that agent starts. A watcher also ends when the server has
    stopped, after WATCH_FAILURES_MAX passes in a row without an answer, or
    once the plugin is disabled, as Herdr stops no process a plugin started
    (see enabled); the next run starts another.

    While agents keep working, a watcher can run for hours, and a second one
    ends at once; so when this script changes, the watcher starts again with
    the new code (see restarter).
    """
    with open(state_file("watching.lock"), "w") as lock:
        if not take(lock):
            return
        log("started")
        restart = restarter()
        held, failures = True, 0
        while True:
            restart()
            if not enabled():
                log("ended: the plugin is disabled")
                return
            # Not while a run reports, which may be reporting the same
            # agents. Listing them under the lock too means their tokens are
            # as the last run left them.
            with open(state_file("running.lock"), "w") as running:
                fcntl.flock(running, fcntl.LOCK_EX)
                try:
                    agents = [agent for agent in herdr("agent", "list")["agents"] if watched(agent)]
                    failures = 0
                except (subprocess.SubprocessError, OSError, ValueError, LookupError, TypeError) as error:
                    agents = None
                    failures += 1
                    log(f"no agent list: {error}")
                if agents and not held:
                    if not take(lock):
                        log("ended: another watcher has taken over")
                        return
                    held = True
                if agents:
                    report_all(agents, {agent["pane_id"]: conversation_file(agent) for agent in agents})
                perf_log(f"watch working={len(agents) if agents else 0}")
            if agents is None:
                if not os.path.exists(SESSION):
                    log("ended: the server has stopped")
                    return
                if failures >= WATCH_FAILURES_MAX:
                    log("ended: Herdr doesn't answer")
                    return
            elif not agents:
                if not held:
                    log("ended: no agent is working")
                    return
                # Look once more without the lock, at once.
                fcntl.flock(lock, fcntl.LOCK_UN)
                held = False
                continue
            time.sleep(WATCH_SECONDS)


def enabled():
    """Whether Herdr still has this plugin enabled, as its list of installed
    plugins says; yes when the list can't be read. `herdr plugin disable`
    stops the runs but not a watcher, which would otherwise go on while any
    agent works."""
    path = os.path.join(os.environ.get("HERDR_CONFIG_DIR") or os.path.expanduser("~/.config/herdr"), "plugins.json")
    try:
        with open(path) as f:
            for plugin in json.load(f):
                if plugin.get("plugin_id") == PLUGIN_ID:
                    return plugin.get("enabled", True) is not False
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    return True


def restarter():
    """A check to run on each pass: once this script has changed and then
    stayed the same for a pass, it starts the new code in this process, if
    the code compiles. Otherwise the old code carries on, so a save made in
    steps, a moment without the file or a mistake in it don't end the
    watcher. The watching lock goes with the old code, as Python doesn't
    pass its files on."""
    seen = tried = modified(SCRIPT)

    def restart():
        nonlocal seen, tried
        stamp = modified(SCRIPT)
        if stamp == seen and stamp != tried:
            tried = stamp
            try:
                with open(SCRIPT, "rb") as f:
                    compile(f.read(), SCRIPT, "exec")
                log("starting again with the changed script")
                os.execv(sys.executable, [sys.executable, SCRIPT, "watch"])
            except (OSError, SyntaxError, ValueError) as error:
                log(f"carrying on with the old code: {error}")
        seen = stamp

    return restart


# --- The ticker (johntrandall's fork) -------------------------------------
#
# One ticker per session rewrites the idle agents' bars at segment
# boundaries only: it sleeps until the next pane is due to lose a segment,
# reports that pane alone, and ends once no idle cache is warm. It never
# lists agents or reads conversations; it works from the windows the last
# run wrote, and a run wakes it with SIGUSR1 after writing new ones. It
# takes the runs' lock while it reports, so a bar never lands on an agent
# a run has just seen start working.


def write_windows(windows):
    path = state_file("windows.json")
    with open(path + ".new", "w") as f:
        json.dump({"written_at": time.time(), "windows": windows}, f)
    os.replace(path + ".new", path)


def read_windows():
    try:
        with open(state_file("windows.json")) as f:
            state = json.load(f)
        return state.get("written_at", 0), state.get("windows", {})
    except (OSError, ValueError, AttributeError):
        return 0, {}


def kick_ticker():
    """Wake the session's ticker, or start one if none is running. Only
    Herdr's runs start one, as with the watcher."""
    if not HERDR_STATE_DIR:
        return
    with open(state_file("ticker.lock"), "w") as lock:
        if not take(lock):
            # Held, so a ticker is alive and its pid file is current.
            try:
                with open(state_file("ticker.pid")) as f:
                    os.kill(int(f.read().strip()), signal.SIGUSR1)
            except (OSError, ValueError):
                pass
            return
    with open(state_file("ticker.log"), "a") as log_file:
        subprocess.Popen(
            [sys.executable, SCRIPT, "ticker"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=log_file,
            start_new_session=True,
            close_fds=True,
        )


class Wake(Exception):
    """Raised by SIGUSR1 to cut the ticker's sleep short."""


def on_wake(_signum, _frame):
    raise Wake


def wait(seconds):
    """Sleep, but wake at once on SIGUSR1. The signal is blocked at all
    other times, so it never lands mid-report; one sent then is delivered as
    soon as the next sleep unblocks it. (macOS has no sigtimedwait.)"""
    try:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGUSR1})
        time.sleep(seconds)
    except Wake:
        pass
    finally:
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGUSR1})


def ticker():
    # Block SIGUSR1 before anyone can find the pid: unhandled, it would end us.
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGUSR1})
    signal.signal(signal.SIGUSR1, on_wake)
    lock = open(state_file("ticker.lock"), "w")
    if not take(lock):
        return
    with open(state_file("ticker.pid"), "w") as f:
        f.write(str(os.getpid()))
    tick_log("started")
    shown = {}
    while True:
        # Again each time: a Wake landing inside wait()'s finally skips its block.
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGUSR1})
        if not enabled():
            tick_log("ended: the plugin is disabled")
            return
        read_at = time.time()
        written_at, windows = read_windows()
        with open(state_file("running.lock"), "w") as running:
            fcntl.flock(running, fcntl.LOCK_EX)
            now = time.time()
            for pane_id, (start, until) in windows.items():
                key = (pane_id, until)
                previous = shown.get(key) or bar(start, until, written_at)
                try:
                    if until <= now:
                        if shown.get(key) != "cleared":
                            # Normally Herdr has dropped it already; after the
                            # Mac slept, its lifetime stood still.
                            herdr("pane", "report-metadata", pane_id, "--source", SOURCE,
                                  "--clear-token", LABEL_TOKEN, "--clear-token", BAR_TOKEN)
                            shown[key] = "cleared"
                        continue
                    current = bar(start, until, now)
                    if current != previous:
                        report_bar(pane_id, start, until, now)
                    shown[key] = current
                except (subprocess.SubprocessError, OSError, ValueError, LookupError) as error:
                    # The pane may have closed; the next run drops it.
                    tick_log(f"{pane_id}: {error}")
                    shown[key] = "cleared"
        perf_log(f"tick warm={sum(1 for _, until in windows.values() if until > now)}")
        live = [(start, until) for start, until in windows.values() if until > now]
        if not live:
            if modified(state_file("windows.json")) > read_at:
                continue
            tick_log("ended: no idle cache is warm")
            return
        wake = min(next_boundary(start, until, now) for start, until in live)
        try:
            wait(min(max(wake - time.time() + 0.5, 1), TICKER_MAX_SLEEP_SECONDS))
        except Wake:
            pass


def tick_log(message):
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} ticker {os.getpid()}: {message}", file=sys.stderr, flush=True)


def log(message):
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} watcher {os.getpid()}: {message}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    if sys.argv[1:] == ["ticker"]:
        ticker()
    elif sys.argv[1:] == ["watch"]:
        if not HERDR_STATE_DIR:
            sys.exit("Only Herdr's runs start the watcher (see start_watcher).")
        watch()
    else:
        with turn() as my_turn:
            if not my_turn:
                perf_log("skip")
            elif reconcile():
                sys.exit(1)
