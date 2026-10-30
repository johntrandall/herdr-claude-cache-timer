"""Tests for the bar and the ticker. Run: python3 -m unittest discover tests"""
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "..", "claude_cache_timer.py")
spec = importlib.util.spec_from_file_location("cct", SCRIPT)
cct = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cct)


class Bar(unittest.TestCase):
    def test_full_at_start_and_one_segment_at_the_end(self):
        self.assertTrue(cct.bar(0, 3600, 0).startswith("▰▰▰▰▰▰ "))
        self.assertTrue(cct.bar(0, 3600, 3599).startswith("▰▱▱▱▱▱ "))

    def test_loses_a_segment_every_sixth(self):
        # 17 minutes into an hour: 43 left, ceil(43/10) = 5 segments.
        self.assertEqual(cct.segments_left(0, 3600, 17 * 60), 5)
        self.assertEqual(cct.segments_left(0, 3600, 600), 5)  # exactly at a boundary
        self.assertEqual(cct.segments_left(0, 3600, 599), 6)

    def test_five_minute_cache_steps_every_50_seconds(self):
        self.assertEqual(cct.next_boundary(0, 300, 0), 50)
        self.assertEqual(cct.next_boundary(0, 300, 260), 300)

    def test_bar_ends_with_expiry_time(self):
        until = time.mktime((2026, 10, 29, 17, 47, 0, 0, 0, -1))
        self.assertTrue(cct.bar(until - 3600, until, until - 3600).endswith(" 17:47"))


class Ticker(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.log = os.path.join(self.dir, "herdr.log")
        self.env = dict(os.environ, HERDR_PLUGIN_STATE_DIR=self.dir, HERDR_SOCKET_PATH="test",
                        HERDR_BIN_PATH=os.path.join(HERE, "fake_herdr.sh"), FAKE_HERDR_LOG=self.log)
        self.env["HERDR_PLUGIN_ID"] = "test"

    def write_state(self, windows):
        key = cct.hashlib.sha256(b"test").hexdigest()[:16]
        path = os.path.join(self.dir, f"windows-{key}.json")
        with open(path + ".tmp", "w") as f:
            json.dump({"written_at": time.time(), "windows": windows}, f)
        os.replace(path + ".tmp", path)

    def until(self, test, seconds=15):
        """Poll the fake herdr's log until test(calls) holds; slow machines are fine."""
        deadline = time.time() + seconds
        while time.time() < deadline:
            if test(self.calls()):
                return True
            time.sleep(0.2)
        return False

    def calls(self):
        try:
            with open(self.log) as f:
                return [line.split(" ", 1)[1].strip() for line in f]
        except OSError:
            return []

    def test_reports_each_boundary_then_clears_and_exits(self):
        now = time.time()
        # A 12-second cache: a segment every 2 s.
        self.write_state({"p1": [now - 0.5, now + 11.5]})
        proc = subprocess.Popen([sys.executable, SCRIPT, "ticker"], env=self.env)
        proc.wait(timeout=40)
        calls = self.calls()
        bars = [c.split("cache_bar=")[1][:6] for c in calls if "cache_bar=" in c]
        self.assertGreaterEqual(len(bars), 3, calls)
        self.assertEqual(bars, sorted(bars, key=lambda b: -b.count("▰")), calls)  # only ever shrinks
        self.assertEqual(len(bars), len(set(bars)), calls)  # never the same bar twice
        self.assertIn("--clear-token cache_bar", calls[-1])
        self.assertEqual(proc.returncode, 0)

    def test_sends_nothing_while_no_boundary_passes(self):
        now = time.time()
        self.write_state({"p1": [now, now + 3600]})
        proc = subprocess.Popen([sys.executable, SCRIPT, "ticker"], env=self.env)
        time.sleep(2)
        self.assertEqual(self.calls(), [])
        proc.terminate(); proc.wait(timeout=5)

    def test_kick_wakes_a_sleeping_ticker_for_new_windows(self):
        now = time.time()
        self.write_state({"p1": [now, now + 3600]})
        proc = subprocess.Popen([sys.executable, SCRIPT, "ticker"], env=self.env)
        time.sleep(1)
        self.write_state({"p1": [now, now + 3600], "p2": [now - 1.4, now + 0.1]})
        time.sleep(0.2)
        subprocess.run([sys.executable, "-c",
                        f"import importlib.util as u;s=u.spec_from_file_location('c',{SCRIPT!r});m=u.module_from_spec(s);s.loader.exec_module(m);m.kick_ticker()"],
                       env=self.env, check=True)
        # Without the kick the ticker would sleep on for p1's 10-minute boundary.
        self.assertTrue(self.until(lambda calls: any("p2" in c and "--clear-token" in c for c in calls)), self.calls())
        self.assertIsNone(proc.poll())  # still alive for p1
        proc.terminate(); proc.wait(timeout=5)

    def test_exits_when_herdr_has_the_plugin_disabled(self):
        conf = os.path.join(self.dir, "conf")
        os.makedirs(conf)
        with open(os.path.join(conf, "plugins.json"), "w") as f:
            json.dump([{"plugin_id": "test", "enabled": False}], f)
        self.env["HERDR_CONFIG_DIR"] = conf
        now = time.time()
        self.write_state({"p1": [now, now + 3600]})
        done = subprocess.run([sys.executable, SCRIPT, "ticker"], env=self.env, timeout=10)
        self.assertEqual(done.returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_second_ticker_exits_at_once(self):
        now = time.time()
        self.write_state({"p1": [now, now + 3600]})
        first = subprocess.Popen([sys.executable, SCRIPT, "ticker"], env=self.env)
        time.sleep(0.5)
        second = subprocess.run([sys.executable, SCRIPT, "ticker"], env=self.env, timeout=5)
        self.assertEqual(second.returncode, 0)
        self.assertIsNone(first.poll())
        first.terminate(); first.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
