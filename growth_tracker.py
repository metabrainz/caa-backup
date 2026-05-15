"""Track disk usage and download count growth over time to produce accurate ETAs.

Samples are recorded periodically and rates are derived from the oldest and newest
samples in a sliding window, naturally accounting for pauses and idle periods.
"""

import shutil
import threading
import time
from collections import deque


class GrowthTracker:
    """Tracks growth of a numeric value over a sliding time window."""

    def __init__(self, window_seconds=3600, max_samples=720):
        self.window_seconds = window_seconds
        self.samples = deque(maxlen=max_samples)
        self._lock = threading.Lock()

    def record(self, value, timestamp=None):
        ts = timestamp if timestamp is not None else time.time()
        with self._lock:
            self.samples.append((ts, value))
            # Evict samples outside the window
            cutoff = ts - self.window_seconds
            while self.samples and self.samples[0][0] < cutoff:
                self.samples.popleft()

    def rate(self):
        """Return growth per second, or None if insufficient data."""
        with self._lock:
            if len(self.samples) < 2:
                return None
            oldest_t, oldest_v = self.samples[0]
            newest_t, newest_v = self.samples[-1]
            elapsed = newest_t - oldest_t
            if elapsed <= 0:
                return None
            return (newest_v - oldest_v) / elapsed


class DiskGrowthTracker:
    """Periodically samples disk usage and download count to estimate ETAs.

    Call start() to begin background sampling. Call stop() before shutdown.
    """

    def __init__(self, path, downloader, sample_interval=5, window_seconds=3600):
        self.path = path
        self.downloader = downloader
        self.sample_interval = sample_interval
        self.disk_tracker = GrowthTracker(window_seconds=window_seconds)
        self.download_tracker = GrowthTracker(window_seconds=window_seconds)
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=self.sample_interval * 2)

    def reset(self):
        """Clear history and record a fresh baseline sample.

        Call after the downloader loads initial counts from the database
        to avoid a false spike in the rate calculation.
        """
        self.disk_tracker.samples.clear()
        self.download_tracker.samples.clear()
        self._record_sample()

    def _sample_loop(self):
        while not self._stop_event.is_set():
            self._record_sample()
            self._stop_event.wait(self.sample_interval)

    def _record_sample(self):
        try:
            total, used, free = shutil.disk_usage(self.path)
        except OSError:
            return
        now = time.time()
        self.disk_tracker.record(used, now)
        self.download_tracker.record(self.downloader.downloaded, now)

    def seconds_before_full(self):
        """Estimate seconds until disk is full based on observed growth."""
        rate = self.disk_tracker.rate()
        if not rate or rate <= 0:
            return None
        try:
            total, used, free = shutil.disk_usage(self.path)
        except OSError:
            return None
        return free / rate

    def seconds_before_completed(self):
        """Estimate seconds until all downloads complete based on observed rate.

        Only counts files that can still be downloaded (excludes permanent errors).
        """
        rate = self.download_tracker.rate()
        if not rate or rate <= 0:
            return None
        total = self.downloader.total or 0
        downloaded = self.downloader.downloaded or 0
        permanent_errors = self.downloader.permanent_errors or 0
        remaining = total - downloaded - permanent_errors
        if remaining <= 0:
            return 0.0
        return remaining / rate
