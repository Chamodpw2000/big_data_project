"""Simulated clock: compresses one simulated day into SIM_DAY_REAL_MINUTES.

Every component (both simulators, the Spark jobs, the API) must agree on what
"now" is in simulated time. A process cannot just start its own clock when it
launches, because a component started 10 minutes later would be 10 simulated
hours behind everyone else.

So the clock is anchored to a shared epoch: the real wall-clock time at which
the simulation started. It is stored once in data/.sim_epoch (or the SIM_EPOCH
environment variable) and every process reads the same value.

Delete data/.sim_epoch to restart the simulation from simulated day 1.
"""
import os
import time
from datetime import datetime, timedelta

from common.config import SIM_COMPRESSION, SIM_START

EPOCH_FILE = os.getenv("SIM_EPOCH_FILE", os.path.join("data", ".sim_epoch"))


def shared_epoch() -> float:
    """Real unix time when the simulation started, shared by all components."""
    env = os.getenv("SIM_EPOCH")
    if env:
        return float(env)

    if os.path.exists(EPOCH_FILE):
        try:
            return float(open(EPOCH_FILE).read().strip())
        except (ValueError, OSError):
            pass

    started = time.time()
    os.makedirs(os.path.dirname(EPOCH_FILE) or ".", exist_ok=True)
    with open(EPOCH_FILE, "w") as f:
        f.write(str(started))
    return started


class SimClock:
    """Maps real elapsed time to simulated time.

    With 1 sim day = 24 real minutes, SIM_COMPRESSION is 60:
    1 real second = 60 simulated seconds = 1 simulated minute.
    """

    def __init__(self, start: datetime = SIM_START, compression: float = SIM_COMPRESSION):
        self.start = start
        self.compression = compression
        self._t0 = shared_epoch()

    def now(self) -> datetime:
        elapsed_real = time.time() - self._t0
        return self.start + timedelta(seconds=elapsed_real * self.compression)

    def sim_date(self) -> str:
        return self.now().strftime("%Y-%m-%d")

    def day_index(self) -> int:
        """Simulated day number, starting at 1."""
        return (self.now().date() - self.start.date()).days + 1

    def real_seconds_for(self, sim_minutes: float) -> float:
        """How many real seconds a given number of simulated minutes takes."""
        return (sim_minutes * 60) / self.compression
