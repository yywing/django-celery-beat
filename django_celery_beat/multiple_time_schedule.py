"""multiple time schedule Implementation."""

from datetime import timedelta
from celery import schedules
from .utils import subtract_time, NEVER_CHECK_TIMEOUT


class multipletime(schedules.BaseSchedule):
    """multiple time schedule."""

    def __init__(self, timezone, times, model=None, nowfun=None, app=None):
        """Initialize multiple time.

        :type times: List[time]"""
        self.timezone = timezone
        self.times = sorted(times)
        super(multipletime, self).__init__(nowfun=nowfun, app=app)

    def get_next_time(self, last_run_at):
        """
        Get next run time for given last_run_at
        :type last_run_at: datetime
        """
        last_run_time = last_run_at.time()
        if last_run_time >= self.times[-1] or last_run_time < self.times[0]:
            # later than last time or earlier than first time
            # return first time.
            return self.times[0]
        for time in self.times:
            if time > last_run_time:
                return time
        raise ValueError("Can't get next time, please report bug")

    def now_with_tz(self):
        return self.now().astimezone(self.timezone)

    def remaining_estimate(self, last_run_at):
        next_time = self.get_next_time(last_run_at)
        now_time = self.now_with_tz().time()
        r = subtract_time(next_time, now_time)
        return r

    def next_estimate(self):
        next_delta = self.remaining_estimate(self.now_with_tz())
        if next_delta.total_seconds() < 0:
            next_delta += timedelta(days=1)
        return next_delta

    def is_due(self, last_run_at):
        if len(self.times) == 0:
            return schedules.schedstate(is_due=False, next=NEVER_CHECK_TIMEOUT)

        last_run_at = last_run_at.astimezone(self.timezone)
        # will update self._next_time
        rem_delta = self.remaining_estimate(last_run_at)
        remaining_s = max(rem_delta.total_seconds(), 0)
        if remaining_s == 0:
            return schedules.schedstate(
                is_due=True, next=self.next_estimate().total_seconds(),
            )
        return schedules.schedstate(is_due=False, next=remaining_s)

    def __repr__(self):
        return "<multipletime: {} {}>".format(self.timezone, self.times)

    def __eq__(self, other):
        if isinstance(other, multipletime):
            return self.times == other.times and self.timezone == other.timezone
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __reduce__(self):
        return self.__class__, (self.timezone, self.times)
