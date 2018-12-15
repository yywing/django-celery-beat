"""Timezone aware Cron schedule Implementation."""
from __future__ import absolute_import, unicode_literals


from celery import schedules

from collections import namedtuple


schedstate = namedtuple('schedstate', ('is_due', 'next'))


class Clocked(schedules.BaseSchedule):
    """clocked schedule."""
    def __init__(self, clocked_time, nowfun=None, app=None):
        self.clocked_time = self.maybe_make_aware(clocked_time)
        self.run_once = False
        super(Clocked, self).__init__(nowfun=nowfun, app=app)

    def remaining_estimate(self, last_run_at):
        return self.clocked_time - self.now()

    def is_due(self, last_run_at):
        last_run_at = self.maybe_make_aware(last_run_at)
        rem_delta = self.remaining_estimate(last_run_at)
        remaining_s = rem_delta.total_seconds()
        if remaining_s <= 0:
            if not self.run_once:
                self.run_once = True
                return schedstate(is_due=True, next=999999)
            else:
                return schedstate(is_due=False, next=999999)
        return schedstate(is_due=False, next=remaining_s)

    def __repr__(self):
        return '<freq: {} {}>'.format(self.clocked_time, self.run_once)

    def __eq__(self, other):
        if isinstance(other, Clocked):
            return self.clocked_time == other.clocked_time and self.run_once == other.run_once
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __reduce__(self):
        return self.__class__, (self.clocked_time, self.nowfun)
