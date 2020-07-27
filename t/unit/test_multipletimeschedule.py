from datetime import datetime, timezone, time, date, timedelta
from django_celery_beat.multiple_time_schedule import multipletime
from django_celery_beat.utils import subtract_time
import pytest


def get_datetime(hour, minute, second):
    return datetime(
        year=2000,
        month=1,
        day=1,
        hour=hour,
        minute=minute,
        second=second,
        tzinfo=timezone.utc,
    )


def time_to_datetime(t):
    return datetime.combine(date.today(), t)


time_233400 = time(hour=23, minute=34, second=0)
time_060300 = time(hour=6, minute=3, second=0)
time_091000 = time(hour=9, minute=10, second=0)


@pytest.mark.parametrize(
    "input,output",
    [
        [time_to_datetime(time_091000), time_233400],
        [time_to_datetime(time_060300), time_091000],
        [time_to_datetime(time_233400), time_060300],
        [get_datetime(23, 59, 59), time_060300],
        [get_datetime(0, 0, 0), time_060300],
        [get_datetime(0, 0, 1), time_060300],
        [get_datetime(6, 2, 59), time_060300],
        [get_datetime(9, 9, 59), time_091000],
        [get_datetime(23, 33, 59), time_233400],
        [get_datetime(23, 34, 1), time_060300],
    ],
)
def test_get_next_time(input, output):
    schedule = multipletime(
        timezone=timezone.utc, times=[time_060300, time_091000, time_233400,]
    )
    assert schedule.get_next_time(input) == output


@pytest.mark.parametrize(
    "now,last_run_at,is_due,next",
    [
        [
            get_datetime(23, 34, 0),
            get_datetime(23, 34, 0),
            True,
            (
                subtract_time(time_060300, time_233400) + timedelta(days=1)
            ).total_seconds(),
        ],
        [get_datetime(23, 33, 59), get_datetime(23, 32, 20), False, 1,],
        [
            get_datetime(23, 34, 1),
            get_datetime(23, 34, 0),
            True,
            (
                subtract_time(time_060300, time_233400)
                + timedelta(days=1)
                - timedelta(seconds=1)
            ).total_seconds(),
        ],
        [
            get_datetime(0, 0, 0),
            get_datetime(23, 34, 0),
            False,
            subtract_time(time_060300, time(0, 0, 0)).total_seconds(),
        ],
        [get_datetime(6, 2, 59), get_datetime(23, 34, 0), False, 1,],
        [
            get_datetime(6, 3, 0),
            get_datetime(23, 34, 0),
            True,
            subtract_time(time_091000, time_060300).total_seconds(),
        ],
    ],
)
def test_multipletime(now, last_run_at, is_due, next):
    schedule = multipletime(
        timezone=timezone.utc, times=[time_233400, time_060300, time_091000,],
    )
    schedule.now = lambda: now
    rv = schedule.is_due(last_run_at)
    assert rv.is_due == is_due
    assert rv.next == next


def test_special_case():
    schedule = multipletime(timezone=timezone.utc, times=[time(0, 6, 30)])
    last_run_at = datetime(2020, 7, 15, 3, 25, 26, 457413, tzinfo=timezone.utc)
    schedule.now = lambda: get_datetime(0, 6, 31)
    rv = schedule.is_due(last_run_at)
    assert rv.is_due == True
    assert rv.next == timedelta(days=1).total_seconds() - 1
