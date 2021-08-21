from datetime import datetime

import pytz

from make_schedule import Schedule


def test_make_schedule_3600(schedule_plan_data):
    forecast = schedule_plan_data['predictions']
    base_schedule = schedule_plan_data['schedule']
    schedule = Schedule(base_schedule)
    tz = pytz.timezone('Asia/Novosibirsk')
    advertisement_schedule = schedule.make_advertisement_schedule(
        screen_ids=[257],
        desired_ots=3600,
        start_date=tz.localize(datetime(2021, 9, 6)),
        end_date=tz.localize(datetime(2021, 9, 7)),
        week_days=[0],
        hours=[1],
        frequency=72,
        ots_forecast=forecast,
    )

    assert advertisement_schedule['ots-forecast'] == 3602
    unique_appropriate_day = tz.localize(datetime(2021, 9, 6, 1)).timestamp()
    assert advertisement_schedule['schedule'][257][unique_appropriate_day]['slots'] == 60


def test_make_schedule_4600(schedule_plan_data):
    forecast = schedule_plan_data['predictions']
    base_schedule = schedule_plan_data['schedule']
    schedule = Schedule(base_schedule)
    tz = pytz.timezone('Asia/Novosibirsk')
    advertisement_schedule = schedule.make_advertisement_schedule(
        screen_ids=[257],
        desired_ots=4600,
        start_date=tz.localize(datetime(2021, 9, 6)),
        end_date=tz.localize(datetime(2021, 9, 7)),
        week_days=[0],
        hours=[1],
        frequency=72,
        ots_forecast=forecast,
    )

    assert advertisement_schedule['ots-forecast'] == 4322
    assert advertisement_schedule['schedule'] is None


def test_make_schedule_2600(schedule_plan_data):
    forecast = schedule_plan_data['predictions']
    base_schedule = schedule_plan_data['schedule']
    schedule = Schedule(base_schedule)
    tz = pytz.timezone('Asia/Novosibirsk')
    advertisement_schedule = schedule.make_advertisement_schedule(
        screen_ids=[257],
        desired_ots=2600,
        start_date=tz.localize(datetime(2021, 9, 6)),
        end_date=tz.localize(datetime(2021, 9, 7)),
        week_days=[0],
        hours=[1],
        frequency=72,
        ots_forecast=forecast,
    )

    assert advertisement_schedule['ots-forecast'] == 2881
    unique_appropriate_day = tz.localize(datetime(2021, 9, 6, 1)).timestamp()
    assert advertisement_schedule['schedule'][257][unique_appropriate_day]['slots'] == 48


def test_make_schedule_2600_x2(schedule_plan_data):
    forecast = schedule_plan_data['predictions']
    base_schedule = schedule_plan_data['schedule']
    schedule = Schedule(base_schedule)
    tz = pytz.timezone('Asia/Novosibirsk')
    advertisement_schedule = schedule.make_advertisement_schedule(
        screen_ids=[257],
        desired_ots=2600,
        start_date=tz.localize(datetime(2021, 9, 6)),
        end_date=tz.localize(datetime(2021, 9, 14)),
        week_days=[0],
        hours=[1],
        frequency=72,
        ots_forecast=forecast,
    )

    assert advertisement_schedule['ots-forecast'] == 3239
    appropriate_day_1 = tz.localize(datetime(2021, 9, 6, 1)).timestamp()
    appropriate_day_2 = tz.localize(datetime(2021, 9, 13, 1)).timestamp()
    assert advertisement_schedule['schedule'][257][appropriate_day_1]['slots'] == 24
    assert advertisement_schedule['schedule'][257][appropriate_day_2]['slots'] == 24



def test_make_schedule_2600_x2_t2(schedule_plan_data):
    forecast = schedule_plan_data['predictions']
    base_schedule = schedule_plan_data['schedule']
    schedule = Schedule(base_schedule)
    tz = pytz.timezone('Asia/Novosibirsk')
    advertisement_schedule = schedule.make_advertisement_schedule(
        screen_ids=[257],
        desired_ots=2600,
        start_date=tz.localize(datetime(2021, 9, 6)),
        end_date=tz.localize(datetime(2021, 9, 14)),
        week_days=[0],
        hours=[1, 15],
        frequency=72,
        ots_forecast=forecast,
    )

    assert advertisement_schedule['ots-forecast'] == 2857
    appropriate_day_1_1 = tz.localize(datetime(2021, 9, 6, 1)).timestamp()
    appropriate_day_1_2 = tz.localize(datetime(2021, 9, 6, 15)).timestamp()
    appropriate_day_2_1 = tz.localize(datetime(2021, 9, 13, 1)).timestamp()
    appropriate_day_2_2 = tz.localize(datetime(2021, 9, 13, 15)).timestamp()
    assert advertisement_schedule['schedule'][257][appropriate_day_1_1]['slots'] == 6
    assert advertisement_schedule['schedule'][257][appropriate_day_1_2]['slots'] == 6
    assert advertisement_schedule['schedule'][257][appropriate_day_2_1]['slots'] == 6
    assert advertisement_schedule['schedule'][257][appropriate_day_2_2]['slots'] == 6
