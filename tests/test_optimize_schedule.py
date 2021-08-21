import pytz
from datetime import datetime
from make_schedule import Schedule
import logging

logging.basicConfig(filename='debug.log', format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


def test_make_schedule(schedule_plan_data):
    forecast = schedule_plan_data['predictions']
    base_schedule = schedule_plan_data['schedule']
    schedule = Schedule(base_schedule)
    tz = pytz.timezone('Asia/Novosibirsk')
    advertisement_schedule = schedule.make_advertisement_schedule(
        screen_ids=[257],
        desired_ots=3600,
        start_date=datetime(2021, 9, 1, tzinfo=tz),
        end_date=datetime(2021, 10, 1, tzinfo=tz),
        week_days=[0],
        hours=[1, 3, 5],
        frequency=72,
        ots_forecast=forecast,
    )

    print(advertisement_schedule)

    print(schedule_plan_data.keys())
