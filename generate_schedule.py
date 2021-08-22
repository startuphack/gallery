import pathlib
import pickle
from datetime import datetime

from make_schedule import Schedule
from utils import parse_inventory, DEFAULT_TZ, SchedulePrinter

resources_path = pathlib.Path(__file__).parent / 'resources'

if __name__ == '__main__':
    # Загружаем сохраненные прогнозы
    with open(str(resources_path / 'predictions_new.pkl'), 'rb') as predictions_stream:
        forecast = pickle.load(predictions_stream)
    # Парсим информацию о свободных слотах
    base_schedule = parse_inventory(resources_path / 'inventory.xlsx', resources_path / 'player_details.csv')

    # Создаем расписание
    schedule = Schedule(base_schedule, chunk_size=50)

    advertisement_schedule = schedule.make_advertisement_schedule(
        screen_ids=[257],
        desired_ots=177812,
        start_date=DEFAULT_TZ.localize(datetime(2021, 9, 1)),
        end_date=DEFAULT_TZ.localize(datetime(2021, 10, 1)),
        week_days=list(range(0, 7)),
        hours=list(range(10, 20)),
        frequency=72,
        ots_forecast=forecast,
    )

    print(advertisement_schedule)
    printer = SchedulePrinter(
        resources_path / 'plan_template.xlsx',
        resources_path / 'player_details.csv',
        plan_date_start=DEFAULT_TZ.localize(datetime(2021, 9, 1)),
        plan_date_stop=DEFAULT_TZ.localize(datetime(2021, 10, 1)),
    )

    printer.write_schedule(advertisement_schedule, resources_path / 'sample.xlsx')
