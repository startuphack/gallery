import gzip
import io
import pytz
from datetime import datetime, timedelta
import pickle
from collections import defaultdict

import pandas as pd


def parse_inventory(inventory_file, screen_file, tz=pytz.timezone('Asia/Novosibirsk')):
    player_ids_dict = pd.read_csv(screen_file, delimiter=';', index_col='PlayerNumber').to_dict()['PlayerId']
    inventory_df = pd.read_excel(
        inventory_file,
        parse_dates=['Дата'],
        date_parser=lambda x: tz.localize(datetime.strptime(x, '%Y-%m-%d')),
    )
    result_schedule = defaultdict(dict)
    for date, screen_name, *hours in inventory_df[['Дата', 'ID экрана'] + list(range(24))].values:
        screen_id = player_ids_dict[screen_name]
        for hour, hour_remains in enumerate(hours):
            hour_date = date + timedelta(hours=hour)
            result_schedule[screen_id][hour_date] = hour_remains

    return dict(result_schedule)


def pickle_dump(object, filename, gzip_file=True):
    """
    Сохранить объект в файл
    :param object: сохраняемый объект
    :param filename: имя файла
    :param gzip_file: сжимать ли сериализацию объекта с gzip
    """
    o_method = gzip.open if gzip_file else open

    with io.BufferedWriter(o_method(filename, 'w')) as output:
        pickle.dump(object, output, protocol=0)


def pickle_load(filename, gzip_file=True):
    """
    Загрузить объект из файла filename
    :param filename: имя файла
    :param gzip_file: является ли файл сжатым с gzip
    :return: объект, загруженный из файла
    """
    i_method = gzip.open if gzip_file else open
    with i_method(filename) as input:
        return pickle.load(input)


HOLIDAYS = [
    # 2020
    '2020-01-01', '2020-01-02', '2020-01-03', '2020-01-04', '2020-01-05', '2020-01-06', '2020-01-07', '2020-01-08',
    '2020-02-22', '2020-02-23', '2020-02-24',
    '2020-03-07', '2020-03-08', '2020-03-09',
    '2020-05-01', '2020-05-02', '2020-05-03', '2020-05-04', '2020-05-05',
    '2020-05-09', '2020-05-10', '2020-05-11',
    '2020-06-12', '2020-06-13', '2020-06-14',
    '2020-07-01',
    '2020-11-04',
    # 2021
    '2021-01-01', '2021-01-02', '2021-01-03', '2021-01-04', '2021-01-05',
    '2021-01-06', '2021-01-07', '2021-01-08', '2021-01-09', '2021-01-10',
    '2021-02-21', '2021-02-22', '2021-02-23',
    '2021-03-06', '2021-03-07', '2021-03-08',
    '2021-05-01', '2021-05-02', '2021-05-03',
    '2021-05-08', '2021-05-09', '2021-05-10',
    '2021-06-12', '2021-06-13', '2021-06-14',
    '2021-11-04', '2021-11-05', '2021-11-06', '2021-11-07',
    '2021-12-31',
]


