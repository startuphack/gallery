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
