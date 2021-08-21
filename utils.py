from collections import defaultdict

import pandas as pd


def parse_inventory(inventory_file, screen_file):
    player_ids_dict = pd.read_csv(screen_file, delimiter=';', index_col='PlayerNumber').to_dict()['PlayerId']
    inventory_df = pd.read_excel(inventory_file, parse_dates=['Дата'])
    result_schedule = defaultdict(dict)
    for date, screen_name, *hours in inventory_df[['Дата', 'ID экрана'] + list(range(24))].values:
        screen_id = player_ids_dict[screen_name]
        for hour, hour_remains in enumerate(hours):
            hour_ts = date.timestamp() + hour * 3600
            result_schedule[screen_id][int(hour_ts)] = hour_remains

    return dict(result_schedule)
