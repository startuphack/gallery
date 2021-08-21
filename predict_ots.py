import datetime as dt
import pathlib
import pickle

import numpy as np
import pandas as pd
from prophet import Prophet

from utils import HOLIDAYS


def build_df(crowd_dir):
    '''
    Собираем данные для обучения прогнозной модели.
    Итоговый датафрейм будет содержать время начала каждого часового слота, ID плеера и кол-во mac-адресов за этот слот
    :param crowd_dir: путь к директории с parquet-файлами
    '''
    df = pd.DataFrame(names=['date_hour', 'mac_count', 'player_id'])

    for player_dir in pathlib.Path(crowd_dir).glob('player=*'):
        for f in player_dir.glob('*.parquet'):
            player_id = int(f.parent.stem.split('=')[1])
            part = pd.read_parquet(f)

            # Выбираем первые 5 сек через каждого 50-секундного интервала
            ts = pd.to_datetime(part.AddedOnTick, unit='ms')
            intervals = part[(ts.dt.minute * 60 + ts.dt.second) / 50 % 1 < 0.1]

            df.append(
                intervals
                .filter(items=['Mac'])
                .set_index(pd.to_datetime(intervals.AddedOnTick, unit='ms').rename('date_hour'))
                .resample('1h')
                .count()
                .rename(columns={'Mac': 'mac_count'})
                .assign(player_id=player_id)
                .sort_index()
                .reset_index()
            )

    return df


def get_admetrix_data(admetrix_data_path, player_details_path):
    admetrix = pd.read_excel(admetrix_data_path)
    player_details = pd.read_csv(player_details_path, delimiter=';')
    admetrix = admetrix.merge(player_details, left_on='ID экрана', right_on='PlayerNumber')
    months = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]

    def get_month(val):
        month, year = val.split(',')
        return dt.date(int(year), months.index(month) + 1, 1)

    admetrix['month'] = admetrix['Период'].map(get_month)
    return admetrix


def predict_ots(
    df,
    admetrix_data,
    changepoints,
    horizon,
):
    '''
    Расчет прогнозных значений OTS с учетом данных Admetrix и известных дат замены оборудования
    :return dict: возвращаем словарь вида {player_id: {timestamp: ots}}
    '''
    predictions = {}

    for player_id in df.player_id.unique():
        X = (
            df[df.player_id == player_id]
            .rename(columns={'date_hour': 'ds', 'mac_count': 'y'})
            .filter(items=['ds', 'y'])
        )
        X.loc[X.y == 0, 'y'] = np.nan
        X = X.loc[X.y.first_valid_index(): X.y.last_valid_index()]

        player_admetrix = admetrix_data[admetrix_data.PlayerId == player_id].set_index('month')

        def get_admetrix_ots(date):
            try:
                idx = player_admetrix.index.get_loc(pd.Timestamp(date), method='nearest')
            except KeyError:
                return np.nan
            else:
                return player_admetrix.iloc[idx]['OTS среднесуточный']

        if changepoints.get(player_id):
            chp_date = changepoints[player_id][0]
            adjust_ratio = X[X.ds >= chp_date].y.mean() / X[X.ds < chp_date].y.mean()
            X.loc[X.ds < chp_date, 'y'] *= adjust_ratio

        m = Prophet(
            yearly_seasonality=3,
            daily_seasonality=True,
            weekly_seasonality=True,
            holidays=holidays,
            changepoint_prior_scale=0.001,
        )
        X['admetrix'] = X.ds.map(get_admetrix_ots)
        if not X['admetrix'].isnull().any():
            m.add_regressor('admetrix')

        m.fit(X)
        n_hours_to_predict = (pd.Timestamp(horizon) - X.ds.max()).days * 24
        future = m.make_future_dataframe(periods=n_hours_to_predict, freq='H')
        future['admetrix'] = future.ds.map(get_admetrix_ots)

        forecast = m.predict(future)

        # Убираем выбросы
        pred = forecast.iloc[-n_hours_to_predict:][['ds', 'yhat']]
        max_diff = pred.yhat.max() - pred.yhat.min()
        if max_diff > X.y.max() * 2:
            pred['yhat'] *= (X.y.max() * 2) / max_diff
        pred['yhat'] += max(0, -pred.yhat.min())

        result = dict(zip(
            (pred.ds.astype(int) / 10**9).astype(int), # each hour's start timestamp
            pred.yhat.astype(int) # predicted OTS
        ))
        predictions[int(player_id)] = result

    return predictions


if __name__ == '__main__':
    data_dir = pathlib.Path('/home/gallery/data/')
    out_dir = pathlib.Path('/home/gallery/our_data/')

    df = build_df(crowd_dir=data_dir / 'RowData' / 'crowd')

    admetrix_data = get_admetrix_data(
        data_dir / 'admetrix_data.xlsx',
        data_dir / 'player_details.csv',
    )

    holidays = pd.DataFrame({
        'holiday': 'holiday',
        'ds': pd.to_datetime(HOLIDAYS),
    })

    changepoints = {
        333: ['2021-01-27'],
        403: ['2021-01-27'],
        271: ['2021-02-02'],
        274: ['2021-01-27'],
        259: ['2021-02-01'],
        272: ['2021-01-27'],
        258: ['2021-01-27'],
        263: ['2021-01-27'],
        265: ['2021-01-27'],
        270: ['2021-01-27'],
        260: ['2021-02-21'],
        267: ['2021-03-06'],
        262: ['2021-03-11'],
        264: ['2021-03-11'],
        257: ['2021-02-22'],
        261: ['2021-02-22'],
        268: ['2021-02-21'],
        269: ['2021-02-22'],
        266: ['2021-03-12'],
        1572: ['2021-03-13'],
        1548: ['2021-06-28'],
        1549: ['2021-06-28'],
    }

    predictions = predict_ots(
        df=df,
        admetrix_data=admetrix_data,
        changepoints=changepoints,
        horizon='2021-09-30',
    )
    with (out_dir / 'predictions.pkl').open('wb') as f:
        pickle.dump(predictions, f)
