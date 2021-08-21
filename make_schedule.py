from datetime import datetime
from collections import defaultdict
from abc import ABC
import typing
import more_itertools as mit
from ortools.sat.python import cp_model

HOUR_SLOT_COUNT = 72  # число слотов в одном часе

OTS_PER_HOUR_MULTIPLIER = 1 / HOUR_SLOT_COUNT

# Стандартные чстоты показов
STANDARD_FREQUENCIES = frozenset([
    6, 9, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72
])


# def slots_to_ots(ots_per_hour):
#     return ots_per_hour/HOUR_SLOT_COUNT

class Schedule(ABC):
    def __init__(self, planned_schedule: dict):
        self.planned_schedule = planned_schedule

    def make_advertisement_schedule(
        self,
        screen_ids: typing.Collection,
        desired_ots: int,
        start_date: datetime,
        end_date: datetime,
        week_days: typing.Collection[int],
        hours: typing.Collection[int],
        frequency: int,
        ots_forecast,  # Сюда надо поставить дикт диктов
        chunk_size=7,
    ):
        '''
        :param screen_ids: Идентификаторы экранов
        :param desired_ots: Количество рекламных контактов, которое должно быть набрано
        :param start_date: Дата начала рекламной кампании
        :param end_date: Дата окончания рекламной кампании
        :param week_days: Дни недели пн-0, вс - 6
        :param hours: Идентификаторы часов показа
        :param frequency: Частота показа
        :param ots_forecast: Прогноз кол-ва OTS по скринам и часам
        :param chunk_size: Число дней в одной пачке численной оптимизации
        :return: Расписание показов рекламного ролика и инфа о частоте
        '''
        if frequency not in STANDARD_FREQUENCIES:
            raise ValueError(f'frequency {frequency} not supported. possible frequencies are {STANDARD_FREQUENCIES}')

        # plan_screens, plan_timestamps, plan_ots, plan_slots = list(), list(), list(), list()

        available_ots = 0
        all_screens = list()
        for screen_id in screen_ids:
            screen_slots = list()
            planned_ots = self.planned_schedule.get(screen_id, {})
            screen_forecast_data = ots_forecast.get(screen_id)
            if screen_forecast_data is None:
                raise ValueError(f'нет плана для экрана {screen_id}')

            for screen_forecast_ts, screen_forecast_ots in screen_forecast_data.items():
                screen_forecast_dt = datetime.fromtimestamp(screen_forecast_ts)
                if (
                    (start_date <= screen_forecast_dt < end_date)
                    and (screen_forecast_dt.hour in hours)
                    and (screen_forecast_dt.weekday() in week_days)
                ):
                    planned_screen_date_slot_count = planned_ots.get(screen_forecast_ts)

                    remains = HOUR_SLOT_COUNT - planned_screen_date_slot_count  # столько слотов осталось

                    screen_slots.append({
                        'screen': screen_id,
                        'hour_ts': screen_forecast_ts,
                        'forecast_ots': screen_forecast_ots,
                        'remains_slots': max(remains, frequency),
                    })

                    available_ots += screen_forecast_ots * remains * OTS_PER_HOUR_MULTIPLIER  # OTS/час * число оставшихся слотов / 72 слота в час

            all_screens.append(screen_slots)

        if available_ots < desired_ots:
            return {
                'schedule': None,
                'ots-forecast': available_ots,
            }

        else:
            chunked_screens = [mit.chunked(screen_slots, chunk_size) for screen_slots in all_screens]

            chunk_matrix, result_ots = self.do_mip_optimization(chunked_screens, desired_ots)

            schedule = defaultdict(dict)

            for screen in chunk_matrix:
                for screen_chunk in screen:
                    for screen_chunk_event in screen_chunk:
                        screen_id = screen_chunk_event['screen']
                        hour = screen_chunk_event['hour_ts']
                        slots = screen_chunk_event['remains_slots']
                        schedule[screen_id][hour] = slots

            return {
                'schedule': schedule,
                'ots-forecast': result_ots
            }

    def do_mip_optimization(self, chunked_screens, desired_ots):
        num_screens = len(chunked_screens)
        num_chunks = len(chunked_screens[0])
        model = cp_model.CpModel()

        x = []
        ots_chunks = []
        for i, screen_data in enumerate(chunked_screens):
            t = []
            screen_ots_data_by_chunks = []
            for j, screen_event in enumerate(screen_data):
                t.append(model.NewBoolVar(f'x[{i},{j}]'))
                chunk_ots_count = 0
                for chunk_event in screen_event:
                    chunk_ots_count += chunk_event['forecast_ots'] * chunk_event[
                        'remains_slots'] * OTS_PER_HOUR_MULTIPLIER

                screen_ots_data_by_chunks.append(chunk_ots_count)

            x.append(t)
            ots_chunks.append(screen_ots_data_by_chunks)

        # Objective
        objective_terms = []
        for i in range(num_screens):
            for j in range(num_chunks):
                objective_terms.append(ots_chunks[i][j] * x[i][j])

        # показываем рекламу все время хотя бы на одном билборде
        for j in range(num_chunks):
            model.Add(sum(x[i][j] for i in range(num_screens)) >= 1)
        model.Add(sum(objective_terms) >= desired_ots)  # нам нужно набрать не меньше желаемого кол-ва ОТС

        model.Minimize(sum(objective_terms))  # нам нужно минимизировать кол-во ОТС

        solver = cp_model.CpSolver()
        status = solver.Solve(model)

        # Print solution.
        if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
            result_ots = solver.ObjectiveValue()

            chunk_matrix = []

            for i, screen_data in enumerate(chunked_screens):
                screen_values = []
                for j, screen_chunk in enumerate(screen_data):
                    chunk_decision = solver.BooleanValue(x[i][j])
                    if chunk_decision:
                        screen_values.append(screen_chunk)
                chunk_matrix.append(screen_values)
                return chunk_matrix, result_ots
        else:
            return None


if __name__ == '__main__':
    Schedule(None).make_advertisement_schedule(['1'], 100, datetime(2010, 1, 1), datetime(2010, 2, 1), [1, 2, 3],
                                               [1, 2, 3], 5, None)
