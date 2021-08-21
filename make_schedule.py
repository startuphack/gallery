import math
from datetime import datetime
import pytz
import itertools as it
from operator import itemgetter
from collections import defaultdict
from abc import ABC
import typing
import more_itertools as mit
from ortools.sat.python import cp_model
import logging

# Стандартные чстоты показов
STANDARD_FREQUENCIES = frozenset([
    6, 9, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72
])

HOUR_SLOT_COUNT = max(STANDARD_FREQUENCIES)  # число слотов в одном часе

OTS_PER_HOUR_MULTIPLIER = 1 / HOUR_SLOT_COUNT


class Schedule(ABC):
    def __init__(self, planned_schedule: dict,
                 optimization_mode='frequencies',
                 chunk_size=7,
                 penalty_rate=1e-5,
                 tz=pytz.timezone('Asia/Novosibirsk'),
                 ):
        self.planned_schedule = planned_schedule
        self.optimization_mode = optimization_mode
        self.chunk_size = chunk_size
        self.penalty_rate = penalty_rate
        self.tz = tz

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

            for screen_forecast_ts, screen_forecast_ots in sorted(screen_forecast_data.items()):
                screen_forecast_dt = datetime.fromtimestamp(screen_forecast_ts, tz=self.tz)
                logging.info(screen_forecast_dt)
                if (
                    (start_date <= screen_forecast_dt < end_date)
                    and (screen_forecast_dt.hour in hours)
                    and (screen_forecast_dt.weekday() in week_days)
                ):
                    # столько слотов осталось
                    remains = planned_ots.get(screen_forecast_dt, HOUR_SLOT_COUNT)

                    screen_slots.append({
                        'screen': screen_id,
                        'hour_ts': screen_forecast_ts,
                        'forecast_ots': screen_forecast_ots,
                        'remains_slots': min(remains, frequency),
                        'hour': screen_forecast_dt.hour,
                        'week-day': screen_forecast_dt.weekday(),
                        'date': screen_forecast_dt,
                    })

                    available_ots += screen_forecast_ots * remains * OTS_PER_HOUR_MULTIPLIER  # OTS/час * число оставшихся слотов / 72 слота в час

            all_screens.append(screen_slots)

        if available_ots < desired_ots:
            return {
                'schedule': None,
                'ots-forecast': available_ots,
            }
        elif self.optimization_mode == 'frequencies':
            slot_list = self.do_mip_optimization_on_frequencies(all_screens, desired_ots)
            schedule = defaultdict(dict)
            result_ots = 0
            for screen_chunk_event in slot_list:
                screen_id = screen_chunk_event['screen']
                hour = screen_chunk_event['hour_ts']
                # remains_slots = screen_chunk_event['remains_slots']
                slots = screen_chunk_event['target_slots']

                forecast_ots = screen_chunk_event['forecast_ots']
                result_ots += forecast_ots * slots * OTS_PER_HOUR_MULTIPLIER

                schedule[screen_id][hour] = {
                    'slots': slots,
                    'ots': forecast_ots * slots * OTS_PER_HOUR_MULTIPLIER
                }

            return {
                'schedule': schedule,
                'ots-forecast': math.round(result_ots)
            }
        else:
            raise ValueError(f'unsupported optimization mode {self.optimization_mode}')

    def do_mip_optimization_on_frequencies(self, all_screens, desired_ots):

        chunk_groups = list()
        for slot_size, slot_group in it.groupby(
            sorted(it.chain.from_iterable(all_screens), key=itemgetter('remains_slots')),
            itemgetter('remains_slots')
        ):
            group_chunks = mit.chunked(slot_group, self.chunk_size)
            chunk_groups.extend(group_chunks)

        x = list()
        penalties = list()
        objectives = list()

        model = cp_model.CpModel()
        for chunk_group in chunk_groups:
            num_slots = chunk_group[0]['remains_slots']
            group_total_ots = sum(group['forecast_ots'] for group in chunk_group)
            domain = cp_model.Domain.FromIntervals(
                [[v, v] for v in STANDARD_FREQUENCIES if v < num_slots]
                + [[num_slots, num_slots]]
            )

            x_var = model.NewIntVarFromDomain(domain, f'{num_slots};{group_total_ots}')
            x.append(x_var)
            penalty = (num_slots - x_var)
            chunk_obj = x_var * group_total_ots  # * OTS_PER_HOUR_MULTIPLIER
            penalties.append(penalty)
            objectives.append(chunk_obj)

        model.Add(sum(objectives) >= desired_ots * int(
            1 / OTS_PER_HOUR_MULTIPLIER))  # нам нужно набрать не меньше желаемого кол-ва ОТС

        model.Minimize(
            sum(objectives) * int(1 / self.penalty_rate) + sum(penalties))  # нам нужно минимизировать кол-во ОТС

        solver = cp_model.CpSolver()
        status = solver.Solve(model)

        # Print solution.
        if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
            chunk_result = list()
            for chunk_var, chunk_group in zip(x, chunk_groups):
                chunk_result.extend(dict(**event, target_slots=solver.Value(chunk_var)) for event in chunk_group)

            return chunk_result
        else:
            return None
