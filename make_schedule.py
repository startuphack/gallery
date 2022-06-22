import itertools as it
import time
import typing
from abc import ABC
from collections import defaultdict
from datetime import datetime
from operator import itemgetter

import more_itertools as mit
import pytz
from ortools.sat.python import cp_model

# Стандартные чстоты показов
STANDARD_FREQUENCIES = frozenset([
    6, 9, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72
])

HOUR_SLOT_COUNT = max(STANDARD_FREQUENCIES)  # число слотов в одном часе

OTS_PER_HOUR_MULTIPLIER = 1 / HOUR_SLOT_COUNT

class AdvertisementRequest(ABC):
    def __init__(
        self,
        screen_ids: typing.Collection,
        desired_ots: int,
        start_date: datetime,
        end_date: datetime,
        week_days: typing.Collection[int],
        hours: typing.Collection[int],
        frequency: int,
    ):
        '''
        :param screen_ids: Идентификаторы экранов
        :param desired_ots: Количество рекламных контактов, которое должно быть набрано
        :param start_date: Дата начала рекламной кампании
        :param end_date: Дата окончания рекламной кампании
        :param week_days: Дни недели пн-0, вс - 6
        :param hours: Идентификаторы часов показа
        :param frequency: Частота показа
        '''
        self.screen_ids = screen_ids
        self.desired_ots = desired_ots
        self.start_date = start_date
        self.end_date = end_date
        self.week_days = week_days
        self.hours = hours
        self.frequency = frequency

class Schedule(ABC):
    '''
    Этот класс представляет собой модуль, отвечающий за расписание
    Предполагается, что есть базовая информация о том, какие слоты заняты другими рекламными кампаниями
    Этот класс умеет строить расписание по требованиям рекламной кампании см make_advertisement_schedule
    Кроме того, нужна информация о временной зоне того места, для которого необходимо построение расписания
    Чем меньше chunk_size, тем точнее будет подобран OTS, но тем больший разброс может быть в частотах.
    Наоборот, рост chunk_size снижает точность подбора OTS, но сглаживает частоты в разных часах
    '''

    def __init__(self, planned_schedule: dict,
                 chunk_size=3,
                 penalty_rate=1e-5,
                 tz=pytz.timezone('Asia/Novosibirsk'),
                 ):
        '''
        :param planned_schedule: текущее расписание активных рекламных кампаний
        :param chunk_size: размер пачки для группировки активных часов строящегося расписания.
            чем он меньше, тем точнее будет подгоняться OTS нового расписания к желаемому, но тем дольше это будет происходить
        :param penalty_rate: уровень штрафа за неравномерность показа рекламы по билбордам. чем больше, тем более равномерно будут распределены показы
        :param tz: временная зона для определения часов
        '''
        self.planned_schedule = planned_schedule
        self.chunk_size = chunk_size
        self.penalty_rate = penalty_rate
        self.tz = tz

    def make_advertisement_schedule(
        self,
        advertisement_requests:typing.List[AdvertisementRequest],
        ots_forecast,  # Сюда надо поставить дикт диктов
    ):
        '''
        :param advertisement_requests: рекламные кампании
        :param ots_forecast: Прогноз кол-ва OTS по скринам и часам
        :return: Расписание показов рекламного ролика и инфа о частоте
        '''
        for req in advertisement_requests:
            if req.frequency not in STANDARD_FREQUENCIES:
                raise ValueError(f'frequency {req.frequency} not supported. possible frequencies are {STANDARD_FREQUENCIES}')

        # plan_screens, plan_timestamps, plan_ots, plan_slots = list(), list(), list(), list()

        available_ots = 0
        all_screen_slots = list()

        all_desired_ots = 0
        # На этом шаге мы выделяем рекламные часовые слоты, для которых мы будем подбирать параметр по частоте.
        for req_id, req in enumerate(advertisement_requests):
            all_desired_ots+=req.desired_ots

            for screen_id in req.screen_ids:
                screen_slots = list()
                planned_ots = self.planned_schedule.get(screen_id, {})
                screen_forecast_data = ots_forecast.get(screen_id)
                if screen_forecast_data is None:
                    raise ValueError(f'нет плана для экрана {screen_id}')

                for screen_forecast_ts, screen_forecast_ots in sorted(screen_forecast_data.items()):
                    screen_forecast_dt = datetime.fromtimestamp(screen_forecast_ts, tz=self.tz)
                    # print(start_date, end_date, screen_forecast_dt, screen_forecast_dt.hour, screen_forecast_dt.weekday(), start_date <= screen_forecast_dt < end_date)
                    if (
                        (req.start_date <= screen_forecast_dt < req.end_date)
                        and (screen_forecast_dt.hour in req.hours)
                        and (screen_forecast_dt.weekday() in req.week_days)
                    ):
                        # столько слотов осталось
                        remains = planned_ots.get(screen_forecast_dt, HOUR_SLOT_COUNT)

                        all_screen_slots.append({
                            'request': req,
                            'req_id': req_id,
                            'screen': screen_id,
                            'hour_ts': screen_forecast_ts,
                            'forecast_ots': screen_forecast_ots,
                            'remains_slots': min(remains, req.frequency),
                            'hour': screen_forecast_dt.hour,
                            'week-day': screen_forecast_dt.weekday(),
                            'date': screen_forecast_dt,
                        })

        # Запускаем целочисленную линейную оптимизацию для поиска частот показов на экранах
        ns_start = time.time_ns()
        slot_list = self.do_mip_optimization_on_frequencies(all_screen_slots)
        ns_stop = time.time_ns()

        # Здесь у нас уже есть вся инфа о том, когда, на каком экране, и на сколько слотов показывать рекламу.
        # Можем сформировать расписание и уточнить OTS
        schedule = defaultdict(dict)
        result_ots = 0
        for screen_chunk_event in slot_list:
            screen_id = screen_chunk_event['screen']
            hour = screen_chunk_event['hour_ts']
            # remains_slots = screen_chunk_event['remains_slots']
            slots = screen_chunk_event['target_slots']  # необходимое число слотов, которое мы должны занять

            forecast_ots = screen_chunk_event['forecast_ots']  # прогнозный часовой OTS

            # пересчитываем OTS на число слотов и добавляем к общей сумме
            result_ots += forecast_ots * slots * OTS_PER_HOUR_MULTIPLIER

            schedule[screen_id][hour] = {
                'slots': slots,
                'ots': forecast_ots * slots * OTS_PER_HOUR_MULTIPLIER
            }

        return {
            'schedule': schedule,
            'ots-forecast': round(result_ots),
            'optimization-time-ms': (ns_stop - ns_start) / 1e6,
        }

    def do_mip_optimization_on_frequencies(self, all_screens):
        '''
        Здесь мы формулируем задачу целочисленного линейного программирования с ограничениями
        Если мы останемся в условиях линейного программирования, мы сохраняем полиномиальную сложность в среднем случае
        :param all_screens: информация о всех доступных слотах всех экранах
        :param desired_ots: требуемое кол-во OTS от рекламной кампании
        :return: информация о всех слотах, которые мы должны занять
        '''

        chunk_groups = list()
        # мы группируем все часовые интервалы, где можем разместить рекламу по числу оставшихся слотов
        for req_id, slot_size, slot_group in it.groupby(
            sorted(all_screens, key=itemgetter('req_id','remains_slots')),
            itemgetter('req_id','remains_slots')
        ):
            # каждую группу разбиваем на чанки. это нужно для того, чтобы сократить размерность задачи
            group_chunks = mit.chunked(slot_group, self.chunk_size)
            chunk_groups.extend(group_chunks)

        x = list()  # параметры задачи - сколько слотов в час занимаем
        penalties = list()  # штрафы задачи - насколько мы отклонямся от желаемого числа слотов
        objectives = list()  # данные OTS по занятым рекламным слотам

        model = cp_model.CpModel()
        for req_id, req_group in it.groupby(chunk_groups, lambda x: x[0]['req_id']):
            req_group = list(req_group)

            for group_num, chunk_group in enumerate(req_group):
                # у нас есть группировка по доступным слотам.
                # для каждой группы у нас есть 1 параметр - число показов в час
                # число OTS для группы в этом случае будет равно
                # OTS_GROUP = OTS1 * SLOTS1 / 72 + ... + OTSn*SLOTSn / 72
                # но SLOTS1==...==SLOTSn = SLOTS. поэтому
                # OTS_GROUP = (OTS1 + ... + OTSn) * SLOTS / 72
                num_slots = chunk_group[0]['remains_slots']
                # (OTS1 + ... + OTSn)
                group_total_ots = sum(group['forecast_ots'] for group in chunk_group)
                # домен у нас состоит из допустимых стандартных частот + мы можем занять полностью текущий оставшийся слот
                domain = cp_model.Domain.FromIntervals(
                    [[v, v] for v in STANDARD_FREQUENCIES if v < num_slots]
                    + [[num_slots, num_slots]]
                )

                x_var = model.NewIntVarFromDomain(domain, f'{num_slots};{group_total_ots};{group_num}')
                x.append(x_var)
                # добавляем штраф
                penalty = (num_slots - x_var)
                # Чтобы не выходить за границы целочисленной оптимизации, делим все переменные на OTS_PER_HOUR_MULTIPLIER
                # Линейная задача от этого не изменится
                chunk_obj = x_var * group_total_ots  # * OTS_PER_HOUR_MULTIPLIER
                penalties.append(penalty)
                objectives.append(chunk_obj)

            # Нам нужно, чтобы OTS >= desired_ots, но у нас все objectives поделены на OTS_PER_HOUR_MULTIPLIER,
            # Значит и констрейнт нужно поправить как OTS/OTS_PER_HOUR_MULTIPLIER >= desired_ots/OTS_PER_HOUR_MULTIPLIER
            model.Add(sum(objectives) >= desired_ots * int(1 / OTS_PER_HOUR_MULTIPLIER))

        # Наша задача - минимизировать излишки, не слишком сильно отступая от желаемых параметров по частотам
        # Делим задачу на penalty_rate. иначе выходим за границу линейной целочисленной задачи
        model.Minimize(
            sum(objectives) * int(1 / self.penalty_rate) + sum(penalties))  # нам нужно минимизировать кол-во ОТС

        solver = cp_model.CpSolver()
        status = solver.Solve(model)

        # Если найдено оптимальное решение, проставляем параметры по числу слотов, которое нужно занять рекламным блоком
        if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
            chunk_result = list()
            for chunk_var, chunk_group in zip(x, chunk_groups):
                chunk_result.extend(dict(**event, target_slots=solver.Value(chunk_var)) for event in chunk_group)

            return chunk_result
        else:
            return None
