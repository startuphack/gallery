import datetime as dt

import pandas as pd

from utils import DEFAULT_TZ


def schedule_simple(forecast, schedule, screen_ids, desired_ots, start_date, end_date, week_days, hours, frequency):
    days = [start_date + dt.timedelta(days=n) for n in range((end_date - start_date).days)]
    slots = []
    for day in days:
        if day.weekday() not in week_days:
            continue
        for hour in hours:
            start = dt.datetime.combine(day, dt.time(hour))
            slots.append(pd.Timestamp(DEFAULT_TZ.localize(start)))

    mean_hour_ots = float(desired_ots) / len(slots)
    planned = {}
    while sum(planned.values()) < desired_ots:
        ots_so_far = sum(planned.values())

        for n, slot in enumerate(slots, start=1):
            ts = int(slot.timestamp())
            available_screens = [
                screen_id for screen_id in screen_ids
                if schedule[screen_id][slot] >= frequency and
                   (slot, screen_id) not in planned
            ]
            otses = {screen_id: forecast[screen_id][ts] * (frequency / 72) for screen_id in available_screens}

            bigger_otses = {screen_id: ots for screen_id, ots in otses.items() if ots > mean_hour_ots}
            if bigger_otses:
                screen_id = min(bigger_otses, key=bigger_otses.get)
                ots = bigger_otses[screen_id]

            elif otses:
                screen_id = max(otses, key=otses.get)
                ots = otses[screen_id]

            planned[(slot, screen_id)] = ots
            if sum(planned.values()) >= desired_ots:
                break

            if n < len(slots):
                mean_hour_ots = float(desired_ots - sum(planned.values())) / (len(slots) - n)

        if sum(planned.values()) == ots_so_far:
            raise RuntimeError('Not enough slots')

    return planned
