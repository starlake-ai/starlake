import re

from croniter import croniter
from croniter.croniter import CroniterBadCronError

from datetime import datetime, timedelta
from typing import Optional, Union

def keep_ascii_only(text):
    return re.sub(r'[^\x00-\x7F]+', '_', text)

def sanitize_id(id: str):
    return keep_ascii_only(re.sub("[^a-zA-Z0-9\-_]", "_", id.replace("$", "S")))

class MissingEnvironmentVariable(Exception):
    pass

from enum import Enum

class StarlakeCronPeriod(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"

    def __str__(self):
        return self.value

    @classmethod
    def from_str(cls, value: str):
        if value.lower() == 'day':
            return cls.DAY
        elif value.lower() == 'week':
            return cls.WEEK
        elif value.lower() == 'month':
            return cls.MONTH
        elif value.lower() == 'year':
            return cls.YEAR
        else:
            raise ValueError(f"Unsupported cron period: {value}")

TODAY = datetime.today().strftime('%Y-%m-%d')

def asQueryParameters(parameters: Union[dict,None]=None) -> str:
    from urllib.parse import quote
    if parameters is None:
        parameters = dict()
    if parameters.__len__() > 0:
        return '?' + '&'.join(list(f'{quote(k)}={quote(v)}' for (k, v) in parameters.items()))
    else:
        return ''

def cron_start_time() -> datetime:
    import pytz
    return datetime.fromtimestamp(datetime.now().timestamp()).astimezone(pytz.timezone('UTC'))

sl_schedule_format = '%Y%m%dT%H%M'

def sl_schedule(cron: str, start_time: datetime = cron_start_time(), format: str = sl_schedule_format) -> str:
    from croniter import croniter
    return croniter(cron, start_time).get_prev(datetime).strftime(format)

def get_cron_frequency(cron_expression, start_time: datetime, period=StarlakeCronPeriod.DAY):
    """
    Calculate the frequency of a cron expression within a specific time period.

    :param cron_expression: A string representing the cron expression.
    :param start_time: The starting datetime to evaluate from.
    :param period: The time period ('day', 'week', 'month', 'year') over which to calculate frequency.
    :return: The frequency of runs in the given period.
    """
    from croniter import croniter
    iter = croniter(cron_expression, start_time)
    end_time = start_time

    if period == StarlakeCronPeriod.DAY:
        end_time += timedelta(days=1)
    elif period == StarlakeCronPeriod.WEEK:
        end_time += timedelta(weeks=1)
    elif period == StarlakeCronPeriod.MONTH:
        end_time += timedelta(days=30)  # Approximate a month
    elif period == StarlakeCronPeriod.YEAR:
        end_time += timedelta(days=365)
    else:
        raise ValueError("Unsupported period. Choose from 'day', 'week', 'month', 'year.")

    frequency = 0
    while True:
        next_run = iter.get_next(datetime)
        if next_run < end_time:
            frequency += 1
        else:
            break
    return frequency

def sort_crons_by_frequency(cron_expressions, period=StarlakeCronPeriod.DAY):
    """
    Sort cron expressions by their frequency.

    :param cron_expressions: A list of cron expressions.
    :param period: The period over which to calculate frequency ('day', 'week', 'month', 'year').
    :return: A sorted list of cron expressions by frequency (most frequent first).
    """
    start_time = cron_start_time()
    frequencies = [(expr, get_cron_frequency(expr, start_time, period)) for expr in cron_expressions]
    # Sort by frequency in descending order
    sorted_expressions = sorted(frequencies, key=lambda x: x[1], reverse=True)
    return sorted_expressions

sl_timestamp_format = '%Y-%m-%d %H:%M:%S%z'

def sl_cron_start_end_dates(cron_expr: str, start_time: datetime = cron_start_time(), format: str = sl_timestamp_format) -> str:
    """
    Returns the start and end dates for a cron expression.

    :param cron_expr: The cron expression.
    :param start_time: The start time.
    :param format: The format to return the dates in.
    """
    from croniter import croniter
    iter = croniter(cron_expr, start_time)
    curr = iter.get_current(datetime)
    previous = iter.get_prev(datetime)
    next = croniter(cron_expr, previous).get_next(datetime)
    if curr == next :
        sl_end_date = curr
    else:
        sl_end_date = previous
    sl_start_date = croniter(cron_expr, sl_end_date).get_prev(datetime)
    return f"sl_start_date='{sl_start_date.strftime(format)}',sl_end_date='{sl_end_date.strftime(format)}'"

def sl_scheduled_dataset(dataset: str, cron: Optional[str], ts: str, parameter_name: str = 'sl_schedule', format: str = sl_timestamp_format, previous: bool=False) -> str:
    """
    Returns the dataset url with the schedule parameter added if a cron expression has been provided.
    Args:
        dataset (str): The dataset name.
        cron (str): The optional cron expression.
        ts (str): The timestamp.
        parameter_name (str): The parameter name. Defaults to 'sl_schedule'.
        format (str): The format to return the schedule in. Defaults to '%Y%m%dT%H%M'.
    """
    if cron:
        if not is_valid_cron(cron):
            raise ValueError(f"Invalid cron expression: {cron} for dataset {dataset}")
        try:
            # Convert ts to a datetime object
            from dateutil import parser
            import pytz
            start_time = parser.isoparse(ts).astimezone(pytz.timezone('UTC'))
            parameters = dict()
            if previous:
                parameters[parameter_name] = croniter(cron, start_time).get_prev(datetime).strftime(format)
            else:
                parameters[parameter_name] = croniter(cron, start_time).get_current(datetime).strftime(format)
            return f"{sanitize_id(dataset).lower()}{asQueryParameters(parameters)}"
        except Exception as e:
            print(f"Error converting timestamp to datetime: {e}")
            # return sanitize_id(dataset).lower()
            raise e
    return sanitize_id(dataset).lower()

def is_valid_cron(cron_expr: str) -> bool:
    try:
        # Attempt to instantiate a croniter object
        croniter(cron_expr)
        return True
    except (CroniterBadCronError, ValueError):
        return False
