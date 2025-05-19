import re

from croniter import croniter
from croniter.croniter import CroniterBadCronError

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

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
        """Returns an instance of StarlakeCronPeriod if the value is valid, otherwise raise a ValueError exception."""
        try:
            return cls(value.lower())
        except ValueError:
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

def sl_schedule(cron: str, start_time: Optional[Union[str, datetime]] = None, format: str = sl_schedule_format) -> str:
    from croniter import croniter
    if start_time is None:
        start_time = cron_start_time()
    elif isinstance(start_time, str):
        from dateutil import parser
        import pytz
        start_time = parser.isoparse(start_time).astimezone(pytz.timezone('UTC'))
    return croniter(cron, start_time).get_prev(datetime).strftime(format)

def get_cron_frequency(cron_expression) -> timedelta:
    """
    Calculate the timedelta between 2 executions of a cron expression.
    :param cron_expression: A string representing the cron expression.
    :raise ValueError: If the cron expression is invalid.
    :return: The timedelta between 2 executions of the cron expression.
    """
    from croniter import croniter
    if not is_valid_cron(cron_expression):
        raise ValueError(f"Invalid cron expression: {cron_expression}")
    iter = croniter(cron_expression)
    next_run = iter.get_next(datetime)
    next_run_2 = iter.get_next(datetime)
    return next_run_2 - next_run

def sort_crons_by_frequency(cron_expressions, reference_time: Optional[datetime] = None) -> Tuple[Dict[int, List[str]], List[str]]:
    """
    Sort cron expressions by their frequency.

    :param cron_expressions: A list of cron expressions.
    :param reference_time: The optional reference date time for calculating next executions. If None, Defaults to today at 00:00:00 UTC.
    :raise ValueError: If the cron expression is invalid.
    :return: A tuple of (cron expressions grouped by frequency, list of cron expressions ordered by frequency in ascending order). For each frequency, sort the list of cron expressions based on the next execution date, from the furthest to the soonest
    """
    if reference_time is None:
        from datetime import date, time
        import pytz
        # Today at midnight UTC
        today_utc_midnight = datetime.combine(
            date.today(),
            time(0, 0, 0)
        )
        reference_time = datetime.fromtimestamp(today_utc_midnight.timestamp()).astimezone(pytz.timezone('UTC'))

    pairs = [(expr, int(get_cron_frequency(expr).total_seconds())) for expr in cron_expressions]
    from collections import defaultdict

    # Create a dictionary to store cron expressions by their frequency
    grouped_crons = defaultdict(list)

    for expr, freq in pairs:
        grouped_crons[freq].append(expr)

    sorted_groups = {}

    for freq, cron_list in grouped_crons.items():
        # For each cron expression, compute its next execution datetime
        next_times = [
            (cron_expr, croniter(cron_expr, reference_time).get_next(datetime))
            for cron_expr in cron_list
        ]
        # Sort by datetime descending (furthest first)
        next_times.sort(key=lambda pair: pair[1], reverse=True)
        # Rebuild the list of cron expressions in that order
        sorted_groups[freq] = [cron_expr for cron_expr, _ in next_times]

    flattened: List[str] = []

    # Iterate frequencies from smallest to largest timedelata
    for freq in sorted(sorted_groups.keys(), reverse=False):
        for expr in sorted_groups[freq]:
            flattened.append(expr)
    return (sorted_groups, flattened)

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

def sl_scheduled_date(cron: Optional[str], ts: Union[datetime, str], previous: bool=False) -> datetime:
    """
    Returns the scheduled date for a cron expression and timestamp or the timestamp itself if no cron expression is provided.
    Args:
        cron (str): The optional cron expression.
        ts (Union[datetime, str]): The timestamp.
        previous (bool): If True, returns the previous scheduled date. Defaults to False.
    """
    try:
        if isinstance(ts, str):
            # Convert ts to a datetime object
            from dateutil import parser
            import pytz
            start_time = parser.isoparse(ts).astimezone(pytz.timezone('UTC'))
        elif isinstance(ts, datetime):
            start_time = ts
        if cron and not is_valid_cron(cron):
            raise ValueError(f"Invalid cron expression: {cron}")
        elif cron:
            if previous:
                return croniter(cron, start_time).get_prev(datetime)
            else:
                return croniter(cron, start_time).get_current(datetime)
        else:
            return start_time
    except Exception as e:
        print(f"Error converting timestamp to datetime: {e}")
        raise e

def sl_scheduled_dataset(dataset: str, cron: Optional[str], ts:  Union[datetime, str], parameter_name: str = 'sl_schedule', format: str = sl_timestamp_format, previous: bool=False) -> str:
    """
    Returns the dataset url with the schedule parameter added if a cron expression has been provided.
    Args:
        dataset (str): The dataset name.
        cron (str): The optional cron expression.
        ts (Union[datetime, str]): The timestamp.
        parameter_name (str): The parameter name. Defaults to 'sl_schedule'.
        format (str): The format to return the schedule in. Defaults to '%Y%m%dT%H%M'.
    """
    if cron:
        scheduled_date = sl_scheduled_date(cron, ts, previous)
        parameters = dict()
        parameters[parameter_name] = scheduled_date.strftime(format)
        return f"{sanitize_id(dataset).lower()}{asQueryParameters(parameters)}"
    return sanitize_id(dataset).lower()

def is_valid_cron(cron_expr: str) -> bool:
    try:
        # Attempt to instantiate a croniter object
        croniter(cron_expr)
        return True
    except (CroniterBadCronError, ValueError, AttributeError) as e:
        # Handle the exception if the cron expression is invalid
        print(f"Invalid cron expression: {cron_expr}. Error: {e}")
        # Return False if the cron expression is invalid
        return False
