import re
from datetime import datetime
from typing import Union

def keep_ascii_only(text):
    return re.sub(r'[^\x00-\x7F]+', '_', text)

def sanitize_id(id: str):
    return keep_ascii_only(re.sub("[^a-zA-Z0-9\-_]", "_", id.replace("$", "S")))

class MissingEnvironmentVariable(Exception):
    pass

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
    return datetime.fromtimestamp(datetime.now().timestamp())

sl_schedule_format = '%Y%m%dT%H%M'

def sl_schedule(cron: str, start_time: datetime = cron_start_time(), format: str = sl_schedule_format) -> str:
    from croniter import croniter
    return croniter(cron, start_time).get_prev(datetime).strftime(format)