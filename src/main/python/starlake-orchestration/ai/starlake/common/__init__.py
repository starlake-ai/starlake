import re
from datetime import datetime

def keep_ascii_only(text):
    return re.sub(r'[^\x00-\x7F]+', '_', text)

def sanitize_id(id: str):
    return keep_ascii_only(re.sub("[^a-zA-Z0-9\-_]", "_", id.replace("$", "S")))

class MissingEnvironmentVariable(Exception):
    pass

TODAY = datetime.today().strftime('%Y-%m-%d')
