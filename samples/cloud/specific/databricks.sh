COMET_SINK_TO_FILE=false
COMET_ASSERTIONS_ACTIVE=true
PYSPARK_PYTHON=/databricks/python3/bin/python3
COMET_GROUPED=false
COMET_METRICS_PATH=/mnt/starlake-app/tmp/quickstart/metrics/{domain}
COMET_METRICS_ACTIVE=true
COMET_ROOT=/mnt/starlake-app/tmp/quickstart
COMET_AUDIT_SINK_TYPE=BigQuerySink
COMET_FS=dbfs://
COMET_ANALYZE=false
COMET_ENV=BQ
COMET_HIVE=true
TEMPORARY_GCS_BUCKET=starlake-app

spark-submit
["--class", "ai.starlake.job.Main", "--jars", "/dbfs/mnt/starlake-app/jackson-dataformat-yaml-2.12.3.jar", "/dbfs/mnt/starlake-app/jackson-dataformat-yaml-2.12.3.jar", "import"]


bucket_name = "starlake-app"
mount_name = "starlake-app"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))


spark.hadoop.google.cloud.auth.service.account.enable true
spark.hadoop.fs.gs.auth.service.account.email hayssam-saleh@starlake-325712.iam.gserviceaccount.com
spark.hadoop.fs.gs.project.id starlake-325712
spark.hadoop.fs.gs.auth.service.account.private.key -----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCyH+tZDAl5nJjb\ng1HD7jOBChcXZwZ9SnsCHZ9Wrg/cWLiFWu5PY/dEmC3U9oebvUIlzS2WbO8XqE+i\nRL+1qVTOPL0wp9UlEwQ86PMKGYKFu0gR1NmWALt2IJrlCGPyM1yZVPxhBLgwoQOO\nyYpEAHen+Xa63CdMtqRKe/uObVxp1ZjJtIurbK1I7OgFmP5t9sutwNE2ZUbEoCix\nh3q0ZWprdkwpP4KuH13h8bU6SDBph+YT3m0BzJ5RCHjySQnB8ZtZhfh4QcqA6+Mg\ncT/FhWp3pI8MQHczejZtJpjPpH1WricEHHj9rFgbEnjbn8ewUnCYO/0yB5x6ejgV\nV9F+BWObAgMBAAECggEALG4Pyy9YcbAg5Kg1zfRtfmNg7SJymO/qDNYMt5dFN4Yd\nyI2s4bqio8Z9sCAqJSupAzrRgzL61GDUNDqHwy1QjPzTh/5lCRFcPkEw1jUUvHzB\ndkksWOEn4UgyuqxEGda74zcymJSPyPlwpL4asemX7xsnhWaCmvz3r+iLD7Sxl2gY\nFziiIPC7ImkIFg1HD8wj8tgaV8lzGeg7V/ZxePQ/aPZbb//NUqwLsMQTTEYJoerP\nA9bhFouolWAyVRWLA0KZ2JEXEk0/JX6JM1Px7wWBOPmIwEtGtd6NYKcta8zuvZlh\n0m1BUUz6NKd8gDX32XrUuImXYmvoz3kzNo055eDCeQKBgQDmJD0Xm3MeOdSMIo6a\nW28PmR1pajCJ3+s+GVjudzxPXSCkpF0HFLtg9+B/CB6xCVXeIP5xWYcxxlppvB8Y\n0y0ONbSEe/uTQz/dIc+tS83H+X1R7lsLgSrHmhJF9+jbot9QS9xJjboPafket4I1\ndLjRl75vEacRYgefuUuKAkNvXQKBgQDGI3lnypYBNIVU5qsIfUiknyNE+IMusRG7\nQAhwd4P2ICThuRu3ZhDXZSFLPO5hkyW5GrNuMMlcTSdeguLLad1DAEkqpE4V/31Y\nOre6PNavd7yAgYrrW4g2qco7tnL5iHVJQzw6K+3IEDWR1YZCg7iYGwemRklIuIsb\nyQWFx1sHVwKBgQDCQLJEO9SmHZ/3DLAbP7P1kYLO2G/R4Gv6RYDX/1KrlmqYLZfM\nA0bJ6U/XSW5Gdh/BV04NNMk1TTxBZSVGWfD8vn35GYFWYnwEVaaqmoI+Gasavbqh\nckw+oCBuaHtm8AnYB8APYY47tnIs6C4CmtvpJVD7BQZkWL8gpVFKUvDCjQKBgQCU\nr7I/bQs8REl5+M+IQ1vsDW/OJh9rPn1r4xVyMH3aiSykJuDhs7oXqVBewY6xslaw\nnZTgr8OrfEp65gWDWCIuUVyWn03pvBw3xXhyTY1dh6DxXkT9cWa6fpfAT53gG8LI\nA6iCjsyVQXSxx2ZFK8uueTo4UK5V4AakByoZxgxNaQKBgQCAVPuSyQ15afnf6Org\nrlMWDE8hOs54TOU2avYGPhzrRUX0H51tjiiDDD0BqiOuINfqjOo7El/eaNYk91Qo\nmcJCJdSREM7I5/cwss8DUAmhqhY63OjAoHC3n6DaxQ7qyKR4404D3s+0mYryfl7y\nNplx9S8J7xHWRgni344ou1cXCg==\n-----END PRIVATE KEY-----\n
spark.hadoop.fs.gs.auth.service.account.private.key.id df728e47e5e6c14402fafe6d39a3b8792a9967c7

