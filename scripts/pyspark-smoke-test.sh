#!/usr/bin/env bash
# Verify that the docker image's python interpreter can run a PySpark job
# against the bundled Spark distribution, the same way Spark's PythonRunner
# does at runtime (python3 + $SPARK_HOME/python/lib zips on PYTHONPATH).
# Run inside the image: docker run --entrypoint bash <image> /pyspark-smoke-test.sh
set -euo pipefail

export SPARK_HOME="${SPARK_HOME:-/app/bin/spark}"
PY4J_ZIP="$(ls "$SPARK_HOME"/python/lib/py4j-*-src.zip)"
export PYTHONPATH="$SPARK_HOME/python/lib/pyspark.zip:$PY4J_ZIP${PYTHONPATH:+:$PYTHONPATH}"

python3 - <<'EOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("pyspark-smoke").getOrCreate()
df = spark.sql("select 1 as ok")
df.createOrReplaceTempView("SL_THIS")
assert df.collect()[0].ok == 1
spark.stop()
print("PYSPARK SMOKE OK")
EOF
