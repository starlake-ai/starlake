import sys
from random import random
from operator import add

from pyspark.sql import SparkSession

# From Apache Spark example
if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession \
        .builder \
        .getOrCreate()

    partitions = 2
    n = 100000 * partitions

    def f(_: int) -> float:
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    result = "Pi is roughly %f" % (4.0 * count / n)
    df = spark.createDataFrame([[result]])
    df.registerTempTable("SL_THIS")
    print(result)

