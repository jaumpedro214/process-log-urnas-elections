from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

SPARK_MASTER_URL = "spark://spark:7077"
spark = (
    SparkSession.builder
    .master(SPARK_MASTER_URL)
    .config("spark.executor.memory", "4g")
    .appName("Calculate metrics")
    .getOrCreate()
)
# Reduce number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 5)


def count_votes_by_uf():
    df = (
        spark.read
        .format("parquet")
        .load("/data/votes/*")
    )

    df = (
        df
        .filter(F.col("operation") == "O voto do eleitor foi computado")
        .groupBy("uf", "turno")
        .count()
    )

    df.show(500, False)


if __name__ == "__main__":
    count_votes_by_uf()
