
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

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


# Compute THE MAX TIME POSSIBLE as the difference
# between the first and last timestamps
# and compute the TIME CALCULATED as the sum of the time spent in each stage
# MAX TIME POSSIBLE should be greater than TIME CALCULATED (IN EACH SECTION)

def max_time_possible():

    df = (
        spark.read
        .format("parquet")
        .load("/data/votos_estatisticas/*")
    )

    df = (
        df
        .select(
            "DT_FIM_VOTO", "DT_INICIO_VOTO", "TEMPO_TOTAL",
            "NR_TURNO", "SG_UF", "NR_ZONA", "NR_SECAO"
        )
        .groupBy("NR_TURNO", "SG_UF", "NR_ZONA", "NR_SECAO")
        .agg(
            F.max("DT_FIM_VOTO").alias("DT_FIM_VOTO"),
            F.min("DT_INICIO_VOTO").alias("DT_INICIO_VOTO"),
            F.sum("TEMPO_TOTAL").alias("TEMPO_TOTAL")
        )
        .withColumn(
            "MAX_TIME_POSSIBLE",
            F.col("DT_FIM_VOTO").cast("long") -
            F.col("DT_INICIO_VOTO").cast("long")
        )
    )

    df.show(10, False)

    # Count the number of sections with MAX_TIME_POSSIBLE < TIME_CALCULATED
    df = (
        df
        .filter(F.col("MAX_TIME_POSSIBLE") < F.col("TEMPO_TOTAL"))
    )

    # Should be 0
    count = df.count()

    if count == 0:
        print("\n\nOK\n\n")
    else:
        print("\n\nERROR\n\n")
        print(f"MAX_TIME_POSSIBLE < TIME_CALCULATED in {count} sections")


if __name__ == "__main__":
    max_time_possible()
