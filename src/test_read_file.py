from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

SPARK_MASTER_URL = "spark://spark:7077"
SCHEMA = StructType([
    StructField("datetime", StringType(), True),
    StructField("operation_label", StringType(), True),
    StructField("log_id", StringType(), True),
    StructField("operation_label_2", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("operation_id", StringType(), True),
])

spark = SparkSession.builder.master(SPARK_MASTER_URL).appName("test read file").getOrCreate()

df = (
    spark.read
    .format("csv")
    .schema(SCHEMA)
    .option("header", "false")
    .option("delimiter", "\t")
    .option("inferSchema", "false")
    .option("encoding", "ISO-8859-1")
    .load('/data/logs/1_AC/*.csv')
)

print("\n\n",df.count())
df.show(20, False)
