from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType
spark = SparkSession.builder.appName("SensorStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
schema = StructType([StructField("sensor_id", IntegerType(), True), StructField("temperature", FloatType(), True), StructField("humidity", FloatType(), True), StructField("timestamp", LongType(), True)])
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "sensor_data").option("startingOffsets", "latest").load()
parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
parsed = parsed.withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))
result = parsed.groupBy(window(col("event_time"), "1 minute"), col("sensor_id")).agg(avg("temperature").alias("avg_temperature"), avg("humidity").alias("avg_humidity"))
query = result.writeStream.outputMode("complete").format("console").option("truncate", "false").start()
query.awaitTermination()
