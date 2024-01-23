from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("Spark SCADA Analysis").getOrCreate()

    # Set the log level to WARN
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("GeneratorID", StringType(), True),
        StructField("PowerOutput", StringType(), True),
        StructField("FuelLevel", StringType(), True),
        StructField("Voltage", StringType(), True),
        StructField("FaultAlarms", StringType(), True)  # Updated to StringType for FaultAlarms
    ])

    # Define the Kafka parameters
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "scada_data_topic"

    # Read data from Kafka as a streaming DataFrame
    streaming_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .load()

    # Extract the JSON value and cast it to the defined schema
    value_df = streaming_df.select(from_json(col("value").cast("string"), schema).alias("value"))
    scada_df = value_df.selectExpr("value.*")

    # Basic analysis - you can customize this based on your needs
    analysis_df = scada_df \
        .withColumn("PowerOutputNumeric", expr("CAST(REGEXP_REPLACE(PowerOutput, ' MW', '') AS DOUBLE)")) \
        .withColumn("FuelLevelNumeric", expr("CAST(REGEXP_REPLACE(FuelLevel, '%', '') AS DOUBLE)")) \
        .withColumn("VoltageNumeric", expr("CAST(REGEXP_REPLACE(Voltage, ' V', '') AS DOUBLE)")) \
        .groupBy("GeneratorID") \
        .agg(
            expr("avg(PowerOutputNumeric) as AvgPowerOutput"),
            expr("min(PowerOutputNumeric) as MinPowerOutput"),
            expr("max(PowerOutputNumeric) as MaxPowerOutput"),
            expr("avg(FuelLevelNumeric) as AvgFuelLevel"),
            expr("avg(VoltageNumeric) as AvgVoltage"),
            expr("sum(case when FaultAlarms is not null then 1 else 0 end) as FaultAlarmsCount")
        )

    # Start the streaming query
    query = analysis_df \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    # Wait for the termination of the query
    query.awaitTermination()

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 analysis.py