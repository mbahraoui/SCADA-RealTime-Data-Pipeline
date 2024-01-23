from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import from_json, col, udf



if __name__=="__main__":
    # Create a Spark session
    spark = SparkSession.builder\
        .appName("Spark SCADA Analysis")\
        .getOrCreate()
    
    schema = StructType([
        StructField("GeneratorID", StringType(), True),
        StructField("PowerOutput", StringType(), True),
        StructField("FuelLevel", StringType(), True),
        StructField("Voltage", StringType(), True),
        StructField("FaultAlarms", IntegerType(), True)
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
        .load() \
        
    value_df = streaming_df.select(from_json(col("value").cast("string"), schema).alias("value"))

    scada_df = value_df.selectExpr("value.*")

    # Start the streaming query
    query = scada_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Wait for the termination of the query
    query.awaitTermination()
