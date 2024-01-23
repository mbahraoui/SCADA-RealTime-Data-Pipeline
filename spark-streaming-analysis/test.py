from pyspark import SparkContext

# Create a SparkContext
sc = SparkContext.getOrCreate()

# Get the Spark version
spark_version = sc.version

# Print the Spark version
print("Apache Spark version:", spark_version)

# Stop the SparkContext
sc.stop()
