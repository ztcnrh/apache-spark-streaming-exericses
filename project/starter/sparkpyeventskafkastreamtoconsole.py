from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, DoubleType

# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
# Sample data: {"customer":"Neeraj.Ahmed@test.com","score":12.0,"riskDate":"2024-09-14T04:31:52.204Z"}
stedi_events_schema = StructType([
    StructField("customer", StringType()),
    StructField("score", DoubleType()),
    StructField("riskDate", StringType())
])

spark = SparkSession.builder.appName("Kafka-Stream-STEDI-Events").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

stedi_kafka_raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()
                                   
# TO-DO: cast the value column in the streaming dataframe as a STRING
stedi_kafka_df = stedi_kafka_raw_df.selectExpr("cast(value as string) as value")

# TO-DO: parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk

stedi_parsed_df = stedi_kafka_df.withColumn("value", from_json(col("value"), stedi_events_schema)) \
    .select(col("value.*"))
stedi_parsed_df.createOrReplaceTempView("CustomerRisk")

# TO-DO: execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customer_risk_df = spark.sql("""
    SELECT customer, score
    FROM CustomerRisk
""")

# TO-DO: sink the customerRiskStreamingDF dataframe to the console in append mode
# 
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
customer_risk_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct
