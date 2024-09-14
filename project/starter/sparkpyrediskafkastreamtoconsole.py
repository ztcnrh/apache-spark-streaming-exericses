from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, DoubleType

# TO-DO: create a StructType for the Kafka redis-server topic which has all changes made to Redis - before Spark 3.0.0, schema inference is not automatic
redis_server_schema = StructType(
    [
        StructField("key", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue", StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("score", StringType())
            ]))
        )
    ]
)

# TO-DO: create a StructType for the Customer JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
# Sample data: "{\"customerName\":\"Trevor Wu\",\"email\":\"Trevor.Wu@test.com\",\"phone\":\"8015551212\",\"birthDay\":\"1944-01-01\"}"
customer_schema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType())
])

#TO-DO: create a spark application object
spark = SparkSession.builder.appName("Redis-Kafka-Stream-Customer-Data").getOrCreate()

#TO-DO: set the spark log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
redis_kafka_raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

# TO-DO: cast the value column in the streaming dataframe as a STRING
redis_kafka_df = redis_kafka_raw_df.selectExpr("cast(value as string) as value")

# TO-DO:; parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "ch":false,
# "incr":false,
# "zSetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }],
# "zsetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }]
# }
# 
# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# storing them in a temporary view called RedisSortedSet
redis_kafka_parsed_df = redis_kafka_df.withColumn("value", from_json(col("value"), redis_server_schema)) \
    .select(col("value.*"))
redis_kafka_parsed_df.createOrReplaceTempView("RedisSortedSet")

# TO-DO: execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column
encoded_customer_df = spark.sql("""
    SELECT zSetEntries[0].element as encodedCustomer
    FROM RedisSortedSet
""")

# TO-DO: take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
#+--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}
decoded_customer_df = encoded_customer_df.withColumn("customer", unbase64(encoded_customer_df.encodedCustomer).cast("string"))

# TO-DO: parse the JSON in the Customer record and store in a temporary view called CustomerRecords
customer_df = decoded_customer_df.withColumn("customer", from_json(col("customer"), customer_schema)) \
    .select(col("customer.*"))
customer_df.createOrReplaceTempView("CustomerRecords")

# TO-DO: JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
email_and_birth_day_df = spark.sql("""
    SELECT email, birthDay
    FROM CustomerRecords
    WHERE email IS NOT NULL AND birthDay IS NOT NULL
""")

# TO-DO: from the emailAndBirthDayStreamingDF dataframe select the email and the birth year (using the split function)
# TO-DO: Split the birth year as a separate field from the birthday
# TO-DO: Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
email_and_birth_year_df = email_and_birth_day_df.withColumn("birthYear", split(col("birthDay"), "-").getItem(0))

# TO-DO: sink the emailAndBirthYearStreamingDF dataframe to the console in append mode
# 
# The output should look like this:
# +--------------------+-----               
# | email         |birthYear|
# +--------------------+-----
# |Gail.Spencer@test...|1963|
# |Craig.Lincoln@tes...|1962|
# |  Edward.Wu@test.com|1961|
# |Santosh.Phillips@...|1960|
# |Sarah.Lincoln@tes...|1959|
# |Sean.Howard@test.com|1958|
# |Sarah.Clark@test.com|1957|
# +--------------------+-----
email_and_birth_year_df \
    .select("email", "birthYear") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

# Run the python script by running the command from the terminal:
# /home/workspace/submit-redis-kafka-streaming.sh
# Verify the data looks correct
