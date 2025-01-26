## Importing necessary modules

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialising SparkSession 

spark = SparkSession  \
    .builder  \
    .appName("RetailDataProject")  \
    .getOrCreate()       
    
spark.sparkContext.setLogLevel('ERROR')


# Reading input data from Kafka 
raw_input_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","18.211.252.152:9092") \
    .option("subscribe","real-time-project") \
    .option("startingOffsets", "latest")  \
    .load()
        
        
# Defining Schema for Input data

JSON_Schema = StructType() \
    .add("invoice_no", LongType()) \
    .add("country",StringType()) \
    .add("timestamp", TimestampType()) \
    .add("type", StringType()) \
    .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", FloatType()),
        StructField("quantity", IntegerType()) 
        ])))

order_data_stream = raw_input_stream.select(from_json(col("value").cast("string"), JSON_Schema).alias("data")).select("data.*")


# Setting up UDFs

# Calculating Total Count of Items in an Order 
def total_item_count(items):
    if items is not None:
        item_count =0
        for item in items:
            item_count = item_count + item['quantity']
        return item_count   

# Calculating Total Cost of Order
def total_cost(items,type):
    if items is not None:
        total_cost =0
        item_price =0
    for item in items:
        item_price = (item['quantity']*item['unit_price'])
        total_cost = total_cost+ item_price
        item_price=0

    if type  == 'RETURN':
        return total_cost *-1
    else:
        return total_cost  

#Checking if it's an order
def is_a_order(type):
   return 1 if type == 'ORDER' else 0

#Checking if it's a Return order
def is_a_return(type):
   return 1 if type == 'RETURN' else 0

# Registering UDFs
is_order = udf(is_a_order, IntegerType())
is_return = udf(is_a_return, IntegerType())
add_total_item_count = udf(total_item_count, IntegerType())
add_total_cost = udf(total_cost, FloatType())


# Calculating additional columns for the stream 
order_output_stream = order_data_stream \
   .withColumn("total_cost", add_total_cost(order_data_stream.items,order_data_stream.type)) \
   .withColumn("total_items", add_total_item_count(order_data_stream.items)) \
   .withColumn("is_order", is_order(order_data_stream.type)) \
   .withColumn("is_return", is_return(order_data_stream.type))


# Writing the summarised input table to the console 
order_batch = order_output_stream \
   .select("invoice_no", "country", "timestamp","total_cost","total_items","is_order","is_return") \
   .writeStream \
   .outputMode("append") \
   .format("console") \
   .option("truncate", "false") \
   .option("path", "/Console_output") \
   .option("checkpointLocation", "/Console_output_checkpoints") \
   .trigger(processingTime="1 minute") \
   .start()
       
# Calculating Time based KPIs
agg_time = order_output_stream \
    .withWatermark("timestamp","1 minutes") \
    .groupby(window("timestamp", "1 minute")) \
    .agg(sum("total_cost").alias("total_volume_of_sales"),
        avg("total_cost").alias("average_transaction_size"),
        count("invoice_no").alias("OPM"),
        avg("is_Return").alias("rate_of_return")) \
    .select("window.start","window.end","OPM","total_volume_of_sales","average_transaction_size","rate_of_return")

# Calculating Time and country based KPIs
agg_time_country = order_output_stream \
    .withWatermark("timestamp", "1 minutes") \
    .groupBy(window("timestamp", "1 minutes"), "country") \
    .agg(sum("total_cost").alias("total_volume_of_sales"),
        count("invoice_no").alias("OPM"),
        avg("is_Return").alias("rate_of_return")) \
    .select("window.start","window.end","country", "OPM","total_volume_of_sales","rate_of_return")


# Writing to the Console : Time based KPI values 
KPIByTime = agg_time.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "timeKPI") \
    .option("checkpointLocation", "timeKPI_checkpoints") \
    .trigger(processingTime="1 minutes") \
    .start()


# Writing to the Console : Time and country based KPI values
KPIByTime_country = agg_time_country.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "time_countryKPI") \
    .option("checkpointLocation", "time_countryKPI_checkpoints") \
    .trigger(processingTime="1 minutes") \
    .start()

order_batch.awaitTermination()
KPIByTime.awaitTermination()
KPIByTime_country.awaitTermination()