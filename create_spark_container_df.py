from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pandas as pd
from pyspark import SparkConf

sc = SparkContext('local')
sqlContext = SQLContext(sc)

# this fails because the package for csv is not loaded
# df = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load("./WAM_table_10000_rows.csv")

rawData = sc.textFile('./WAM_table_10000_rows.csv')

print 'one line of csv input'
print rawData.take(1)

parts = rawData.map(lambda l: l.split(","))
messages = parts.map(lambda p: (
    p[0].strip(), p[1].strip(), p[2].strip(), p[3].strip(), p[4].strip(),
    p[5].strip(), p[6].strip(), p[7].strip(), p[8].strip(), p[9].strip(),
    p[10].strip(), p[11].strip(), p[12].strip(), p[13].strip(), p[14].strip(),
    p[15].strip(), p[16].strip(), p[17].strip()
))


print 'one line of semi-parsed csv input'
print messages.take(1)


# The schema is encoded in a string.
schemaString = 'imsi container_id  transaction_type  send_date  receive_date  rcd_lat  rcd_long  lac  mcc'
schemaString += 'mnc  year_receive  month_receive day_receive hour_receive  min_receive'
schemaString += 'year_send  month_send  day_send  hour_send  min_send'

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaMessages = sqlContext.createDataFrame(messages, schema)

# Register the DataFrame as a table.
schemaMessages.registerTempTable("messages")

results = sqlContext.sql("SELECT container_id FROM messages")

print '4 rows of the result of selecting only container_id'
print results.take(4)

ids = results.map(lambda p: "id: " + p.container_id)

for id in ids.take(10):
    print(id)

print 'saving / overwriting whole dataframe to parquet'
schemaMessages.write.mode('overwrite').parquet('messages.parquet', 'overwrite')

print 'reading the parquet files into a dataframe'
parquetFile = sqlContext.read.parquet("messages.parquet")

print '1 row from the parquet file'
print parquetFile.take(1)

