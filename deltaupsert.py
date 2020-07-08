
from pyspark.sql import SparkSession,SQLContext,HiveContext
from pyspark import SparkConf,SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
import sys
import time
import json, os, re
from delta.tables import *


spark = SparkSession.builder.appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.11:0.5.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport()\
    .getOrCreate()


spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.sparkContext.setLogLevel('WARN')
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

print("=================================================================================================")

def infer_topic_schema_json():
    df_json = (spark.read
               .format("kafka") \
               .option("kafka.bootstrap.servers", "ec2-3-229-134-50.compute-1.amazonaws.com:9092")\
               .option("subscribe", "topic3")\
               .option("startingOffsets", "earliest")\
               .option("endingOffsets", "latest")\
               .option("failOnDataLoss", "false")\
               .load()\
               .withColumn("value", expr("string(value)"))\
               .filter(col("value").isNotNull())\
               .select("key", expr("struct(offset, value) r"))\
               .groupBy("key").agg(expr("max(r) r"))\
               .select("r.value"))
    df_read = spark.read.json(df_json.rdd.map(lambda x: x.value), multiLine=True)
               
    if "_corrupt_record" in df_read.columns:
        df_read = (df_read
                    .filter(col("_corrupt_record").isNotNull())
                    .drop("_corrupt_record"))

    return df_read.schema.json()

topic_schema_txt = infer_topic_schema_json()

topic_schema = StructType.fromJson(json.loads(topic_schema_txt))
print(topic_schema)




print("+++++++++++++schema save++++++++++++++++++++")

df = spark \
    .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "ec2-3-229-134-50.compute-1.amazonaws.com:9092") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("subscribe", "topic3") \
        .load()\
        .withColumn("value", expr("string(value)"))\
        .filter(col("value").isNotNull())\
        .select(
                expr("offset as kafka_offset"),
                expr("timestamp as kafka_ts"),
                expr("string(key) as kafka_key"),
                "value"
                )\
        .select("kafka_key", expr("struct(*) as r"))\
        .groupBy("kafka_key")\
        .agg(expr("max(r) r"))\
        .withColumn('value',
                    from_json(col("r.value"), topic_schema))\
        .select('r.kafka_key',
                'r.kafka_offset',
                'r.kafka_ts',
                'value.*'
                )


print("dataframe schema is ",df.schema)
print("type is", type(df))
(spark
 .createDataFrame([], df.schema)
 .write
 .option("mergeSchema", "true")
 .format("delta")
 .mode("append")
 .save("s3://rubikondemo/delta/datajuly8"))



deltaTable = DeltaTable.forPath(spark, "s3://rubikondemo/delta/datajuly8")
deltaTable.vacuum()

def upsertToDelta(df, batch_id):
    (DeltaTable
     .forPath(spark, "s3://rubikondemo/delta/datajuly8")
     .alias("t")
     .merge(df.alias("s"), "s.id = t.id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
#query = df.writeStream.format("console").start()
print("writing")
print("IsStreaming: ", df.isStreaming)
print("df type is : " ,type(df))

df.printSchema()
df.writeStream.format("delta").option("checkpointLocation", "s3://rubikondemo/delta/checkpoint5/").foreachBatch(upsertToDelta).outputMode("update").start("s3://rubikondemo/delta/datajuly8")

print("++++++++++++++++++++wrote++++++++++++++++")


time.sleep(10)
print("going to stop")
df_delta = (spark.read
            .format("delta")
            .load("s3://rubikondemo/delta/datajuly8"))
df_delta.shape()
print("read")
#spark.sql("CREATE EXTERNAL TABLE if NOT exists bostondelta3 (kafka_key STRING,kafka_offset STRING, kafka_ts STRING, age STRING, black STRING, chas STRING, crim STRING, dis STRING, id STRING, indus STRING, lstat STRING, medv STRING, nox STRING, ptratio STRING, rad STRING, rm STRING, tax STRING, zn STRING) LOCATION 's3a://rubikondemo/delta/datajuly72' ")
#spark.sql("SELECT * FROM bostondelta3").show()

df_delta.show()
#df_delta.write.parquet("s3://rubikondemo/delta/datafiledeltaformat",mode="overwrite")

#query.stop()
