import pyspark
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName("Spark introduction").getOrCreate()
dy_pyspark = spark.read.csv("tips.csv", header=True, inferSchema=True)

# print(dy_pyspark.show(10))
# print(dy_pyspark.printSchema(), type(dy_pyspark), dy_pyspark.columns)

# DF
# select a particular column
# print(dy_pyspark.select("sex").show(5))
# print(dy_pyspark.select(['tip', 'sex']).show(5))
# print(dy_pyspark.select(dy_pyspark[1], dy_pyspark[2]).show(5))
# print(dy_pyspark.describe())

# Generating new columns easily
dy_pyspark = dy_pyspark.withColumn("top_bill_ratio", (dy_pyspark["tip"]/dy_pyspark["total_bill"] * 100))
print(dy_pyspark.show(5))

# Dropping columns
dy_pyspark = dy_pyspark.drop('top_bill_ratio')
print(dy_pyspark.show(5))
