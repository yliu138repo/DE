import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Ch03 - Analyzing the vocabulary of Pride and Prejudice."
).master("spark://localhost:7077").getOrCreate()

book = spark.read.text("test.txt")

lines = book.select(F.split(F.col("value"), " ").alias("line"))

words = lines.select(F.explode(F.col("line")).alias("word"))

words_lower = words.select(F.lower(F.col("word")).alias("word_lower"))
words_clean = words_lower.select(
    F.regexp_extract(F.col("word_lower"), "[a-z]*", 0).alias("word")
)
words_nonull = words_clean.where(F.col("word") != "")

results = words_nonull.groupby(F.col("word")).count()

results.orderBy(F.col("count").desc()).show(10)

results.coalesce(1).write.csv("simple_count.csv")
