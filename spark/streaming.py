from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =========================================================
# Spark Session
# =========================================================

spark = SparkSession.builder \
    .appName("AmazonReviewsStreaming") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# Schema
# =========================================================

schema = StructType([
    StructField("marketplace", StringType()),
    StructField("customer_id", StringType()),
    StructField("review_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_parent", StringType()),
    StructField("product_title", StringType()),
    StructField("product_category", StringType()),
    StructField("star_rating", StringType()),
    StructField("helpful_votes", StringType()),
    StructField("total_votes", StringType()),
    StructField("vine", StringType()),
    StructField("verified_purchase", StringType()),
    StructField("review_headline", StringType()),
    StructField("review_body", StringType()),
    StructField("review_date", StringType())
])

# =========================================================
# Kafka Stream Read
# =========================================================

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "amazon_reviews") \
    .option("startingOffsets", "latest") \
    .load()

# =========================================================
# Kafka Value -> String
# =========================================================

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = json_df.select(
    from_json(col("json_str"), schema).alias("data")
).select("data.*")

# =========================================================
# Data Cleaning
# =========================================================

clean_df = parsed_df.dropna(
    subset=[
        "review_id",
        "product_id",
        "review_body",
        "star_rating"
    ]
)

# =========================================================
# Type Conversion
# =========================================================

clean_df = clean_df \
    .withColumn("star_rating", col("star_rating").cast("int")) \
    .withColumn("helpful_votes", col("helpful_votes").cast("int")) \
    .withColumn("total_votes", col("total_votes").cast("int"))

# =========================================================
# Date Conversion
# =========================================================

clean_df = clean_df.withColumn(
    "review_date",
    to_date(col("review_date"), "yyyy-MM-dd")
)

# =========================================================
# Feature Engineering (Mini)
# =========================================================

clean_df = clean_df.withColumn(
    "review_length",
    length(col("review_body"))
)

# =========================================================
# Streaming Analytics
# =========================================================

rating_count = clean_df.groupBy("star_rating").count()

analytics_query = rating_count.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()

# =========================================================
# Write streaming output to a shared path
# =========================================================

output_path = "/data/processed/amazon_reviews"
checkpoint_path = "/data/checkpoints/amazon_reviews"

delta_query = clean_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .start(output_path)

delta_query.awaitTermination()