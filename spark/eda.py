from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def build_spark_session():
    return SparkSession.builder \
        .appName("AmazonReviewsEDA") \
        .master("local[*]") \
        .getOrCreate()


def safe_write(df, path, mode="overwrite"):
    df.coalesce(1).write.mode(mode).option("header", True).csv(path)


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    input_path = "/opt/spark-apps/output/processed/amazon_reviews"
    output_path = "/opt/spark-apps/output/eda_outputs"

    reviews_df = spark.read.parquet(input_path)

    if reviews_df.rdd.isEmpty():
        raise ValueError(f"No data found in {input_path}. Run streaming first.")

    reviews_df = reviews_df.withColumn(
        "review_month",
        date_format(col("review_date"), "yyyy-MM")
    )

    total_records = reviews_df.count()
    distinct_reviews = reviews_df.select("review_id").distinct().count()
    distinct_products = reviews_df.select("product_id").distinct().count()
    distinct_customers = reviews_df.select("customer_id").distinct().count()

    summary_df = spark.createDataFrame([
        ("total_records", total_records),
        ("distinct_reviews", distinct_reviews),
        ("distinct_products", distinct_products),
        ("distinct_customers", distinct_customers),
    ], ["metric", "value"])

    numeric_stats_df = reviews_df.select(
        "star_rating",
        "helpful_votes",
        "total_votes",
        "review_length"
    ).describe()

    missing_values_df = spark.createDataFrame([
        (column_name, reviews_df.filter(col(column_name).isNull()).count())
        for column_name in reviews_df.columns
    ], ["column_name", "missing_count"])

    rating_distribution_df = reviews_df.groupBy("star_rating").count().orderBy("star_rating")
    verified_distribution_df = reviews_df.groupBy("verified_purchase").count().orderBy(desc("count"))
    vine_distribution_df = reviews_df.groupBy("vine").count().orderBy(desc("count"))
    monthly_reviews_df = reviews_df.groupBy("review_month").count().orderBy("review_month")

    top_products_df = reviews_df.groupBy("product_id") \
        .count() \
        .orderBy(desc("count")) \
        .limit(10)

    top_customers_df = reviews_df.groupBy("customer_id") \
        .count() \
        .orderBy(desc("count")) \
        .limit(10)

    average_votes_by_rating_df = reviews_df.groupBy("star_rating") \
        .agg(
            round(avg("helpful_votes"), 2).alias("avg_helpful_votes"),
            round(avg("total_votes"), 2).alias("avg_total_votes"),
            round(avg("review_length"), 2).alias("avg_review_length")
        ) \
        .orderBy("star_rating")

    print("\n=== Amazon Reviews EDA Summary ===")
    summary_df.show(truncate=False)

    print("\n=== Numeric Statistics ===")
    numeric_stats_df.show(truncate=False)

    print("\n=== Missing Values ===")
    missing_values_df.orderBy(desc("missing_count")).show(truncate=False)

    print("\n=== Rating Distribution ===")
    rating_distribution_df.show(truncate=False)

    print("\n=== Verified Purchase Distribution ===")
    verified_distribution_df.show(truncate=False)

    print("\n=== Vine Distribution ===")
    vine_distribution_df.show(truncate=False)

    print("\n=== Monthly Review Trend ===")
    monthly_reviews_df.show(truncate=False)

    print("\n=== Top Products ===")
    top_products_df.show(truncate=False)

    print("\n=== Top Customers ===")
    top_customers_df.show(truncate=False)

    print("\n=== Average Votes by Rating ===")
    average_votes_by_rating_df.show(truncate=False)

    safe_write(summary_df, f"{output_path}/summary")
    safe_write(numeric_stats_df, f"{output_path}/numeric_stats")
    safe_write(missing_values_df, f"{output_path}/missing_values")
    safe_write(rating_distribution_df, f"{output_path}/rating_distribution")
    safe_write(verified_distribution_df, f"{output_path}/verified_distribution")
    safe_write(vine_distribution_df, f"{output_path}/vine_distribution")
    safe_write(monthly_reviews_df, f"{output_path}/monthly_reviews")
    safe_write(top_products_df, f"{output_path}/top_products")
    safe_write(top_customers_df, f"{output_path}/top_customers")
    safe_write(average_votes_by_rating_df, f"{output_path}/avg_votes_by_rating")

    spark.stop()


if __name__ == "__main__":
    main()