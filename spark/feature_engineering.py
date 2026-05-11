from pyspark.sql import SparkSession
from pyspark.sql.functions import *


SILVER_INPUT_PATH = "/opt/spark-apps/output/processed/amazon_reviews"
GOLD_OUTPUT_PATH = "/opt/spark-apps/output/processed/gold_features"


def build_spark_session():
    return SparkSession.builder \
        .appName("AmazonReviewsFeatureEngineering") \
        .master("spark://spark-master:7077") \
        .getOrCreate()


def load_silver_data(spark, input_path):
    # Silver katmanını mümkünse Delta olarak okur; mevcut altyapıda parquet tutuluyorsa uyumluluk için parquet'e düşer.
    try:
        return spark.read.format("delta").load(input_path)
    except Exception:
        return spark.read.parquet(input_path)


def add_review_word_count(df):
    # Uzun yorumlar genellikle ürün hakkında daha fazla bağlam ve detay içerir.
    cleaned_body = trim(regexp_replace(coalesce(col("review_body"), lit("")), "\\s+", " "))
    return df.withColumn(
        "review_word_count",
        when(length(cleaned_body) == 0, lit(0)).otherwise(size(split(cleaned_body, " ")))
    )


def add_helpfulness_ratio(df):
    # Diğer kullanıcılar tarafından faydalı bulunan yorumlar daha güvenilir sinyal üretir.
    helpful_votes = coalesce(col("helpful_votes").cast("double"), lit(0.0))
    total_votes = coalesce(col("total_votes").cast("double"), lit(0.0))
    return df.withColumn(
        "helpfulness_ratio",
        when(total_votes > 0, helpful_votes / total_votes).otherwise(lit(0.0))
    )


def add_headline_length(df):
    # Başlık uzunluğu, kullanıcının ürün hakkında ne kadar güçlü bir yargıya sahip olduğunu gösterebilir.
    return df.withColumn(
        "headline_length",
        length(coalesce(col("review_headline"), lit("")))
    )


def add_verified_flag(df):
    # Doğrulanmış satın alım bilgisi, yorumun gerçek kullanım deneyimini yansıtma ihtimalini artırır.
    return df.withColumn(
        "is_verified",
        when(upper(trim(coalesce(col("verified_purchase"), lit("")))) == "Y", lit(1)).otherwise(lit(0))
    )


def add_long_review_flag(df):
    # Ortalama üstü uzun yorumlar, detay seviyesi yüksek olduğu için ayrı bir davranış sinyali oluşturur.
    avg_word_count_df = df.agg(avg(col("review_word_count")).alias("avg_review_word_count"))
    return df.crossJoin(avg_word_count_df).withColumn(
        "is_long_review",
        when(col("review_word_count") > col("avg_review_word_count"), lit(1)).otherwise(lit(0))
    ).drop("avg_review_word_count")


def engineer_features(df):
    feature_df = add_review_word_count(df)
    feature_df = add_helpfulness_ratio(feature_df)
    feature_df = add_headline_length(feature_df)
    feature_df = add_verified_flag(feature_df)
    feature_df = add_long_review_flag(feature_df)
    return feature_df


def write_gold_features(df, output_path):
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(output_path)


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = load_silver_data(spark, SILVER_INPUT_PATH)

    if silver_df.rdd.isEmpty():
        raise ValueError(f"No data found in {SILVER_INPUT_PATH}. Run the Silver ingestion step first.")

    gold_df = engineer_features(silver_df)
    write_gold_features(gold_df, GOLD_OUTPUT_PATH)

    print(f"Gold feature table written successfully to {GOLD_OUTPUT_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
