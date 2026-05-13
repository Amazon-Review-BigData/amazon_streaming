import csv
import os
import tempfile
from pathlib import Path

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    DecisionTreeClassifier,
    GBTClassifier,
    LogisticRegression,
    NaiveBayes,
    RandomForestClassifier,
    OneVsRest,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import DenseVector, SparseVector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

INPUT_PATH = "/opt/spark-apps/output/processed/gold_features"
TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "file:///opt/spark-apps/output/mlruns")
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "amazon_reviews_multiclass_training")
REGISTERED_MODEL_NAME = os.getenv("MLFLOW_REGISTERED_MODEL_NAME", "amazon_reviews_best_model")
FEATURE_COLUMNS = [
    "review_word_count",
    "helpfulness_ratio",
    "headline_length",
    "is_verified",
    "is_long_review",
]
CLASS_LABELS = [1, 2, 3, 4, 5]

def build_spark_session():
    return SparkSession.builder \
        .appName("AmazonReviewsModelTraining") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

def load_gold_features(spark, input_path):
    # kafka-streaming branch'indeki parquet okuma tercihini koruyoruz
    return spark.read.parquet(input_path)

def prepare_training_data(df):
    prepared_df = df.select(*FEATURE_COLUMNS, "star_rating") \
        .dropna(subset=FEATURE_COLUMNS + ["star_rating"]) \
        .withColumn("label", col("star_rating").cast("double") - lit(1.0))

    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features"
    )

    return assembler.transform(prepared_df).select("label", "features", "star_rating", *FEATURE_COLUMNS)

def build_model_specs():
    gbt_base = GBTClassifier(
        labelCol="label",
        featuresCol="features",
        predictionCol="prediction",
        maxIter=20,
        maxDepth=5,
        seed=42,
    )

    return [
        (
            "logistic_regression",
            LogisticRegression(
                labelCol="label",
                featuresCol="features",
                predictionCol="prediction",
                probabilityCol="probability",
                rawPredictionCol="rawPrediction",
                maxIter=100,
                regParam=0.0,
                elasticNetParam=0.0,
                family="multinomial",
            ),
        ),
        (
            "decision_tree",
            DecisionTreeClassifier(
                labelCol="label",
                featuresCol="features",
                predictionCol="prediction",
                probabilityCol="probability",
                rawPredictionCol="rawPrediction",
                maxDepth=8,
                seed=42,
            ),
        ),
        (
            "random_forest",
            RandomForestClassifier(
                labelCol="label",
                featuresCol="features",
                predictionCol="prediction",
                probabilityCol="probability",
                rawPredictionCol="rawPrediction",
                numTrees=80,
                maxDepth=10,
                seed=42,
            ),
        ),
        (
            "gbt",
            OneVsRest(
                classifier=gbt_base,
                labelCol="label",
                featuresCol="features",
                predictionCol="prediction",
                parallelism=2,
            ),
        ),
        (
            "naive_bayes",
            NaiveBayes(
                labelCol="label",
                featuresCol="features",
                predictionCol="prediction",
                probabilityCol="probability",
                rawPredictionCol="rawPrediction",
                smoothing=1.0,
                modelType="multinomial",
            ),
        ),
    ]

def vector_to_list(vector, size=None):
    if isinstance(vector, SparseVector):
        length = size if size is not None else vector.size
        values = [0.0] * length
        for index, value in zip(vector.indices, vector.values):
            values[int(index)] = float(value)
        return values
    if isinstance(vector, DenseVector):
        return [float(value) for value in vector.values]
    if hasattr(vector, "toArray"):
        return [float(value) for value in vector.toArray()]
    return [float(value) for value in vector]

def matrix_column_values(matrix, column_index):
    values = matrix.values
    rows = matrix.numRows
    return [float(values[column_index * rows + row]) for row in range(rows)]

def compute_binary_auc(scores, labels):
    positive_count = sum(labels)
    negative_count = len(labels) - positive_count
    if positive_count == 0 or negative_count == 0:
        return None
    ordered_indices = sorted(range(len(scores)), key=lambda index: scores[index])
    ranks = [0.0] * len(scores)
    position = 0
    current_rank = 1.0
    while position < len(scores):
        tie_end = position + 1
        while tie_end < len(scores) and scores[ordered_indices[tie_end]] == scores[ordered_indices[position]]:
            tie_end += 1
        average_rank = (current_rank + (current_rank + (tie_end - position) - 1.0)) / 2.0
        for tie_index in range(position, tie_end):
            ranks[ordered_indices[tie_index]] = average_rank
        current_rank += tie_end - position
        position = tie_end
    positive_rank_sum = sum(ranks[index] for index, label in enumerate(labels) if label == 1)
    return (positive_rank_sum - positive_count * (positive_count + 1.0) / 2.0) / (positive_count * negative_count)

def compute_multiclass_auc(prediction_rows, class_count):
    label_values = [int(row[0]) for row in prediction_rows]
    score_vectors = [row[1] for row in prediction_rows]
    auc_values = []
    for class_index in range(class_count):
        binary_labels = [1 if label == class_index else 0 for label in label_values]
        class_scores = [float(scores[class_index]) for scores in score_vectors]
        class_auc = compute_binary_auc(class_scores, binary_labels)
        if class_auc is not None:
            auc_values.append(class_auc)
    return sum(auc_values) / len(auc_values) if auc_values else 0.0

def evaluate_predictions(predictions):
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    metrics = {
        "accuracy": evaluator.setMetricName("accuracy").evaluate(predictions),
        "f1_score": evaluator.setMetricName("f1").evaluate(predictions),
        "precision": evaluator.setMetricName("weightedPrecision").evaluate(predictions),
        "recall": evaluator.setMetricName("weightedRecall").evaluate(predictions),
    }
    score_column = "probability" if "probability" in predictions.columns else "rawPrediction"
    prediction_rows = predictions.select("label", score_column).collect()
    metrics["auc_roc"] = compute_multiclass_auc(prediction_rows, len(CLASS_LABELS))
    return metrics

def build_confusion_matrix(predictions, class_count):
    matrix = [[0 for _ in range(class_count)] for _ in range(class_count)]
    row_counts = predictions.groupBy("label", "prediction").count().collect()
    for row in row_counts:
        actual = int(row[0])
        predicted = int(row[1])
        count = int(row[2])
        if 0 <= actual < class_count and 0 <= predicted < class_count:
            matrix[actual][predicted] += count
    return matrix

def save_confusion_matrix_svg(matrix, class_labels, output_path):
    cell_size = 72
    label_margin = 120
    top_margin = 70
    width = label_margin + cell_size * len(class_labels) + 40
    height = top_margin + cell_size * len(class_labels) + 70
    max_value = max((value for row in matrix for value in row), default=0)
    max_value = max_value if max_value > 0 else 1
    def color_for_value(value):
        intensity = int(235 - (value / max_value) * 155)
        return f"rgb(40,{intensity},{255 - intensity // 2})"
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#f8fafc"/>',
        '<text x="20" y="34" font-family="Arial, sans-serif" font-size="24" font-weight="700" fill="#0f172a">Confusion Matrix</text>',
        '<text x="20" y="58" font-family="Arial, sans-serif" font-size="13" fill="#475569">Actual labels on rows, predicted labels on columns</text>',
    ]
    for index, label in enumerate(class_labels):
        x = label_margin + index * cell_size + cell_size / 2
        y = top_margin - 18
        lines.append(f'<text x="{x}" y="{y}" text-anchor="middle" font-family="Arial, sans-serif" font-size="14" fill="#334155">{label}</text>')
    for index, label in enumerate(class_labels):
        x = label_margin - 16
        y = top_margin + index * cell_size + cell_size / 2 + 5
        lines.append(f'<text x="{x}" y="{y}" text-anchor="end" font-family="Arial, sans-serif" font-size="14" fill="#334155">{label}</text>')
    for row_index, row in enumerate(matrix):
        for col_index, value in enumerate(row):
            x = label_margin + col_index * cell_size
            y = top_margin + row_index * cell_size
            lines.append(f'<rect x="{x}" y="{y}" width="{cell_size}" height="{cell_size}" fill="{color_for_value(value)}" stroke="#ffffff" stroke-width="2"/>')
            lines.append(f'<text x="{x + cell_size / 2}" y="{y + cell_size / 2 + 6}" text-anchor="middle" font-family="Arial, sans-serif" font-size="16" font-weight="700" fill="#0f172a">{value}</text>')
    lines.append(f'<text x="{label_margin + (cell_size * len(class_labels)) / 2}" y="{height - 18}" text-anchor="middle" font-family="Arial, sans-serif" font-size="14" fill="#334155">Predicted class</text>')
    lines.append(f'<text x="28" y="{top_margin + (cell_size * len(class_labels)) / 2}" transform="rotate(-90 28 {top_margin + (cell_size * len(class_labels)) / 2})" text-anchor="middle" font-family="Arial, sans-serif" font-size="14" fill="#334155">Actual class</text>')
    lines.append("</svg>")
    Path(output_path).write_text("\n".join(lines), encoding="utf-8")

def flatten_feature_importance(model_stage, feature_names):
    # Çakışmaları temizleyip tüm model tipleri için uyumlu hale getirdik
    if hasattr(model_stage, "featureImportances"):
        importances = vector_to_list(model_stage.featureImportances, len(feature_names))
        return list(zip(feature_names, importances))
    if hasattr(model_stage, "coefficients"):
        coefficients = vector_to_list(model_stage.coefficients, len(feature_names))
        return list(zip(feature_names, [abs(value) for value in coefficients]))
    if hasattr(model_stage, "coefficientMatrix"):
        matrix = model_stage.coefficientMatrix
        importances = []
        for column_index in range(matrix.numCols):
            column_values = [abs(value) for value in matrix_column_values(matrix, column_index)]
            importances.append(sum(column_values) / len(column_values))
        return list(zip(feature_names, importances))
    if hasattr(model_stage, "theta"):
        matrix = model_stage.theta
        importances = []
        for column_index in range(matrix.numCols):
            column_values = matrix_column_values(matrix, column_index)
            spread = max(column_values) - min(column_values)
            importances.append(abs(spread))
        return list(zip(feature_names, importances))
    return [(feature_name, 0.0) for feature_name in feature_names]

def feature_importance_for_model(pipeline_model, feature_names):
    classifier_stage = pipeline_model.stages[-1]
    if hasattr(classifier_stage, "models"):
        aggregated = [0.0] * len(feature_names)
        for submodel in classifier_stage.models:
            submodel_importance = flatten_feature_importance(submodel, feature_names)
            for index, (_, importance) in enumerate(submodel_importance):
                aggregated[index] += float(importance)
        aggregated = [value / len(classifier_stage.models) for value in aggregated]
        return list(zip(feature_names, aggregated))
    return flatten_feature_importance(classifier_stage, feature_names)

def write_feature_importance_artifact(feature_importance_rows, output_path):
    sorted_rows = sorted(feature_importance_rows, key=lambda item: item[1], reverse=True)
    total_importance = sum(importance for _, importance in sorted_rows)
    total_importance = total_importance if total_importance > 0 else 1.0
    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["feature", "importance", "normalized_importance", "rank"])
        for rank, (feature_name, importance) in enumerate(sorted_rows, start=1):
            writer.writerow([feature_name, round(float(importance), 6), round(float(importance) / total_importance, 6), rank])

def train_one_model(model_name, estimator, train_df, test_df, feature_names):
    with mlflow.start_run(run_name=model_name) as run:
        mlflow.log_param("model_name", model_name)
        mlflow.log_param("train_rows", train_df.count())
        mlflow.log_param("test_rows", test_df.count())
        mlflow.log_param("label_definition", "label = star_rating - 1.0")

        pipeline = Pipeline(stages=[estimator])
        pipeline_model = pipeline.fit(train_df)
        predictions = pipeline_model.transform(test_df).cache()
        metrics = evaluate_predictions(predictions)

        for metric_name, metric_value in metrics.items():
            mlflow.log_metric(metric_name, float(metric_value))

        confusion_matrix = build_confusion_matrix(predictions, len(CLASS_LABELS))
        artifact_dir = Path(tempfile.mkdtemp(prefix=f"{model_name}_"))
        confusion_matrix_path = artifact_dir / "confusion_matrix.svg"
        feature_importance_path = artifact_dir / "feature_importance.csv"

        save_confusion_matrix_svg(confusion_matrix, CLASS_LABELS, str(confusion_matrix_path))
        feature_importance_rows = feature_importance_for_model(pipeline_model, feature_names)
        write_feature_importance_artifact(feature_importance_rows, str(feature_importance_path))

        mlflow.log_artifact(str(confusion_matrix_path), artifact_path="artifacts")
        mlflow.log_artifact(str(feature_importance_path), artifact_path="artifacts")

        best_feature_name, best_feature_score = max(feature_importance_rows, key=lambda item: item[1])
        mlflow.log_param("top_feature_name", best_feature_name)
        mlflow.log_metric("top_feature_importance", float(best_feature_score))

        return {
            "run_id": run.info.run_id,
            "model_name": model_name,
            "pipeline_model": pipeline_model,
            "metrics": metrics,
            "feature_importance": feature_importance_rows,
        }

def choose_best_model(results):
    return max(
        results,
        key=lambda item: (
            item["metrics"]["f1_score"],
            item["metrics"]["accuracy"],
            item["metrics"]["auc_roc"],
        ),
    )

def register_best_model(best_result):
    with mlflow.start_run(run_id=best_result["run_id"]):
        mlflow.set_tag("best_model", "true")
        mlflow.log_metric("selected_for_registry", 1.0)
        mlflow.spark.log_model(
            best_result["pipeline_model"],
            artifact_path="model",
            registered_model_name=REGISTERED_MODEL_NAME,
        )

def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    gold_df = load_gold_features(spark, INPUT_PATH)
    if gold_df.rdd.isEmpty():
        raise ValueError(f"No data found in {INPUT_PATH}.")

    prepared_df = prepare_training_data(gold_df).cache()
    train_df, test_df = prepared_df.randomSplit([0.8, 0.2], seed=42)

    model_specs = build_model_specs()
    results = []
    for model_name, estimator in model_specs:
        results.append(train_one_model(model_name, estimator, train_df, test_df, FEATURE_COLUMNS))

    best_result = choose_best_model(results)
    register_best_model(best_result)

    print("Training completed successfully.")
    print(f"Best model: {best_result['model_name']}")
    spark.stop()

if __name__ == "__main__":
    main()