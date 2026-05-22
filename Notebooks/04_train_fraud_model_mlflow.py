# Databricks notebook source
# ------------------------------------------------------------
# BLOCO 1 — Configuração de caminhos para treinamento
# ------------------------------------------------------------

# ------------------------------------------------------------
# Configuracao segura para GitHub
# ------------------------------------------------------------
# Ajuste os placeholders em src/config.py no seu ambiente Databricks.

import sys
from pathlib import Path

repo_root = Path.cwd().parent if Path.cwd().name.lower() == "notebooks" else Path.cwd()
if str(repo_root) not in sys.path:
    sys.path.append(str(repo_root))

from src.config import DATABRICKS_USER, STORAGE_ACCOUNT

storage_account = STORAGE_ACCOUNT

gold_ml_baseline_path = (
    f"abfss://gold@{storage_account}.dfs.core.windows.net/"
    "fraud/paysim/tables/ml_dataset_baseline/"
)

gold_ml_features_path = (
    f"abfss://gold@{storage_account}.dfs.core.windows.net/"
    "fraud/paysim/tables/ml_dataset_features/"
)

print("Baseline dataset:")
print(gold_ml_baseline_path)

print("Features dataset:")
print(gold_ml_features_path)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 2 — Ler datasets Gold de ML
# ------------------------------------------------------------

baseline_df = spark.read.format("delta").load(gold_ml_baseline_path)
features_df = spark.read.format("delta").load(gold_ml_features_path)

print("Baseline:")
print("Linhas:", baseline_df.count())
print("Colunas:", len(baseline_df.columns))
display(baseline_df.limit(10))

print("Features:")
print("Linhas:", features_df.count())
print("Colunas:", len(features_df.columns))
display(features_df.limit(10))

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 1 — Configuração de caminhos e MLflow
# ------------------------------------------------------------

import mlflow

# ------------------------------------------------------------
# Configuracao segura para GitHub
# ------------------------------------------------------------
# Ajuste os placeholders em src/config.py no seu ambiente Databricks.

import sys
from pathlib import Path

repo_root = Path.cwd().parent if Path.cwd().name.lower() == "notebooks" else Path.cwd()
if str(repo_root) not in sys.path:
    sys.path.append(str(repo_root))

from src.config import DATABRICKS_USER, STORAGE_ACCOUNT

storage_account = STORAGE_ACCOUNT

gold_ml_baseline_path = (
    f"abfss://gold@{storage_account}.dfs.core.windows.net/"
    "fraud/paysim/tables/ml_dataset_baseline/"
)

gold_ml_features_path = (
    f"abfss://gold@{storage_account}.dfs.core.windows.net/"
    "fraud/paysim/tables/ml_dataset_features/"
)

experiment_name = f"/Users/{DATABRICKS_USER}/fraud_detection_paysim"

mlflow.set_experiment(experiment_name)

print("Experimento MLflow configurado:")
print(experiment_name)

print("Baseline:")
print(gold_ml_baseline_path)

print("Features:")
print(gold_ml_features_path)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 3 — Conferir distribuição da variável alvo
# ------------------------------------------------------------

from pyspark.sql.functions import col, count, round

def show_target_distribution(df, dataset_name):
    total = df.count()

    dist_df = (
        df.groupBy("isFraud")
        .agg(count("*").alias("quantidade"))
        .withColumn("percentual", round((col("quantidade") / total) * 100, 4))
        .orderBy("isFraud")
    )

    print(dataset_name)
    display(dist_df)

show_target_distribution(baseline_df, "Distribuição baseline")
show_target_distribution(features_df, "Distribuição features")

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 4 — Função de avaliação para classificação binária
# ------------------------------------------------------------

from pyspark.sql.functions import col
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

def evaluate_binary_classifier(predictions, label_col="isFraud"):
    # Avalia ROC-AUC
    roc_evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )

    # Avalia PR-AUC
    pr_evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR"
    )

    # Avaliadores multiclass para accuracy, precision, recall e f1
    accuracy_evaluator = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="accuracy"
    )

    precision_evaluator = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="weightedPrecision"
    )

    recall_evaluator = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="weightedRecall"
    )

    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="f1"
    )

    metrics = {
        "accuracy": accuracy_evaluator.evaluate(predictions),
        "weighted_precision": precision_evaluator.evaluate(predictions),
        "weighted_recall": recall_evaluator.evaluate(predictions),
        "f1": f1_evaluator.evaluate(predictions),
        "roc_auc": roc_evaluator.evaluate(predictions),
        "pr_auc": pr_evaluator.evaluate(predictions)
    }

    return metrics

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 5 — Funções de treino múltiplo com MLflow
# Versão corrigida para Serverless/Shared:
# registra métricas e parâmetros, mas NÃO salva o SparkML model artifact.
# ------------------------------------------------------------

import mlflow

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import (
    LogisticRegression,
    DecisionTreeClassifier,
    RandomForestClassifier,
    GBTClassifier
)

def train_model_with_mlflow(
    df,
    feature_cols,
    estimator,
    run_name,
    dataset_version,
    model_type
):
    # Divide treino/teste com seed fixa para comparação justa
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    # Une as colunas de entrada em uma única coluna vetorial chamada "features"
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip"
    )

    # Pipeline = transformação + modelo
    pipeline = Pipeline(stages=[assembler, estimator])

    print("\n" + "=" * 90)
    print(f"INICIANDO RUN: {run_name}")
    print(f"Dataset: {dataset_version}")
    print(f"Modelo: {model_type}")
    print("=" * 90)

    with mlflow.start_run(run_name=run_name):
        # Log de parâmetros gerais
        mlflow.log_param("model_type", model_type)
        mlflow.log_param("dataset_version", dataset_version)
        mlflow.log_param("num_features", len(feature_cols))
        mlflow.log_param("feature_columns", ",".join(feature_cols))
        mlflow.log_param("train_ratio", 0.8)
        mlflow.log_param("test_ratio", 0.2)
        mlflow.log_param("seed", 42)

        # Log dos hiperparâmetros do modelo
        for param, value in estimator.extractParamMap().items():
            mlflow.log_param(param.name, str(value))

        print("Treinando modelo...")
        model = pipeline.fit(train_df)

        print("Gerando predições...")
        predictions = model.transform(test_df)

        print("Calculando métricas...")
        metrics = evaluate_binary_classifier(predictions, label_col="isFraud")

        # Log das métricas no MLflow
        for metric_name, metric_value in metrics.items():
            mlflow.log_metric(metric_name, float(metric_value))

        # Importante:
        # Não salvamos o modelo agora porque Serverless/Shared pode exigir UC Volume.
        # Depois criamos um Volume e salvamos só o melhor modelo.
        mlflow.log_param("model_artifact_logged", "false")
        mlflow.log_param("model_artifact_reason", "serverless_requires_uc_volume")

        print("Métricas:")
        for k, v in metrics.items():
            print(f"{k}: {v}")

        print("=" * 90)
        print(f"RUN FINALIZADA: {run_name}")
        print("=" * 90)

        return {
            "run_name": run_name,
            "model_type": model_type,
            "dataset_version": dataset_version,
            "model": model,
            "predictions": predictions,
            "metrics": metrics
        }


def train_multiple_models(
    df,
    feature_cols,
    dataset_version
):
    results = []

    # Modelos para testar.
    # Mantive configs moderadas para não pesar tanto no Serverless.
    models_to_test = [
        {
            "model_type": "LogisticRegression",
            "estimator": LogisticRegression(
                featuresCol="features",
                labelCol="isFraud",
                maxIter=20,
                regParam=0.01,
                elasticNetParam=0.0
            )
        },
        {
            "model_type": "DecisionTreeClassifier",
            "estimator": DecisionTreeClassifier(
                featuresCol="features",
                labelCol="isFraud",
                maxDepth=5,
                seed=42
            )
        },
        {
            "model_type": "RandomForestClassifier",
            "estimator": RandomForestClassifier(
                featuresCol="features",
                labelCol="isFraud",
                numTrees=20,
                maxDepth=8,
                seed=42
            )
        },
        {
            "model_type": "GBTClassifier",
            "estimator": GBTClassifier(
                featuresCol="features",
                labelCol="isFraud",
                maxIter=15,
                maxDepth=5,
                seed=42
            )
        }
    ]

    for i, item in enumerate(models_to_test, start=1):
        model_type = item["model_type"]
        estimator = item["estimator"]

        run_name = f"{dataset_version}_{model_type}"

        print("\n" + "#" * 90)
        print(f"MODELO {i}/{len(models_to_test)}")
        print(f"COMEÇANDO: {run_name}")
        print("#" * 90)

        result = train_model_with_mlflow(
            df=df,
            feature_cols=feature_cols,
            estimator=estimator,
            run_name=run_name,
            dataset_version=dataset_version,
            model_type=model_type
        )

        results.append(result)

        print("\n" + "#" * 90)
        print(f"TERMINOU: {run_name}")
        print("#" * 90)

    return results

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 6 — Definir colunas de entrada dos dois experimentos
# ------------------------------------------------------------

# Baseline: versão mais simples, com variáveis originais numéricas
# e flags básicas de tipo.
baseline_feature_cols = [
    "step",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "isFlaggedFraud",
    "is_transfer",
    "is_cash_out"
]

# Engineered features: versão com as features que criamos na Silver.
engineered_feature_cols = [
    "step",
    "amount",
    "amount_log",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "origin_balance_error_signed",
    "origin_balance_error_abs",
    "dest_balance_error_signed",
    "dest_balance_error_abs",
    "is_zero_balance_origin_before",
    "is_zero_balance_origin_after",
    "is_zero_balance_dest_before",
    "is_zero_balance_dest_after",
    "is_cash_out_or_transfer",
    "is_transfer",
    "is_cash_out",
    "isFlaggedFraud"
]

print("Baseline features:", len(baseline_feature_cols))
print(baseline_feature_cols)

print("Engineered features:", len(engineered_feature_cols))
print(engineered_feature_cols)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 7 — Treinar vários modelos no dataset baseline
# ------------------------------------------------------------

baseline_results = train_multiple_models(
    df=baseline_df,
    feature_cols=baseline_feature_cols,
    dataset_version="baseline"
)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 8 — Treinar vários modelos no dataset com features
# ------------------------------------------------------------

features_results = train_multiple_models(
    df=features_df,
    feature_cols=engineered_feature_cols,
    dataset_version="engineered_features"
)

# COMMAND ----------



# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 9 — Comparar baseline vs engineered features
# Métricas principais: PR-AUC, ROC-AUC, Recall, Precision, F1 e Accuracy
# ------------------------------------------------------------

from pyspark.sql.functions import col, round

comparison_rows = []

all_results = baseline_results + features_results

for result in all_results:
    metrics = result["metrics"]

    row = {
        "run_name": result["run_name"],
        "dataset_version": result["dataset_version"],
        "model_type": result["model_type"],
        "accuracy": float(metrics.get("accuracy")),
        "weighted_precision": float(metrics.get("weighted_precision")),
        "weighted_recall": float(metrics.get("weighted_recall")),
        "f1": float(metrics.get("f1")),
        "roc_auc": float(metrics.get("roc_auc")),
        "pr_auc": float(metrics.get("pr_auc"))
    }

    comparison_rows.append(row)

comparison_df = spark.createDataFrame(comparison_rows)

# Ordena pela métrica mais importante para fraude desbalanceada
display(
    comparison_df
    .orderBy(col("pr_auc").desc())
)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 10 — Calcular métricas específicas da classe fraude
# TP, FP, TN, FN, Precision Fraud, Recall Fraud e F1 Fraud
# ------------------------------------------------------------

from pyspark.sql.functions import col, sum as spark_sum, when, lit, round

def fraud_class_metrics(predictions, model_name, dataset_version, model_type):
    # Garante que prediction e label estejam como inteiros
    pred_df = (
        predictions
        .withColumn("label", col("isFraud").cast("int"))
        .withColumn("pred", col("prediction").cast("int"))
    )

    metrics_df = pred_df.agg(
        spark_sum(when((col("label") == 1) & (col("pred") == 1), 1).otherwise(0)).alias("tp"),
        spark_sum(when((col("label") == 0) & (col("pred") == 1), 1).otherwise(0)).alias("fp"),
        spark_sum(when((col("label") == 0) & (col("pred") == 0), 1).otherwise(0)).alias("tn"),
        spark_sum(when((col("label") == 1) & (col("pred") == 0), 1).otherwise(0)).alias("fn")
    )

    row = metrics_df.collect()[0]

    tp = row["tp"]
    fp = row["fp"]
    tn = row["tn"]
    fn = row["fn"]

    precision_fraud = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall_fraud = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1_fraud = (
        2 * precision_fraud * recall_fraud / (precision_fraud + recall_fraud)
        if (precision_fraud + recall_fraud) > 0
        else 0
    )

    return {
        "run_name": model_name,
        "dataset_version": dataset_version,
        "model_type": model_type,
        "tp": int(tp),
        "fp": int(fp),
        "tn": int(tn),
        "fn": int(fn),
        "precision_fraud": float(precision_fraud),
        "recall_fraud": float(recall_fraud),
        "f1_fraud": float(f1_fraud)
    }


fraud_metric_rows = []

for result in baseline_results + features_results:
    fraud_metric_rows.append(
        fraud_class_metrics(
            predictions=result["predictions"],
            model_name=result["run_name"],
            dataset_version=result["dataset_version"],
            model_type=result["model_type"]
        )
    )

fraud_metrics_df = spark.createDataFrame(fraud_metric_rows)

display(
    fraud_metrics_df
    .orderBy(col("recall_fraud").desc(), col("precision_fraud").desc())
)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 11 — Tabela final comparando ranking geral + classe fraude
# ------------------------------------------------------------

final_comparison_df = (
    comparison_df
    .join(
        fraud_metrics_df,
        on=["run_name", "dataset_version", "model_type"],
        how="inner"
    )
)

display(
    final_comparison_df
    .select(
        "run_name",
        "dataset_version",
        "model_type",
        "pr_auc",
        "roc_auc",
        "precision_fraud",
        "recall_fraud",
        "f1_fraud",
        "tp",
        "fp",
        "tn",
        "fn",
        "accuracy",
        "f1"
    )
    .orderBy(col("pr_auc").desc(), col("recall_fraud").desc())
)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 12 — Logar métricas de fraude no MLflow nas runs existentes
# ------------------------------------------------------------

import mlflow

experiment = mlflow.get_experiment_by_name(experiment_name)

runs_df = mlflow.search_runs(
    experiment_ids=[experiment.experiment_id]
)

for row in fraud_metric_rows:
    run_name = row["run_name"]

    matched_runs = runs_df[runs_df["tags.mlflow.runName"] == run_name]

    if len(matched_runs) == 0:
        print(f"Run não encontrada no MLflow: {run_name}")
        continue

    run_id = matched_runs.iloc[0]["run_id"]

    with mlflow.start_run(run_id=run_id):
        mlflow.log_metric("tp", row["tp"])
        mlflow.log_metric("fp", row["fp"])
        mlflow.log_metric("tn", row["tn"])
        mlflow.log_metric("fn", row["fn"])
        mlflow.log_metric("precision_fraud", row["precision_fraud"])
        mlflow.log_metric("recall_fraud", row["recall_fraud"])
        mlflow.log_metric("f1_fraud", row["f1_fraud"])

    print(f"Métricas de fraude logadas na run: {run_name}")

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 10 — Criar métricas de negócio por modelo
# ------------------------------------------------------------

from pyspark.sql.functions import col
from pyspark.sql import Row

def calculate_business_metrics(result):
    predictions = (
        result["predictions"]
        .withColumn("label", col("isFraud").cast("int"))
        .withColumn("pred", col("prediction").cast("int"))
    )

    counts = predictions.groupBy("label", "pred").count().collect()

    tp = fp = tn = fn = 0

    for row in counts:
        label = int(row["label"])
        pred = int(row["pred"])
        qtd = int(row["count"])

        if label == 1 and pred == 1:
            tp = qtd
        elif label == 0 and pred == 1:
            fp = qtd
        elif label == 0 and pred == 0:
            tn = qtd
        elif label == 1 and pred == 0:
            fn = qtd

    precision_fraud = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall_fraud = tp / (tp + fn) if (tp + fn) > 0 else 0.0

    f1_fraud = (
        2 * precision_fraud * recall_fraud / (precision_fraud + recall_fraud)
        if (precision_fraud + recall_fraud) > 0
        else 0.0
    )

    total_test = tp + fp + tn + fn
    total_fraudes_teste = tp + fn
    total_normais_teste = tn + fp

    metrics = result["metrics"]

    return Row(
        run_name=result["run_name"],
        dataset_version=result["dataset_version"],
        model_type=result["model_type"],

        accuracy=float(metrics["accuracy"]),
        weighted_precision=float(metrics["weighted_precision"]),
        weighted_recall=float(metrics["weighted_recall"]),
        f1_weighted=float(metrics["f1"]),
        roc_auc=float(metrics["roc_auc"]),
        pr_auc=float(metrics["pr_auc"]),

        total_test=int(total_test),
        total_fraudes_teste=int(total_fraudes_teste),
        total_normais_teste=int(total_normais_teste),

        true_positives=int(tp),
        false_positives=int(fp),
        true_negatives=int(tn),
        false_negatives=int(fn),

        fraudes_detectadas=int(tp),
        fraudes_nao_detectadas=int(fn),
        normais_corretas=int(tn),
        normais_marcadas_como_fraude=int(fp),

        precision_fraud=float(precision_fraud),
        recall_fraud=float(recall_fraud),
        f1_fraud=float(f1_fraud)
    )


all_results = baseline_results + features_results

business_rows = [calculate_business_metrics(result) for result in all_results]

model_business_metrics_df = spark.createDataFrame(business_rows)

display(
    model_business_metrics_df
    .orderBy(col("pr_auc").desc(), col("recall_fraud").desc())
)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 11 — Comparação direta: baseline vs engineered features
# ------------------------------------------------------------

from pyspark.sql.functions import col, round

baseline_metrics_df = (
    model_business_metrics_df
    .filter(col("dataset_version") == "baseline")
    .select(
        col("model_type"),
        col("pr_auc").alias("baseline_pr_auc"),
        col("roc_auc").alias("baseline_roc_auc"),
        col("recall_fraud").alias("baseline_recall_fraud"),
        col("precision_fraud").alias("baseline_precision_fraud"),
        col("f1_fraud").alias("baseline_f1_fraud"),
        col("fraudes_detectadas").alias("baseline_fraudes_detectadas"),
        col("fraudes_nao_detectadas").alias("baseline_fraudes_nao_detectadas"),
        col("false_positives").alias("baseline_false_positives")
    )
)

features_metrics_df = (
    model_business_metrics_df
    .filter(col("dataset_version") == "engineered_features")
    .select(
        col("model_type"),
        col("pr_auc").alias("features_pr_auc"),
        col("roc_auc").alias("features_roc_auc"),
        col("recall_fraud").alias("features_recall_fraud"),
        col("precision_fraud").alias("features_precision_fraud"),
        col("f1_fraud").alias("features_f1_fraud"),
        col("fraudes_detectadas").alias("features_fraudes_detectadas"),
        col("fraudes_nao_detectadas").alias("features_fraudes_nao_detectadas"),
        col("false_positives").alias("features_false_positives")
    )
)

model_before_after_df = (
    baseline_metrics_df
    .join(features_metrics_df, on="model_type", how="inner")

    .withColumn(
        "ganho_pr_auc_abs",
        round(col("features_pr_auc") - col("baseline_pr_auc"), 6)
    )
    .withColumn(
        "ganho_pr_auc_percentual",
        round(((col("features_pr_auc") - col("baseline_pr_auc")) / col("baseline_pr_auc")) * 100, 2)
    )
    .withColumn(
        "ganho_recall_fraud_abs",
        round(col("features_recall_fraud") - col("baseline_recall_fraud"), 6)
    )
    .withColumn(
        "fraudes_detectadas_a_mais",
        col("features_fraudes_detectadas") - col("baseline_fraudes_detectadas")
    )
    .withColumn(
        "fraudes_perdidas_a_menos",
        col("baseline_fraudes_nao_detectadas") - col("features_fraudes_nao_detectadas")
    )
    .orderBy(col("features_pr_auc").desc())
)

display(model_before_after_df)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 12 — Resumo textual para relatório
# ------------------------------------------------------------

from pyspark.sql.functions import concat, lit

report_summary_df = (
    model_before_after_df
    .select(
        "model_type",
        "baseline_pr_auc",
        "features_pr_auc",
        "ganho_pr_auc_abs",
        "ganho_pr_auc_percentual",
        "baseline_recall_fraud",
        "features_recall_fraud",
        "fraudes_detectadas_a_mais",
        "fraudes_perdidas_a_menos"
    )
    .withColumn(
        "resumo_relatorio",
        concat(
            lit("No modelo "), col("model_type"),
            lit(", o uso das features engenheiradas elevou o PR-AUC de "),
            round(col("baseline_pr_auc"), 4).cast("string"),
            lit(" para "),
            round(col("features_pr_auc"), 4).cast("string"),
            lit(", com ganho absoluto de "),
            round(col("ganho_pr_auc_abs"), 4).cast("string"),
            lit(" e ganho percentual de "),
            round(col("ganho_pr_auc_percentual"), 2).cast("string"),
            lit("%. O modelo detectou "),
            col("fraudes_detectadas_a_mais").cast("string"),
            lit(" fraudes a mais e reduziu em "),
            col("fraudes_perdidas_a_menos").cast("string"),
            lit(" as fraudes não detectadas em relação ao baseline.")
        )
    )
)

display(report_summary_df)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 13 — Salvar métricas finais em CSV no Gold
# ------------------------------------------------------------

# ------------------------------------------------------------
# Configuracao segura para GitHub
# ------------------------------------------------------------
# Ajuste os placeholders em src/config.py no seu ambiente Databricks.

import sys
from pathlib import Path

repo_root = Path.cwd().parent if Path.cwd().name.lower() == "notebooks" else Path.cwd()
if str(repo_root) not in sys.path:
    sys.path.append(str(repo_root))

from src.config import DATABRICKS_USER, STORAGE_ACCOUNT

storage_account = STORAGE_ACCOUNT

gold_exports_root = (
    f"abfss://gold@{storage_account}.dfs.core.windows.net/"
    "fraud/paysim/exports/"
)

model_business_metrics_csv_path = gold_exports_root + "model_business_metrics_csv/"
model_before_after_csv_path = gold_exports_root + "model_before_after_comparison_csv/"
report_summary_csv_path = gold_exports_root + "model_report_summary_csv/"

(
    model_business_metrics_df
    .coalesce(1)
    .write
    .format("csv")
    .mode("overwrite")
    .option("header", True)
    .save(model_business_metrics_csv_path)
)

(
    model_before_after_df
    .coalesce(1)
    .write
    .format("csv")
    .mode("overwrite")
    .option("header", True)
    .save(model_before_after_csv_path)
)

(
    report_summary_df
    .coalesce(1)
    .write
    .format("csv")
    .mode("overwrite")
    .option("header", True)
    .save(report_summary_csv_path)
)

print("CSVs salvos em:")
print(model_business_metrics_csv_path)
print(model_before_after_csv_path)
print(report_summary_csv_path)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 14 — Conferir CSVs exportados
# ------------------------------------------------------------

print("Model business metrics CSV:")
display(dbutils.fs.ls(model_business_metrics_csv_path))

print("Before/after comparison CSV:")
display(dbutils.fs.ls(model_before_after_csv_path))

print("Report summary CSV:")
display(dbutils.fs.ls(report_summary_csv_path))

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 15 — Métricas financeiras por modelo
# Quanto dinheiro fraudulento foi detectado/perdido
# ------------------------------------------------------------

from pyspark.sql.functions import col, sum as spark_sum, when
from pyspark.sql import Row

def calculate_financial_metrics(result):
    predictions = (
        result["predictions"]
        .withColumn("label", col("isFraud").cast("int"))
        .withColumn("pred", col("prediction").cast("int"))
    )

    agg = predictions.agg(
        # Valor total das fraudes reais no teste
        spark_sum(
            when(col("label") == 1, col("amount")).otherwise(0)
        ).alias("valor_total_fraude_teste"),

        # Valor das fraudes que o modelo detectou
        spark_sum(
            when((col("label") == 1) & (col("pred") == 1), col("amount")).otherwise(0)
        ).alias("valor_fraude_detectado"),

        # Valor das fraudes que passaram despercebidas
        spark_sum(
            when((col("label") == 1) & (col("pred") == 0), col("amount")).otherwise(0)
        ).alias("valor_fraude_nao_detectado"),

        # Valor de transações normais marcadas como fraude
        spark_sum(
            when((col("label") == 0) & (col("pred") == 1), col("amount")).otherwise(0)
        ).alias("valor_normal_marcado_como_fraude")
    ).collect()[0]

    valor_total_fraude = float(agg["valor_total_fraude_teste"] or 0)
    valor_detectado = float(agg["valor_fraude_detectado"] or 0)
    valor_nao_detectado = float(agg["valor_fraude_nao_detectado"] or 0)
    valor_falso_positivo = float(agg["valor_normal_marcado_como_fraude"] or 0)

    taxa_valor_detectado = (
        valor_detectado / valor_total_fraude
        if valor_total_fraude > 0
        else 0
    )

    return Row(
        run_name=result["run_name"],
        dataset_version=result["dataset_version"],
        model_type=result["model_type"],
        valor_total_fraude_teste=valor_total_fraude,
        valor_fraude_detectado=valor_detectado,
        valor_fraude_nao_detectado=valor_nao_detectado,
        valor_normal_marcado_como_fraude=valor_falso_positivo,
        taxa_valor_fraude_detectado=taxa_valor_detectado
    )


financial_rows = [
    calculate_financial_metrics(result)
    for result in baseline_results + features_results
]

model_financial_metrics_df = spark.createDataFrame(financial_rows)

display(
    model_financial_metrics_df
    .orderBy(col("taxa_valor_fraude_detectado").desc())
)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 16 — Comparação financeira baseline vs features
# ------------------------------------------------------------

from pyspark.sql.functions import round

baseline_financial_df = (
    model_financial_metrics_df
    .filter(col("dataset_version") == "baseline")
    .select(
        "model_type",
        col("valor_fraude_detectado").alias("baseline_valor_fraude_detectado"),
        col("valor_fraude_nao_detectado").alias("baseline_valor_fraude_nao_detectado"),
        col("taxa_valor_fraude_detectado").alias("baseline_taxa_valor_detectado")
    )
)

features_financial_df = (
    model_financial_metrics_df
    .filter(col("dataset_version") == "engineered_features")
    .select(
        "model_type",
        col("valor_fraude_detectado").alias("features_valor_fraude_detectado"),
        col("valor_fraude_nao_detectado").alias("features_valor_fraude_nao_detectado"),
        col("taxa_valor_fraude_detectado").alias("features_taxa_valor_detectado")
    )
)

financial_before_after_df = (
    baseline_financial_df
    .join(features_financial_df, on="model_type", how="inner")
    .withColumn(
        "valor_fraude_detectado_a_mais",
        col("features_valor_fraude_detectado") - col("baseline_valor_fraude_detectado")
    )
    .withColumn(
        "valor_fraude_perdido_a_menos",
        col("baseline_valor_fraude_nao_detectado") - col("features_valor_fraude_nao_detectado")
    )
    .withColumn(
        "ganho_taxa_valor_detectado",
        col("features_taxa_valor_detectado") - col("baseline_taxa_valor_detectado")
    )
)

display(
    financial_before_after_df
    .orderBy(col("valor_fraude_detectado_a_mais").desc())
)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 17 — Salvar comparação financeira em CSV no Gold
# ------------------------------------------------------------

# ------------------------------------------------------------
# Configuracao segura para GitHub
# ------------------------------------------------------------
# Ajuste os placeholders em src/config.py no seu ambiente Databricks.

import sys
from pathlib import Path

repo_root = Path.cwd().parent if Path.cwd().name.lower() == "notebooks" else Path.cwd()
if str(repo_root) not in sys.path:
    sys.path.append(str(repo_root))

from src.config import DATABRICKS_USER, STORAGE_ACCOUNT

storage_account = STORAGE_ACCOUNT

financial_csv_path = (
    f"abfss://gold@{storage_account}.dfs.core.windows.net/"
    "fraud/paysim/exports/model_financial_comparison_csv/"
)

(
    financial_before_after_df
    .coalesce(1)
    .write
    .format("csv")
    .mode("overwrite")
    .option("header", True)
    .save(financial_csv_path)
)

print("CSV financeiro salvo em:")
print(financial_csv_path)

display(dbutils.fs.ls(financial_csv_path))

# COMMAND ----------

