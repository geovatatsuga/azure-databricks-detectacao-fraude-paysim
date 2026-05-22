# Databricks notebook source
# ------------------------------------------------------------
# BLOCO 1 — Configuração de caminhos Gold
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

# Entrada: tabela Silver criada no notebook anterior
silver_delta_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/fraud/paysim/tables/transactions_clean/"

# Saídas Gold
gold_kpis_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/fraud/paysim/tables/fraud_kpis/"
gold_by_type_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/fraud/paysim/tables/fraud_by_type/"
gold_ml_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/fraud/paysim/tables/ml_dataset/"

print("Silver:", silver_delta_path)
print("Gold KPIs:", gold_kpis_path)
print("Gold by type:", gold_by_type_path)
print("Gold ML:", gold_ml_path)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 2 — Ler tabela Silver
# ------------------------------------------------------------

silver_df = spark.read.format("delta").load(silver_delta_path)

display(silver_df.limit(10))

print("Linhas na Silver:", silver_df.count())
print("Colunas na Silver:", len(silver_df.columns))

silver_df.printSchema()

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 3 — Criar tabela Gold de KPIs gerais
# ------------------------------------------------------------

from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    round,
    when,
    lit
)

# Conta total de linhas da Silver
total_rows = silver_df.count()

# Cria uma tabela pequena com indicadores gerais do dataset
gold_kpis_df = (
    silver_df
    .agg(
        count("*").alias("total_transacoes"),
        spark_sum(col("isFraud")).alias("total_fraudes"),
        spark_sum(col("amount")).alias("valor_total_transacionado"),
        spark_sum(
            when(col("isFraud") == 1, col("amount")).otherwise(0)
        ).alias("valor_total_fraude"),
        avg(col("amount")).alias("ticket_medio_geral"),
        avg(
            when(col("isFraud") == 1, col("amount"))
        ).alias("ticket_medio_fraude"),
        avg(
            when(col("isFraud") == 0, col("amount"))
        ).alias("ticket_medio_normal"),
        spark_sum(col("isFlaggedFraud")).alias("total_flagged_fraud")
    )
    .withColumn(
        "taxa_fraude_percentual",
        round((col("total_fraudes") / col("total_transacoes")) * 100, 4)
    )
    .withColumn(
        "taxa_flagged_percentual",
        round((col("total_flagged_fraud") / col("total_transacoes")) * 100, 6)
    )
    .withColumn("dataset", lit("PaySim"))
)

display(gold_kpis_df)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 4 — Salvar KPIs gerais em Delta no Gold
# ------------------------------------------------------------

gold_kpis_path = (
    f"abfss://gold@{storage_account}.dfs.core.windows.net/"
    "fraud/paysim/tables/fraud_kpis/"
)

(
    gold_kpis_df.write
    .format("delta")
    .mode("overwrite")
    .save(gold_kpis_path)
)

print("Gold KPIs salvo em:")
print(gold_kpis_path)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 5 — Criar tabela Gold por tipo de transação
# ------------------------------------------------------------

gold_by_type_df = (
    silver_df
    .groupBy("type")
    .agg(
        count("*").alias("total_transacoes"),
        spark_sum(col("isFraud")).alias("total_fraudes"),
        spark_sum(col("amount")).alias("valor_total"),
        avg(col("amount")).alias("valor_medio"),
        avg(col("amount_log")).alias("amount_log_medio"),
        spark_sum(col("isFlaggedFraud")).alias("total_flagged_fraud")
    )
    .withColumn(
        "taxa_fraude_percentual",
        round((col("total_fraudes") / col("total_transacoes")) * 100, 4)
    )
    .withColumn(
        "percentual_transacoes",
        round((col("total_transacoes") / total_rows) * 100, 4)
    )
    .orderBy(col("taxa_fraude_percentual").desc())
)

display(gold_by_type_df)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 6 — Salvar tabela por tipo em Delta no Gold
# ------------------------------------------------------------

gold_by_type_path = (
    f"abfss://gold@{storage_account}.dfs.core.windows.net/"
    "fraud/paysim/tables/fraud_by_type/"
)

(
    gold_by_type_df.write
    .format("delta")
    .mode("overwrite")
    .save(gold_by_type_path)
)

print("Gold by type salvo em:")
print(gold_by_type_path)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 7 — Criar dataset Gold baseline para Machine Learning
# ------------------------------------------------------------

# Baseline = versão simples, quase sem feature engineering.
# Objetivo: servir como comparação contra o modelo com features criadas.
#
# Não usamos nameOrig e nameDest porque são IDs com cardinalidade muito alta.
# Também não usamos a coluna type como string diretamente, porque modelos Spark ML
# geralmente precisam de variáveis numéricas.

baseline_columns = [
    "step",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "isFlaggedFraud",
    "is_transfer",
    "is_cash_out",
    "isFraud"
]

gold_ml_baseline_df = silver_df.select(baseline_columns)

display(gold_ml_baseline_df.limit(20))

print("Linhas baseline:", gold_ml_baseline_df.count())
print("Colunas baseline:", len(gold_ml_baseline_df.columns))

gold_ml_baseline_df.printSchema()

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 8 — Salvar dataset baseline em Delta no Gold
# ------------------------------------------------------------

gold_ml_baseline_path = (
    f"abfss://gold@{storage_account}.dfs.core.windows.net/"
    "fraud/paysim/tables/ml_dataset_baseline/"
)

(
    gold_ml_baseline_df.write
    .format("delta")
    .mode("overwrite")
    .save(gold_ml_baseline_path)
)

print("Gold ML baseline salvo em:")
print(gold_ml_baseline_path)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 9 — Criar dataset Gold com features para Machine Learning
# ------------------------------------------------------------

# Features = versão enriquecida.
# Aqui entram as variáveis criadas na Silver:
# - amount_log
# - erros de saldo
# - flags de saldo zerado
# - flags de tipo suspeito

feature_columns = [
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
    "isFlaggedFraud",
    "isFraud"
]

gold_ml_features_df = silver_df.select(feature_columns)

display(gold_ml_features_df.limit(20))

print("Linhas features:", gold_ml_features_df.count())
print("Colunas features:", len(gold_ml_features_df.columns))

gold_ml_features_df.printSchema()

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 10 — Salvar dataset com features em Delta no Gold
# ------------------------------------------------------------

gold_ml_features_path = (
    f"abfss://gold@{storage_account}.dfs.core.windows.net/"
    "fraud/paysim/tables/ml_dataset_features/"
)

(
    gold_ml_features_df.write
    .format("delta")
    .mode("overwrite")
    .save(gold_ml_features_path)
)

print("Gold ML features salvo em:")
print(gold_ml_features_path)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 11 — Validar datasets Gold de ML
# ------------------------------------------------------------

baseline_check_df = spark.read.format("delta").load(gold_ml_baseline_path)
features_check_df = spark.read.format("delta").load(gold_ml_features_path)

print("Baseline:")
display(baseline_check_df.limit(10))
print("Linhas baseline:", baseline_check_df.count())
print("Colunas baseline:", len(baseline_check_df.columns))

print("Features:")
display(features_check_df.limit(10))
print("Linhas features:", features_check_df.count())
print("Colunas features:", len(features_check_df.columns))

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 12 — Conferir distribuição de fraude nos datasets de ML
# ------------------------------------------------------------

from pyspark.sql.functions import col, count, round

baseline_total = baseline_check_df.count()
features_total = features_check_df.count()

baseline_target_dist = (
    baseline_check_df
    .groupBy("isFraud")
    .agg(count("*").alias("quantidade"))
    .withColumn("percentual", round((col("quantidade") / baseline_total) * 100, 4))
    .orderBy("isFraud")
)

features_target_dist = (
    features_check_df
    .groupBy("isFraud")
    .agg(count("*").alias("quantidade"))
    .withColumn("percentual", round((col("quantidade") / features_total) * 100, 4))
    .orderBy("isFraud")
)

print("Distribuição baseline:")
display(baseline_target_dist)

print("Distribuição features:")
display(features_target_dist)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 13 — Conferir arquivos físicos no ADLS Gold
# ------------------------------------------------------------

print("Gold KPIs:")
display(dbutils.fs.ls(gold_kpis_path))

print("Gold by type:")
display(dbutils.fs.ls(gold_by_type_path))

print("Gold ML baseline:")
display(dbutils.fs.ls(gold_ml_baseline_path))

print("Gold ML features:")
display(dbutils.fs.ls(gold_ml_features_path))

# COMMAND ----------

