# Databricks notebook source
# MAGIC %pip install kaggle

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# ------------------------------------------------------------
# Configuracao segura das credenciais Kaggle
# ------------------------------------------------------------
# Crie um secret scope no Databricks com as chaves abaixo antes de executar.

import os

os.environ["KAGGLE_USERNAME"] = dbutils.secrets.get(scope="kaggle", key="username")
os.environ["KAGGLE_KEY"] = dbutils.secrets.get(scope="kaggle", key="key")

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd
# MAGIC whoami
# MAGIC mkdir -p /tmp/paysim_test
# MAGIC echo ok > /tmp/paysim_test/test.txt
# MAGIC ls -lh /tmp/paysim_test

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/paysim
# MAGIC mkdir -p /tmp/paysim
# MAGIC
# MAGIC kaggle datasets download -d ealaxi/paysim1 -p /tmp/paysim --unzip
# MAGIC
# MAGIC ls -lh /tmp/paysim

# COMMAND ----------

# MAGIC %sh
# MAGIC find /tmp/paysim -type f -maxdepth 2

# COMMAND ----------

# Caminho LOCAL onde o Kaggle baixou o CSV dentro do ambiente do Databricks.

# Nome da sua Storage Account no Azure.
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

# Camada bronze: cópia fiel do arquivo original baixado.
# Aqui guardamos o CSV bruto exatamente como veio da fonte.
bronze_source_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/fraud/paysim/source_files/"

# Camada bronze em Delta.
# Delta usa Parquet por baixo, mas adiciona controle transacional, histórico e melhor confiabilidade.
bronze_delta_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/fraud/paysim/tables/raw_transactions/"

print("CSV local:", local_csv_path)
print("Bronze source:", bronze_source_path)
print("Bronze Delta:", bronze_delta_path)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 2 — Definir pasta permitida no Workspace
# ------------------------------------------------------------

import os

# Pega sua pasta atual do notebook no Workspace.
workspace_dir = os.getcwd()

# Pasta onde vamos guardar temporariamente o arquivo baixado.
workspace_paysim_dir = f"{workspace_dir}/paysim_download"

print("Workspace atual:", workspace_dir)
print("Pasta temporária permitida:", workspace_paysim_dir)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 3 — Baixar o PaySim dentro da pasta do Workspace
# ------------------------------------------------------------

import os
import subprocess

os.makedirs(workspace_paysim_dir, exist_ok=True)

result = subprocess.run(
    [
        "kaggle",
        "datasets",
        "download",
        "-d",
        "ealaxi/paysim1",
        "-p",
        workspace_paysim_dir,
        "--unzip"
    ],
    capture_output=True,
    text=True
)

print(result.stdout)
print(result.stderr)

print("Arquivos baixados:")
print(os.listdir(workspace_paysim_dir))

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 4 — Apontar para o CSV baixado no Workspace
# ------------------------------------------------------------

import glob

csv_files = glob.glob(f"{workspace_paysim_dir}/*.csv")

print("CSVs encontrados:", csv_files)

local_csv_path = "file:" + csv_files[0]

print("Novo caminho local_csv_path:", local_csv_path)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 5 — Copiar CSV do Workspace para o ADLS Bronze
# ------------------------------------------------------------

dbutils.fs.mkdirs(bronze_source_path)

dbutils.fs.cp(
    local_csv_path,
    bronze_source_path + "paysim.csv",
    recurse=False
)

display(dbutils.fs.ls(bronze_source_path))

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 6 — Ler o CSV bruto diretamente do Azure Storage
# ------------------------------------------------------------

# Caminho do CSV que você acabou de salvar no container bronze
source_csv_path = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
    "fraud/paysim/source_files/paysim.csv"
)

# Lê o CSV com Spark
# header=True usa a primeira linha como nomes das colunas
# inferSchema=True tenta descobrir automaticamente os tipos das colunas
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(source_csv_path)
)

# Mostra uma amostra dos dados
display(df.limit(20))

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 7 — Validar volume e estrutura do dataset
# ------------------------------------------------------------

total_linhas = df.count()
total_colunas = len(df.columns)

print("Total de linhas:", total_linhas)
print("Total de colunas:", total_colunas)

# Mostra os tipos das colunas
df.printSchema()

# Mostra nomes das colunas
print("Colunas:", df.columns)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 8 — Salvar CSV bruto como tabela Delta no Bronze
# ------------------------------------------------------------

# Caminho onde ficará a versão Delta da camada bronze
bronze_delta_path = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
    "fraud/paysim/tables/raw_transactions/"
)

# Salva o DataFrame em formato Delta.
# Delta usa arquivos Parquet por baixo, mas adiciona log transacional,
# controle ACID e metadata mais confiável para lakehouse.
(
    df.write
    .format("delta")
    .mode("overwrite")
    .save(bronze_delta_path)
)

print("Bronze Delta salvo em:")
print(bronze_delta_path)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 9 — Validar se o Bronze Delta foi criado corretamente
# ------------------------------------------------------------

# Lê a tabela Delta criada no bloco anterior
bronze_df = spark.read.format("delta").load(bronze_delta_path)

# Mostra uma amostra dos dados
display(bronze_df.limit(10))

# Confere se a quantidade continua correta
print("Linhas no bronze delta:", bronze_df.count())
print("Colunas no bronze delta:", len(bronze_df.columns))

# Mostra o schema salvo em Delta
bronze_df.printSchema()

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 10 — Ver distribuição da variável alvo isFraud
# ------------------------------------------------------------

from pyspark.sql.functions import col, count, round

# Conta total de registros
total_rows = bronze_df.count()

# Calcula quantidade e percentual de fraude e não fraude
fraud_dist = (
    bronze_df
    .groupBy("isFraud")
    .agg(count("*").alias("quantidade"))
    .withColumn("percentual", round((col("quantidade") / total_rows) * 100, 4))
    .orderBy("isFraud")
)

display(fraud_dist)

# COMMAND ----------

