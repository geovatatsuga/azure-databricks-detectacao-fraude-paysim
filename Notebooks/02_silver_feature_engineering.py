# Databricks notebook source
# ------------------------------------------------------------
# BLOCO 1 — Configuração de caminhos Silver
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

# Caminho da tabela Delta bronze criada no notebook anterior
bronze_delta_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/fraud/paysim/tables/raw_transactions/"

# Caminho onde vamos salvar a tabela Silver
silver_delta_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/fraud/paysim/tables/transactions_clean/"

print("Bronze Delta:", bronze_delta_path)
print("Silver Delta:", silver_delta_path)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 2 — Ler a tabela Bronze Delta
# ------------------------------------------------------------

bronze_df = spark.read.format("delta").load(bronze_delta_path)

display(bronze_df.limit(10))

print("Linhas no bronze:", bronze_df.count())
print("Colunas no bronze:", len(bronze_df.columns))

bronze_df.printSchema()

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 3 — Diagnóstico inicial do dataset Bronze
# - Ver volume
# - Ver schema
# - Ver nulos
# - Ver duplicatas
# - Ver distribuição da variável alvo
# - Ver estatísticas numéricas
# - Ver possíveis inconsistências
# ------------------------------------------------------------

from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    when,
    round,
    min as spark_min,
    max as spark_max,
    mean,
    stddev,
    expr
)

# DataFrame base vindo do bronze
df_diag = bronze_df

print("Total de linhas:", df_diag.count())
print("Total de colunas:", len(df_diag.columns))

df_diag.printSchema()

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 3.2 — Verificar valores nulos por coluna
# ------------------------------------------------------------

from pyspark.sql.functions import col, sum as spark_sum, when, round

total_rows = bronze_df.count()

null_exprs = [
    spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in bronze_df.columns
]

null_counts_df = bronze_df.select(null_exprs)

nulls_long_df = null_counts_df.selectExpr(
    "stack(" + str(len(bronze_df.columns)) + ", " +
    ", ".join([f"'{c}', `{c}`" for c in bronze_df.columns]) +
    ") as (coluna, nulos)"
).withColumn(
    "percentual_nulos",
    round((col("nulos") / total_rows) * 100, 4)
).orderBy(col("nulos").desc())

display(nulls_long_df)

# COMMAND ----------

# DBTITLE 1,Cell 5
# ------------------------------------------------------------
# BLOCO 3.3 — Verificar duplicatas exatas
# ------------------------------------------------------------

total_rows = bronze_df.count()
unique_rows = bronze_df.dropDuplicates().count()
duplicated_rows = total_rows - unique_rows
duplicate_pct = (duplicated_rows / total_rows) * 100 if total_rows else 0

print("Total de linhas:", total_rows)
print("Linhas únicas:", unique_rows)
print("Duplicatas exatas:", duplicated_rows)
print(f"Percentual duplicado: {duplicate_pct:.6f} %")

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 3.5 — Distribuição por tipo de transação
# ------------------------------------------------------------

type_dist_df = (
    bronze_df
    .groupBy("type")
    .agg(count("*").alias("quantidade"))
    .withColumn("percentual", round((col("quantidade") / total_rows) * 100, 4))
    .orderBy(col("quantidade").desc())
)

display(type_dist_df)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 3.6 — Fraudes por tipo de transação
# ------------------------------------------------------------

fraud_by_type_df = (
    bronze_df
    .groupBy("type", "isFraud")
    .agg(count("*").alias("quantidade"))
    .withColumn("percentual_sobre_total", round((col("quantidade") / total_rows) * 100, 4))
    .orderBy("type", "isFraud")
)

display(fraud_by_type_df)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 3.7 — Taxa de fraude por tipo de transação
# ------------------------------------------------------------

from pyspark.sql.functions import sum as spark_sum

fraud_rate_by_type_df = (
    bronze_df
    .groupBy("type")
    .agg(
        count("*").alias("total_transacoes"),
        spark_sum(col("isFraud")).alias("total_fraudes")
    )
    .withColumn(
        "taxa_fraude_percentual",
        round((col("total_fraudes") / col("total_transacoes")) * 100, 4)
    )
    .orderBy(col("taxa_fraude_percentual").desc())
)

display(fraud_rate_by_type_df)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 3.8 — Estatísticas descritivas das colunas numéricas
# ------------------------------------------------------------

numeric_cols = [
    "step",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud"
]

summary_df = bronze_df.select(numeric_cols).summary(
    "count",
    "mean",
    "stddev",
    "min",
    "25%",
    "50%",
    "75%",
    "max"
)

display(summary_df)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 3.9 — Diagnóstico de erros de saldo
# ------------------------------------------------------------

balance_diag_df = (
    bronze_df
    .withColumn(
        "origin_balance_error",
        ((col("oldbalanceOrg") - col("amount")) - col("newbalanceOrig"))
    )
    .withColumn(
        "dest_balance_error",
        ((col("oldbalanceDest") + col("amount")) - col("newbalanceDest"))
    )
)

balance_summary_df = balance_diag_df.select(
    "origin_balance_error",
    "dest_balance_error"
).summary(
    "count",
    "mean",
    "stddev",
    "min",
    "25%",
    "50%",
    "75%",
    "max"
)

display(balance_summary_df)

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 3.10 — Ver exemplos de transações fraudulentas
# ------------------------------------------------------------

fraud_examples_df = (
    bronze_df
    .filter(col("isFraud") == 1)
    .select(
        "step",
        "type",
        "amount",
        "nameOrig",
        "oldbalanceOrg",
        "newbalanceOrig",
        "nameDest",
        "oldbalanceDest",
        "newbalanceDest",
        "isFlaggedFraud"
    )
    .orderBy(col("amount").desc())
)

display(fraud_examples_df.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Fraude só aparece em **TRANSFER e CASH_OUT**
# MAGIC
# MAGIC
# MAGIC **TRANSFER   → 532.909 transações | 4.097 fraudes | 0,7688%
# MAGIC **
# MAGIC **CASH_OUT   → 2.237.500 transações | 4.116 fraudes | 0,1840%**
# MAGIC
# MAGIC DEBIT      → 0 fraudes
# MAGIC PAYMENT    → 0 fraudes
# MAGIC CASH_IN    → 0 fraudes
# MAGIC
# MAGIC Isso é um achado muito importante. No PaySim, a fraude está concentrada apenas em operações de TRANSFER e CASH_OUT. Isso significa que, para o modelo, a coluna type é extremamente informativa.
# MAGIC
# MAGIC As transações fraudulentas se concentram em movimentos de saída de dinheiro, principalmente **transferências e saques**. Operações como pagamento, débito e entrada de dinheiro não apresentam fraude no dataset analisado.
# MAGIC
# MAGIC Para o projeto, isso justifica criar uma feature como:
# MAGIC
# MAGIC **is_cash_out_or_transfer**
# MAGIC
# MAGIC porque ela separa os tipos com risco real dos tipos sem fraude.

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Valores de amount são muito assimétricos**
# MAGIC
# MAGIC Resumo:
# MAGIC
# MAGIC mean amount: 179.861
# MAGIC median amount: 74.857
# MAGIC max amount: 92.445.516
# MAGIC
# MAGIC A média é bem maior que a mediana, e o máximo é gigantesco. Isso mostra que a distribuição de amount é muito assimétrica, com poucos valores muito altos puxando a média para cima.
# MAGIC
# MAGIC Interpretação:
# MAGIC
# MAGIC A variável amount tem cauda longa, com transações muito grandes em relação ao comportamento central. Por isso, usar uma transformação logarítmica pode ajudar o modelo.
# MAGIC
# MAGIC Isso justifica criar:
# MAGIC
# MAGIC **amount_log = log1p(amount)**

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Saldos têm muitos zeros nasa transações fraudulentas
# MAGIC
# MAGIC Pelo resumo:
# MAGIC
# MAGIC oldbalanceOrg 25% = 0
# MAGIC newbalanceOrig 50% = 0
# MAGIC oldbalanceDest 25% = 0
# MAGIC newbalanceDest 25% = 0
# MAGIC
# MAGIC Isso quer dizer que uma parte grande das transações envolve contas com saldo inicial ou final zerado.
# MAGIC
# MAGIC Interpretação:
# MAGIC
# MAGIC A presença de saldo zero é comum no dataset e pode indicar comportamento relevante para fraude, principalmente quando a origem zera após uma transação.
# MAGIC
# MAGIC Isso justifica criar:
# MAGIC
# MAGIC is_zero_balance_origin_before
# MAGIC
# MAGIC is_zero_balance_origin_after
# MAGIC
# MAGIC is_zero_balance_dest_before
# MAGIC
# MAGIC is_zero_balance_dest_after

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Transaç~eos financeiras nem semrpe sã obem sucedidas
# MAGIC
# MAGIC O PaySim simula transações financeiras e tem colunas de saldo antes/depois da origem e do destino. O próprio dataset informa essas colunas: oldbalanceOrg, newbalanceOrig, oldbalanceDest e newbalanceDest. Ele também alerta que, em transações detectadas como fraude, a transação é cancelada, então essas colunas de saldo podem se comportar de forma diferente e precisam ser interpretadas com cuidado.
# MAGIC
# MAGIC A ideia simples
# MAGIC
# MAGIC Imagine uma transação de R$ 100 saindo de uma conta.
# MAGIC
# MAGIC Para a origem, o esperado seria:
# MAGIC
# MAGIC saldo antigo da origem - valor da transação = novo saldo da origem
# MAGIC
# MAGIC Exemplo normal:
# MAGIC
# MAGIC oldbalanceOrg = 1000
# MAGIC amount = 100
# MAGIC newbalanceOrig = 900
# MAGIC
# MAGIC Então:
# MAGIC
# MAGIC 1000 - 100 = 900
# MAGIC
# MAGIC Erro:
# MAGIC
# MAGIC (1000 - 100) - 900 = 0
# MAGIC
# MAGIC Erro zero significa: o saldo bateu certinho.
# MAGIC
# MAGIC Agora imagine:
# MAGIC
# MAGIC oldbalanceOrg = 1000
# MAGIC amount = 100
# MAGIC newbalanceOrig = 1000
# MAGIC
# MAGIC Então:
# MAGIC
# MAGIC (1000 - 100) - 1000 = -100
# MAGIC
# MAGIC Isso indica que, pelo saldo registrado, o dinheiro saiu, mas o saldo da origem não mudou como esperado.
# MAGIC
# MAGIC O que calculamos
# MAGIC
# MAGIC Você calculou:
# MAGIC
# MAGIC origin_balance_error = (oldbalanceOrg - amount) - newbalanceOrig
# MAGIC
# MAGIC e:
# MAGIC
# MAGIC dest_balance_error = (oldbalanceDest + amount) - newbalanceDest
# MAGIC
# MAGIC Para o destino, a lógica é:
# MAGIC
# MAGIC saldo antigo do destino + valor recebido = novo saldo do destino
# MAGIC
# MAGIC Então, se o destino tinha 500, recebeu 100 e ficou com 600, o erro é zero.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Usando o erro de balnaço das transações
# MAGIC
# MAGIC Não vamos usar só o erro “com sinal”, porque ele pode ser negativo ou positivo. Vamos criar duas versões:
# MAGIC
# MAGIC origin_balance_error_signed
# MAGIC
# MAGIC origin_balance_error_abs
# MAGIC
# MAGIC dest_balance_error_signed
# MAGIC
# MAGIC dest_balance_error_abs
# MAGIC
# MAGIC
# MAGIC A versão signed mantém a direção do erro:
# MAGIC
# MAGIC positivo ou negativo
# MAGIC
# MAGIC A versão abs mede o tamanho do erro, ignorando o sinal:
# MAGIC
# MAGIC quanto maior, mais inconsistente
# MAGIC
# MAGIC Exemplo:
# MAGIC
# MAGIC erro = -100
# MAGIC erro absoluto = 100
# MAGIC
# MAGIC Para machine learning, isso é útil porque o modelo pode aprender que certas inconsistências de saldo aparecem mais em transações suspeitas.

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 4 — Feature Engineering da camada Silver
# ------------------------------------------------------------

from pyspark.sql.functions import (
    col,
    when,
    abs as spark_abs,
    log1p,
    lit
)

silver_df = (
    bronze_df
    # Remove duplicatas exatas, caso existam
    .dropDuplicates()

    # Padronização de tipos
    .withColumn("step", col("step").cast("int"))
    .withColumn("type", col("type").cast("string"))
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("nameOrig", col("nameOrig").cast("string"))
    .withColumn("oldbalanceOrg", col("oldbalanceOrg").cast("double"))
    .withColumn("newbalanceOrig", col("newbalanceOrig").cast("double"))
    .withColumn("nameDest", col("nameDest").cast("string"))
    .withColumn("oldbalanceDest", col("oldbalanceDest").cast("double"))
    .withColumn("newbalanceDest", col("newbalanceDest").cast("double"))
    .withColumn("isFraud", col("isFraud").cast("int"))
    .withColumn("isFlaggedFraud", col("isFlaggedFraud").cast("int"))

    # Erro de saldo da origem com sinal:
    # compara o saldo esperado após a saída do dinheiro com o saldo registrado.
    .withColumn(
        "origin_balance_error_signed",
        (col("oldbalanceOrg") - col("amount")) - col("newbalanceOrig")
    )

    # Erro absoluto da origem:
    # mede apenas o tamanho da inconsistência, ignorando se foi positiva ou negativa.
    .withColumn(
        "origin_balance_error_abs",
        spark_abs(col("origin_balance_error_signed"))
    )

    # Erro de saldo do destino com sinal:
    # compara o saldo esperado após receber o dinheiro com o saldo registrado.
    .withColumn(
        "dest_balance_error_signed",
        (col("oldbalanceDest") + col("amount")) - col("newbalanceDest")
    )

    # Erro absoluto do destino:
    # mede a intensidade da inconsistência no saldo do destino.
    .withColumn(
        "dest_balance_error_abs",
        spark_abs(col("dest_balance_error_signed"))
    )

    # Transformação logarítmica do valor:
    # reduz impacto de valores extremamente altos.
    .withColumn(
        "amount_log",
        log1p(col("amount"))
    )

    # Flags de saldo zerado na origem
    .withColumn(
        "is_zero_balance_origin_before",
        when(col("oldbalanceOrg") == 0, 1).otherwise(0)
    )
    .withColumn(
        "is_zero_balance_origin_after",
        when(col("newbalanceOrig") == 0, 1).otherwise(0)
    )

    # Flags de saldo zerado no destino
    .withColumn(
        "is_zero_balance_dest_before",
        when(col("oldbalanceDest") == 0, 1).otherwise(0)
    )
    .withColumn(
        "is_zero_balance_dest_after",
        when(col("newbalanceDest") == 0, 1).otherwise(0)
    )

    # Flag geral para tipos que apresentaram fraude no diagnóstico
    .withColumn(
        "is_cash_out_or_transfer",
        when(col("type").isin("CASH_OUT", "TRANSFER"), 1).otherwise(0)
    )

    # Flags individuais dos dois tipos com fraude
    .withColumn(
        "is_transfer",
        when(col("type") == "TRANSFER", 1).otherwise(0)
    )
    .withColumn(
        "is_cash_out",
        when(col("type") == "CASH_OUT", 1).otherwise(0)
    )

    # Rastreabilidade da fonte
    .withColumn("data_source", lit("paysim"))
)

display(silver_df.limit(20))

print("Linhas no Silver:", silver_df.count())
print("Colunas no Silver:", len(silver_df.columns))

silver_df.printSchema()

# COMMAND ----------

# ------------------------------------------------------------
# BLOCO 5 — Validar features criadas
# ------------------------------------------------------------

display(
    silver_df.select(
        "step",
        "type",
        "amount",
        "amount_log",
        "oldbalanceOrg",
        "newbalanceOrig",
        "origin_balance_error_signed",
        "origin_balance_error_abs",
        "oldbalanceDest",
        "newbalanceDest",
        "dest_balance_error_signed",
        "dest_balance_error_abs",
        "is_zero_balance_origin_before",
        "is_zero_balance_origin_after",
        "is_zero_balance_dest_before",
        "is_zero_balance_dest_after",
        "is_cash_out_or_transfer",
        "is_transfer",
        "is_cash_out",
        "isFraud"
    ).limit(30)
)

# COMMAND ----------

# ------------------------------------------------------------
# SALVAR SILVER — Salvar tabela Silver no Azure Storage
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

silver_delta_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/fraud/paysim/tables/transactions_clean/"

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .save(silver_delta_path)
)

print("Silver Delta salva em:")
print(silver_delta_path)

# COMMAND ----------

