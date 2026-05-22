# Pipeline de Dados

Este documento resume o papel de cada notebook no fluxo Bronze, Silver e Gold.

## Visao geral

```text
Kaggle PaySim CSV
  -> bronze/source_files/paysim.csv
  -> bronze/tables/raw_transactions/              Delta Lake
  -> silver/tables/transactions_clean/            Delta Lake
  -> gold/tables/fraud_kpis/                      Delta Lake
  -> gold/tables/fraud_by_type/                   Delta Lake
  -> gold/tables/ml_dataset_baseline/             Delta Lake
  -> gold/tables/ml_dataset_features/             Delta Lake
  -> MLflow
  -> gold/exports/model_*_csv/                    CSV
```

## 01 - Bronze ingestion

Arquivo:

```text
Notebooks/01_bronze_ingestion.py
```

Responsabilidades:

- Instala e configura o pacote Kaggle no ambiente Databricks.
- Carrega as credenciais Kaggle via Databricks Secrets.
- Baixa o dataset PaySim.
- Copia o CSV bruto para o conteiner `bronze`.
- Le o CSV com Spark.
- Grava a tabela `raw_transactions` em Delta Lake.

Saidas no Azure:

```text
abfss://bronze@<storage-account>.dfs.core.windows.net/fraud/paysim/source_files/paysim.csv
abfss://bronze@<storage-account>.dfs.core.windows.net/fraud/paysim/tables/raw_transactions/
```

## 02 - Silver feature engineering

Arquivo:

```text
Notebooks/02_silver_feature_engineering.py
```

Responsabilidades:

- Le a tabela Delta `raw_transactions`.
- Executa diagnosticos iniciais de qualidade e distribuicao.
- Remove duplicatas exatas.
- Padroniza tipos de dados.
- Cria features de erro de saldo.
- Cria flags de saldo zerado.
- Cria flags para `TRANSFER`, `CASH_OUT` e tipos com risco.
- Cria `amount_log`.
- Grava a tabela Silver `transactions_clean`.

Saida no Azure:

```text
abfss://silver@<storage-account>.dfs.core.windows.net/fraud/paysim/tables/transactions_clean/
```

## 03 - Gold ML dataset

Arquivo:

```text
Notebooks/03_gold_ml_dataset.py
```

Responsabilidades:

- Le a tabela Silver `transactions_clean`.
- Cria KPIs gerais de fraude.
- Cria agregacoes por tipo de transacao.
- Cria dataset baseline para ML.
- Cria dataset com features engenheiradas para ML.
- Grava as tabelas Gold em Delta Lake.

Saidas no Azure:

```text
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/tables/fraud_kpis/
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/tables/fraud_by_type/
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/tables/ml_dataset_baseline/
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/tables/ml_dataset_features/
```

## 04 - Treinamento e MLflow

Arquivo:

```text
Notebooks/04_train_fraud_model_mlflow.py
```

Responsabilidades:

- Le os datasets Gold de ML.
- Treina modelos baseline e modelos com features.
- Registra execucoes no MLflow.
- Calcula metricas tecnicas como PR-AUC, ROC-AUC, precision, recall e F1.
- Calcula metricas de negocio, incluindo fraudes detectadas e nao detectadas.
- Calcula comparacao financeira entre baseline e features.
- Exporta relatorios finais em CSV.

Saidas no Azure:

```text
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/exports/model_business_metrics_csv/
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/exports/model_before_after_comparison_csv/
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/exports/model_report_summary_csv/
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/exports/model_financial_comparison_csv/
```

## Observacao sobre Delta e Parquet

As tabelas Delta sao armazenadas fisicamente como arquivos Parquet. O Delta Lake adiciona o diretorio `_delta_log`, responsavel pelo historico, transacoes e controle de versao da tabela.
