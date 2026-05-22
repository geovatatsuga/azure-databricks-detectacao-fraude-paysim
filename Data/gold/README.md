# Dados Gold

Esta camada contem tabelas analiticas e resultados finais derivados das camadas Bronze e Silver.

No Azure Data Lake, a camada Gold fica no conteiner `gold`. As tabelas analiticas sao gravadas em Delta Lake sobre Parquet, e alguns resultados finais tambem sao exportados como CSV para facilitar leitura, publicacao e relatorios.

## Arquivos

- `fraud_kpis.csv`: KPIs gerais do dataset PaySim.
- `fraud_kpis_schema.csv`: schema da tabela de KPIs gerais.
- `fraud_by_type.csv`: metricas agregadas por tipo de transacao.
- `fraud_by_type_schema.csv`: schema da tabela por tipo de transacao.
- `model_financial_comparison_csv.csv`: comparacao financeira dos modelos baseline vs features.
- `model_financial_comparison_schema.csv`: schema da comparacao financeira.

## Tabelas

## Escrita no Azure

O notebook `Notebooks/03_gold_ml_dataset.py` grava tabelas Delta no conteiner `gold`:

```text
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/tables/fraud_kpis/
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/tables/fraud_by_type/
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/tables/ml_dataset_baseline/
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/tables/ml_dataset_features/
```

O notebook `Notebooks/04_train_fraud_model_mlflow.py` exporta resultados finais em CSV no conteiner `gold`:

```text
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/exports/model_business_metrics_csv/
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/exports/model_before_after_comparison_csv/
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/exports/model_report_summary_csv/
abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/exports/model_financial_comparison_csv/
```

### `fraud_kpis`

Resume o dataset completo: total de transacoes, total de fraudes, valor total transacionado, valor total fraudulento, tickets medios e taxas percentuais.

Principais leituras:

- O dataset tem `6,362,620` transacoes.
- Existem `8,213` fraudes.
- A taxa de fraude e aproximadamente `0.1291%`.
- O ticket medio das fraudes e muito maior que o ticket medio normal.

### `fraud_by_type`

Mostra como a fraude se distribui por tipo de transacao.

Principais leituras:

- Fraudes aparecem em `TRANSFER` e `CASH_OUT`.
- `TRANSFER` tem a maior taxa de fraude percentual.
- `CASH_OUT` tem o maior volume absoluto de transacoes entre os tipos com fraude.
- `PAYMENT`, `CASH_IN` e `DEBIT` nao apresentam fraude na amostra agregada.

### `model_financial_comparison_csv`

Compara o resultado financeiro dos modelos treinados com dataset baseline contra os mesmos modelos usando features engenheiradas.

Ela mede:

- Valor de fraude detectado no baseline.
- Valor de fraude nao detectado no baseline.
- Taxa de valor fraudulento detectado no baseline.
- Valor de fraude detectado com features.
- Valor de fraude nao detectado com features.
- Taxa de valor fraudulento detectado com features.
- Ganho financeiro absoluto e ganho de taxa.

Principais leituras:

- `DecisionTreeClassifier`, `RandomForestClassifier` e `GBTClassifier` chegaram a aproximadamente `99.94%` de taxa de valor fraudulento detectado com features.
- `RandomForestClassifier` teve o maior ganho absoluto de valor fraudulento detectado a mais entre os modelos de arvore: cerca de `73.2M`.
- `LogisticRegression` melhorou bastante em termos relativos, mas continuou abaixo dos modelos de arvore: saiu de cerca de `0.82%` para `16.37%` de taxa de valor detectado.
- O uso das features reduziu fortemente o valor de fraude nao detectado nos modelos de arvore.

## Uso

Use esta camada para comunicacao executiva, relatorios finais e comparacao de impacto dos modelos. Os CSVs desta pasta sao pequenos e seguros para documentacao publica do projeto.
