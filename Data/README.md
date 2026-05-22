# Dados

Esta pasta contem amostras documentadas das camadas do lakehouse usadas no projeto de deteccao de fraude com PaySim.

Os arquivos aqui nao substituem as tabelas Delta do Azure Data Lake. Eles servem como referencia publica para GitHub, com pequenas amostras e schemas das tabelas principais.

## Estrutura

```text
Data/
|-- bronze/
|   |-- README.md
|   |-- raw_transactions_schema.csv
|   `-- bronze_raw_transactions_sample_20.csv
|-- silver/
|   |-- README.md
|   |-- transactions_clean_schema.csv
|   `-- silver_transactions_clean_sample_20.csv
`-- gold/
    |-- README.md
    |-- fraud_by_type.csv
    |-- fraud_by_type_schema.csv
    |-- fraud_kpis.csv
    |-- fraud_kpis_schema.csv
    |-- model_financial_comparison_csv.csv
    `-- model_financial_comparison_schema.csv
```

## Camadas

- **Bronze**: dados brutos do PaySim, preservando a estrutura original do CSV.
- **Silver**: dados limpos e enriquecidos com features para analise e machine learning.
- **Gold**: agregacoes analiticas, KPIs e comparacoes finais dos modelos.

## Armazenamento no Azure

No ambiente Databricks, cada camada e gravada em um conteiner separado no Azure Data Lake Storage Gen2:

| Camada | Conteiner | Exemplo de caminho |
| --- | --- | --- |
| Bronze | `bronze` | `abfss://bronze@<storage-account>.dfs.core.windows.net/fraud/paysim/` |
| Silver | `silver` | `abfss://silver@<storage-account>.dfs.core.windows.net/fraud/paysim/` |
| Gold | `gold` | `abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/` |

As tabelas sao salvas em Delta Lake. Na pratica, o Delta armazena os dados em arquivos Parquet e cria um diretorio `_delta_log` com o historico transacional da tabela.

## Arquivos Gold principais

- `fraud_kpis.csv`: indicadores gerais do dataset.
- `fraud_by_type.csv`: distribuicao e taxa de fraude por tipo de transacao.
- `model_financial_comparison_csv.csv`: impacto financeiro dos modelos com baseline vs features.

## Observacao de seguranca

Nao coloque dados sensiveis, credenciais, nomes reais de storage accounts, tokens, chaves ou arquivos completos de dados nesta pasta. Para GitHub, mantenha apenas amostras pequenas e documentacao.
