# Dados Bronze

Esta camada representa a entrada bruta do dataset PaySim apos a ingestao inicial.

## Arquivos

- `bronze_raw_transactions_sample_20.csv`: amostra com 20 transacoes da tabela Bronze.
- `raw_transactions_schema.csv`: schema documentado da tabela `raw_transactions`.

## Tabela

| Item | Valor |
| --- | --- |
| Camada | Bronze |
| Tabela | `raw_transactions` |
| Fonte | PaySim |
| Formato original | CSV |
| Formato no lakehouse | Delta Lake sobre Parquet |
| Conteiner Azure | `bronze` |
| Granularidade | Uma linha por transacao |

## Descricao

A camada Bronze mantem as colunas originais do PaySim. Ela e usada como base para validacoes iniciais, analise exploratoria e criacao da camada Silver.

As colunas de saldo representam os valores antes e depois da transacao na origem e no destino. As colunas `isFraud` e `isFlaggedFraud` sao os indicadores originais do dataset.

## Escrita no Azure

O notebook `Notebooks/01_bronze_ingestion.py` faz duas escritas principais no conteiner `bronze`:

```text
abfss://bronze@<storage-account>.dfs.core.windows.net/fraud/paysim/source_files/paysim.csv
abfss://bronze@<storage-account>.dfs.core.windows.net/fraud/paysim/tables/raw_transactions/
```

Primeiro o CSV bruto e copiado para `source_files`. Depois o Spark le esse CSV e grava a tabela `raw_transactions` em Delta Lake. Por baixo, a tabela Delta usa arquivos Parquet e um log transacional `_delta_log`.

## Colunas principais

- `step`: unidade de tempo da simulacao.
- `type`: tipo de transacao.
- `amount`: valor movimentado.
- `nameOrig`: identificador da conta de origem.
- `nameDest`: identificador da conta de destino.
- `isFraud`: flag de fraude real.
- `isFlaggedFraud`: flag de fraude marcada pelo simulador.
