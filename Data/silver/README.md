# Dados Silver

Esta camada representa a tabela limpa e enriquecida para analise e machine learning.

## Arquivos

- `silver_transactions_clean_sample_20.csv`: amostra com 20 transacoes da tabela Silver.
- `transactions_clean_schema.csv`: schema documentado da tabela `transactions_clean`.

## Tabela

| Item | Valor |
| --- | --- |
| Camada | Silver |
| Tabela | `transactions_clean` |
| Entrada | Bronze `raw_transactions` |
| Formato no lakehouse | Delta Lake sobre Parquet |
| Conteiner Azure | `silver` |
| Granularidade | Uma linha por transacao |

## Transformacoes

A camada Silver remove duplicatas exatas, padroniza tipos de dados e cria features derivadas para melhorar a analise de fraude.

As principais features criadas sao:

- Erros de saldo da origem e do destino, com sinal e em valor absoluto.
- Transformacao logaritmica do valor da transacao.
- Flags de saldo zerado antes/depois na origem e no destino.
- Flags para tipos de transacao associados a fraude no PaySim.
- Coluna `data_source` para rastreabilidade.

## Escrita no Azure

O notebook `Notebooks/02_silver_feature_engineering.py` le a tabela Delta da Bronze e grava a Silver no conteiner `silver`:

```text
abfss://silver@<storage-account>.dfs.core.windows.net/fraud/paysim/tables/transactions_clean/
```

Essa saida tambem e uma tabela Delta Lake. Os dados ficam em arquivos Parquet, e o diretorio `_delta_log` guarda o historico de transacoes da tabela.

## Uso recomendado

Use esta camada para analise exploratoria, criacao de datasets Gold e treinamento de modelos. Para publicacao no GitHub, mantenha apenas amostras pequenas como a presente nesta pasta.
