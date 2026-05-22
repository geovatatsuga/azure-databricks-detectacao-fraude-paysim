# Engenharia de Features

A camada Silver cria features pensadas para ajudar os modelos a identificar padroes de fraude nas transacoes PaySim.

## Colunas de origem

O dataset Bronze inclui valor da transacao, tipo da transacao, saldos da conta de origem e destino, alem das flags de fraude.

Colunas importantes:

- `type`
- `amount`
- `oldbalanceOrg`
- `newbalanceOrig`
- `oldbalanceDest`
- `newbalanceDest`
- `isFraud`
- `isFlaggedFraud`

## Features criadas

### Erros de saldo

Essas features comparam os saldos esperados com os saldos registrados.

Para a conta de origem:

```text
origin_balance_error_signed = (oldbalanceOrg - amount) - newbalanceOrig
origin_balance_error_abs = abs(origin_balance_error_signed)
```

Para a conta de destino:

```text
dest_balance_error_signed = (oldbalanceDest + amount) - newbalanceDest
dest_balance_error_abs = abs(dest_balance_error_signed)
```

Por que importa:

Transacoes fraudulentas ou incomuns podem apresentar comportamento inconsistente de saldo. A versao com sinal preserva a direcao do erro, enquanto a versao absoluta mede o tamanho da inconsistencia.

### Valor em log

```text
amount_log = log1p(amount)
```

Por que importa:

Os valores das transacoes sao muito assimetricos. A transformacao logaritmica reduz o impacto de valores extremos e ajuda modelos lineares e modelos baseados em arvore a lidar melhor com a variavel.

### Flags de saldo zerado

Flags criadas:

- `is_zero_balance_origin_before`
- `is_zero_balance_origin_after`
- `is_zero_balance_dest_before`
- `is_zero_balance_dest_after`

Por que importa:

Saldos zerados sao frequentes no dataset e podem indicar comportamento relevante, principalmente quando a conta de origem e esvaziada apos a transacao.

### Flags de tipo de transacao

Flags criadas:

- `is_cash_out_or_transfer`
- `is_transfer`
- `is_cash_out`

Por que importa:

No PaySim, as fraudes aparecem em `TRANSFER` e `CASH_OUT`. Essas flags ajudam o modelo a separar tipos de transacao com risco observado de tipos sem fraude observada.

### Rastreabilidade

```text
data_source = 'paysim'
```

Por que importa:

A coluna de rastreabilidade deixa a origem explicita e facilita futuras expansoes caso novos datasets sejam adicionados.

## Resultado

As features engenheiradas geraram grandes melhorias em recall, PR-AUC e deteccao financeira de fraude para modelos baseados em arvore. Os melhores modelos com features detectaram `1,623` fraudes e deixaram de detectar apenas `9` no split de teste avaliado.
