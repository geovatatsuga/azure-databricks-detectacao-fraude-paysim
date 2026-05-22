# Entendimento de Negocio

O objetivo do projeto e detectar transacoes fraudulentas no dataset PaySim e avaliar o impacto das features na reducao de perdas financeiras.

## Problema

Fraudes sao eventos raros. No PaySim, a quantidade de transacoes fraudulentas e muito menor que a quantidade de transacoes normais.

Isso cria um problema classico de dados desbalanceados:

- Um modelo pode ter alta acuracia mesmo detectando pouca fraude.
- A classe minoritaria e justamente a mais importante.
- Falsos negativos podem representar perda financeira direta.
- Falsos positivos podem gerar custo operacional ou atrito com clientes.

## Por que acuracia nao basta

Em datasets desbalanceados, acuracia pode ser enganosa. Se a fraude representa uma parcela muito pequena do dataset, um modelo que sempre prediz "nao fraude" pode parecer bom em acuracia, mas ser inutil para o negocio.

Por isso, este projeto usa metricas mais adequadas:

- `recall_fraud`: mede quanto das fraudes reais o modelo conseguiu encontrar.
- `precision_fraud`: mede a qualidade dos alertas de fraude.
- `f1_fraud`: equilibra precision e recall.
- `pr_auc`: avalia desempenho em cenarios de classe desbalanceada.
- Metricas financeiras: medem o valor de fraude detectado e perdido.

## Interpretacao dos erros

### Falso negativo

Uma transacao fraudulenta que o modelo classificou como normal.

Impacto:

- Fraude passa sem alerta.
- Valor financeiro pode ser perdido.
- O risco operacional continua invisivel.

### Falso positivo

Uma transacao normal que o modelo classificou como fraude.

Impacto:

- Pode gerar revisao manual.
- Pode causar atrito com cliente.
- Pode aumentar custo operacional.

## Por que medir impacto financeiro

Nem toda fraude tem o mesmo valor. Detectar uma fraude pequena e deixar passar uma fraude muito grande pode ser ruim para o negocio, mesmo que a metrica de contagem pareca boa.

Por isso, o projeto mede:

- Valor de fraude detectado.
- Valor de fraude nao detectado.
- Taxa de valor fraudulento detectado.
- Ganho de valor detectado apos usar features.

## Principais aprendizados

- Fraudes aparecem em `TRANSFER` e `CASH_OUT`.
- Features de saldo ajudam o modelo a identificar inconsistencias.
- Features de tipo de transacao ajudam a separar operacoes com risco observado.
- Modelos baseados em arvore tiveram desempenho muito forte apos feature engineering.
- A comparacao financeira mostra que as features reduzem significativamente o valor de fraude perdido.
