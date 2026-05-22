# Resultados dos Modelos

Este documento resume a comparacao dos modelos a partir dos relatorios exportados em `Reports/`.

## Cenarios comparados

O projeto comparou dois cenarios:

1. **Baseline**: modelos treinados com variaveis originais e flags simples.
2. **Engineered features**: modelos treinados com novas features criadas na camada Silver.

Como o problema e altamente desbalanceado, a analise prioriza:

- PR-AUC
- Recall da fraude
- Precision da fraude
- F1 da fraude
- Impacto financeiro simulado

A acuracia nao e suficiente neste problema, porque ela pode ficar alta mesmo quando o modelo quase nao detecta fraude.

## Comparacao tecnica

| Modelo | PR-AUC baseline | PR-AUC com features | Recall fraude baseline | Recall fraude com features | Fraudes detectadas a mais |
| --- | ---: | ---: | ---: | ---: | ---: |
| Logistic Regression | 0.0265 | 0.1859 | 0.24% | 2.57% | +38 |
| Decision Tree | 0.5345 | 0.9963 | 69.57% | 99.45% | +471 |
| Random Forest | 0.7901 | 0.9947 | 68.42% | 99.45% | +490 |
| GBT | 0.7930 | 0.9947 | 69.14% | 99.45% | +478 |

![Comparacao PR-AUC](../images/comparison_pr_auc.png)

![Comparacao recall fraude](../images/comparison_recall_fraud.png)

As features criadas na Silver melhoraram drasticamente os modelos baseados em arvore. Antes, Random Forest e GBT ja tinham PR-AUC proximo de `0.79`; depois das features, os modelos baseados em arvore ficaram proximos de `0.995` a `0.996`.

## Fraudes detectadas e perdidas

| Modelo | Fraudes detectadas baseline | Fraudes perdidas baseline | Fraudes detectadas com features | Fraudes perdidas com features |
| --- | ---: | ---: | ---: | ---: |
| Logistic Regression | 4 | 1,652 | 42 | 1,590 |
| Decision Tree | 1,152 | 504 | 1,623 | 9 |
| Random Forest | 1,133 | 523 | 1,623 | 9 |
| GBT | 1,145 | 511 | 1,623 | 9 |

![Fraudes detectadas vs perdidas](../images/comparison_frauds_detected_missed.png)

Antes da engenharia de features, os melhores modelos deixavam passar mais de `500` fraudes no conjunto de teste. Depois da engenharia de features, Decision Tree, Random Forest e GBT deixaram passar apenas `9` fraudes.

Metricas dos modelos de arvore com features:

- `precision_fraud = 1.0000`
- `recall_fraud = 0.9945`
- `f1_fraud = 0.9972`

## Impacto financeiro simulado

O PaySim e um dataset sintetico gerado por simulador. Portanto, os valores abaixo representam impacto financeiro simulado.

A comparacao mais clara para negocio e em tres estagios:

1. **Sem modelo**: `100%` do valor fraudulento ficaria perdido.
2. **Baseline**: os modelos de arvore ja detectam cerca de `95%` do valor fraudulento.
3. **Com features**: os modelos de arvore passam a detectar cerca de `99.94%` do valor fraudulento e deixam perdido cerca de `0.06%`.

| Modelo | Valor detectado baseline | Valor perdido baseline | Valor detectado com features | Valor perdido com features | Ganho detectado |
| --- | ---: | ---: | ---: | ---: | ---: |
| Logistic Regression | R$ 19.29M | R$ 2.33B | R$ 376.81M | R$ 1.93B | +R$ 357.52M |
| Decision Tree | R$ 2.23B | R$ 110.66M | R$ 2.30B | R$ 1.43M | +R$ 66.40M |
| Random Forest | R$ 2.23B | R$ 117.47M | R$ 2.30B | R$ 1.43M | +R$ 73.21M |
| GBT | R$ 2.23B | R$ 110.93M | R$ 2.30B | R$ 1.43M | +R$ 66.67M |

![Taxa de valor fraudulento detectado em tres estagios](../images/comparison_financial_detection_rate_3stage.png)

![Taxa de valor fraudulento perdido em tres estagios](../images/comparison_financial_loss_rate_3stage.png)

![Ganho de valor fraudulento detectado](../images/comparison_financial_gain.png)

Com as features engenheiradas, os modelos baseados em arvore reduziram o valor fraudulento nao detectado de aproximadamente `R$ 110M` a `R$ 117M` para cerca de `R$ 1.43M`.

Por isso, os graficos principais usam taxa detectada, taxa perdida e o resumo de tres estagios do Random Forest. Essa leitura evita que diferencas pequenas em escala visual, como `R$ 117.47M` contra `R$ 1.43M`, desaparecam quando comparadas com barras de aproximadamente `R$ 2.30B`.

## Modelo recomendado

O modelo recomendado para apresentacao e o **Random Forest com features**.

![Historia financeira em tres estagios do Random Forest](../images/random_forest_3stage_financial_story.png)

![Resumo Random Forest](../images/random_forest_summary.png)

Motivos:

- Teve PR-AUC alto: `0.9947`.
- Detectou `1,623` fraudes.
- Perdeu apenas `9` fraudes.
- Teve `precision_fraud = 1.0000`.
- Teve `recall_fraud = 0.9945`.
- Teve o maior ganho financeiro absoluto entre os modelos baseados em arvore: `+R$ 73.21M`.
- Como ensemble, e mais defensavel tecnicamente que uma unica arvore.

## Resumo executivo

A engenharia de features foi o principal fator de melhoria do projeto. No baseline, os melhores modelos detectavam cerca de `68%` a `70%` das fraudes, deixando mais de `500` casos fraudulentos passarem no conjunto de teste. Apos a criacao de features comportamentais, como inconsistencia de saldo, saldo zerado, transformacao logaritmica do valor e flags de tipos transacionais de risco, os modelos baseados em arvore passaram a detectar `99.45%` das fraudes.

No Random Forest, o PR-AUC subiu de `0.7901` para `0.9947`, e o valor financeiro simulado nao detectado caiu de `R$ 117.47M` para apenas `R$ 1.43M`. O resultado mostra que a engenharia de features teve impacto direto tanto na performance tecnica quanto na reducao de perda financeira simulada.

## Sugestao de visualizacoes

Para um dashboard, os blocos mais importantes seriam:

1. PR-AUC baseline vs features por modelo.
2. Recall da fraude baseline vs features.
3. Fraudes detectadas vs perdidas.
4. Valor financeiro simulado detectado vs perdido.

## Arquivos de relatorio

- `Reports/model_business_metrics_csv.csv`
- `Reports/model_before_after_comparison_csv.csv`
- `Reports/model_financial_comparison_csv.csv`
- `Reports/model_report_summary_csv.csv`
- `Data/gold/model_financial_comparison_csv.csv`
