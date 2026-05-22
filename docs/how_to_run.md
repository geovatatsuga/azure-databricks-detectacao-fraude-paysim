# Como Executar

Este guia descreve como preparar Azure, Databricks, Unity Catalog e Storage Account para executar o projeto de ponta a ponta.

## Visao geral do ambiente

Componentes usados:

- Azure Storage Account com Data Lake Storage Gen2 habilitado.
- Tres conteineres privados: `bronze`, `silver` e `gold`.
- Azure Databricks Workspace.
- Unity Catalog para governanca de dados.
- Storage Credential e External Locations apontando para o ADLS.
- Databricks Secrets para credenciais Kaggle.
- MLflow para rastrear os modelos.

## 1. Criar a Storage Account ADLS Gen2

No Azure Portal, crie uma Storage Account.

Configuracoes recomendadas:

| Configuracao | Valor recomendado |
| --- | --- |
| Performance | Standard |
| Redundancia | LRS ou ZRS |
| Hierarchical namespace | Enabled |
| Public access | Disabled |
| Access tier | Hot |

O item mais importante e habilitar **Hierarchical namespace**, pois isso transforma a Storage Account em ADLS Gen2.

Depois, crie os conteineres:

- `bronze`
- `silver`
- `gold`

Eles devem ficar privados.

## 2. Definir a estrutura esperada no ADLS

O projeto usa o seguinte padrao:

```text
bronze/
`-- fraud/paysim/
    |-- source_files/
    `-- tables/raw_transactions/

silver/
`-- fraud/paysim/
    `-- tables/transactions_clean/

gold/
`-- fraud/paysim/
    |-- tables/fraud_kpis/
    |-- tables/fraud_by_type/
    |-- tables/ml_dataset_baseline/
    |-- tables/ml_dataset_features/
    `-- exports/model_*_csv/
```

Os diretorios sao criados pelos notebooks durante a execucao.

## 3. Conectar Databricks ao Azure Storage

Ha duas formas comuns:

- Via Unity Catalog, recomendada para ambiente governado.
- Via acesso direto com credenciais no cluster, menos recomendado para projeto publico.

Este projeto documenta a abordagem com Unity Catalog.

## 4. Configurar Unity Catalog

No Databricks, confirme que o workspace esta associado a um metastore do Unity Catalog.

Depois, crie ou selecione:

- Um catalogo para o projeto.
- Um schema/database para os objetos.

Exemplo conceitual:

```sql
CREATE CATALOG IF NOT EXISTS fraud_lakehouse;
CREATE SCHEMA IF NOT EXISTS fraud_lakehouse.paysim;
```

Os nomes acima sao exemplos. Ajuste conforme o seu ambiente.

## 5. Criar Storage Credential

Crie uma credencial de armazenamento no Unity Catalog para permitir que o Databricks acesse o ADLS.

Em ambientes Azure, isso normalmente usa:

- Managed Identity, ou
- Service Principal.

Recomendacao:

Use Managed Identity quando possivel. Evite chaves de acesso ou connection strings.

Exemplo conceitual em SQL:

```sql
CREATE STORAGE CREDENTIAL IF NOT EXISTS adls_paysim_credential
WITH AZURE_MANAGED_IDENTITY
COMMENT 'Credencial para acessar o ADLS do projeto PaySim';
```

Esse comando pode variar conforme o tipo de identidade configurada no seu workspace. Use a documentacao do Databricks do seu ambiente para criar a credencial corretamente.

## 6. Dar permissao no Azure IAM

A identidade usada pelo Databricks precisa ter permissao na Storage Account ou nos conteineres.

Permissao recomendada:

```text
Storage Blob Data Contributor
```

Atribua essa role para a Managed Identity ou Service Principal usado pela Storage Credential.

Escopo recomendado:

- Storage Account inteira, ou
- Cada conteiner `bronze`, `silver`, `gold`.

## 7. Criar External Locations

No Unity Catalog, crie External Locations para cada camada.

Exemplo conceitual:

```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_bronze_paysim
URL 'abfss://bronze@<storage-account>.dfs.core.windows.net/fraud/paysim/'
WITH (STORAGE CREDENTIAL adls_paysim_credential);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_silver_paysim
URL 'abfss://silver@<storage-account>.dfs.core.windows.net/fraud/paysim/'
WITH (STORAGE CREDENTIAL adls_paysim_credential);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_gold_paysim
URL 'abfss://gold@<storage-account>.dfs.core.windows.net/fraud/paysim/'
WITH (STORAGE CREDENTIAL adls_paysim_credential);
```

Substitua `<storage-account>` apenas no seu ambiente Databricks. Nao commite o nome real.

## 8. Conceder permissoes no Unity Catalog

Conceda permissoes para o usuario, grupo ou service principal que executara os notebooks.

Exemplo conceitual:

```sql
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_bronze_paysim TO `<user-or-group>`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_silver_paysim TO `<user-or-group>`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_gold_paysim TO `<user-or-group>`;
```

Se voce for registrar tabelas externas no catalogo, tambem podera precisar de permissoes como:

```sql
GRANT USE CATALOG ON CATALOG fraud_lakehouse TO `<user-or-group>`;
GRANT USE SCHEMA ON SCHEMA fraud_lakehouse.paysim TO `<user-or-group>`;
GRANT CREATE TABLE ON SCHEMA fraud_lakehouse.paysim TO `<user-or-group>`;
```

## 9. Validar acesso ao ADLS no Databricks

Antes de rodar o pipeline, teste os caminhos no notebook:

```python
storage_account = "<your-storage-account-name>"

dbutils.fs.ls(
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/fraud/paysim/"
)
```

Se a pasta ainda nao existir, voce pode validar o conteiner:

```python
dbutils.fs.ls(
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
)
```

Repita para `silver` e `gold`.

## 10. Configurar `src/config.py`

O arquivo `src/config.py` fica com placeholders no GitHub:

```python
STORAGE_ACCOUNT = "<your-storage-account-name>"
DATABRICKS_USER = "<your-databricks-user>"

BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"
GOLD_CONTAINER = "gold"
PROJECT_ROOT = "fraud/paysim"
```

No ambiente Databricks, ajuste os valores reais antes da execucao, mas nao publique esses valores no GitHub.

## 11. Configurar credenciais Kaggle

Crie um secret scope no Databricks chamado:

```text
kaggle
```

Crie duas chaves:

```text
username
key
```

Os notebooks usam:

```python
dbutils.secrets.get(scope="kaggle", key="username")
dbutils.secrets.get(scope="kaggle", key="key")
```

Nao use `kaggle.json` dentro do repositorio.

## 12. Preparar o cluster Databricks

Recomendacoes:

- Use Databricks Runtime com suporte a ML.
- Use um cluster com acesso ao Unity Catalog.
- Confirme que o cluster tem permissao para acessar o metastore.
- Execute os notebooks com um usuario ou grupo que tenha acesso as External Locations.

Dependencias principais:

```text
pyspark
mlflow
kaggle
pandas
scikit-learn
```

## 13. Executar os notebooks

Execute nesta ordem:

```text
Notebooks/01_bronze_ingestion.py
Notebooks/02_silver_feature_engineering.py
Notebooks/03_gold_ml_dataset.py
Notebooks/04_train_fraud_model_mlflow.py
```

## 14. Conferir as saidas no Azure

Depois da execucao, verifique:

```text
bronze/source_files/paysim.csv
bronze/tables/raw_transactions/
silver/tables/transactions_clean/
gold/tables/fraud_kpis/
gold/tables/fraud_by_type/
gold/tables/ml_dataset_baseline/
gold/tables/ml_dataset_features/
gold/exports/model_*_csv/
```

As pastas de tabelas Delta devem conter:

- Arquivos Parquet.
- Diretorio `_delta_log`.

## 15. Conferir MLflow

No Databricks, abra o experimento:

```text
fraud_detection_paysim
```

Runs esperadas:

- `baseline_LogisticRegression`
- `baseline_DecisionTreeClassifier`
- `baseline_RandomForestClassifier`
- `baseline_GBTClassifier`
- `engineered_features_LogisticRegression`
- `engineered_features_DecisionTreeClassifier`
- `engineered_features_RandomForestClassifier`
- `engineered_features_GBTClassifier`

## 16. Publicar no GitHub

Antes do primeiro commit, rode uma busca por termos sensiveis:

```powershell
rg -n -i "secret|token|password|client_secret|tenant_id|subscription_id|connection string|kaggle|hotmail|gmail|your_real_identifier|your_real_storage_prefix"
```

Se encontrar valor real, substitua por placeholder antes de publicar.

## Checklist rapido

- Storage Account criada com hierarchical namespace habilitado.
- Conteineres `bronze`, `silver` e `gold` criados.
- Unity Catalog ativo no workspace.
- Storage Credential criada.
- External Locations criadas para Bronze, Silver e Gold.
- Permissoes `READ FILES` e `WRITE FILES` concedidas.
- Kaggle Secrets configurados.
- `src/config.py` ajustado no ambiente.
- Notebooks executados na ordem.
- MLflow com runs baseline e engineered features.
- Busca de segredos feita antes do push.
