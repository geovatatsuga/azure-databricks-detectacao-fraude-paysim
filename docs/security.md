# Seguranca

Este repositorio foi preparado para uso publico no GitHub. Ele nao deve conter segredos, contas pessoais, nomes reais de contas de armazenamento, tokens ou strings de conexao.

## O que nao deve ser commitado

Nao commite:

- Nomes reais de Azure Storage Account
- IDs de tenant do Azure
- IDs de subscription do Azure
- Client IDs ou client secrets
- Chaves de acesso
- Strings de conexao
- Chaves de API do Kaggle
- `kaggle.json`
- Emails pessoais
- Arquivos `.env`
- Exports do Databricks contendo credenciais

## Configuracao

A configuracao publica usa placeholders em:

```text
src/config.py
```

Exemplo:

```python
STORAGE_ACCOUNT = "<your-storage-account-name>"
DATABRICKS_USER = "<your-databricks-user>"
```

Substitua esses valores apenas no ambiente de execucao. Nao commite valores reais.

## Databricks Secrets

Use Databricks Secrets para credenciais.

Exemplo para Kaggle:

```python
os.environ["KAGGLE_USERNAME"] = dbutils.secrets.get(scope="kaggle", key="username")
os.environ["KAGGLE_KEY"] = dbutils.secrets.get(scope="kaggle", key="key")
```

## Unity Catalog e ADLS

Para acessar o Azure Data Lake, prefira Unity Catalog com Storage Credential e External Locations.

Evite colocar no notebook:

- Chaves de acesso da Storage Account.
- Strings de conexao.
- Client secrets.
- Tokens pessoais.

As permissoes devem ser gerenciadas por IAM no Azure e por grants no Unity Catalog.

## Antes de fazer push

Rode uma busca final antes do primeiro commit:

```powershell
rg -n -i "secret|token|password|client_secret|tenant_id|subscription_id|connection string|kaggle|hotmail|gmail|your_real_identifier|your_real_storage_prefix"
```

Se um secret real ja tiver sido enviado ao GitHub, remova-o do historico do Git e rotacione ou revogue o secret. Apagar em um commit posterior nao e suficiente.
