# AWS Price List Extractor

Script desenvolvido para automatizar a extração dos dados de precificação da AWS (AWS Price List). Os arquivos CSV gerados são processados e divididos em partes para facilitar a posterior ingestão em ferramentas de BI e análise de dados.

## Funcionalidades

- **Download via AWS Price List Bulk API:** Realiza o download dos dados em lote consumindo os arquivos estáticos CSV por streaming, como uma alternativa a API de paginação tradicional, acelerando a extração.
- **Processamento em Memória (S3 Streaming):** Os dados são processados e enviados diretamente para o bucket S3 via Multipart Upload (`io.StringIO`), sem a necessidade de persistir os dados no armazenamento e disco local.
- **Divisão Automática de CSVs (Split):** O script rotaciona a escrita de arquivos extensos (como os preços do AWS EC2) a cada 900.000 registros criados. A separação impede que os arquivos finais ultrapassem limites de linhas de editores de planilhas e simplifica o tratamento dos dados posteriormente.

---

## Pré-requisitos

O ambiente de execução requer a instalação do Python e de pacotes para comunicação com as APIs e requisições HTTP:

- `boto3`: AWS SDK for Python.
- `requests`: Para chamadas eficientes nos endpoints do AWS Price List.

```bash
pip install boto3 requests
```

---

## Autenticação (AWS SSO)

O script utiliza as configurações do profile local da AWS. Atualmente, espera as credenciais relativas a um acesso do AWS Identity Center (SSO).

O `profile` configurado é o: **Core-AWSAdministratorAccess-047303556943**.

Certifique-se de realizar o login (ou renovar seu token diário) na AWS CLI antes da execução:

```bash
aws sso login --profile Core-AWSAdministratorAccess-047303556943
```

---

## Como Executar

Com as dependências instaladas e o perfil logado no terminal, execute o job usando:

```bash
python extract_aws_prices_bulk_split.py
```

O scritp irá escanear os serviços disponíveis, processar os downloads e exibir mensagens reportando os chunks que forem transferidos pelo S3 Multipart Upload. 

Ao final da execução, os CSVs extraídos (fatiados quando necessário) serão salvos no seguinte destino do S3:
`s3://price-list-aws/split_csvs_bulk/`