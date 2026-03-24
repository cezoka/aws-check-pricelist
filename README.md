# AWS Price List Extractor (Bulk API & S3 Chunking)

Este projeto foi construído para realizar a extração ultrarrápida do catálogo de preços global da AWS, exportando diretamente para a nuvem da empresa sem consumir disco local.

## 🚀 Arquitetura e Diferenciais

O script utiliza a poderosa **AWS Price List Bulk API**, abandonando a tradicional API de paginação do Boto3. 
Em vez de fazer múltiplas requisições por preço, o script faz o download de arquivos estáticos CSV por HTTP (Streaming) hospedados nativamente pela AWS.

* **Extrema Velocidade:** Varre milhões de SKUs (como do Amazon EC2 e S3) em minutos ao invés de horas.
* **Ausência de Throttling (Limites da AWS):** Imune a bloqueios de "Rate Exceeded", pois utiliza arquivos da CDN da própria AWS.
* **Zero Disco (S3 Streaming):** O script não cria arquivos pesados no seu notebook. Utiliza **S3 Multipart Upload** combinando com `io.StringIO` na memória RAM.
* **Corte Inteligente Anti-Crash do Excel:** Ao identificar que o arquivo irá ultrapassar **900.000 linhas**, o script rotaciona proativamente os arquivos (ex: `parte_1`, `parte_2`). Isso impede que o limite do Microsoft Excel (~1.048.576 linhas) seja estourado, garantindo abertura segura para diretores ou analistas financeiros.

---

## 🛠️ Pré-Requisitos e Dependências

Para a máquina que for rodar o script (seja o seu computador ou um servidor), você precisará instalar o Python e duas bibliotecas principais:

1. **boto3:** O SDK oficial da AWS para Python, responsável pelo gerenciamento e envio de arquivos para o S3.
2. **requests:** Biblioteca robusta para fazer os downloads em HTTP Streaming da API estática da AWS.

Abra o terminal e instale-os usando o gerenciador de pacotes pip:

```bash
pip install boto3 requests
```

---

## 🔑 Autenticação e Credenciais (AWS SSO)

O código foi padronizado para herdar o acesso direto de sistemas Single Sign-On (SSO) como o AWS Identity Center. Nesse repositório, o Python espera acessar um _Profile_ pré-configurado da AWS chamado **Core-AWSAdministratorAccess-047303556943** provindo da nuvem do cliente.

Antes de iniciar a rodar o script pela primeira vez no dia (ou quando o Token SSO expirar), garanta a renovação das suas senhas temporárias com:

```bash
aws sso login --profile Core-AWSAdministratorAccess-047303556943
```
> Após o navegador confirmar *"Successfully logged in"*, seu terminal estará devidamente credenciado pelos próximos turnos.

---

## ⚙️ Como Executar

Com as dependências instaladas e o terminal logado na AWS, basta navegar até o diretório do seu script e disparar a execução:

```bash
python extract_aws_prices_bulk_split.py
```

### O que vai acontecer?
1. O terminal avisará que interceptou todos os ~200+ serviços de faturamento da AWS.
2. Os downloads começarão via HTTP (arquivos maciços do EC2, RDS, etc).
3. A cada 10 MB consolidados, você verá a mensagem que um "Bloco foi transferido para nuvem".
4. Ao final do projeto, acesse o bucket S3 `s3://price-list-aws/split_csvs_bulk/`.
5. Seus CSVs (particionados e numerados perfeitamente) estarão lá seguros para importar no PowerBI ou no Microsoft Excel!