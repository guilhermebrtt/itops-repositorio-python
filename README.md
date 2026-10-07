# Data Lake ITOps — AWS S3

## Sobre o projeto

Este projeto implementa um fluxo simples de **Data Lake para monitoramento de infraestrutura de rede**, utilizando Python, Amazon S3 e uma instância Amazon EC2.

A ideia é coletar dados de componentes de rede, armazená-los inicialmente em sua forma bruta e, em seguida, realizar transformações e consolidações em diferentes camadas do Data Lake.

A arquitetura segue o conceito de três camadas:

- **Bronze (Raw):** dados brutos, preservados com alta retenção.
- **Silver (Trusted/Cleaned):** dados filtrados, tipados, enriquecidos e consolidados.
- **Gold (Curated/Analytics):** dados agregados e preparados para relatórios, dashboards e análises.

O material da disciplina apresenta justamente esse fluxo de coleta, armazenamento, tratamento e análise utilizando **Python + Amazon S3**.

## Arquitetura

```text
┌──────────────────────┐
│     ON-PREMISE      │
│                      │
│  Firewall            │
│  local2bronze.py     │
│                      │
│  Hardware / APs      │
│  local2bronze.py     │
└──────────┬───────────┘
           │
           │ s3://itops/01-bronze/
           ▼
┌──────────────────────────────┐
│         Amazon S3            │
│                              │
│  01-bronze/                  │
│       │                      │
│       ▼                      │
│  02-silver/                  │
│       │                      │
│       ▼                      │
│  03-gold/                    │
└──────────────┬───────────────┘
               │
               │ processamento
               ▼
┌──────────────────────────────┐
│         AWS EC2              │
│                              │
│  bronze2silver.py             │
│  silver2gold.py               │
└──────────────────────────────┘
```

No exemplo da arquitetura, o bucket utilizado é:

```text
itops-ra04261009
```

Os dados são organizados por prefixos:

```text
s3://itops-ra04261009/01-bronze/
s3://itops-ra04261009/02-silver/
s3://itops-ra04261009/03-gold/
```

> Os nomes do bucket e dos prefixos podem ser alterados conforme o ambiente utilizado.

## Arquivos Python

### `local2bronze.py`

Executado no ambiente **on-premise**.

Sua responsabilidade é coletar informações dos equipamentos/fontes de rede e enviar os dados brutos para a camada **Bronze** do Data Lake.

Exemplos de informações coletadas:

**Antenas / Access Points**
- ID da antena;
- bytes enviados e recebidos;
- conexões ativas;
- utilização de CPU;
- utilização de memória.

**Firewall**
- sessões TCP/UDP ativas;
- pacotes descartados;
- IP mais bloqueado;
- utilização de CPU e memória;
- bytes enviados e recebidos.

Os dados da camada Bronze devem ser mantidos próximos do formato original da coleta, preservando o histórico.

### `bronze2silver.py`

Executado na **instância EC2**.

Lê os dados JSON armazenados na camada Bronze, realiza a preparação dos dados e gera informações consolidadas na camada **Silver**.

Entre os tratamentos previstos estão:

- limpeza e organização dos dados;
- conversão dos dados brutos para um formato consolidado;
- formatação de timestamps;
- cálculo de vazão/consumo;
- classificação da carga dos equipamentos;
- cruzamento entre informações das antenas e do firewall;
- verificação de consistência do tráfego.

Exemplos de regras:

```text
active_conn > 40  → alta densidade
CPU_usage > 80%   → gargalo de processamento
RAM_usage > 75%   → OOM (Out Of Memory)
```

### `silver2gold.py`

Também executado na **EC2**.

Utiliza os dados tratados da camada Silver para gerar informações agregadas na camada **Gold**, voltadas para análise e tomada de decisão.

Alguns exemplos de relatórios que podem ser produzidos:

- zonas mortas ou subutilizadas;
- previsão de sobrecarga de antenas;
- eficiência de hardware;
- análise da taxa de pacotes bloqueados;
- identificação de setores com maior consumo de internet;
- análise de possíveis gargalos no firewall.

A camada Gold representa, portanto, uma visão mais próxima do consumo analítico e das necessidades do negócio.

## Estrutura sugerida do projeto

```text
.
├── README.md
├── local2bronze.py
├── bronze2silver.py
└── silver2gold.py
```

Uma organização possível para os dados no S3:

```text
itops-ra04261009/
├── 01-bronze/
│   ├── firewall/
│   └── antennas/
├── 02-silver/
│   └── consolidated/
└── 03-gold/
    ├── reports/
    └── analytics/
```

## Tecnologias utilizadas

- **Python**
- **Amazon S3**
- **Amazon EC2**
- **AWS CLI**
- **Boto3**
- **Pandas**
- **psutil**

O `boto3` pode ser utilizado pelos scripts Python para acessar o S3, enquanto o `pandas` auxilia no tratamento e consolidação dos dados.

## Preparação do ambiente

Na EC2, pode ser criado um ambiente virtual Python:

```bash
python -m venv amb
source amb/bin/activate
```

Instalação das dependências:

```bash
pip install pandas boto3
```

A AWS CLI também deve estar instalada e configurada na instância.

Para validar o acesso ao S3:

```bash
aws s3 ls
```

Para visualizar os objetos de um bucket:

```bash
aws s3 ls s3://itops-ra04261009 --recursive --human-readable
```

## Sincronização com o S3

Uma das operações principais do projeto é a sincronização dos dados coletados com o Data Lake:

```bash
aws s3 sync /home/ubuntu/dados_coletados/ s3://itops-ra04261009/01-bronze/
```

O `sync` compara origem e destino e transfere arquivos novos ou que foram modificados.

Antes de executar uma sincronização, é possível utilizar:

```bash
aws s3 sync . s3://itops-ra04261009/01-bronze/ --dryrun
```

O `--dryrun` permite verificar o que seria alterado sem efetivamente transferir os arquivos.

## Acesso ao S3 pelo Python

Os scripts podem utilizar o `boto3` para acessar o bucket. Um exemplo simplificado:

```python
import boto3

s3_client = boto3.client("s3")

bucket_name = "itops-ra04261009"
```

Para leitura de um CSV armazenado no S3:

```python
import io
import boto3
import pandas

s3_client = boto3.client("s3")

bucket_name = "itops-ra04261009"
file_key = "02-silver/arquivo.csv"

response = s3_client.get_object(
    Bucket=bucket_name,
    Key=file_key
)

df = pandas.read_csv(
    io.BytesIO(response["Body"].read()),
    sep=";"
)

print(df.head())
```

## Fluxo de processamento

O fluxo completo pode ser resumido da seguinte maneira:

```text
1. Coleta
   ↓
2. JSON bruto
   ↓
3. S3 / 01-bronze
   ↓
4. bronze2silver.py
   ↓
5. Dados limpos e consolidados
   ↓
6. S3 / 02-silver
   ↓
7. silver2gold.py
   ↓
8. Dados agregados / relatórios
   ↓
9. S3 / 03-gold
```

## Objetivo acadêmico

O projeto demonstra, de forma prática, conceitos de **Sistemas Operacionais, armazenamento em nuvem, AWS S3, Data Lake e automação com Python**.

Além de armazenar arquivos, o objetivo é demonstrar a evolução dos dados desde a coleta até uma camada preparada para responder perguntas de negócio.

A separação em Bronze, Silver e Gold facilita a organização do pipeline e permite que diferentes etapas do processamento tenham responsabilidades bem definidas.

## Segurança

Não coloque credenciais AWS diretamente nos arquivos `.py` ou no `README.md`.

Evite publicar:

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_SESSION_TOKEN
```

Prefira utilizar uma **IAM Role associada à EC2** quando o ambiente permitir. Para desenvolvimento local, utilize os mecanismos de configuração de credenciais da AWS.

---

**Projeto:** Data Lake ITOps  
**Stack:** Python + AWS S3 + EC2 + Boto3 + Pandas  
**Camadas:** Bronze → Silver → Gold
