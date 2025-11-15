# AWS Lakehouse - Mini Projeto para Portfólio

Um mini-lakehouse completo na AWS demonstrando arquitetura moderna com **Apache Iceberg**, **Medallion Architecture** (Bronze/Silver/Gold), e orquestração com **Step Functions** e **Lambda**.

## 🏗️ Arquitetura

```
┌─────────────────┐
│  Data Sources   │
├─────────────────┤
│  NYC TLC        │──┐
│  (Open Data)    │  │
└─────────────────┘  │
                     ▼
            ┌─────────────────┐
            │  Lambda Function│──► Ingestão Automática
            │  (EventBridge)  │    (Diária às 2 AM UTC)
            └─────────────────┘
                     │
                     ▼
            ┌─────────────────┐
            │  S3 Bronze      │──► Raw Data
            │  (Imutável)     │    s3://.../bronze/nyc_tlc/
            └─────────────────┘
                     │
                     ▼
            ┌─────────────────┐
            │  Glue Job       │──► Transformação
            │  Bronze→Silver  │    Limpeza, Dedup, Qualidade
            └─────────────────┘
                     │
                     ▼
            ┌─────────────────┐
            │  S3 Silver      │──► Iceberg Tables
            │  (Curado)       │    ACID, Time Travel
            └─────────────────┘
                     │
                     ▼
            ┌─────────────────┐
            │  Glue Job       │──► Modelagem Dimensional
            │  Silver→Gold    │    Fatos & Dimensões
            └─────────────────┘
                     │
                     ▼
            ┌─────────────────┐
            │  S3 Gold        │──► Iceberg Tables (Analytics)
            │  (Analytics)    │    Pronto para BI
            └─────────────────┘
                     │
                     ▼
            ┌─────────────────┐
            │  Athena         │──► Consultas SQL
            │  QuickSight     │──► Dashboards
            └─────────────────┘
```

## 📦 Componentes

### Infraestrutura (Terraform)
- **S3 Buckets**: Bronze, Silver, Gold, Scripts
- **Glue Databases**: bronze, silver, gold
- **Glue Jobs**: Transformação Bronze→Silver→Gold, Manutenção Iceberg
- **Lambda Function**: Ingestão automática de dados
- **Step Functions**: Orquestração do pipeline completo
- **EventBridge**: Agendamento (diário pipeline, semanal manutenção)
- **IAM Roles**: Permissões adequadas para cada serviço
- **Resource Group**: Agrupa todos os recursos para visualização e gerenciamento centralizado

### Camadas do Lakehouse

#### 🥉 Bronze (Raw)
- **Localização**: `s3://<bucket>/bronze/nyc_tlc/<vehicle_type>/<year>/<month>/`
- **Formato**: Parquet/CSV (conforme origem)
- **Características**: 
  - Dados imutáveis, como recebidos
  - Particionamento por tipo, ano e mês
  - Versionamento habilitado

#### 🥈 Silver (Curated)
- **Localização**: `s3://<bucket>/silver/nyc_trips/`
- **Formato**: Apache Iceberg
- **Características**:
  - Limpeza de dados (validação de qualidade)
  - Deduplicação (window functions)
  - Schema padronizado (snake_case, tipos consistentes)
  - ACID transactions
  - Time travel queries
  - Particionamento otimizado

**Regras de Qualidade**:
- Duração de viagem: 1 min ≤ duração ≤ 3 horas
- Distância: 0 < trip_distance < 100 milhas
- Valor: fare_amount ≥ 0

**Deduplicação**: Por `vendor_id + pickup_datetime + dropoff_datetime`

#### 🥇 Gold (Analytics)
- **Localização**: `s3://<bucket>/gold/`
- **Formato**: Apache Iceberg
- **Modelagem**: Star Schema (Dimensional)
  - **Fato**: `fact_trips` (métricas de viagens)
  - **Dimensões**: 
    - `dim_vendor` (fornecedores)
    - `dim_taxi_zone` (zonas de táxi)

### Orquestração

#### Step Functions Pipeline
```
IngestBronze → BronzeToSilver → SilverToGold → Maintenance
```

**Agendamento**:
- **Pipeline Diário**: 3 AM UTC (via EventBridge)
- **Manutenção Semanal**: 4 AM UTC domingos (via EventBridge)

#### Lambda Function (Ingestão)
- **Trigger**: EventBridge (diário às 2 AM UTC)
- **Função**: Copia dados do AWS Open Data Registry (NYC TLC) para S3 Bronze
- **Fonte**: `s3://nyc-tlc/trip data/`
- **Volume**: Últimos 6 meses de dados (yellow + green)

## 🚀 Deploy Passo a Passo

### Pré-requisitos

1. **AWS CLI** configurado
   ```bash
   aws configure
   ```

2. **Terraform** instalado (>= 1.0)
   ```bash
   terraform --version
   ```

3. **Python 3.9+** (para scripts locais, opcional)

4. **Conta AWS** com permissões adequadas:
   - S3 (criar buckets, listar, copiar)
   - Glue (criar databases, jobs, catalog)
   - Lambda (criar functions, executar)
   - Step Functions (criar state machines)
   - IAM (criar roles, policies)
   - EventBridge (criar rules)

### Passo 1: Configurar Terraform

```bash
cd terraform
terraform init
```

### Passo 2: Deploy da Infraestrutura

```bash
terraform plan
terraform apply
```

Isso cria:
- ✅ S3 buckets (Bronze, Silver, Gold, Scripts)
- ✅ Glue databases (bronze, silver, gold)
- ✅ Glue jobs (3 jobs)
- ✅ Lambda function (ingestão)
- ✅ Step Functions state machine
- ✅ EventBridge rules (agendamento)
- ✅ IAM roles e políticas
- ✅ Resource Group (agrupa todos os recursos)

### Passo 3: Upload dos Scripts Glue

Após o deploy, obtenha o nome do bucket de scripts:

```bash
SCRIPTS_BUCKET=$(terraform output -raw scripts_bucket)
echo $SCRIPTS_BUCKET
```

Upload dos scripts:

```bash
# Upload scripts Glue
aws s3 cp glue-jobs/nyc_tlc_to_silver.py s3://$SCRIPTS_BUCKET/glue-jobs/
aws s3 cp glue-jobs/nyc_trips_gold.py s3://$SCRIPTS_BUCKET/glue-jobs/
aws s3 cp glue-jobs/iceberg_maintenance.py s3://$SCRIPTS_BUCKET/glue-jobs/
```

### Passo 4: Executar Ingestão Inicial

**Opção 1: Via Lambda (Automático)**
- A Lambda executa automaticamente às 2 AM UTC via EventBridge
- Ou invoque manualmente:

```bash
LAMBDA_ARN=$(terraform output -raw lambda_function_arn)
aws lambda invoke --function-name $LAMBDA_ARN response.json
```

**Opção 2: Via Step Functions (Pipeline Completo)**
```bash
SM_ARN=$(terraform output -raw stepfunctions_state_machine_arn)
aws stepfunctions start-execution --state-machine-arn $SM_ARN
```

### Passo 5: Verificar Dados

#### Verificar S3 Bronze
```bash
BRONZE_BUCKET=$(terraform output -raw bronze_bucket)
aws s3 ls s3://$BRONZE_BUCKET/bronze/nyc_tlc/ --recursive
```

#### Verificar Glue Tables
```bash
# Listar databases
aws glue get-databases

# Listar tabelas no Silver
aws glue get-tables --database-name silver

# Listar tabelas no Gold
aws glue get-tables --database-name gold
```

#### Visualizar Resource Group
Todos os recursos estão agrupados em um **Resource Group** para fácil visualização:

1. Acesse o **AWS Resource Groups Console** (https://console.aws.amazon.com/resource-groups)
2. Procure por `lakehouse-resources`
3. Você verá todos os recursos do projeto agrupados:
   - S3 buckets
   - Glue databases e jobs
   - Lambda functions
   - Step Functions
   - IAM roles
   - EventBridge rules

Ou via CLI:
```bash
RESOURCE_GROUP_NAME=$(terraform output -raw resource_group_name)
aws resource-groups get-group --group-name $RESOURCE_GROUP_NAME --profile gudy
```

#### Consultar com Athena

1. Abra **Amazon Athena Console**
2. Selecione database `silver` ou `gold`
3. Execute queries:

```sql
-- Contar registros Silver
SELECT COUNT(*) FROM silver.nyc_trips;

-- Consulta Gold (Fato + Dimensões)
SELECT 
  d.vendor_name,
  COUNT(*) as total_trips,
  AVG(f.fare_amount) as avg_fare,
  SUM(f.total_amount) as total_revenue
FROM gold.fact_trips f
JOIN gold.dim_vendor d ON f.vendor_id = d.vendor_id
GROUP BY d.vendor_name
ORDER BY total_revenue DESC;

-- Top 10 zonas de pickup
SELECT 
  dz.zone_name,
  COUNT(*) as total_trips,
  AVG(f.fare_amount) as avg_fare
FROM gold.fact_trips f
JOIN gold.dim_taxi_zone dz ON f.pickup_zone_id = dz.zone_id
GROUP BY dz.zone_name
ORDER BY total_trips DESC
LIMIT 10;
```

## 📊 Fluxo de Dados Completo

### 1. Ingestão (Lambda)

**Fonte**: AWS Open Data Registry - NYC TLC Trip Records
- **Bucket**: `s3://nyc-tlc/`
- **Formato**: Parquet/CSV
- **Dados**: Yellow + Green taxis
- **Período**: Últimos 6 meses

**Processo**:
1. Lambda é acionada via EventBridge (diário às 2 AM UTC)
2. Lista arquivos do período no bucket público
3. Copia para `s3://<bronze-bucket>/bronze/nyc_tlc/<type>/<year>/<month>/`
4. Verifica se arquivo já existe (idempotência)

### 2. Transformação Bronze → Silver (Glue Job)

**Job**: `nyc-tlc-bronze-to-silver`

**Processo**:
1. **Leitura**: Lê dados do Bronze (Parquet/CSV)
2. **Limpeza**:
   - Conversão de tipos (timestamp, integer, double)
   - Padronização de nomes (snake_case)
   - Validação de qualidade:
     - Duração: 1 min ≤ duração ≤ 3h
     - Distância: 0 < distance < 100
     - Valor: fare_amount ≥ 0
3. **Deduplicação**: Window function por `vendor_id + pickup_datetime + dropoff_datetime`
4. **Escrita**: Cria/atualiza tabela Iceberg no Silver
   - Particionamento: `pickup_date` (ano/mês/dia)
   - Formato: Apache Iceberg
   - ACID transactions habilitadas

**Schema Silver**:
```sql
CREATE TABLE silver.nyc_trips (
  vendor_id STRING,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  passenger_count INT,
  trip_distance DOUBLE,
  pickup_location_id INT,
  dropoff_location_id INT,
  payment_type INT,
  fare_amount DOUBLE,
  tip_amount DOUBLE,
  total_amount DOUBLE,
  pickup_date DATE,
  pickup_year INT,
  pickup_month INT,
  pickup_day INT
)
USING ICEBERG
PARTITIONED BY (pickup_date)
```

### 3. Modelagem Silver → Gold (Glue Job)

**Job**: `nyc-trips-silver-to-gold`

**Processo**:
1. **Leitura**: Lê tabela `silver.nyc_trips`
2. **Dimensões**:
   - `dim_vendor`: Mapeia vendor_id para nome
   - `dim_taxi_zone`: Zonas de pickup/dropoff
3. **Fato**: `fact_trips`
   - Métricas: trip_distance, fare_amount, tip_amount, total_amount
   - Dimensões: vendor_id, pickup_zone_id, dropoff_zone_id
   - Calculados: trip_duration_seconds, fare_per_mile
4. **Escrita**: Cria tabelas Iceberg no Gold

**Schema Gold**:

```sql
-- Dimensão Vendor
CREATE TABLE gold.dim_vendor (
  vendor_id STRING,
  vendor_name STRING
)
USING ICEBERG;

-- Dimensão Taxi Zone
CREATE TABLE gold.dim_taxi_zone (
  zone_id INT,
  zone_name STRING
)
USING ICEBERG;

-- Fato Trips
CREATE TABLE gold.fact_trips (
  vendor_id STRING,
  pickup_zone_id INT,
  dropoff_zone_id INT,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  pickup_date DATE,
  trip_distance DOUBLE,
  fare_amount DOUBLE,
  tip_amount DOUBLE,
  total_amount DOUBLE,
  passenger_count INT,
  payment_type INT,
  trip_duration_seconds INT,
  fare_per_mile DOUBLE
)
USING ICEBERG
PARTITIONED BY (pickup_date);
```

### 4. Manutenção Iceberg (Glue Job)

**Job**: `iceberg-maintenance`

**Processo** (executado semanalmente):
1. **Expire Snapshots**: Remove snapshots antigos (retenção: 7 dias)
2. **Compaction**: Compacta arquivos pequenos (target: 256MB)
3. **Remove Orphan Files**: Remove arquivos órfãos

**Benefícios**:
- Reduz custos de storage
- Melhora performance de queries
- Otimiza leitura (menos arquivos)

## 🔧 Configuração e Customização

### Variáveis Terraform

Edite `terraform/variables.tf`:

```hcl
variable "aws_region" {
  default = "us-east-1"  # Altere para sua região
}
```

### Agendamento

Edite `terraform/stepfunctions.tf` para alterar horários:

```hcl
# Pipeline diário
schedule_expression = "cron(0 3 * * ? *)"  # 3 AM UTC

# Manutenção semanal
schedule_expression = "cron(0 4 ? * SUN *)"  # 4 AM UTC domingos
```

### Volume de Dados

Edite `lambda/nyc_tlc_ingest.py`:

```python
MONTHS_TO_INGEST = 6  # Altere para mais/menos meses
VEHICLE_TYPES = ["yellow", "green"]  # Adicione "fhv" se necessário
```

## 💰 Custos Estimados

**MVP Mensal**: ~$10-20

**Detalhamento**:
- **S3**: ~$2-5 (dependendo do volume)
- **Glue**: ~$5-10 (jobs on-demand, G.1X)
- **Lambda**: ~$0.10 (1 execução/dia, 5min)
- **Step Functions**: ~$0.10 (1 execução/dia)
- **Athena**: ~$1-5 (consultas, otimizado com partições)
- **EventBridge**: Gratuito (até 1M eventos/mês)

**Otimizações**:
- Lifecycle policies (transição para IA após 30 dias)
- Compaction Iceberg (arquivos maiores = menos requests)
- Particionamento adequado (reduz dados lidos)

## 🎯 Diferenciais do Projeto

### Arquitetura Moderna
- ✅ **Apache Iceberg**: ACID transactions, time travel, schema evolution
- ✅ **Medallion Architecture**: Bronze → Silver → Gold
- ✅ **Serverless**: Lambda + Step Functions (sem servidores)

### Boas Práticas
- ✅ **IaC**: Terraform (infraestrutura como código)
- ✅ **Qualidade de Dados**: Validação e deduplicação
- ✅ **Orquestração**: Step Functions com retry logic
- ✅ **Agendamento**: EventBridge (cron expressions)
- ✅ **Governança**: IAM roles com least privilege

### Pronto para Produção
- ✅ **Idempotência**: Verifica se arquivo já existe antes de copiar
- ✅ **Error Handling**: Retry logic no Step Functions
- ✅ **Monitoramento**: CloudWatch logs habilitados
- ✅ **Manutenção**: Compaction e expire snapshots automáticos

## 📚 Referências

- [AWS Open Data Registry - NYC TLC](https://registry.opendata.aws/nyc-tlc-trip-records-pds/)
- [Apache Iceberg on AWS](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)
- [AWS Glue Best Practices](https://docs.aws.amazon.com/glue/latest/dg/best-practices.html)
- [Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/welcome.html)

## 🐛 Troubleshooting

### Lambda não executa
- Verifique CloudWatch Logs: `/aws/lambda/lakehouse-nyc-tlc-ingest`
- Verifique permissões IAM do Lambda
- Verifique se EventBridge rule está habilitada

### Glue Job falha
- Verifique logs no CloudWatch: `/aws-glue/jobs/output`
- Verifique se scripts estão no S3
- Verifique permissões IAM do Glue

### Tabela Iceberg não encontrada
- Execute o job Bronze→Silver primeiro
- Verifique se database existe: `aws glue get-database --name silver`
- Verifique logs do Glue job

### Step Functions falha
- Verifique execução no console Step Functions
- Verifique permissões IAM
- Verifique se jobs Glue existem

## 📝 Estrutura do Projeto

```
aws-lakehouse/
├── terraform/              # Infraestrutura como Código
│   ├── main.tf            # S3 buckets
│   ├── glue.tf            # Glue databases e jobs
│   ├── lambda.tf          # Lambda function
│   ├── stepfunctions.tf   # Step Functions e EventBridge
│   ├── variables.tf       # Variáveis
│   └── outputs.tf         # Outputs
├── lambda/                # Lambda functions
│   └── nyc_tlc_ingest.py  # Ingestão de dados
├── glue-jobs/             # Glue jobs
│   ├── nyc_tlc_to_silver.py      # Bronze → Silver
│   ├── nyc_trips_gold.py         # Silver → Gold
│   └── iceberg_maintenance.py    # Manutenção
└── README.md              # Este arquivo
```

## 🚀 Próximos Passos

1. ✅ Deploy da infraestrutura
2. ✅ Upload dos scripts Glue
3. ✅ Executar ingestão inicial
4. ✅ Verificar dados no Athena
5. ✅ Criar dashboards no QuickSight (opcional)
6. ✅ Monitorar custos no Cost Explorer

## 📄 Licença

MIT
