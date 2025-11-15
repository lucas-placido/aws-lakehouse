#!/bin/bash
# Script de deploy do AWS Lakehouse

set -e

echo "🚀 Deploy do AWS Lakehouse"
echo "=========================="

# Verificar pré-requisitos
echo "✓ Verificando pré-requisitos..."
command -v aws >/dev/null 2>&1 || { echo "❌ AWS CLI não encontrado"; exit 1; }
command -v terraform >/dev/null 2>&1 || { echo "❌ Terraform não encontrado"; exit 1; }

# Deploy Terraform
echo ""
echo "📦 Deploy da infraestrutura com Terraform..."
cd terraform
terraform init
terraform plan
terraform apply -auto-approve

# Obter outputs
SCRIPTS_BUCKET=$(terraform output -raw scripts_bucket)
echo ""
echo "✅ Infraestrutura deployada!"
echo "   Scripts Bucket: $SCRIPTS_BUCKET"

# Upload scripts Glue
echo ""
echo "📤 Upload de scripts Glue..."
cd ..
aws s3 cp glue-jobs/nyc_tlc_to_silver.py s3://$SCRIPTS_BUCKET/glue-jobs/
aws s3 cp glue-jobs/nyc_trips_gold.py s3://$SCRIPTS_BUCKET/glue-jobs/
aws s3 cp glue-jobs/iceberg_maintenance.py s3://$SCRIPTS_BUCKET/glue-jobs/

echo ""
echo "✅ Deploy concluído!"
echo ""
echo "Próximos passos:"
echo "1. Aguarde alguns minutos para a primeira execução automática (2 AM UTC)"
echo "2. Ou execute manualmente: aws lambda invoke --function-name lakehouse-nyc-tlc-ingest response.json"
echo "3. Execute o pipeline completo: aws stepfunctions start-execution --state-machine-arn <arn>"
echo ""
echo "Para obter o ARN da state machine:"
echo "  cd terraform && terraform output stepfunctions_state_machine_arn"

