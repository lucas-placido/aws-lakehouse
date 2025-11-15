# Makefile para AWS Lakehouse

.ONESHELL:

.PHONY: initialize upload_scripts deploy help

help:
	@echo "Comandos disponíveis:"
	@echo "  make initialize      - Inicializa e aplica o Terraform"
	@echo "  make upload_scripts  - Faz upload dos scripts Glue para S3"
	@echo "  make deploy          - Executa initialize + upload_scripts"

initialize:
	@echo "📦 Deploy da infraestrutura com Terraform..."
	cd terraform
	terraform init
	terraform plan

apply:
	cd terraform 
	terraform apply -auto-approve

upload_scripts:
	@echo "📤 Upload de scripts Glue..."
	@powershell -Command "$$b = terraform -chdir=terraform output -raw scripts_bucket; \
	aws s3 cp --profile gudy glue-jobs/noaa_ghcn_bronze_to_silver.py s3://$$b/glue-jobs/; \
	aws s3 cp --profile gudy glue-jobs/noaa_dimensions_bronze_to_silver.py s3://$$b/glue-jobs/; \
	aws s3 cp --profile gudy glue-jobs/noaa_ghcn_silver_to_gold.py s3://$$b/glue-jobs/; \
	aws s3 cp --profile gudy glue-jobs/iceberg_maintenance.py s3://$$b/glue-jobs/"

deploy: initialize upload_scripts
	@echo "✅ Deploy concluído!"