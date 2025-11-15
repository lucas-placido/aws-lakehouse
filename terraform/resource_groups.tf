# AWS Resource Group para agrupar todos os recursos do Lakehouse
# Isso permite visualizar e gerenciar todos os recursos relacionados em um só lugar
resource "aws_resourcegroups_group" "lakehouse" {
  name        = "lakehouse-resources"
  description = "Recursos do AWS Lakehouse Bronze Silver Gold"  # Removidos hífens e caracteres especiais

  resource_query {
    query = jsonencode({
      ResourceTypeFilters = ["AWS::AllSupported"]
      TagFilters = [
        {
          Key    = "Project"
          Values = ["aws-lakehouse"]
        }
      ]
    })
  }

  tags = local.common_tags
}

