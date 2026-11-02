terraform {
  required_version = ">= 1.6.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.60"
    }
  }
}

# Authenticates with your `az login` session. Azure CLI auth is also what lets
# Terraform create the Key Vault-backed secret scope (a service principal can't).
provider "databricks" {
  host                        = var.workspace_url
  azure_workspace_resource_id = var.workspace_resource_id
  auth_type                   = "azure-cli"
}
