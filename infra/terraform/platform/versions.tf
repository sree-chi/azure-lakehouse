terraform {
  required_version = ">= 1.6.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.30"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }
}

provider "azurerm" {
  subscription_id = var.subscription_id

  features {
    key_vault {
      # Lets `terraform destroy` fully remove the vault so a redeploy can reuse the name.
      purge_soft_delete_on_destroy = true
    }
    resource_group {
      # The SQL database and ADF pipeline are deployed by Bicep into this RG,
      # so Terraform must be allowed to delete an RG that contains them.
      prevent_deletion_if_contains_resources = false
    }
  }
}

provider "azuread" {}
