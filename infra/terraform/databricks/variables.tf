# Values come from the platform stage outputs; scripts/Deploy-All.ps1 wires them up.

variable "workspace_url" {
  type = string
}

variable "workspace_resource_id" {
  type = string
}

variable "access_connector_id" {
  type = string
}

variable "storage_account_name" {
  type = string
}

variable "key_vault_id" {
  type = string
}

variable "key_vault_uri" {
  type = string
}

variable "adf_identity_app_id" {
  description = "Client ID of Azure Data Factory's managed identity."
  type        = string
}

variable "sql_server_fqdn" {
  type = string
}

variable "sql_database_name" {
  type = string
}

variable "catalog_name" {
  description = "Unity Catalog catalog for bronze/silver/gold."
  type        = string
  default     = "tri_lakehouse"
}

variable "node_type_id" {
  description = "VM size for the single-node job cluster. 4 vCPUs fits Azure for Students quota."
  type        = string
  default     = "Standard_DS3_v2"
}

variable "reporting_year" {
  description = "Default TRI reporting year for manual runs (ADF passes its own)."
  type        = string
  default     = "2024"
}

variable "enable_snowflake" {
  description = "Load gold tables to Snowflake (needs the snowflake-* secrets in Key Vault)."
  type        = bool
  default     = false
}
