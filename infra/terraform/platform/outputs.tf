output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}

output "location" {
  value = azurerm_resource_group.rg.location
}

output "storage_account_name" {
  value = azurerm_storage_account.lake.name
}

output "key_vault_id" {
  value = azurerm_key_vault.kv.id
}

output "key_vault_name" {
  value = azurerm_key_vault.kv.name
}

output "key_vault_uri" {
  value = azurerm_key_vault.kv.vault_uri
}

output "data_factory_name" {
  value = azurerm_data_factory.adf.name
}

# Application (client) ID of ADF's managed identity. The Databricks stage
# registers it as a workspace service principal.
output "data_factory_identity_app_id" {
  value = data.azuread_service_principal.adf.client_id
}

output "databricks_workspace_url" {
  value = "https://${azurerm_databricks_workspace.dbw.workspace_url}"
}

output "databricks_workspace_resource_id" {
  value = azurerm_databricks_workspace.dbw.id
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.uc.id
}

output "sql_server_name" {
  value = azurerm_mssql_server.sql.name
}

output "sql_server_fqdn" {
  value = azurerm_mssql_server.sql.fully_qualified_domain_name
}

output "sql_database_name" {
  value = var.sql_database_name
}
