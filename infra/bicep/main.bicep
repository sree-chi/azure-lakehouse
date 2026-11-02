// Pipeline layer, deployed into the resource group the Terraform platform stage created:
//   * Azure SQL Database (free offer) on the existing logical server
//   * Azure Data Factory linked services, datasets, pipeline and trigger
//
// Deploy: az deployment group create -g <rg> -f infra/bicep/main.bicep -p @params.json
// (scripts/Deploy-All.ps1 does this with values from the Terraform outputs.)

targetScope = 'resourceGroup'

param location string = resourceGroup().location
param factoryName string
param storageAccountName string
param databricksWorkspaceUrl string
param databricksJobId string
param sqlServerName string
param sqlDatabaseName string = 'tri_gold'
param useSqlFreeLimit bool = true
param reportingYear string = '2024'

module sqlDb 'modules/sql-database.bicep' = {
  name: 'sql-database'
  params: {
    sqlServerName: sqlServerName
    databaseName: sqlDatabaseName
    location: location
    useFreeLimit: useSqlFreeLimit
  }
}

module adf 'modules/adf-pipeline.bicep' = {
  name: 'adf-pipeline'
  params: {
    factoryName: factoryName
    storageAccountName: storageAccountName
    databricksWorkspaceUrl: databricksWorkspaceUrl
    databricksJobId: databricksJobId
    reportingYear: reportingYear
  }
}

output sqlDatabaseName string = sqlDb.outputs.databaseName
output pipelineName string = adf.outputs.pipelineName
