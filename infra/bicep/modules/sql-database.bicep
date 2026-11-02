// Azure SQL Database on the free offer: serverless General Purpose, 2 vCores max,
// auto-pauses after 60 idle minutes. Pauses (instead of billing) when the monthly
// free allowance runs out. One free database is allowed per subscription.

@description('Existing logical server created by the Terraform platform stage.')
param sqlServerName string

param databaseName string
param location string

@description('Use the free monthly allowance. Set false if this subscription already has a free database.')
param useFreeLimit bool = true

resource server 'Microsoft.Sql/servers@2023-08-01-preview' existing = {
  name: sqlServerName
}

var baseProperties = {
  autoPauseDelay: 60
  minCapacity: json('0.5')
  maxSizeBytes: 34359738368 // 32 GB, the free-offer cap
  zoneRedundant: false
  requestedBackupStorageRedundancy: 'Local'
}

var freeProperties = useFreeLimit ? {
  useFreeLimit: true
  freeLimitExhaustionBehavior: 'AutoPause'
} : {}

resource database 'Microsoft.Sql/servers/databases@2023-08-01-preview' = {
  parent: server
  name: databaseName
  location: location
  sku: {
    name: 'GP_S_Gen5'
    tier: 'GeneralPurpose'
    family: 'Gen5'
    capacity: 2
  }
  properties: union(baseProperties, freeProperties)
}

output databaseName string = database.name
