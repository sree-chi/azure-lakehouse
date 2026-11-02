// Azure Data Factory content: linked services, datasets, the orchestration
// pipeline, and a monthly trigger. The factory itself (and its managed identity)
// is created by the Terraform platform stage.
//
// Pipeline: pl_tri_lakehouse
//   Set_IngestDate
//     ├─ Copy_Releases_CSV     EPA TRI Basic Data File (CSV)       -> raw/tri/releases/...
//     └─ Copy_Facilities_JSON  EPA Envirofacts facility registry   -> raw/tri/facilities/...
//   Run_Databricks_Job         Jobs API run-now, authenticated with ADF's managed identity
//   Until_Job_Finishes         poll runs/get every 30 s
//   If_Job_Failed              fail the pipeline with the Databricks run URL

param factoryName string
param storageAccountName string
param databricksWorkspaceUrl string
param databricksJobId string
param reportingYear string = '2024'
param releasesPathPrefix string = 'efservice/downloads/tri/mv_tri_basic_download/'
param releasesPathSuffix string = '_US/csv'
param facilitiesRelativeUrl string = 'dmapservice/tri.tri_facility/JSON'

@description('First trigger fire time (UTC). The trigger deploys STOPPED; start it when you want monthly refreshes.')
param triggerStartTime string = '2026-11-01T13:00:00Z'

// AzureDatabricks first-party application ID: the token audience for Databricks REST APIs.
var databricksResource = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d'

// ADF expressions, kept as raw strings so their single quotes need no escaping.
var exprIngestDate = '''@formatDateTime(utcNow(), 'yyyy-MM-dd')'''
var exprReleasesUrl = '''@concat(pipeline().parameters.releasesPathPrefix, pipeline().parameters.reportingYear, pipeline().parameters.releasesPathSuffix)'''
var exprReleasesFolder = '''@concat('tri/releases/reporting_year=', pipeline().parameters.reportingYear, '/ingest_date=', variables('ingestDate'))'''
var exprReleasesFile = '''@concat('tri_', pipeline().parameters.reportingYear, '_us.csv')'''
var exprFacilitiesFolder = '''@concat('tri/facilities/ingest_date=', variables('ingestDate'))'''
var exprRunNowUrl = '''@concat(pipeline().parameters.databricksWorkspaceUrl, '/api/2.1/jobs/run-now')'''
var exprRunNowBody = '''@json(concat('{"job_id": ', pipeline().parameters.databricksJobId, ', "job_parameters": {"reporting_year": "', pipeline().parameters.reportingYear, '", "ingest_date": "', variables('ingestDate'), '"}}'))'''
var exprRunId = '''@string(activity('Run_Databricks_Job').output.run_id)'''
var exprRunsGetUrl = '''@concat(pipeline().parameters.databricksWorkspaceUrl, '/api/2.1/jobs/runs/get?run_id=', variables('runId'))'''
var exprLifeCycle = '''@activity('Get_Run_Status').output.state.life_cycle_state'''
var exprResultState = '''@coalesce(activity('Get_Run_Status').output.state?.result_state, '')'''
var exprRunPageUrl = '''@coalesce(activity('Get_Run_Status').output?.run_page_url, '')'''
var exprUntilDone = '''@or(equals(variables('lifeCycleState'), 'TERMINATED'), or(equals(variables('lifeCycleState'), 'SKIPPED'), equals(variables('lifeCycleState'), 'INTERNAL_ERROR')))'''
var exprJobFailed = '''@not(equals(variables('resultState'), 'SUCCESS'))'''
var exprFailMessage = '''@concat('Databricks run ', variables('runId'), ' ended ', variables('lifeCycleState'), '/', variables('resultState'), '. Details: ', variables('runPageUrl'))'''

var copyPolicy = {
  timeout: '0.01:00:00'
  retry: 2
  retryIntervalInSeconds: 60
  secureOutput: false
  secureInput: false
}

var msiAuth = {
  type: 'MSI'
  resource: databricksResource
}

resource factory 'Microsoft.DataFactory/factories@2018-06-01' existing = {
  name: factoryName
}

// ---------------------------------------------------------------------------
// Linked services
// ---------------------------------------------------------------------------
resource lsHttp 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: factory
  name: 'ls_http_epa'
  properties: {
    type: 'HttpServer'
    description: 'EPA public data services (anonymous)'
    typeProperties: {
      url: 'https://data.epa.gov/'
      enableServerCertificateValidation: true
      authenticationType: 'Anonymous'
    }
  }
}

// No credential = ADF's system-assigned managed identity (granted Storage Blob Data Contributor).
resource lsAdls 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: factory
  name: 'ls_adls_lake'
  properties: {
    type: 'AzureBlobFS'
    description: 'Lakehouse ADLS Gen2 account via managed identity'
    typeProperties: {
      url: 'https://${storageAccountName}.dfs.core.windows.net/'
    }
  }
}

// ---------------------------------------------------------------------------
// Datasets (binary copy: land source bytes untouched; parsing happens in Databricks)
// ---------------------------------------------------------------------------
resource dsHttp 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  parent: factory
  name: 'ds_http_binary'
  properties: {
    type: 'Binary'
    linkedServiceName: {
      referenceName: lsHttp.name
      type: 'LinkedServiceReference'
    }
    parameters: {
      relativeUrl: {
        type: 'String'
      }
    }
    typeProperties: {
      location: {
        type: 'HttpServerLocation'
        relativeUrl: {
          value: '@dataset().relativeUrl'
          type: 'Expression'
        }
      }
    }
  }
}

resource dsAdls 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  parent: factory
  name: 'ds_adls_raw_binary'
  properties: {
    type: 'Binary'
    linkedServiceName: {
      referenceName: lsAdls.name
      type: 'LinkedServiceReference'
    }
    parameters: {
      folderPath: {
        type: 'String'
      }
      fileName: {
        type: 'String'
      }
    }
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        fileSystem: 'raw'
        folderPath: {
          value: '@dataset().folderPath'
          type: 'Expression'
        }
        fileName: {
          value: '@dataset().fileName'
          type: 'Expression'
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------
var httpSource = {
  type: 'BinarySource'
  storeSettings: {
    type: 'HttpReadSettings'
    requestMethod: 'GET'
    requestTimeout: '00:15:00' // EPA builds national files on the fly
  }
}

var adlsSink = {
  type: 'BinarySink'
  storeSettings: {
    type: 'AzureBlobFSWriteSettings'
  }
}

resource pipeline 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  parent: factory
  name: 'pl_tri_lakehouse'
  properties: {
    description: 'Ingest EPA TRI releases (CSV) + facility registry (JSON) to ADLS, then run the Databricks bronze/silver/gold job.'
    parameters: {
      reportingYear: {
        type: 'String'
        defaultValue: reportingYear
      }
      releasesPathPrefix: {
        type: 'String'
        defaultValue: releasesPathPrefix
      }
      releasesPathSuffix: {
        type: 'String'
        defaultValue: releasesPathSuffix
      }
      facilitiesRelativeUrl: {
        type: 'String'
        defaultValue: facilitiesRelativeUrl
      }
      databricksWorkspaceUrl: {
        type: 'String'
        defaultValue: databricksWorkspaceUrl
      }
      databricksJobId: {
        type: 'String'
        defaultValue: databricksJobId
      }
    }
    variables: {
      ingestDate: {
        type: 'String'
      }
      runId: {
        type: 'String'
      }
      lifeCycleState: {
        type: 'String'
        defaultValue: 'PENDING'
      }
      resultState: {
        type: 'String'
        defaultValue: ''
      }
      runPageUrl: {
        type: 'String'
        defaultValue: ''
      }
    }
    activities: [
      {
        name: 'Set_IngestDate'
        type: 'SetVariable'
        typeProperties: {
          variableName: 'ingestDate'
          value: {
            value: exprIngestDate
            type: 'Expression'
          }
        }
      }
      {
        name: 'Copy_Releases_CSV'
        description: 'TRI Basic Data File (US, one reporting year)'
        type: 'Copy'
        dependsOn: [
          {
            activity: 'Set_IngestDate'
            dependencyConditions: [ 'Succeeded' ]
          }
        ]
        policy: copyPolicy
        typeProperties: {
          source: httpSource
          sink: adlsSink
        }
        inputs: [
          {
            referenceName: dsHttp.name
            type: 'DatasetReference'
            parameters: {
              relativeUrl: {
                value: exprReleasesUrl
                type: 'Expression'
              }
            }
          }
        ]
        outputs: [
          {
            referenceName: dsAdls.name
            type: 'DatasetReference'
            parameters: {
              folderPath: {
                value: exprReleasesFolder
                type: 'Expression'
              }
              fileName: {
                value: exprReleasesFile
                type: 'Expression'
              }
            }
          }
        ]
      }
      {
        name: 'Copy_Facilities_JSON'
        description: 'Envirofacts TRI facility registry (JSON)'
        type: 'Copy'
        dependsOn: [
          {
            activity: 'Set_IngestDate'
            dependencyConditions: [ 'Succeeded' ]
          }
        ]
        policy: copyPolicy
        typeProperties: {
          source: httpSource
          sink: adlsSink
        }
        inputs: [
          {
            referenceName: dsHttp.name
            type: 'DatasetReference'
            parameters: {
              relativeUrl: {
                value: '@pipeline().parameters.facilitiesRelativeUrl'
                type: 'Expression'
              }
            }
          }
        ]
        outputs: [
          {
            referenceName: dsAdls.name
            type: 'DatasetReference'
            parameters: {
              folderPath: {
                value: exprFacilitiesFolder
                type: 'Expression'
              }
              fileName: 'tri_facilities.json'
            }
          }
        ]
      }
      {
        name: 'Run_Databricks_Job'
        description: 'Jobs API run-now as ADF\'s managed identity (CAN_MANAGE_RUN on this job only)'
        type: 'WebActivity'
        dependsOn: [
          {
            activity: 'Copy_Releases_CSV'
            dependencyConditions: [ 'Succeeded' ]
          }
          {
            activity: 'Copy_Facilities_JSON'
            dependencyConditions: [ 'Succeeded' ]
          }
        ]
        policy: {
          timeout: '0.00:05:00'
          retry: 1
          retryIntervalInSeconds: 30
        }
        typeProperties: {
          method: 'POST'
          url: {
            value: exprRunNowUrl
            type: 'Expression'
          }
          headers: {
            'Content-Type': 'application/json'
          }
          body: {
            value: exprRunNowBody
            type: 'Expression'
          }
          authentication: msiAuth
        }
      }
      {
        name: 'Set_RunId'
        type: 'SetVariable'
        dependsOn: [
          {
            activity: 'Run_Databricks_Job'
            dependencyConditions: [ 'Succeeded' ]
          }
        ]
        typeProperties: {
          variableName: 'runId'
          value: {
            value: exprRunId
            type: 'Expression'
          }
        }
      }
      {
        name: 'Until_Job_Finishes'
        type: 'Until'
        dependsOn: [
          {
            activity: 'Set_RunId'
            dependencyConditions: [ 'Succeeded' ]
          }
        ]
        typeProperties: {
          expression: {
            value: exprUntilDone
            type: 'Expression'
          }
          timeout: '0.02:00:00'
          activities: [
            {
              name: 'Wait_30s'
              type: 'Wait'
              typeProperties: {
                waitTimeInSeconds: 30
              }
            }
            {
              name: 'Get_Run_Status'
              type: 'WebActivity'
              dependsOn: [
                {
                  activity: 'Wait_30s'
                  dependencyConditions: [ 'Succeeded' ]
                }
              ]
              policy: {
                timeout: '0.00:02:00'
                retry: 3
                retryIntervalInSeconds: 20
              }
              typeProperties: {
                method: 'GET'
                url: {
                  value: exprRunsGetUrl
                  type: 'Expression'
                }
                authentication: msiAuth
              }
            }
            {
              name: 'Set_LifeCycleState'
              type: 'SetVariable'
              dependsOn: [
                {
                  activity: 'Get_Run_Status'
                  dependencyConditions: [ 'Succeeded' ]
                }
              ]
              typeProperties: {
                variableName: 'lifeCycleState'
                value: {
                  value: exprLifeCycle
                  type: 'Expression'
                }
              }
            }
            {
              name: 'Set_ResultState'
              type: 'SetVariable'
              dependsOn: [
                {
                  activity: 'Get_Run_Status'
                  dependencyConditions: [ 'Succeeded' ]
                }
              ]
              typeProperties: {
                variableName: 'resultState'
                value: {
                  value: exprResultState
                  type: 'Expression'
                }
              }
            }
            {
              name: 'Set_RunPageUrl'
              type: 'SetVariable'
              dependsOn: [
                {
                  activity: 'Get_Run_Status'
                  dependencyConditions: [ 'Succeeded' ]
                }
              ]
              typeProperties: {
                variableName: 'runPageUrl'
                value: {
                  value: exprRunPageUrl
                  type: 'Expression'
                }
              }
            }
          ]
        }
      }
      {
        name: 'If_Job_Failed'
        type: 'IfCondition'
        dependsOn: [
          {
            activity: 'Until_Job_Finishes'
            dependencyConditions: [ 'Succeeded' ]
          }
        ]
        typeProperties: {
          expression: {
            value: exprJobFailed
            type: 'Expression'
          }
          ifTrueActivities: [
            {
              name: 'Fail_Pipeline'
              type: 'Fail'
              typeProperties: {
                message: {
                  value: exprFailMessage
                  type: 'Expression'
                }
                errorCode: '500'
              }
            }
          ]
        }
      }
    ]
  }
}

// ---------------------------------------------------------------------------
// Monthly refresh (EPA revises TRI data after the annual release). Deployed stopped.
// ---------------------------------------------------------------------------
resource trigger 'Microsoft.DataFactory/factories/triggers@2018-06-01' = {
  parent: factory
  name: 'tr_monthly_refresh'
  properties: {
    type: 'ScheduleTrigger'
    description: 'Re-ingest the current reporting year on the 1st of each month'
    typeProperties: {
      recurrence: {
        frequency: 'Month'
        interval: 1
        startTime: triggerStartTime
        timeZone: 'UTC'
        schedule: {
          monthDays: [ 1 ]
          hours: [ 13 ]
          minutes: [ 0 ]
        }
      }
    }
    pipelines: [
      {
        pipelineReference: {
          referenceName: pipeline.name
          type: 'PipelineReference'
        }
        parameters: {
          reportingYear: reportingYear
        }
      }
    ]
  }
}

output pipelineName string = pipeline.name
