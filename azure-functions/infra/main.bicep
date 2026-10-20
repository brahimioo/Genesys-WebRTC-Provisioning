targetScope = 'resourceGroup'

@minLength(3)
@maxLength(20)
param projectName string = 'genesys-webrtc'

@allowed(['test', 'prod'])
param stage string = 'test'
param location string = resourceGroup().location

@description('Single-tenant API application registration client ID, not the deployment identity.')
param entraApiClientId string
param adminObjectIds array
param genesysEnvironment string = 'mypurecloud.de'
param genesysClientId string
@secure()
param genesysClientSecret string
param sourceGroupId string
param siteId string
param phoneBaseSettingsId string
param lineBaseSettingsId string
param routingSkillId string = ''
param routingLanguageId string = ''
@minValue(0)
@maxValue(5)
param skillProficiency int = 0
@minValue(0)
@maxValue(5)
param languageProficiency int = 0
param pilotUserIds array = []
param dryRun bool = true
param provisioningPaused bool = false
@allowed(['if_missing', 'enforce'])
param defaultStationPolicy string = 'if_missing'
@allowed(['disabled', 'fill_if_empty'])
param preferredNamePolicy string = 'disabled'
param givenNames object = {}
@minValue(1)
@maxValue(1000)
param maxJobsPerScan int = 50
param scanSchedule string = '0 */15 * * * *'
param recoverySchedule string = '0 */5 * * * *'
param reconcileSchedule string = '0 0 1 * * *'
@minValue(1)
@maxValue(50)
param maxAttempts int = 8
@minValue(1)
param reconcileHours int = 24
param enableAlerts bool = false
param alertEmail string = ''

var suffix = uniqueString(resourceGroup().id, projectName, stage)
var appName = 'func-${projectName}-${stage}-${take(suffix, 6)}'
var tags = {
  application: 'genesys-webrtc-provisioning'
  environment: stage
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: 'st${suffix}'
  location: location
  tags: tags
  kind: 'StorageV2'
  sku: { name: 'Standard_LRS' }
  properties: {
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    publicNetworkAccess: 'Enabled'
  }
}

resource blobs 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
  properties: {}
}

resource deploymentContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobs
  name: 'deployment'
  properties: { publicAccess: 'None' }
}

resource queues 'Microsoft.Storage/storageAccounts/queueServices@2023-05-01' = {
  parent: storage
  name: 'default'
  properties: {}
}

resource queueResources 'Microsoft.Storage/storageAccounts/queueServices/queues@2023-05-01' = [for name in [
  'webrtc-jobs'
  'webrtc-jobs-poison'
  'webrtc-scans'
  'webrtc-scans-poison'
]: {
  parent: queues
  name: name
  properties: {}
}]

resource tables 'Microsoft.Storage/storageAccounts/tableServices@2023-05-01' = {
  parent: storage
  name: 'default'
  properties: {}
}

resource tableResources 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = [for name in [
  'WebRtcState'
  'WebRtcControl'
  'WebRtcMembers'
  'WebRtcAudit'
]: {
  parent: tables
  name: name
  properties: {}
}]

resource vault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: 'kv-${stage}-${suffix}'
  location: location
  tags: tags
  properties: {
    tenantId: tenant().tenantId
    sku: { family: 'A'
 name: 'standard' }
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    enablePurgeProtection: true
    publicNetworkAccess: 'Enabled'
    accessPolicies: []
  }
}

resource secret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: vault
  name: 'genesys-client-secret'
  properties: { value: genesysClientSecret }
}

resource workspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: 'log-${projectName}-${stage}-${take(suffix, 6)}'
  location: location
  tags: tags
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
    features: { enableLogAccessUsingOnlyResourcePermissions: true }
  }
}

resource insights 'Microsoft.Insights/components@2020-02-02' = {
  name: 'appi-${projectName}-${stage}-${take(suffix, 6)}'
  location: location
  kind: 'web'
  tags: tags
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: workspace.id
    DisableLocalAuth: true
  }
}

resource plan 'Microsoft.Web/serverfarms@2024-04-01' = {
  name: 'plan-${projectName}-${stage}-${take(suffix, 6)}'
  location: location
  tags: tags
  kind: 'functionapp'
  sku: { name: 'FC1'
 tier: 'FlexConsumption' }
  properties: { reserved: true }
}

resource functionApp 'Microsoft.Web/sites@2024-04-01' = {
  name: appName
  location: location
  tags: tags
  kind: 'functionapp,linux'
  identity: { type: 'SystemAssigned' }
  properties: {
    serverFarmId: plan.id
    httpsOnly: true
    publicNetworkAccess: 'Enabled'
    functionAppConfig: {
      runtime: { name: 'python'
 version: '3.12' }
      scaleAndConcurrency: {
        maximumInstanceCount: 1
        instanceMemoryMB: 2048
        http: { perInstanceConcurrency: 1 }
      }
      deployment: {
        storage: {
          type: 'blobContainer'
          value: '${storage.properties.primaryEndpoints.blob}${deploymentContainer.name}'
          authentication: { type: 'SystemAssignedIdentity' }
        }
      }
    }
    siteConfig: {
      minTlsVersion: '1.2'
      scmMinTlsVersion: '1.2'
      ftpsState: 'Disabled'
      http20Enabled: true
      appSettings: [
        { name: 'AzureWebJobsStorage__accountName'
 value: storage.name }
        { name: 'AzureWebJobsStorage__credential'
 value: 'managedidentity' }
        { name: 'ProvisioningStorage__queueServiceUri'
 value: storage.properties.primaryEndpoints.queue }
        { name: 'ProvisioningStorage__credential'
 value: 'managedidentity' }
        { name: 'STORAGE_ACCOUNT_NAME'
 value: storage.name }
        { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
 value: insights.properties.ConnectionString }
        { name: 'APPLICATIONINSIGHTS_AUTHENTICATION_STRING'
 value: 'Authorization=AAD' }
        { name: 'GENESYS_ENVIRONMENT'
 value: genesysEnvironment }
        { name: 'GENESYS_CLIENT_ID'
 value: genesysClientId }
        { name: 'GENESYS_CLIENT_SECRET'
 value: '@Microsoft.KeyVault(SecretUri=${vault.properties.vaultUri}secrets/${secret.name})' }
        { name: 'ENTRA_TENANT_ID'
 value: tenant().tenantId }
        { name: 'ADMIN_OBJECT_IDS'
 value: join(adminObjectIds, ',') }
        { name: 'SOURCE_GROUP_ID'
 value: sourceGroupId }
        { name: 'SITE_ID'
 value: siteId }
        { name: 'PHONE_BASE_SETTINGS_ID'
 value: phoneBaseSettingsId }
        { name: 'LINE_BASE_SETTINGS_ID'
 value: lineBaseSettingsId }
        { name: 'ROUTING_SKILL_ID'
 value: routingSkillId }
        { name: 'ROUTING_LANGUAGE_ID'
 value: routingLanguageId }
        { name: 'SKILL_PROFICIENCY'
 value: string(skillProficiency) }
        { name: 'LANGUAGE_PROFICIENCY'
 value: string(languageProficiency) }
        { name: 'DRY_RUN'
 value: toLower(string(dryRun)) }
        { name: 'PROVISIONING_PAUSED'
 value: toLower(string(provisioningPaused)) }
        { name: 'DEFAULT_STATION_POLICY'
 value: defaultStationPolicy }
        { name: 'PREFERRED_NAME_POLICY'
 value: preferredNamePolicy }
        { name: 'GIVEN_NAMES_JSON'
 value: string(givenNames) }
        { name: 'PILOT_USER_IDS'
 value: join(pilotUserIds, ',') }
        { name: 'MAX_JOBS_PER_SCAN'
 value: string(maxJobsPerScan) }
        { name: 'MAX_ATTEMPTS'
 value: string(maxAttempts) }
        { name: 'RECONCILE_HOURS'
 value: string(reconcileHours) }
        { name: 'SCAN_SCHEDULE'
 value: scanSchedule }
        { name: 'RECOVERY_SCHEDULE'
 value: recoverySchedule }
        { name: 'RECONCILE_SCHEDULE'
 value: reconcileSchedule }
      ]
    }
  }
}

resource auth 'Microsoft.Web/sites/config@2024-04-01' = {
  parent: functionApp
  name: 'authsettingsV2'
  properties: {
    platform: { enabled: true }
    globalValidation: { requireAuthentication: true
 unauthenticatedClientAction: 'Return401' }
    httpSettings: { requireHttps: true }
    login: { tokenStore: { enabled: false } }
    identityProviders: {
      azureActiveDirectory: {
        enabled: true
        registration: {
          clientId: entraApiClientId
          openIdIssuer: 'https://login.microsoftonline.com/${tenant().tenantId}/v2.0'
        }
        validation: { allowedAudiences: [entraApiClientId, 'api://${entraApiClientId}'] }
      }
    }
  }
}

resource basicAuth 'Microsoft.Web/sites/basicPublishingCredentialsPolicies@2024-04-01' = [for name in ['scm', 'ftp']: {
  parent: functionApp
  name: name
  properties: { allow: false }
}]

var storageRoles = [
  'b7e6dc6d-f1e8-4753-8033-0f276bb0955b' // Blob Data Owner: host leases and deployment packages
  '974c5e8b-45b9-4653-ba55-5f855dd0fb88' // Queue Data Contributor
  '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3' // Table Data Contributor
]
resource storageGrants 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for role in storageRoles: {
  scope: storage
  name: guid(storage.id, functionApp.id, role)
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', role)
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}]

resource vaultGrant 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: vault
  name: guid(vault.id, functionApp.id, 'secrets-user')
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource monitorGrant 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: insights
  name: guid(insights.id, functionApp.id, 'monitor-publisher')
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '3913510d-42f4-4e42-8a64-420c390055eb')
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource actionGroup 'Microsoft.Insights/actionGroups@2023-01-01' = {
  name: 'ag-${projectName}-${stage}'
  location: 'global'
  tags: tags
  properties: {
    groupShortName: 'WebRTC'
    enabled: true
    emailReceivers: empty(alertEmail) ? [] : [
      { name: 'operations'
 emailAddress: alertEmail
 useCommonAlertSchema: true }
    ]
  }
}

resource failureAlert 'Microsoft.Insights/scheduledQueryRules@2023-12-01' = {
  name: 'alert-webrtc-failures-${stage}-${suffix}'
  location: location
  properties: {
    displayName: 'WebRTC: provisioning failure'
    description: 'A user is blocked, a scan failed, or a poison message was received.'
    enabled: enableAlerts
    severity: 2
    evaluationFrequency: 'PT5M'
    windowSize: 'PT15M'
    scopes: [workspace.id]
    skipQueryValidation: true
    autoMitigate: true
    criteria: {
      allOf: [{
        query: 'AppTraces | where Message startswith "webrtc_event " | extend e = parse_json(substring(Message, 13)) | where tostring(e.event) in ("user_failed", "scan_failed", "poison_message")'
        timeAggregation: 'Count'
        operator: 'GreaterThan'
        threshold: 0
        failingPeriods: { numberOfEvaluationPeriods: 1
 minFailingPeriodsToAlert: 1 }
      }]
    }
    actions: { actionGroups: [actionGroup.id] }
  }
}

resource heartbeatAlert 'Microsoft.Insights/scheduledQueryRules@2023-12-01' = {
  name: 'alert-webrtc-scan-missing-${stage}-${suffix}'
  location: location
  properties: {
    displayName: 'WebRTC: no successful scan'
    description: 'No successful scan in the last 45 minutes; also fires during an intentional pause.'
    enabled: enableAlerts
    severity: 2
    evaluationFrequency: 'PT15M'
    windowSize: 'PT1H'
    scopes: [workspace.id]
    skipQueryValidation: true
    autoMitigate: true
    criteria: {
      allOf: [{
        query: 'let lastScan = toscalar(AppTraces | where Message startswith "webrtc_event " | extend e = parse_json(substring(Message, 13)) | where tostring(e.event) == "scan_completed" | summarize max(TimeGenerated)); print missing = iff(isnull(lastScan) or lastScan < ago(45m), 1, 0) | where missing == 1'
        timeAggregation: 'Count'
        operator: 'GreaterThan'
        threshold: 0
        failingPeriods: { numberOfEvaluationPeriods: 1
 minFailingPeriodsToAlert: 1 }
      }]
    }
    actions: { actionGroups: [actionGroup.id] }
  }
}

resource workbook 'Microsoft.Insights/workbooks@2023-06-01' = {
  name: guid(resourceGroup().id, 'webrtc-workbook', stage)
  location: location
  kind: 'shared'
  tags: tags
  properties: {
    displayName: 'WebRTC Provisioning - ${stage}'
    category: 'workbook'
    sourceId: insights.id
    serializedData: replace(loadTextContent('./workbook.json'), 'WORKSPACE_RESOURCE_ID', workspace.id)
  }
}

output functionAppName string = functionApp.name
output functionAppResourceId string = functionApp.id
output apiBaseUrl string = 'https://${functionApp.properties.defaultHostName}/api'
output storageAccountName string = storage.name
output keyVaultName string = vault.name
output workspaceResourceId string = workspace.id
output workbookResourceId string = workbook.id
output entraApiClientId string = entraApiClientId
output tenantId string = tenant().tenantId
