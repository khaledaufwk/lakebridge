# WakeCapDW ADF Pipeline Deployment Script
# This script deploys the ADF pipelines for extracting data from SQL Server to ADLS

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,

    [Parameter(Mandatory=$true)]
    [string]$FactoryName = "wakecap-adf",

    [Parameter(Mandatory=$true)]
    [string]$SqlConnectionString,

    [Parameter(Mandatory=$true)]
    [string]$AdlsAccountName,

    [Parameter(Mandatory=$true)]
    [string]$AdlsAccountKey,

    [string]$Location = "eastus"
)

$ErrorActionPreference = "Stop"

Write-Host "=============================================="
Write-Host "WakeCapDW ADF Deployment"
Write-Host "=============================================="
Write-Host ""

# Check if logged in to Azure
$context = Get-AzContext
if (-not $context) {
    Write-Host "Not logged in to Azure. Please run Connect-AzAccount first."
    exit 1
}

Write-Host "Subscription: $($context.Subscription.Name)"
Write-Host "Resource Group: $ResourceGroupName"
Write-Host "Data Factory: $FactoryName"
Write-Host ""

# Check if resource group exists
$rg = Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue
if (-not $rg) {
    Write-Host "Creating resource group $ResourceGroupName..."
    New-AzResourceGroup -Name $ResourceGroupName -Location $Location
}

# Check if Data Factory exists
$adf = Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $FactoryName -ErrorAction SilentlyContinue
if (-not $adf) {
    Write-Host "Creating Data Factory $FactoryName..."
    Set-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $FactoryName -Location $Location
}

# Deploy ARM template
Write-Host ""
Write-Host "Deploying ARM template..."

$templateFile = Join-Path $PSScriptRoot "arm_template.json"

$parameters = @{
    factoryName = $FactoryName
    sqlServerConnectionString = (ConvertTo-SecureString -String $SqlConnectionString -AsPlainText -Force)
    adlsAccountName = $AdlsAccountName
    adlsAccountKey = (ConvertTo-SecureString -String $AdlsAccountKey -AsPlainText -Force)
}

$deployment = New-AzResourceGroupDeployment `
    -ResourceGroupName $ResourceGroupName `
    -TemplateFile $templateFile `
    -TemplateParameterObject $parameters `
    -Name "WakeCapADFDeployment-$(Get-Date -Format 'yyyyMMddHHmmss')" `
    -Verbose

if ($deployment.ProvisioningState -eq "Succeeded") {
    Write-Host ""
    Write-Host "=============================================="
    Write-Host "Deployment Successful!"
    Write-Host "=============================================="
    Write-Host ""
    Write-Host "Next Steps:"
    Write-Host "1. Go to Azure Portal > Data Factory > $FactoryName"
    Write-Host "2. Open 'Author & Monitor'"
    Write-Host "3. Navigate to Pipelines > WakeCapDW_FullExtract"
    Write-Host "4. Click 'Add Trigger' > 'Trigger Now' for initial full load"
    Write-Host "5. After full load, schedule WakeCapDW_IncrementalExtract for daily runs"
    Write-Host ""
    Write-Host "ADLS Output Path: abfss://raw@$AdlsAccountName.dfs.core.windows.net/wakecap/"
}
else {
    Write-Host "Deployment failed with state: $($deployment.ProvisioningState)"
    exit 1
}
