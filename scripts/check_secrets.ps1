param(
    [switch]$Strict
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

$requiredKeys = @(
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHANNEL_ID",
    "GEMINI_API_KEY",
    "MAX_BOT_TOKEN",
    "MAX_CHANNEL_ID",
    "VK_API_KEY",
    "VK_GROUP_ID"
)

$envFiles = @(
    (Join-Path $projectRoot "secrets\.env.local"),
    (Join-Path $projectRoot ".env.local"),
    (Join-Path $projectRoot ".env")
)

function Parse-EnvFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $result = @{
        Values = @{}
        HasBom = $false
    }

    if (-not (Test-Path -LiteralPath $Path)) {
        return $result
    }

    $bytes = [System.IO.File]::ReadAllBytes($Path)
    if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
        $result.HasBom = $true
    }

    $text = [System.IO.File]::ReadAllText($Path, [System.Text.Encoding]::UTF8)
    if ($text.Length -gt 0 -and [int]$text[0] -eq 0xFEFF) {
        $text = $text.Substring(1)
    }

    foreach ($line in ($text -split "`r?`n")) {
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }
        if ($line.TrimStart().StartsWith("#")) {
            continue
        }
        if ($line -match '^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$') {
            $key = $Matches[1]
            $value = $Matches[2]
            $result.Values[$key] = $value
        }
    }

    return $result
}

$parsedFiles = @{}
foreach ($envFile in $envFiles) {
    $parsedFiles[$envFile] = Parse-EnvFile -Path $envFile
}

$effective = @{}
$sources = @{}

foreach ($key in $requiredKeys) {
    $procValue = [Environment]::GetEnvironmentVariable($key, "Process")
    if (-not [string]::IsNullOrWhiteSpace($procValue)) {
        $effective[$key] = $procValue
        $sources[$key] = "process env"
        continue
    }

    foreach ($envFile in $envFiles) {
        $fileInfo = $parsedFiles[$envFile]
        if ($fileInfo.Values.ContainsKey($key)) {
            $effective[$key] = [string]$fileInfo.Values[$key]
            $sources[$key] = $envFile.Replace($projectRoot + "\", "")
            break
        }
    }
}

$missing = @()

Write-Host "Secrets check" -ForegroundColor Cyan
Write-Host "Project root: $projectRoot"
Write-Host ""

foreach ($envFile in $envFiles) {
    $relative = $envFile.Replace($projectRoot + "\", "")
    if (Test-Path -LiteralPath $envFile) {
        $bomLabel = if ($parsedFiles[$envFile].HasBom) { "BOM detected" } else { "UTF-8 OK" }
        Write-Host "[FILE] $relative - present ($bomLabel)"
    } else {
        Write-Host "[FILE] $relative - missing"
    }
}

Write-Host ""

foreach ($key in $requiredKeys) {
    $value = ""
    if ($effective.ContainsKey($key)) {
        $value = [string]$effective[$key]
    }

    $isSet = -not [string]::IsNullOrWhiteSpace($value)
    $source = if ($sources.ContainsKey($key)) { $sources[$key] } else { "-" }

    if ($isSet) {
        Write-Host ("[ OK ] {0} <- {1}" -f $key, $source) -ForegroundColor Green
    } else {
        Write-Host ("[MISS] {0} <- {1}" -f $key, $source) -ForegroundColor Yellow
        $missing += $key
    }
}

if ($missing.Count -gt 0) {
    Write-Host ""
    Write-Host ("Missing required secrets: {0}" -f ($missing -join ", ")) -ForegroundColor Yellow
    if ($Strict) {
        exit 1
    }
    exit 0
}

Write-Host ""
Write-Host "All required secrets are available." -ForegroundColor Green
