param(
    [int]$MaxPosts = 1,
    [string]$LogDir = "out\logs"
)

$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

if (-not (Test-Path -LiteralPath $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$logFile = Join-Path $LogDir "pipeline_$timestamp.log"

$venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
$requiredImports = "import requests, dotenv, feedparser; from google import genai; import telegram"

if (Test-Path -LiteralPath $venvPython) {
    $previousEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & $venvPython -c $requiredImports *> $null
    $venvImportExitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousEap
    if ($venvImportExitCode -eq 0) {
        $previousEap = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        & $venvPython "main.py" "--max-posts" "$MaxPosts" *>&1 | Tee-Object -FilePath $logFile
        $pipelineExitCode = $LASTEXITCODE
        $ErrorActionPreference = $previousEap
        exit $pipelineExitCode
    }
}

$pyCmd = Get-Command py -ErrorAction SilentlyContinue
if ($pyCmd) {
    $previousEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & $pyCmd.Source "main.py" "--max-posts" "$MaxPosts" *>&1 | Tee-Object -FilePath $logFile
    $pipelineExitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousEap
    exit $pipelineExitCode
}

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    throw "Python not found. Install Python, repair .venv, or ensure py/python is available."
}

$previousEap = $ErrorActionPreference
$ErrorActionPreference = "Continue"
& $pythonCmd.Source "main.py" "--max-posts" "$MaxPosts" *>&1 | Tee-Object -FilePath $logFile
$pipelineExitCode = $LASTEXITCODE
$ErrorActionPreference = $previousEap
exit $pipelineExitCode
