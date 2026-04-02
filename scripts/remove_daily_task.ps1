param(
    [string]$TaskName = "LesDal Daily Pipeline"
)

$ErrorActionPreference = "Stop"

schtasks /Query /TN $TaskName *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Task not found: $TaskName"
    exit 0
}

schtasks /Delete /TN $TaskName /F *> $null
Write-Host "Task removed: $TaskName"
