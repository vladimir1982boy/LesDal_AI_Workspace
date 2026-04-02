param(
    [string]$TaskName = "LesDal Daily Pipeline",
    [string]$RunAt = "09:00",
    [int]$MaxPosts = 1
)

$ErrorActionPreference = "Stop"

if ($RunAt -notmatch "^(?:[01]\d|2[0-3]):[0-5]\d$") {
    throw "RunAt must be HH:mm, for example 09:00 or 18:30"
}

$runScript = Join-Path $PSScriptRoot "run_daily.ps1"
if (-not (Test-Path -LiteralPath $runScript)) {
    throw "Run script not found: $runScript"
}

$scheduledTasksModule = Get-Module -ListAvailable -Name ScheduledTasks
if (-not $scheduledTasksModule) {
    throw "ScheduledTasks module not found. This script requires Windows Task Scheduler cmdlets."
}

$runHour = [int]$RunAt.Split(":")[0]
$runMinute = [int]$RunAt.Split(":")[1]
$triggerAt = (Get-Date).Date.AddHours($runHour).AddMinutes($runMinute)

$taskAction = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$runScript`" -MaxPosts $MaxPosts"
$taskTrigger = New-ScheduledTaskTrigger -Daily -At $triggerAt
$taskSettings = New-ScheduledTaskSettingsSet -StartWhenAvailable

$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $taskAction `
    -Trigger $taskTrigger `
    -Settings $taskSettings `
    -Description "LesDal RSS pipeline daily run" `
    -Force | Out-Null

Write-Host "Task created successfully."
Write-Host "TaskName : $TaskName"
Write-Host "RunAt    : $RunAt (local Windows time)"
Write-Host "MaxPosts : $MaxPosts"
Write-Host "Action   : powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$runScript`" -MaxPosts $MaxPosts"
