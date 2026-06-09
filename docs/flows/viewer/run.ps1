<#
.SYNOPSIS
    Geonera Ingestion — Flow Diagram Viewer
    Create (or recreate) the container from the published image.

.DESCRIPTION
    Pulls `sansalfian/geonera-ingestion-docs` from Docker Hub and runs it,
    replacing any previous instance — handy for (re)deploying on a Windows
    host (or local dev machine) without needing this repo's source checked out.

.PARAMETER ImageTag
    Image tag to pull/run. Default: latest

.PARAMETER Port
    Host port to publish the viewer on. Default: 4040

.PARAMETER ContainerName
    Name to give the running container. Default: geonera-flow-viewer

.PARAMETER Network
    Optional: attach the container to an existing Docker network.

.EXAMPLE
    .\run.ps1
    Runs sansalfian/geonera-ingestion-docs:latest on http://localhost:4040/viewer/

.EXAMPLE
    .\run.ps1 -Port 8080 -ImageTag 1.0
    Pulls the :1.0 tag and publishes it on host port 8080 instead.
#>
param(
    [string]$ImageRepo      = "sansalfian/geonera-ingestion-docs",
    [string]$ImageTag       = "latest",
    [int]   $Port           = 4040,
    [string]$ContainerName  = "geonera-flow-viewer",
    [string]$Network        = "",
    [string]$RestartPolicy  = "unless-stopped"
)

$ErrorActionPreference = "Stop"

$Image = "${ImageRepo}:${ImageTag}"
$ContainerPort = 4040

Write-Host "==> Pulling $Image" -ForegroundColor Cyan
docker pull $Image

$existing = docker ps -a --format '{{.Names}}' | Where-Object { $_ -eq $ContainerName }
if ($existing) {
    Write-Host "==> Removing existing container '$ContainerName'" -ForegroundColor Yellow
    docker rm -f $ContainerName | Out-Null
}

$runArgs = @(
    "run", "-d",
    "--name", $ContainerName,
    "--restart", $RestartPolicy,
    "-p", "${Port}:${ContainerPort}",
    "-e", "PORT=$ContainerPort"
)

if ($Network -ne "") {
    $runArgs += @("--network", $Network)
}

$runArgs += $Image

Write-Host "==> Starting container '$ContainerName' (host port $Port -> container $ContainerPort)" -ForegroundColor Cyan
docker @runArgs

Write-Host ""
Write-Host "Container '$ContainerName' is up." -ForegroundColor Green
Write-Host "  Open:   http://localhost:$Port/viewer/"
Write-Host "  Logs:   docker logs -f $ContainerName"
Write-Host "  Health: docker inspect --format='{{.State.Health.Status}}' $ContainerName"
Write-Host "  Stop:   docker rm -f $ContainerName"
