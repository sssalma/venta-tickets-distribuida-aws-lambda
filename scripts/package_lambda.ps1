param(
    [string]$OutputZip = "lambda_worker.zip",
    [string]$BuildDir = ".lambda_build"
)

$ErrorActionPreference = "Stop"

# Para AWS real, crear el ZIP en Linux/Amazon Linux o usar una Layer para psycopg2.

if (Test-Path $BuildDir) {
    Remove-Item -Recurse -Force $BuildDir
}

New-Item -ItemType Directory -Path $BuildDir | Out-Null

python -m pip install -r lambda_requirements.txt -t $BuildDir

Copy-Item lambda_worker.py -Destination $BuildDir
Copy-Item base -Destination $BuildDir -Recurse

if (Test-Path $OutputZip) {
    Remove-Item -Force $OutputZip
}

Compress-Archive -Path "$BuildDir\*" -DestinationPath $OutputZip -Force

Write-Host "Created $OutputZip"
Write-Host "Note: psycopg2-binary may need an Amazon Linux compatible build or Lambda Layer."
