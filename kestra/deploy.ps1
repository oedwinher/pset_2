# Equivalente en PowerShell de kestra/deploy.sh. Uso (desde la raiz del repo):  .\kestra\deploy.ps1
$ErrorActionPreference = "Stop"
$port = if ($env:KESTRA_PORT) { $env:KESTRA_PORT } else { "8080" }
$Kestra = "http://localhost:$port/api/v1/main"
$Usuario = if ($env:KESTRA_USER) { $env:KESTRA_USER } else { "admin@kestra.io:Admin1234" }
$Ns = "ecuador.bce.coyuntura"
$Id = "f104_to_snowflake_raw"
$Flow = "kestra/flows/f104_to_snowflake_raw.yml"

function Subir($local, $nombre) {
    curl.exe -sf -u $Usuario -X POST "$Kestra/namespaces/$Ns/files?path=/$nombre" -F "fileContent=@$local" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Fallo la subida de $nombre" }
    Write-Host "Archivo $nombre subido al namespace $Ns"
}

Subir "kestra/scripts/load_raw_f104.py" "load_raw_f104.py"
Subir "data/seed/f104_ventas_46_ramas.csv" "f104_ventas_46_ramas.csv"

curl.exe -sf -u $Usuario "$Kestra/flows/$Ns/$Id" | Out-Null
if ($LASTEXITCODE -eq 0) {
    curl.exe -sf -u $Usuario -X PUT -H "Content-Type: application/x-yaml" --data-binary "@$Flow" "$Kestra/flows/$Ns/$Id" | Out-Null
    Write-Host "Flow $Ns.$Id actualizado"
} else {
    curl.exe -sf -u $Usuario -X POST -H "Content-Type: application/x-yaml" --data-binary "@$Flow" "$Kestra/flows" | Out-Null
    Write-Host "Flow $Ns.$Id creado"
}
