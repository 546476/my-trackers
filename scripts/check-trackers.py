Set-Content -Path .\check-trackers.ps1 -Value @'
$ErrorActionPreference = "Stop"

# Загружаем список трекеров из trackers.txt
$trackers = Get-Content -Path .\trackers.txt -Raw -Encoding UTF8
$trackers = $trackers -split "`n" | Where-Object { $_.Trim() -and -not $_.Trim().StartsWith("#") } | ForEach-Object { $_.Trim() }

# Проверяем каждый трекер
foreach ($url in $urls) {
    try {
        $response = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 20
        $content = $response.Content

        # Проверка: есть ли в ответе полезные данные
        if ($content.Length -gt 10) {
            # Проверяем наличие ключевых слов: announce, peers, info_hash
            $lowerContent = $content.ToLower()
            if ($lowerContent -match "announce" -or $lowerContent -match "peers" -or $lowerContent -match "info_hash") {
                [void]$working.Add($url)
            }
        }

        Write-Host "OK: $url"
    }
    catch {
        $failed += $url
        Write-Warning "Не удалось получить: $url"
    }
}

# Форматируем выходной список: каждый трекер — на отдельной строке, без пустых строк
$output = ($working | Sort-Object) -join "`n"

# Сохраняем результат в trackers.txt
Set-Content -Path .\trackers.txt -Value $output -Encoding UTF8
}
'@
