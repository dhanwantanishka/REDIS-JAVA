# Simple PowerShell script to test Redis server
$tcpClient = New-Object System.Net.Sockets.TcpClient
try {
    $tcpClient.Connect("localhost", 6379)
    $stream = $tcpClient.GetStream()
    $writer = New-Object System.IO.StreamWriter($stream)
    $reader = New-Object System.IO.StreamReader($stream)
    
    # Send PING command
    $writer.Write("*1`r`n`$4`r`nPING`r`n")
    $writer.Flush()
    
    # Read response
    $response = $reader.ReadToEnd()
    Write-Host "Response: $response"
    
} catch {
    Write-Host "Error: $($_.Exception.Message)"
} finally {
    $tcpClient.Close()
}
