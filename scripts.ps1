function WifiIP {
    $wifiIPAddress = Get-NetIPAddress | Where-Object -FilterScript { $_.InterfaceAlias -Eq "Wi-Fi" -and $_.AddressFamily -Eq "IPv4" }
    $ipAddress = $wifiIPAddress.IPAddress
    return $ipAddress
}

function StartWithWindowsTerminal {
    param (
        [string]$Command,
        [string]$Title = "Server"
    )
    $initDir = Get-Location
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = "wt.exe"
    $startInfo.Arguments = "-w 0 nt --suppressApplicationTitle --title " + $Title + " -d " + $initDir + " cmd /k " + $Command
    $startInfo.UseShellExecute = $false

    #     Set environment variables (Uncomment if needed)
    #    $startInfo.Environment["Path"] = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" +
    #            [System.Environment]::GetEnvironmentVariable("Path", "User")

    # Start the process
    [System.Diagnostics.Process]::Start($startInfo)
}

function Follower {
    param (
        [string]$Addr,
        [string]$LeaderAddr = "0.0.0.0:8080",
        [string]$LBAddr = "0.0.0.0:8000",
        [int]$Port = 8081
    )
    if (-not $Addr) {
        $Addr = WifiIP
        if (-not $Addr) {
            $Addr = "0.0.0.0"
        }
    }
    
    $command = "cargo run -- follower ${Addr}:${Port} ${LeaderAddr} ${LBAddr}"
    
    Write-Host "Starting follower on ${Addr}:${Port}"
    Write-Host "Command is: ${command}"
    Write-Host "Addr is: ${Addr}"

    # Write-Host $command

    StartWithWindowsTerminal -Command $command > $null

    # In case StartWithWindowsTerminal does not work
    # Start-Process -FilePath "cmd" -ArgumentList "/c start cmd /k", $command -NoNewWindow > $null
}

function Followers {
    param (
        [int]$Size = 3,
        [string]$Addr,
        [string]$LeaderAddr = "0.0.0.0:8080",
        [string]$LBAddr = "0.0.0.0:8000",
        [int]$Port = 8081
    )
    if (-not $Addr) {
        $Addr = WifiIP
        if (-not $Addr) {
            $Addr = "0.0.0.0"
        }
    }

    Write-Host "Starting $Size followers"
    for ($i = 0; $i -lt $Size; $i++) {
        $nPort = $Port + $i
        Follower -Addr $Addr -Port $nPort -LeaderAddr $LeaderAddr -LBAddr $LBAddr
    }
}

function Leader {
    param (
        [string]$Addr,
        [string]$LBAddr = "0.0.0.0:8000",
        [int]$Port = 8080
    )
    if (-not $Addr) {
        $Addr = WifiIP
        if (-not $Addr) {
            $Addr = "0.0.0.0"
        }
    }
    
    $command = "cargo run -- leader ${Addr}:${Port} ${LBAddr}"
    
    Write-Host "Starting leader on ${Addr}:${Port}"
    Write-Host "Command is: ${command}"

    # Write-Host $command

    StartWithWindowsTerminal -Command $command > $null

    # In case StartWithWindowsTerminal does not work
    # Start-Process -FilePath "cmd" -ArgumentList "/c start cmd /k", $command -NoNewWindow > $null
}

function LoadBalancer {
    param (
        [string]$Addr,
        [int]$Port = 8000
    )
    if (-not $Addr) {
        $Addr = WifiIP
        if (-not $Addr) {
            $Addr = "0.0.0.0"
        }
    }
    
    $command = "cargo run -- load_balancer ${Addr}:${Port}"
    
    Write-Host "Starting load balancer on ${Addr}:${Port}"
    Write-Host "Command is: ${command}"

    # Write-Host $command

    StartWithWindowsTerminal -Command $command > $null

    # In case StartWithWindowsTerminal does not work
    # Start-Process -FilePath "cmd" -ArgumentList "/c start cmd /k", $command -NoNewWindow > $null
}

function RunAll {
    param (
        [int]$Size = 3,
        [string]$Addr,
        [int]$LBPort = 8000,
        [int]$LeaderPort = 8080,
        [int]$FollowerPort = 8081
    )
    if (-not $Addr) {
        $Addr = WifiIP
        if (-not $Addr) {
            $Addr = "0.0.0.0"
        }
    }

    # LoadBalancer -Addr ${Addr} -Port ${LBPort}
    # Leader -Addr ${Addr} -LBAddr "${Addr}:${LBPort}" -Port ${LeaderPort}
    Followers -Addr ${Addr} -LeaderAddr "${Addr}:${LeaderPort}" -LBAddr "${Addr}:${LBPort}" -Port ${FollowerPort}
}