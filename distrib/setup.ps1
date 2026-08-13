# Convert this script so it can run on Windows
function print_starlake_ascii_art {
    Write-Host "   _____ _______       _____  _               _  ________"
    Write-Host "  / ____|__   __|/\   |  __ \| |        /\   | |/ /  ____|"
    Write-Host " | (___    | |  /  \  | |__) | |       /  \  | ' /| |__"
    Write-Host "  \___ \   | | / /\ \ |  _  /| |      / /\ \ |  < |  __|"
    Write-Host "  ____) |  | |/ ____ \| | \ \| |____ / ____ \| . \| |____"
    Write-Host " |_____/   |_/_/    \_\_|  \_\______/_/    \_\_|\_\______|"
}

function get_installation_directory {
    $INSTALL_DIR = Read-Host "Where do you want to install Starlake? [$HOME\starlake]"
    if ($INSTALL_DIR -eq "") {
        $INSTALL_DIR = "$HOME\starlake"
    }
    $INSTALL_DIR = Invoke-Expression "Write-Output $INSTALL_DIR"
    New-Item -ItemType Directory -Path $INSTALL_DIR -Force | Out-Null
    $INSTALL_DIR
}


function get_version_to_install {
    param([string]$RequestedVersion = "")

    $RELEASE_VERSIONS = @()
    try {
        $releases = Invoke-RestMethod -Uri "https://api.github.com/repos/starlake-ai/starlake/releases?per_page=15" -UseBasicParsing
        $RELEASE_VERSIONS = @($releases |
            ForEach-Object { $_.tag_name } |
            Where-Object { $_ -match '^v\d+\.\d+\.\d+$' } |
            ForEach-Object { $_.TrimStart('v') } |
            Sort-Object -Descending { [version]$_ } |
            Select-Object -First 5)
    } catch {}

    if ($RELEASE_VERSIONS.Count -eq 0) {
        Write-Host "Error: no releases found at https://github.com/starlake-ai/starlake/releases"
        exit 1
    }

    $VERSIONS = $RELEASE_VERSIONS

    $DEFAULT_VERSION = $VERSIONS[0]

    $VERSION = $RequestedVersion
    while ($VERSION -notin $VERSIONS) {
        if ($VERSION -ne "") {
            Write-Host "Invalid version $VERSION. Please choose from the available versions."
        }
        Write-Host "Last available versions:"
        foreach ($v in $VERSIONS) { Write-Host "  $v" }
        $VERSION = Read-Host "Which version do you want to install? [$DEFAULT_VERSION]"
        if ($VERSION -eq "") {
            $VERSION = $DEFAULT_VERSION
        }
    }

    $VERSION
}

function install_starlake {
    param (
        [string]$INSTALL_DIR,
        [string]$VERSION
    )
    Write-Host "installing $VERSION"
    $url = "https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/starlake.cmd"

    Write-Host "Downloading $url to $INSTALL_DIR"
    try {
        Invoke-WebRequest -Uri $url -OutFile "$INSTALL_DIR\starlake.cmd" -UseBasicParsing -ErrorAction Stop
        # Ensure CRLF line endings for Windows batch file
        $content = [System.IO.File]::ReadAllText("$INSTALL_DIR\starlake.cmd")
        $content = $content -replace "`r`n", "`n" -replace "`n", "`r`n"
        [System.IO.File]::WriteAllText("$INSTALL_DIR\starlake.cmd", $content)
    } catch {
        Write-Host "Error: Failed to download starlake.cmd from $url"
        Write-Host $_.Exception.Message
        exit 1
    }

    Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope Process
}

function add_starlake_to_path {
    param([string]$x)
    if (!($env:PATH -split ';' -contains $X)){
        $Env:Path+= ";" +  $x
        Write-Output $Env:Path
        $write = Read-Host 'Set PATH permanently ? (yes|no)'
        if ($write -eq "yes")
        {
            [Environment]::SetEnvironmentVariable("Path",$env:Path, [System.EnvironmentVariableTarget]::User)
            Write-Output 'PATH updated'
        }
    }
}

function run_installation_command {
    param([string]$InstallDir, [string]$Version)
    # Remove stale versions.cmd so setup.jar uses the correct SL_VERSION from the env
    if (Test-Path "$InstallDir\versions.cmd") {
        Remove-Item "$InstallDir\versions.cmd"
    }
    $env:SL_VERSION = $Version
    Start-Process -FilePath "$InstallDir\starlake.cmd" -ArgumentList 'install' -Wait -NoNewWindow
    if (Test-Path "$InstallDir\setup.jar") {
        Remove-Item "$InstallDir\setup.jar"
    }
}


function print_success_message {
    Write-Host "Starlake has been successfully installed!"
}

function get_java_major_version {
    # Parse the REAL runtime version from `java -version` (stderr). The file
    # version of java.exe is unreliable, and string comparison is lexicographic
    # ("8.0" -lt "11" is false), which used to let Java 8 pass the check.
    # Handles both version schemes: "17.0.12" -> 17, "1.8.0_292" -> 8.
    param([string]$JavaExe)
    if (-not (Test-Path $JavaExe) -and -not (Get-Command $JavaExe -ErrorAction SilentlyContinue)) {
        return 0
    }
    $line = & $JavaExe -version 2>&1 | Select-Object -First 1
    if ("$line" -match 'version "(\d+)(?:\.(\d+))?') {
        $major = [int]$Matches[1]
        if ($major -eq 1 -and $Matches[2]) { $major = [int]$Matches[2] }
        return $major
    }
    return 0
}

function resolve_java {
    # JAVA_HOME wins when it is set (that is also what starlake.cmd executes);
    # otherwise fall back to `java` from the PATH.
    if ($env:JAVA_HOME) {
        $exe = Join-Path $env:JAVA_HOME "bin\java.exe"
        if (Test-Path $exe) {
            return @{ Exe = $exe; Major = (get_java_major_version $exe); Source = "JAVA_HOME ($env:JAVA_HOME)" }
        }
    }
    $cmd = Get-Command java -ErrorAction SilentlyContinue
    if ($cmd) {
        return @{ Exe = $cmd.Source; Major = (get_java_major_version $cmd.Source); Source = "PATH ($($cmd.Source))" }
    }
    return @{ Exe = ""; Major = 0; Source = "none" }
}

function ensure_java {
    # Check the installed Java (JAVA_HOME first). If none is found, or its
    # version is below the required minimum, install an EMBEDDED portable
    # Temurin JDK inside the starlake install directory (<install-dir>\jdk)
    # and update the SESSION environment (JAVA_HOME + PATH). No administrator
    # rights: portable zip + process-scoped variables only. starlake.cmd picks
    # the embedded JDK up automatically in later sessions.
    param([string]$InstallDir, [int]$MinVersion = 11, [int]$EmbeddedVersion = 17)

    $java = resolve_java
    if ($java.Major -ge $MinVersion) {
        Write-Host "Using Java $($java.Major) from $($java.Source)"
        return
    }
    if ($java.Major -gt 0) {
        Write-Host "Java $($java.Major) found via $($java.Source) but Java $MinVersion or above is required."
    } else {
        Write-Host "No Java found (checked JAVA_HOME and PATH). Java $MinVersion or above is required."
    }

    $jdkDir = Join-Path $InstallDir "jdk"
    Write-Host "Installing an embedded Temurin $EmbeddedVersion JDK into $jdkDir (portable zip, no administrator rights)"
    # The Adoptium API redirects to the latest GA windows x64 JDK zip. On
    # Windows-on-ARM the x64 build runs fine under the built-in emulation.
    $adoptiumUrl = "https://api.adoptium.net/v3/binary/latest/$EmbeddedVersion/ga/windows/x64/jdk/hotspot/normal/eclipse?project=jdk"
    $zip = Join-Path ([System.IO.Path]::GetTempPath()) "starlake-embedded-jdk.zip"
    $unpack = Join-Path ([System.IO.Path]::GetTempPath()) ("starlake-jdk-" + [System.IO.Path]::GetRandomFileName())
    try {
        Invoke-WebRequest -UseBasicParsing -Uri $adoptiumUrl -OutFile $zip -ErrorAction Stop
    } catch {
        Write-Host "Error: failed to download the embedded JDK from $adoptiumUrl"
        Write-Host $_.Exception.Message
        exit 1
    }
    Expand-Archive -Path $zip -DestinationPath $unpack -Force
    Remove-Item $zip
    # the archive unpacks as jdk-<version>+<build>\ - flatten it to <install-dir>\jdk
    $inner = Get-ChildItem $unpack -Directory | Select-Object -First 1
    if ($null -eq $inner -or -not (Test-Path (Join-Path $inner.FullName "bin\java.exe"))) {
        Write-Host "Error: unexpected JDK archive layout"
        exit 1
    }
    if (Test-Path $jdkDir) { Remove-Item $jdkDir -Recurse -Force }
    Move-Item $inner.FullName $jdkDir
    Remove-Item $unpack -Recurse -Force

    # SESSION environment only: JAVA_HOME + PATH first, so this very install
    # (starlake.cmd install below) and everything started from this shell use
    # the embedded JDK. Later sessions are covered by starlake.cmd itself,
    # which adopts <install-dir>\jdk when JAVA_HOME is not set.
    $env:JAVA_HOME = $jdkDir
    $env:Path = (Join-Path $jdkDir "bin") + ";" + $env:Path

    $major = get_java_major_version (Join-Path $jdkDir "bin\java.exe")
    if ($major -lt $MinVersion) {
        Write-Host "Error: the embedded JDK did not install correctly (got version $major)"
        exit 1
    }
    Write-Host "Embedded JDK $major ready: JAVA_HOME=$jdkDir (session)"
}

function main {
    param([string[]]$ScriptArgs = @())
    $RequestedVersion = ""
    foreach ($arg in $ScriptArgs) {
        if ($arg.StartsWith("--version=")) {
            $RequestedVersion = $arg.Substring(10)
        }
    }
    print_starlake_ascii_art
    $INSTALL_DIR = get_installation_directory
    # java check needs the install dir: an embedded JDK lands in <install-dir>\jdk
    ensure_java -InstallDir $INSTALL_DIR
    $VERSION = get_version_to_install -RequestedVersion $RequestedVersion
    install_starlake $INSTALL_DIR $VERSION
    add_starlake_to_path $INSTALL_DIR
    run_installation_command -InstallDir $INSTALL_DIR -Version $VERSION
    print_success_message
}

# Run the main function
main $args
