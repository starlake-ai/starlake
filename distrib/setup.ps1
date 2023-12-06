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
    New-Item -ItemType Directory -Path $INSTALL_DIR -Force | Out-Null
    $INSTALL_DIR
}

function get_versions_from_url {
    param (
        [string]$url
    )
    # Download the content from the URL
    $response = Invoke-RestMethod -Uri $url

    # Extract text content within <text> tags
    $textContents = $response.SelectNodes("//text").InnerText

    # Define the version pattern regex
    $versionPattern = '(\d+\.\d+\.\d+(-SNAPSHOT)?)'

    # Filter and extract only the matching versions
    $matchingVersions = $textContents | Where-Object { $_ -match $versionPattern }

    # Sort the matching versions in descending order
    $matchingVersions | Sort-Object -Descending
}

function get_version_to_install {
    # Extract the version number from command-line arguments
    for ($i=0; $i -lt $args.Length; $i++) {
        $arg = $args[$i]
        if ($arg.StartsWith("--version=")) {
            $VERSION=$arg.Substring(10)
        }
    }

    $ALL_SNAPSHOT_VERSIONS = (get_versions_from_url https://s01.oss.sonatype.org/service/local/repositories/snapshots/content/ai/starlake/starlake-spark3_2.12/)
    $ALL_RELEASE_VERSIONS = (get_versions_from_url https://s01.oss.sonatype.org/service/local/repositories/releases/content/ai/starlake/starlake-spark3_2.12/)

    $SNAPSHOT_VERSION = $ALL_SNAPSHOT_VERSIONS[0]
    $LATEST_RELEASE_VERSIONS = $ALL_RELEASE_VERSIONS[0..4]

    $VERSIONS = @($SNAPSHOT_VERSION) + $LATEST_RELEASE_VERSIONS

    do {
        Write-Host "Invalid version $VERSION. Please choose from the available versions."
        Write-Host "Last 5 available versions:"
        foreach ($version in $VERSIONS) {
            Write-Host $version
        }
        $VERSION = Read-Host "Which version do you want to install? [$($VERSIONS[0])]"
        if ($VERSION -eq "") {
            $VERSION = $VERSIONS[0]
        }
    } while ($VERSION -notin $VERSIONS)

    $VERSION
}

function install_starlake {
    param (
        [string]$INSTALL_DIR,
        [string]$VERSION
    )
    Write-Host "installing $VERSION"
    if ($VERSION -like "*SNAPSHOT*") {
        $url = "https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/starlake.cmd"
        $versions_url = "https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/versions.cmd"
    } else {
        $url = "https://raw.githubusercontent.com/starlake-ai/starlake/v$VERSION/distrib/starlake.cmd"
        $versions_url = "https://raw.githubusercontent.com/starlake-ai/starlake/v$VERSION/distrib/versions.cmd"
    }

    Write-Host "Downloading $url to $INSTALL_DIR"
    wget $url -OutFile $INSTALL_DIR/starlake.cmd
    wget $versions_url -OutFile $INSTALL_DIR/versions.cmd

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
    $env:SL_VERSION = $VERSION
    Start-Process -FilePath "$INSTALL_DIR\starlake.cmd" -ArgumentList 'install' -Wait
    del "$INSTALL_DIR/setup.jar"

}


function print_success_message {
    Write-Host "Starlake has been successfully installed!"
}

function check_java_version {
    # Check if Java is installed using JAVA_HOME env variable
    if ($env:JAVA_HOME -eq $null) {
        $runner = "java"
    } else {
        $runner = "$env:JAVA_HOME\bin\java"
    }
    $javaVersion = (Get-Command $runner | Select-Object -ExpandProperty Version).tostring()
    if ($javaVersion -eq $null) {
        Write-Host "Java is not installed. Please install Java 11 or above."
        exit 1
    }
    if ($javaVersion -lt "11") {
        Write-Host "Java version $javaVersion is not supported. Please install Java 11 or above."
        exit 1
    }
}

function main {
    check_java_version
    print_starlake_ascii_art
    $INSTALL_DIR=get_installation_directory
    $VERSION=get_version_to_install
    install_starlake $INSTALL_DIR $VERSION
    add_starlake_to_path $INSTALL_DIR
    run_installation_command $INSTALL_DIR
    print_success_message
}

# Run the main function
main $args
