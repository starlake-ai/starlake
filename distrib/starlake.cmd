@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "SL_SCRIPT_DIR=%SCRIPT_DIR%"
if not defined SL_API_STARLAKE_CORE_PATH set "SL_API_STARLAKE_CORE_PATH=%SCRIPT_DIR%starlake.cmd"
set "API_BIN_DIR=%SCRIPT_DIR%\bin\api\bin"

if not defined SL_ROOT (
    set "SL_ROOT=%cd%"
)
set "SL_ROOT=!SL_ROOT:\=/!"

rem Prefer the embedded JDK installed by setup.ps1 when JAVA_HOME is not set,
rem then put that java first on the SESSION PATH: some run paths invoke bare
rem `java`, and on corporate machines the system PATH often pins an older
rem default Java first (user PATH entries can never outrank the system PATH).
if not defined JAVA_HOME if exist "%SCRIPT_DIR%jdk\bin\java.exe" set "JAVA_HOME=%SCRIPT_DIR%jdk"
if defined JAVA_HOME if exist "%JAVA_HOME%\bin\java.exe" set "PATH=%JAVA_HOME%\bin;%PATH%"

if not defined HADOOP_HOME (
    set "HADOOP_HOME=%SCRIPT_DIR%bin\hadoop"
)
set "PATH=%HADOOP_HOME%\bin;%PATH%"

if /i "%1" == "reinstall" (
    rem Read the currently-pinned SL_VERSION (if any) before wiping versions.cmd,
    rem so install_command below can reinstall AT that version instead of
    rem falling back to "latest github release". Unlike starlake.sh, no
    rem explicit export is needed: variables set in this cmd.exe process are
    rem automatically part of the environment block child processes (java) see.
    if exist "%SCRIPT_DIR%versions.cmd" (
        call "%SCRIPT_DIR%versions.cmd"
    )
    if exist "%SCRIPT_DIR%versions.cmd" del "%SCRIPT_DIR%versions.cmd"
    if exist "%SCRIPT_DIR%bin\spark" rmdir /s /q "%SCRIPT_DIR%bin\spark"
    if exist "%SCRIPT_DIR%bin\deps" rmdir /s /q "%SCRIPT_DIR%bin\deps"
    if exist "%SCRIPT_DIR%bin\sl" rmdir /s /q "%SCRIPT_DIR%bin\sl"
) else (
    if exist "%SCRIPT_DIR%versions.cmd" (
        call "%SCRIPT_DIR%versions.cmd"
    )
)

rem Launch-time consistency guard: rescue installs left poisoned by an older
rem starlake.cmd whose `upgrade` only swapped the core jar/API and never
rem touched bin\spark. Cheap check: does bin\spark\jars contain a spark-core
rem jar for the SPARK_VERSION versions.cmd declares? Skipped for the commands
rem that are themselves how you fix this, and for a not-yet-installed tree.
if not defined SL_SKIP_CONSISTENCY_CHECK if defined SPARK_VERSION if exist "%SCRIPT_DIR%bin\spark\jars" (
    set "_sl_guard_skip="
    if /i "%1" == "install" set "_sl_guard_skip=1"
    if /i "%1" == "reinstall" set "_sl_guard_skip=1"
    if /i "%1" == "upgrade" set "_sl_guard_skip=1"
    if /i "%1" == "_do_upgrade" set "_sl_guard_skip=1"
    if not defined _sl_guard_skip (
        if not exist "%SCRIPT_DIR%bin\spark\jars\spark-core_*-%SPARK_VERSION%.jar" (
            echo ERROR: Starlake installation is inconsistent.
            echo versions.cmd declares Spark %SPARK_VERSION% but %SCRIPT_DIR%bin\spark\jars has no matching spark-core jar.
            echo This usually happens after upgrading with an older starlake.cmd that did not refresh the Spark runtime.
            echo Run "%SCRIPT_DIR%starlake.cmd" reinstall to fix it ^(wipes and re-downloads bin\spark, bin\deps and bin\sl for the currently pinned SL_VERSION^).
            echo Set SL_SKIP_CONSISTENCY_CHECK=1 to bypass this check.
            exit /b 1
        )
    )
)

if not defined SCALA_VERSION set "SCALA_VERSION=2.13"
set "SL_ARTIFACT_NAME=starlake-core_%SCALA_VERSION%"
set "SPARK_DIR_NAME=spark-%SPARK_VERSION%-bin-hadoop%HADOOP_VERSION%"
set "SPARK_TARGET_FOLDER=%SCRIPT_DIR%bin\spark"
set "SPARK_EXTRA_LIB_FOLDER=%SCRIPT_DIR%bin"
set "DEPS_EXTRA_LIB_FOLDER=%SPARK_EXTRA_LIB_FOLDER%\deps"
set "STARLAKE_EXTRA_LIB_FOLDER=%SPARK_EXTRA_LIB_FOLDER%\sl"
if not defined SL_DATASETS (
    set "SL_SQL_WH=%SL_ROOT%/datasets"
) else (
    set "SL_SQL_WH=%SL_DATASETS%"
)

if not defined SPARK_DRIVER_MEMORY set "SPARK_DRIVER_MEMORY=4g"
set "SL_MAIN=ai.starlake.job.Main"
if not defined SPARK_MASTER_URL set "SPARK_MASTER_URL=local[*]"
if not defined SL_PYTHON_LIBS_DIR set "SL_PYTHON_LIBS_DIR=%SCRIPT_DIR%bin\deps\python-libs"

if defined SL_VERSION (
    set "SL_JAR_NAME=%SL_ARTIFACT_NAME%-%SL_VERSION%-assembly.jar"
)

set "PROXY="
if defined https_proxy (
    set "PROXY=%https_proxy%"
) else if defined http_proxy (
    set "PROXY=%http_proxy%"
)

set "JAVA_ARGS="

REM The JVM prefers IPv4 by default; on dual-stack networks where an IPv4 path is
REM broken this makes it pick a dead address that curl/python would avoid.
REM "system" follows the OS address ordering and behaves identically on IPv4-only
REM networks. Users can still override via SPARK_DRIVER_OPTIONS/JAVA_OPTS.
echo.%JAVA_OPTS% %SPARK_DRIVER_OPTIONS% | findstr /C:"java.net.preferIPv6Addresses" >nul
if errorlevel 1 set "JAVA_ARGS=-Djava.net.preferIPv6Addresses=system"

if defined HTTPS_PROXY (
    echo Using HTTPS_PROXY: %HTTPS_PROXY%
    call :parse_proxy_and_build_args "https" "%HTTPS_PROXY%"
    if defined proxy_args set "JAVA_ARGS=%JAVA_ARGS% %proxy_args%"
)

if defined HTTP_PROXY (
    echo Using HTTP_PROXY: %HTTP_PROXY%
    call :parse_proxy_and_build_args "http" "%HTTP_PROXY%"
    if defined proxy_args set "JAVA_ARGS=%JAVA_ARGS% %proxy_args%"
)

if defined SPARK_DRIVER_OPTIONS (
    set "SPARK_DRIVER_OPTIONS=%SPARK_DRIVER_OPTIONS% %JAVA_ARGS%"
) else (
    set "SPARK_DRIVER_OPTIONS=%JAVA_ARGS%"
)

set "JAVA_OPTS=%JAVA_OPTS% %JAVA_ARGS%"

goto :handle_command

:parse_proxy_and_build_args
    REM Args: %1=type (http/https), %2=url
    REM Sets proxy_args variable with -Dtype.proxyHost/Port/User/Password flags
    set "proxy_args="
    set "_ptype=%~1"
    set "_purl=%~2"

    set "_tmpps=%TEMP%\sl_proxy_%RANDOM%.ps1"
    > "%_tmpps%" (
        echo $url = '%_purl%'
        echo if ($url -notmatch '://') { Write-Error "No protocol in proxy URL, assuming http://"; $url = 'http://' + $url }
        echo try { $uri = [uri]$url } catch { Write-Error "Cannot parse proxy URL: $url"; exit 0 }
        echo if (-not $uri.Host) { Write-Error "Warning: Could not parse %_ptype% proxy URL: $url"; exit 0 }
        echo $r = '-D%_ptype%.proxyHost=' + $uri.Host
        echo if ($uri.Port -gt 0) { $r += ' -D%_ptype%.proxyPort=' + [string]$uri.Port }
        echo if ($uri.UserInfo) {
        echo     $p = $uri.UserInfo.Split(':', 2)
        echo     $r += ' -D%_ptype%.proxyUser=' + [uri]::UnescapeDataString($p[0])
        echo     if ($p.Length -gt 1) { $r += ' -D%_ptype%.proxyPassword=' + [uri]::UnescapeDataString($p[1]) }
        echo }
        echo Write-Output $r
    )
    for /f "usebackq delims=" %%a in (`powershell -NoProfile -File "%_tmpps%"`) do set "proxy_args=%%a"
    del "%_tmpps%" 2>nul
    goto :eof

:get_binary_from_url
    set "url=%~1"
    set "target=%~2"
    if defined PROXY (
        if defined SL_INSECURE (
            curl -L --insecure --proxy "%PROXY%" --progress-bar -o "%target%" "%url%"
        ) else (
            curl -L --proxy "%PROXY%" --progress-bar -o "%target%" "%url%"
        )
    ) else (
        curl -L --progress-bar -o "%target%" "%url%"
    )
    if errorlevel 1 (
        echo Error: Failed to retrieve data from %url%.
        exit /b 1
    )
    exit /b 0

:verify_sha256
    set "vs_file=%~1"
    set "vs_url=%~2"
    call :get_binary_from_url "%vs_url%" "%vs_file%.sha256"
    if errorlevel 1 exit /b 1
    powershell -Command "$expected = (Get-Content '%vs_file%.sha256' -Raw).Trim().Split(' ')[0].ToLower(); $actual = (Get-FileHash -Algorithm SHA256 '%vs_file%').Hash.ToLower(); if ($actual -ne $expected) { exit 1 }"
    if errorlevel 1 (
        echo Error: checksum verification failed for %vs_file%
        exit /b 1
    )
    echo Checksum OK for %vs_file%
    del "%vs_file%.sha256" 2>nul
    goto :eof

:launch_setup
    rem %1: optional git ref (tag, e.g. "v1.8.0") to fetch setup.jar from;
    rem defaults to "master" (the existing install/reinstall behavior,
    rem unchanged). Upgrades pass the target release tag so Setup.java's
    rem compiled-in version defaults match that exact release.
    set "_ls_ref=%~1"
    if "%_ls_ref%" == "" set "_ls_ref=master"
    set "setup_url=https://raw.githubusercontent.com/starlake-ai/starlake/%_ls_ref%/distrib/setup.jar"
    echo Downloading %setup_url% to %SCRIPT_DIR%setup.jar
    call :get_binary_from_url "%setup_url%" "%SCRIPT_DIR%setup.jar"
    if errorlevel 1 exit /b 1

    set "RUNNER="
    if defined JAVA_HOME (
        set "RUNNER=%JAVA_HOME%\bin\java.exe"
    ) else (
        for %%X in (java.exe) do (set RUNNER=%%~$PATH:X)
        if not defined RUNNER (
            echo JAVA_HOME is not set and java not in PATH
            exit /b 1
        )
    )
    "%RUNNER%" -cp "%SCRIPT_DIR%setup.jar" Setup "%SCRIPT_DIR:~0,-1%" "windows"

    if exist "%API_BIN_DIR%" (
        for %%f in ("%API_BIN_DIR%\local-*") do (
            rem In Windows, .bat/.cmd files are executable by default.
            echo Granting execute permission to %%f is not necessary on Windows.
        )
    )
goto :eof

:launch_starlake
    if not exist "%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%" (
        echo Starlake jar %SL_JAR_NAME% does not exist. Please install it.
        exit /b 1
    )

    if defined SL_LOG_LEVEL (
        if /i not "%SL_LOG_LEVEL%" == "error" (
            echo - JAVA_HOME=%JAVA_HOME%
            echo - SL_ROOT=%SL_ROOT%
        )
    )

    if defined SL_ENV (
        echo - SL_ENV=%SL_ENV%
    )

    if defined SL_DEBUG (
        set "SPARK_DRIVER_OPTIONS=%SPARK_DRIVER_OPTIONS% -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
    )

    set "SL_RUN_MODE="
    if /i "%1" == "import" set SL_RUN_MODE=main
    if /i "%1" == "xls2yml" set SL_RUN_MODE=main
    if /i "%1" == "yml2xls" set SL_RUN_MODE=main

    if /i "%SL_RUN_MODE%" == "main" (
        set "SL_ROOT=!SL_ROOT!"
        java --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Dlog4j.configurationFile="%SPARK_TARGET_FOLDER%/conf/log4j2.properties" -cp "%SPARK_TARGET_FOLDER%\jars\*;%DEPS_EXTRA_LIB_FOLDER%\*;%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%" %SL_MAIN% %*
    ) else (
        set "extra_classpath=%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%"
        set "extra_jars=%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%"
        set "SPARK_SUBMIT=%SPARK_TARGET_FOLDER%\bin\spark-submit.cmd"

        set "deps_jars="
        for %%f in ("%DEPS_EXTRA_LIB_FOLDER%\*.jar") do (
            if defined deps_jars (
                call set "deps_jars=%%deps_jars%%,%%f"
            ) else (
                set "deps_jars=%%f"
            )
        )

        if /i "%SPARK_MASTER_URL:~0,5%" == "local" (
            if defined deps_jars (
                set "extra_classpath=!extra_classpath!;!deps_jars:,=;!"
            )
            set "SPARK_LOCAL_HOSTNAME=127.0.0.1"
            set "SPARK_HOME=%SCRIPT_DIR%bin\spark"
            set "SL_ROOT=!SL_ROOT!"
            call "!SPARK_SUBMIT!" %SPARK_EXTRA_PACKAGES% --driver-java-options "!SPARK_DRIVER_OPTIONS!" %SPARK_CONF_OPTIONS% --driver-class-path "!extra_classpath!" --class "%SL_MAIN%" --master "%SPARK_MASTER_URL%" "%SPARK_TARGET_FOLDER%\README.md" %*
        ) else (
            if defined deps_jars (
                set "extra_classpath=!deps_jars:,=;!"
                set "extra_jars=!extra_jars!,!deps_jars!"
            )
            set "SPARK_HOME=%SCRIPT_DIR%bin\spark"
            set "SL_ROOT=!SL_ROOT!"
            call "!SPARK_SUBMIT!" %SPARK_EXTRA_PACKAGES% %SPARK_CONF_OPTIONS% --driver-java-options "!SPARK_DRIVER_OPTIONS!" --driver-class-path "!extra_classpath!" --class "%SL_MAIN%" --master "%SPARK_MASTER_URL%" --jars "!extra_jars!" "%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%" %*
        )
    )
goto :eof

:handle_command
    if /i "%1" == "--version" goto :version_command
    if /i "%1" == "version" goto :version_command
    if /i "%1" == "install" goto :install_command
    if /i "%1" == "reinstall" goto :install_command
    if /i "%1" == "serve" goto :serve_command
    if /i "%1" == "upgrade" goto :upgrade_command
    if /i "%1" == "_do_upgrade" goto :do_upgrade_command
    goto :default_command

:version_command
    echo Starlake %SL_VERSION%
    echo Duckdb JDBC driver %DUCKDB_VERSION%
    echo BigQuery Spark connector %SPARK_BQ_VERSION%
    echo Hadoop for Azure %HADOOP_AZURE_VERSION%
    echo Azure Storage %AZURE_STORAGE_VERSION%
    echo Spark %SPARK_VERSION%
    echo Hadoop %HADOOP_VERSION%
    echo Snowflake Spark connector %SPARK_SNOWFLAKE_VERSION%
    echo Snowflake JDBC driver %SNOWFLAKE_JDBC_VERSION%
    echo Postgres JDBC driver %POSTGRESQL_VERSION%
    echo AWS SDK %AWS_JAVA_SDK_V2_VERSION%
    echo Hadoop for AWS %HADOOP_AWS_VERSION%
    echo Redshift JDBC driver %REDSHIFT_JDBC_VERSION%
    echo Redshift Spark connector %SPARK_REDSHIFT_VERSION%
    goto :eof

:select_starlake_version
    rem Non-interactive override: `upgrade --version X.Y.Z` / SL_UPGRADE_VERSION,
    rem so scripted/CI upgrades don't have to drive the interactive prompt.
    if not "%~1" == "" (
        set "NEW_SL_VERSION=%~1"
        echo Selected version: %NEW_SL_VERSION% ^(forced^)
        goto :eof
    )

    echo Fetching available versions...

    set "temp_meta=%TEMP%\sl_releases_%RANDOM%.json"
    call :get_binary_from_url "https://api.github.com/repos/starlake-ai/starlake/releases?per_page=15" "%temp_meta%"
    if exist "%temp_meta%" (
         for /f "usebackq tokens=*" %%v in (`powershell -Command "$releases = Get-Content '%temp_meta%' -Raw | ConvertFrom-Json; $releases | ForEach-Object { $_.tag_name } | Where-Object { $_ -match '^v\d+\.\d+\.\d+$' } | ForEach-Object { $_.TrimStart('v') } | Sort-Object { [version]$_ } -Descending | Select-Object -First 5"`) do (
             set "LATEST_RELEASE_VERSIONS=!LATEST_RELEASE_VERSIONS! %%v"
             if not defined DEFAULT_VERSION set "DEFAULT_VERSION=%%v"
         )
         del "%temp_meta%"
    )

    if not defined DEFAULT_VERSION (
        echo Error: no releases found at https://github.com/starlake-ai/starlake/releases
        exit /b 1
    )

    set "VERSIONS=%LATEST_RELEASE_VERSIONS%"

    :ask_version
    echo Last 5 available versions:
    for %%v in (%VERSIONS%) do echo %%v

    set /p "NEW_SL_VERSION=Which version do you want to install? [%DEFAULT_VERSION%]: "
    if not defined NEW_SL_VERSION set "NEW_SL_VERSION=%DEFAULT_VERSION%"

    echo Selected version: %NEW_SL_VERSION%
    goto :eof

:upgrade_command
    REM Self-update: download latest starlake.cmd and re-launch, forwarding any
    REM extra args (e.g. --version X.Y.Z) through to _do_upgrade.
    echo Updating starlake script...
    call :get_binary_from_url "https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/starlake.cmd" "%SCRIPT_DIR%starlake.cmd.tmp"
    REM Ensure CRLF line endings
    powershell -Command "$c = [IO.File]::ReadAllText('%SCRIPT_DIR%starlake.cmd.tmp'); $c = $c -replace \"`r`n\",\"`n\" -replace \"`n\",\"`r`n\"; [IO.File]::WriteAllText('%SCRIPT_DIR%starlake.cmd.tmp', $c)"
    copy /y "%SCRIPT_DIR%starlake.cmd.tmp" "%SCRIPT_DIR%starlake.cmd" >nul
    del "%SCRIPT_DIR%starlake.cmd.tmp" 2>nul
    REM Re-launch with updated script, dropping %1 ("upgrade") from the args
    "%SCRIPT_DIR%starlake.cmd" _do_upgrade %2 %3 %4 %5 %6 %7 %8 %9
    goto :eof

:do_upgrade_command
    rem Non-interactive version selection: `upgrade --version X.Y.Z` or
    rem SL_UPGRADE_VERSION env var. Falls back to the interactive prompt when
    rem neither is set (unchanged default).
    set "FORCED_SL_VERSION=%SL_UPGRADE_VERSION%"
    rem NOTE: `shift` followed by `%~1` must NEVER sit inside the same
    rem parenthesized ( ... ) block - cmd.exe substitutes every %1..%9 in a
    rem block once, at parse time, before the block's first line runs, so a
    rem shift part-way through would not affect a later %~1 read in that same
    rem block (a classic batch pitfall). Each branch below is therefore a
    rem separate unparenthesized line/label, so shift takes effect before the
    rem next line's %~1 is read.
    :du_parse_args
    if "%~1" == "" goto :du_parse_done
    if /i "%~1" == "--version" goto :du_version_flag
    set "_du_arg=%~1"
    if /i "!_du_arg:~0,10!" == "--version=" goto :du_version_eq
    shift
    goto :du_parse_args

    :du_version_flag
    shift
    set "FORCED_SL_VERSION=%~1"
    shift
    goto :du_parse_args

    :du_version_eq
    set "FORCED_SL_VERSION=!_du_arg:~10!"
    shift
    goto :du_parse_args

    :du_parse_done

    call :select_starlake_version "%FORCED_SL_VERSION%"
    if defined NEW_SL_VERSION (
        echo Upgrading Starlake to %NEW_SL_VERSION%...
        set "TARGET_REF=v%NEW_SL_VERSION%"

        rem Setup.java at the target release tag is the single source of truth
        rem for that release's Spark/Hadoop/connector version pins - it is also
        rem what generates versions.cmd on a fresh install. Fetch just its
        rem SPARK_VERSION default to decide whether bin\spark needs replacing,
        rem without duplicating those pins here.
        set "TARGET_SETUP_JAVA=%SCRIPT_DIR%.target-setup-java.tmp"
        call :get_binary_from_url "https://raw.githubusercontent.com/starlake-ai/starlake/!TARGET_REF!/src/main/java/Setup.java" "!TARGET_SETUP_JAVA!"
        if errorlevel 1 exit /b 1
        rem The regex pattern is passed base64-encoded (UTF8) rather than typed
        rem literally: it contains parens and embedded double-quote
        rem characters that are not protected by PowerShell's single-quote
        rem string (cmd.exe does not recognize single quotes as quoting at
        rem all), which would otherwise unbalance cmd own double-quote
        rem parity for this line or be misread as block syntax. The whole
        rem -Command argument stays a single physical line with exactly one
        rem opening and one closing double-quote, matching the already-working
        rem verify_sha256 -Command usage above.
        rem decoded pattern is: getEnv followed by open-paren quote SPARK_VERSION
        rem quote close-paren dot orElse open-paren quote capture-group quote close-paren
        set "TARGET_SPARK_VERSION="
        for /f "usebackq delims=" %%v in (`powershell -NoProfile -Command "$m = Select-String -Path '!TARGET_SETUP_JAVA!' -Pattern ([System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('Z2V0RW52XCgiU1BBUktfVkVSU0lPTiJcKVwub3JFbHNlXCgiKFteIl0qKSJcKQ=='))) | Select-Object -First 1; if ($m) { $m.Matches[0].Groups[1].Value }"`) do set "TARGET_SPARK_VERSION=%%v"
        del "!TARGET_SETUP_JAVA!" 2>nul

        if not defined TARGET_SPARK_VERSION (
            echo Warning: could not determine the target Spark version for %NEW_SL_VERSION%; re-provisioning bin\spark unconditionally to be safe.
            if exist "%SCRIPT_DIR%bin\spark" rmdir /s /q "%SCRIPT_DIR%bin\spark"
        ) else (
            if exist "%SCRIPT_DIR%bin\spark\jars\spark-core_%SCALA_VERSION%-!TARGET_SPARK_VERSION!.jar" (
                echo Spark runtime already at !TARGET_SPARK_VERSION!, keeping bin\spark as-is.
            ) else (
                echo Spark runtime is changing ^(%SPARK_VERSION% -^> !TARGET_SPARK_VERSION!^): re-provisioning bin\spark.
                if exist "%SCRIPT_DIR%bin\spark" rmdir /s /q "%SCRIPT_DIR%bin\spark"
            )
        )

        rem bin\deps is always refreshed by launch_setup below: Setup.java
        rem deletes each dependency category by artefact-name match and
        rem re-downloads it at the target's pinned version, fixing stale
        rem connector jars even when Spark itself did not change. ENABLE_*
        rem choices from versions.cmd are already in this process's
        rem environment (set via `call versions.cmd` above), and Windows child
        rem processes inherit the full environment block automatically, so -
        rem unlike starlake.sh - no re-derivation from bin\deps is needed here.

        set "SL_VERSION=%NEW_SL_VERSION%"

        rem Re-provision via the real install machinery instead of a
        rem hand-rolled download of the core jar/API zip: Setup.java (fetched
        rem pinned to TARGET_REF) replaces bin\sl, bin\api and
        rem bin\deps\python-libs, only skips bin\spark when already present
        rem (handled above), and writes a brand new versions.cmd from scratch.
        call :launch_setup "!TARGET_REF!"

        echo Upgrade complete.
    )
    goto :eof

:install_command
    rem reinstall preserved SL_VERSION above (if any was pinned); fetch that
    rem exact release's setup.jar so its version defaults match, instead of
    rem master's (which may have moved on since this box was installed). A
    rem first `install` has no prior SL_VERSION, so it falls back to master.
    set "_is_ref="
    if /i "%1" == "reinstall" if defined SL_VERSION set "_is_ref=v%SL_VERSION%"
    if defined _is_ref (
        call :launch_setup "%_is_ref%"
    ) else (
        call :launch_setup
    )
    echo.
    echo Installation done. You're ready to enjoy Starlake!
    echo If any errors happen during installation. Please try to install again or open an issue.
    goto :eof

:serve_command
    if defined SL_API_DEBUG (
        set "JAVA_OPTS=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED %JAVA_OPTS% -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
    ) else (
        set "JAVA_OPTS=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED %JAVA_OPTS%"
    )
    call "%SCRIPT_DIR%bin\api\bin\local-run-api.bat" "%SCRIPT_DIR%" dummy
    goto :eof

:default_command
    if not defined SL_HTTP_PORT (
        call :launch_starlake %*
    ) else (
        if not defined SL_HTTP_HOST set "SL_HTTP_HOST=127.0.0.1"
        set "SL_SERVE_URI=http://%SL_HTTP_HOST%:%SL_HTTP_PORT%"
        for %%v in (validation run transform compile) do (
            set "log=!SL_ROOT!\out\%%v.log"
            if exist "!log!" del "!log!"
        )
        curl "%SL_SERVE_URI%?ROOT=!SL_ROOT!&PARAMS=%*"
        for %%v in (validation run transform compile) do (
            set "log=!SL_ROOT!\out\%%v.log"
            if exist "!log!" type "!log!"
        )
    )
    goto :eof


