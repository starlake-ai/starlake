@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "SL_SCRIPT_DIR=%SCRIPT_DIR%"
set "API_BIN_DIR=%SCRIPT_DIR%\bin\api\bin"

if not defined SL_ROOT (
    set "SL_ROOT=%cd%"
    set "SL_ROOT=!SL_ROOT:\=/!"
)

if /i "%1" == "reinstall" (
    if exist "%SCRIPT_DIR%versions.bat" del "%SCRIPT_DIR%versions.bat"
    if exist "%SCRIPT_DIR%bin\spark" rmdir /s /q "%SCRIPT_DIR%bin\spark"
) else (
    if exist "%SCRIPT_DIR%versions.bat" (
        call "%SCRIPT_DIR%versions.bat"
    )
)

set "SL_ARTIFACT_NAME=starlake-core_%SCALA_VERSION%"
set "SPARK_DIR_NAME=spark-%SPARK_VERSION%-bin-hadoop%HADOOP_VERSION%"
set "SPARK_TARGET_FOLDER=%SCRIPT_DIR%bin\spark"
set "SPARK_EXTRA_LIB_FOLDER=%SCRIPT_DIR%bin"
set "DEPS_EXTRA_LIB_FOLDER=%SPARK_EXTRA_LIB_FOLDER%\deps"
set "STARLAKE_EXTRA_LIB_FOLDER=%SPARK_EXTRA_LIB_FOLDER%\sl"

if not defined SPARK_DRIVER_MEMORY set "SPARK_DRIVER_MEMORY=4g"
set "SL_MAIN=ai.starlake.job.Main"
if not defined SPARK_MASTER_URL set "SPARK_MASTER_URL=local[*]"

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
if defined HTTPS_PROXY (
    echo Using HTTPS_PROXY: %HTTPS_PROXY%
    set "JAVA_ARGS=%JAVA_ARGS% -Dhttps.proxyHost=%HTTPS_PROXY_HOST% -Dhttps.proxyPort=%HTTPS_PROXY_PORT%"
    if defined HTTPS_PROXY_USER set "JAVA_ARGS=%JAVA_ARGS% -Dhttps.proxyUser=%HTTPS_PROXY_USER%"
    if defined HTTPS_PROXY_PASS set "JAVA_ARGS=%JAVA_ARGS% -Dhttps.proxyPassword=%HTTPS_PROXY_PASS%"
)
if defined HTTP_PROXY (
    echo Using HTTP_PROXY: %HTTP_PROXY%
    set "JAVA_ARGS=%JAVA_ARGS% -Dhttp.proxyHost=%HTTP_PROXY_HOST% -Dhttp.proxyPort=%HTTP_PROXY_PORT%"
    if defined HTTP_PROXY_USER set "JAVA_ARGS=%JAVA_ARGS% -Dhttp.proxyUser=%HTTP_PROXY_USER%"
    if defined HTTP_PROXY_PASS set "JAVA_ARGS=%JAVA_ARGS% -Dhttp.proxyPassword=%HTTP_PROXY_PASS%"
)

if defined SPARK_DRIVER_OPTIONS (
    set "SPARK_DRIVER_OPTIONS=%SPARK_DRIVER_OPTIONS% %JAVA_ARGS%"
) else (
    set "SPARK_DRIVER_OPTIONS=%JAVA_ARGS%"
)

set "JAVA_OPTS=%JAVA_OPTS% %JAVA_ARGS%"

goto :handle_command

:get_binary_from_url
    set "url=%~1"
    set "target=%~2"
    if defined PROXY (
        if defined SL_INSECURE (
            curl --insecure --proxy "%PROXY%" -s -o "%target%" "%url%"
        ) else (
            curl --proxy "%PROXY%" -s -o "%target%" "%url%"
        )
    ) else (
        curl -s -o "%target%" "%url%"
    )
    if errorlevel 1 (
        echo Error: Failed to retrieve data from %url%.
        exit /b 1
    )
    exit /b 0

:launch_setup
    set "setup_url=https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/setup.jar"
    echo Downloading %setup_url% to %SCRIPT_DIR%setup.jar
    call :get_binary_from_url "%setup_url%" "%SCRIPT_DIR%setup.jar"
    if errorlevel 1 exit /b 1

    set "RUNNER="
    if defined JAVA_HOME (
        set "RUNNER=%JAVA_HOME%\bin\java.exe"
    ) else (
        for %%X in (java.exe) do (set RUNNER=%%~dp$PATH:X)
        if not defined RUNNER (
            echo JAVA_HOME is not set and java not in PATH
            exit /b 1
        )
    )
    "%RUNNER%" -cp "%SCRIPT_DIR%setup.jar" Setup "%SCRIPT_DIR%" "windows"

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
                set "extra_classpath=%extra_classpath%;%deps_jars:,=;%"
            )
            set "SPARK_LOCAL_HOSTNAME=127.0.0.1"
            set "SPARK_HOME=%SCRIPT_DIR%bin\spark"
            set "SL_ROOT=!SL_ROOT!"
            call "%SPARK_SUBMIT%" %SPARK_EXTRA_PACKAGES% --driver-java-options "%SPARK_DRIVER_OPTIONS%" %SPARK_CONF_OPTIONS% --driver-class-path "%extra_classpath%" --class "%SL_MAIN%" --master "%SPARK_MASTER_URL%" "%SPARK_TARGET_FOLDER%\README.md" %*
        ) else (
            if defined deps_jars (
                set "extra_classpath=%deps_jars:,=;%"
                set "extra_jars=%extra_jars%,%deps_jars%"
            )
            set "SPARK_HOME=%SCRIPT_DIR%bin\spark"
            set "SL_ROOT=!SL_ROOT!"
            call "%SPARK_SUBMIT%" %SPARK_EXTRA_PACKAGES% %SPARK_CONF_OPTIONS% --driver-java-options "%SPARK_DRIVER_OPTIONS%" --driver-class-path "%extra_classpath%" --class "%SL_MAIN%" --master "%SPARK_MASTER_URL%" --jars "%extra_jars%" "%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%" %*
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
    echo AWS SDK %AWS_JAVA_SDK_VERSION%
    echo Hadoop for AWS %HADOOP_AWS_VERSION%
    echo Redshift JDBC driver %REDSHIFT_JDBC_VERSION%
    echo Redshift Spark connector %SPARK_REDSHIFT_VERSION%
    goto :eof

:select_starlake_version
    echo Fetching available versions...
    
    set "temp_meta=%TEMP%\sl_metadata_%RANDOM%.xml"
    call :get_binary_from_url "https://central.sonatype.com/repository/maven-snapshots/ai/starlake/starlake-core_%SCALA_VERSION%/maven-metadata.xml" "%temp_meta%"
    if exist "%temp_meta%" (
         for /f "usebackq tokens=*" %%v in (`powershell -Command "[xml]$xml = Get-Content '%temp_meta%'; $xml.metadata.versioning.versions.version | Where-Object { $_ -match 'SNAPSHOT' } | Sort-Object -Descending | Select-Object -First 1"`) do set "SNAPSHOT_VERSION=%%v"
         del "%temp_meta%"
    )

    set "temp_meta=%TEMP%\sl_metadata_%RANDOM%.xml"
    call :get_binary_from_url "https://repo1.maven.org/maven2/ai/starlake/starlake-core_%SCALA_VERSION%/maven-metadata.xml" "%temp_meta%"
    if exist "%temp_meta%" (
         for /f "usebackq tokens=*" %%v in (`powershell -Command "[xml]$xml = Get-Content '%temp_meta%'; $xml.metadata.versioning.versions.version | Where-Object { $_ -match '^\d+\.\d+\.\d+$' } | Sort-Object -Descending | Select-Object -First 5"`) do (
             set "LATEST_RELEASE_VERSIONS=!LATEST_RELEASE_VERSIONS! %%v"
         )
         del "%temp_meta%"
    )

    set "VERSIONS=%SNAPSHOT_VERSION% %LATEST_RELEASE_VERSIONS%"
    
    :ask_version
    echo Last 5 available versions:
    for %%v in (%VERSIONS%) do echo %%v
    
    set "DEFAULT_VERSION=%SNAPSHOT_VERSION%"
    set /p "NEW_SL_VERSION=Which version do you want to install? [%DEFAULT_VERSION%]: "
    if not defined NEW_SL_VERSION set "NEW_SL_VERSION=%DEFAULT_VERSION%"
    
    echo Selected version: %NEW_SL_VERSION%
    goto :eof

:upgrade_command
    call :select_starlake_version
    if defined NEW_SL_VERSION (
        if exist "%SCRIPT_DIR%versions.bat" (
             powershell -Command "(Get-Content '%SCRIPT_DIR%versions.bat') -replace 'SL_VERSION=.*', 'SL_VERSION=%NEW_SL_VERSION%' | Set-Content '%SCRIPT_DIR%versions.bat'"
             echo Updated versions.bat with SL_VERSION=%NEW_SL_VERSION%
        )
        set "SL_VERSION=%NEW_SL_VERSION%"
        echo Upgrading Starlake to %NEW_SL_VERSION%...

        echo %NEW_SL_VERSION% | findstr /C:"SNAPSHOT" >nul
        if not errorlevel 1 (
             set "BASE_URL=https://central.sonatype.com/repository/maven-snapshots/ai/starlake"
        ) else (
             set "BASE_URL=https://repo1.maven.org/maven2/ai/starlake"
        )

        set "SL_LIB_DIR=%STARLAKE_EXTRA_LIB_FOLDER%"
        set "API_LIB_DIR=%SCRIPT_DIR%bin\api\lib"

        if not exist "%SL_LIB_DIR%" mkdir "%SL_LIB_DIR%"
        if not exist "%API_LIB_DIR%" mkdir "%API_LIB_DIR%"

        set "CORE_ASSEMBLY_NAME=starlake-core_%SCALA_VERSION%-%NEW_SL_VERSION%-assembly.jar"
        set "CORE_ASSEMBLY_URL=!BASE_URL!/starlake-core_%SCALA_VERSION%/%NEW_SL_VERSION%/!CORE_ASSEMBLY_NAME!"

        set "CORE_JAR_NAME=starlake-core_%SCALA_VERSION%-%NEW_SL_VERSION%.jar"
        set "CORE_JAR_URL=!BASE_URL!/starlake-core_%SCALA_VERSION%/%NEW_SL_VERSION%/!CORE_JAR_NAME!"

        set "API_JAR_NAME=starlake-api_%SCALA_VERSION%-%NEW_SL_VERSION%.jar"
        set "API_JAR_URL=!BASE_URL!/starlake-api_%SCALA_VERSION%/%NEW_SL_VERSION%/!API_JAR_NAME!"

        REM Delete old files
        del /q "%API_LIB_DIR%\ai.starlake.starlake-api-*.jar" 2>nul
        del /q "%API_LIB_DIR%\starlake-api_*.jar" 2>nul
        del /q "%API_LIB_DIR%\starlake-core_*.jar" 2>nul
        del /q "%SL_LIB_DIR%\starlake-core_*-assembly.jar" 2>nul

        REM Download new files
        echo Downloading !CORE_ASSEMBLY_NAME! to !SL_LIB_DIR!...
        call :get_binary_from_url "!CORE_ASSEMBLY_URL!" "!SL_LIB_DIR!\!CORE_ASSEMBLY_NAME!"
        
        echo Downloading !CORE_JAR_NAME! to !API_LIB_DIR!...
        call :get_binary_from_url "!CORE_JAR_URL!" "!API_LIB_DIR!\!CORE_JAR_NAME!"
        
        echo Downloading !API_JAR_NAME! to !API_LIB_DIR!...
        call :get_binary_from_url "!API_JAR_URL!" "!API_LIB_DIR!\ai.starlake.!API_JAR_NAME!"

        echo Upgrade complete.
    )
    goto :eof

:install_command
    call :launch_setup
    echo.
    echo Installation done. You're ready to enjoy Starlake!
    echo If any errors happen during installation. Please try to install again or open an issue.
    goto :eof

:serve_command
    if defined SL_API_DEBUG (
        set "JAVA_OPTS=%JAVA_OPTS% -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
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
