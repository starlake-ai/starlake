@echo off

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
if exist "%SCRIPT_DIR%versions.cmd" (
    call "%SCRIPT_DIR%versions.cmd"
)

:: default versions
set "SPARK_DEFAULT_VERSION=3.4.1"
set "HADOOP_DEFAULT_VERSION=3.2.2"
set "SPARK_BQ_DEFAULT_VERSION=0.32.2"
set "HADOOP_AZURE_DEFAULT_VERSION=3.3.5"
set "AZURE_STORAGE_DEFAULT_VERSION=8.6.6"
set "JETTY_DEFAULT_VERSION=9.4.51.v20230217"
set "SPARK_SNOWFLAKE_DEFAULT_VERSION=3.4"
set "SNOWFLAKE_JDBC_DEFAULT_VERSION=3.14.0"

:: Common
if "%SPARK_VERSION%"=="" set "SPARK_VERSION=%SPARK_DEFAULT_VERSION%"
if "%HADOOP_VERSION%"=="" set "HADOOP_VERSION=%HADOOP_DEFAULT_VERSION%"
for /f %%i in ('powershell -Command "[System.Version]::new('%HADOOP_VERSION%').Major"') do set "HADOOP_MAJOR_VERSION=%%i"
if "%HADOOP_HOME%"=="" set "HADOOP_HOME=%SCRIPT_DIR%bin\hadoop"

:: GCP
if "%DOWNLOAD_GCP_DEPS%"=="" set "DOWNLOAD_GCP_DEPS=1"
if "%SPARK_BQ_VERSION%"=="" set "SPARK_BQ_VERSION=%SPARK_BQ_DEFAULT_VERSION%"

:: AZURE
if "%DOWNLOAD_AZURE_DEPS%"=="" set "DOWNLOAD_AZURE_DEPS=1"
if "%HADOOP_AZURE_VERSION%"=="" set "HADOOP_AZURE_VERSION=%HADOOP_AZURE_DEFAULT_VERSION%"
if "%AZURE_STORAGE_VERSION%"=="" set "AZURE_STORAGE_VERSION=%AZURE_STORAGE_DEFAULT_VERSION%"
if "%JETTY_VERSION%"=="" set "JETTY_VERSION=%JETTY_DEFAULT_VERSION%"
if "%JETTY_UTIL_VERSION%"=="" set "JETTY_UTIL_VERSION=%JETTY_VERSION%"
if "%JETTY_UTIL_AJAX_VERSION%"=="" set "JETTY_UTIL_AJAX_VERSION=%JETTY_VERSION%"

:: SNOWFLAKE
if "%DOWNLOAD_SNOWFLAKE_DEPS%"=="" set "DOWNLOAD_SNOWFLAKE_DEPS=1"
if "%SPARK_SNOWFLAKE_VERSION%"=="" set "SPARK_SNOWFLAKE_VERSION=%SPARK_SNOWFLAKE_DEFAULT_VERSION%"
if "%SNOWFLAKE_JDBC_VERSION%"=="" set "SNOWFLAKE_JDBC_VERSION=%SNOWFLAKE_JDBC_DEFAULT_VERSION%"

:: Internal variables
if "%SL_ROOT%"=="" set "SL_ROOT=!cd!"
set "SCALA_VERSION=2.12"
set "SKIP_INSTALL=1"
set "SL_ARTIFACT_NAME=starlake-spark3_!SCALA_VERSION!"
set "SPARK_DIR_NAME=spark-%SPARK_VERSION%-bin-hadoop%HADOOP_MAJOR_VERSION%"
set "SPARK_TARGET_FOLDER=!SCRIPT_DIR!bin\spark"
if "%SPARK_DRIVER_MEMORY%"=="" set "SPARK_DRIVER_MEMORY=4G"
set "SL_MAIN=ai.starlake.job.Main"
set "SL_VALIDATE_ON_LOAD=false"
if not "%SL_VERSION%"=="" SET SL_JAR_NAME=%SL_ARTIFACT_NAME%-%SL_VERSION%-assembly.jar

set "SPARK_TGZ_NAME=%SPARK_DIR_NAME%.tgz"
set "SPARK_TGZ_URL=https://archive.apache.org/dist/spark/spark-%SPARK_VERSION%/%SPARK_TGZ_NAME%"
set "HADOOP_DLL_URL=https://github.com/cdarlint/winutils/raw/master/hadoop-%HADOOP_VERSION%/bin/hadoop.dll"
set "WINUTILS_EXE_URL=https://github.com/cdarlint/winutils/raw/master/hadoop-%HADOOP_VERSION%/bin/winutils.exe"

:: GCP
set "SPARK_BQ_ARTIFACT_NAME=spark-bigquery-with-dependencies_%SCALA_VERSION%"
set "SPARK_BQ_JAR_NAME=%SPARK_BQ_ARTIFACT_NAME%-%SPARK_BQ_VERSION%.jar"
set "SPARK_BQ_URL=https://repo1.maven.org/maven2/com/google/cloud/spark/%SPARK_BQ_ARTIFACT_NAME%/%SPARK_BQ_VERSION%/%SPARK_BQ_JAR_NAME%"

:: AZURE
set "HADOOP_AZURE_ARTIFACT_NAME=hadoop-azure"
set "HADOOP_AZURE_JAR_NAME=%HADOOP_AZURE_ARTIFACT_NAME%-%HADOOP_AZURE_VERSION%.jar"
set "HADOOP_AZURE_URL=https://repo1.maven.org/maven2/org/apache/hadoop/%HADOOP_AZURE_ARTIFACT_NAME%/%HADOOP_AZURE_VERSION%/%HADOOP_AZURE_JAR_NAME%"

set "AZURE_STORAGE_ARTIFACT_NAME=azure-storage"
set "AZURE_STORAGE_JAR_NAME=%AZURE_STORAGE_ARTIFACT_NAME%-%AZURE_STORAGE_VERSION%.jar"
set "AZURE_STORAGE_URL=https://repo1.maven.org/maven2/com/microsoft/azure/%AZURE_STORAGE_ARTIFACT_NAME%/%AZURE_STORAGE_VERSION%/%AZURE_STORAGE_JAR_NAME%"

set "JETTY_UTIL_ARTIFACT_NAME=jetty-util"
set "JETTY_UTIL_JAR_NAME=%JETTY_UTIL_ARTIFACT_NAME%-%JETTY_UTIL_VERSION%.jar"
set "JETTY_UTIL_URL=https://repo1.maven.org/maven2/org/eclipse/jetty/%JETTY_UTIL_ARTIFACT_NAME%/%JETTY_UTIL_VERSION%/%JETTY_UTIL_JAR_NAME%"

set "JETTY_UTIL_AJAX_ARTIFACT_NAME=jetty-util-ajax"
set "JETTY_UTIL_AJAX_JAR_NAME=%JETTY_UTIL_AJAX_ARTIFACT_NAME%-%JETTY_UTIL_AJAX_VERSION%.jar"
set "JETTY_UTIL_AJAX_URL=https://repo1.maven.org/maven2/org/eclipse/jetty/%JETTY_UTIL_AJAX_ARTIFACT_NAME%/%JETTY_UTIL_AJAX_VERSION%/%JETTY_UTIL_AJAX_JAR_NAME%"

:: SNOWFLAKE
set "SPARK_SNOWFLAKE_ARTIFACT_NAME=spark-snowflake_%SCALA_VERSION%"
set "SPARK_SNOWFLAKE_FULL_VERSION=%SCALA_VERSION%.0-spark_%SPARK_SNOWFLAKE_VERSION%"
set "SPARK_SNOWFLAKE_JAR_NAME=%SPARK_SNOWFLAKE_ARTIFACT_NAME%-%SPARK_SNOWFLAKE_FULL_VERSION%.jar"
set "SPARK_SNOWFLAKE_URL=https://repo1.maven.org/maven2/net/snowflake/%SPARK_SNOWFLAKE_ARTIFACT_NAME%/%SPARK_SNOWFLAKE_FULL_VERSION%/%SPARK_SNOWFLAKE_JAR_NAME%"

set "SNOWFLAKE_JDBC_ARTIFACT_NAME=snowflake-jdbc"
set "SNOWFLAKE_JDBC_JAR_NAME=%SNOWFLAKE_JDBC_ARTIFACT_NAME%-%SNOWFLAKE_JDBC_VERSION%.jar"
set "SNOWFLAKE_JDBC_URL=https://repo1.maven.org/maven2/net/snowflake/%SNOWFLAKE_JDBC_ARTIFACT_NAME%/%SNOWFLAKE_JDBC_VERSION%/%SNOWFLAKE_JDBC_JAR_NAME%"

set "SKIP_INSTALL=0"

if "%~1"=="install" (
    call :checkCurrentState
    call :clean_additional_jars
    if !SKIP_INSTALL! equ 1 (
        call :initEnv
    )
    call :save_installed_versions
    echo. 
    echo Installation done. If any errors happen during installation. Please try to install again or open an issue.
) else (
    call :launch_starlake %*
)

goto :eof

:initStarlakeInstallVariables
if "%SL_VERSION%"=="" (
@REM     for /f %%v in ('powershell -command "& { (Invoke-RestMethod -Uri 'https://search.maven.org/solrsearch/select?q=g:ai.starlake+AND+a:%SL_ARTIFACT_NAME%&core=gav&start=0&rows=42&wt=json').response.docs} | ForEach-Object { $_.v } | %%{[System.Version]$_} | sort | Select-Object -Last 1 | %%{$_.ToString()}"') do (
    for /f %%v in ('powershell -command "& { (Invoke-RestMethod -Uri 'https://search.maven.org/solrsearch/select?q=g:ai.starlake+AND+a:%SL_ARTIFACT_NAME%&core=gav&start=0&rows=42&wt=json').response.docs.v | sort {[version] $_} -Descending | Select-Object -First 1}"') do (
        set "SL_VERSION=%%v"
    )
)

set "SL_JAR_NAME=!SL_ARTIFACT_NAME!-!SL_VERSION!-assembly.jar"
if "!SL_VERSION:SNAPSHOT=!"=="!SL_VERSION!" (
    set "SL_JAR_URL=https://s01.oss.sonatype.org/content/repositories/releases/ai/starlake/starlake-spark3_!SCALA_VERSION!/!SL_VERSION!/!SL_JAR_NAME!"
) else (
    set "SL_JAR_URL=https://s01.oss.sonatype.org/content/repositories/snapshots/ai/starlake/starlake-spark3_!SCALA_VERSION!/!SL_VERSION!/!SL_JAR_NAME!"
)
goto :eof

:checkCurrentState
echo -------------------
echo   Current state
echo -------------------
echo.

set "SPARK_DOWNLOADED=1"
set "SL_DOWNLOADED=1"
set "HADOOP_DOWNLOADED=1"
set "HADOOP_DLL_DOWNLOADED=1"
set "HADOOP_WINUTILS_DOWNLOADED=1"
set "SPARK_BQ_DOWNLOADED=1"
set "HADOOP_AZURE_DOWNLOADED=1"
set "AZURE_STORAGE_DOWNLOADED=1"
set "JETTY_UTIL_DOWNLOADED=1"
set "JETTY_UTIL_AJAX_DOWNLOADED=1"
set "SPARK_SNOWFLAKE_DOWNLOADED=1"
set "SNOWFLAKE_JDBC_DOWNLOADED=1"

if exist "%SPARK_TARGET_FOLDER%\jars" (
    rem Initialize variables
    set "SL_JAR_COUNT=0"
    set "SL_INSTALLED_VERSION="

    rem Count files matching the pattern
    for %%f in ("%SPARK_TARGET_FOLDER%\jars\%SL_ARTIFACT_NAME%*") do (
        set /a "SL_JAR_COUNT+=1"
        set "SL_INSTALLED_JAR_NAME=%%~nf"
        set "SL_INSTALLED_VERSION=!SL_INSTALLED_JAR_NAME!"
        set "SL_INSTALLED_VERSION=!SL_INSTALLED_VERSION:%SL_ARTIFACT_NAME%-=!"
        set "SL_INSTALLED_VERSION=!SL_INSTALLED_VERSION:-assembly=!"
    )

    rem Check if only one file was found
    if !SL_JAR_COUNT! equ 1 (
        rem Check if SL_VERSION is set and matches SL_INSTALLED_VERSION
        if defined SL_VERSION (
            if "!SL_INSTALLED_VERSION!" equ "!SL_VERSION!" (
                echo - starlake: OK
                set "SL_DOWNLOADED=0"
            ) else (
                call :initStarlakeInstallVariables
                echo - starlake: KO
                set "SKIP_INSTALL=1"
            )
        ) else (
            call :initStarlakeInstallVariables
            echo - starlake: KO
            set "SKIP_INSTALL=1"
        )
    ) else (
        call :initStarlakeInstallVariables
        echo - starlake: KO
        set "SKIP_INSTALL=1"
    )
) else (
    call :initStarlakeInstallVariables
    echo - starlake: KO
    set "SKIP_INSTALL=1"
)

if exist "%HADOOP_HOME%" (
    ECHO - hadoop: OK
    SET "HADOOP_DOWNLOADED=0"

    if exist "%HADOOP_HOME%\bin\hadoop.dll" (
        ECHO - hadoop dll: OK
        SET "HADOOP_DLL_DOWNLOADED=0"
    ) else (
        set "SKIP_INSTALL=1"
        ECHO - hadoop dll: KO
    )

    if exist "%HADOOP_HOME%\bin\winutils.exe" (
        ECHO - hadoop winutils: OK
        SET "HADOOP_WINUTILS_DOWNLOADED=0"
    ) else (
        SET "SKIP_INSTALL=1"
        ECHO - hadoop winutils: KO
    )
) else (
    set "SKIP_INSTALL=1"
    ECHO - hadoop: KO
    ECHO - hadoop dll: KO
    ECHO - hadoop winutils: KO
)

if exist "%SPARK_TARGET_FOLDER%\bin" (
    echo - spark: OK
    set "SPARK_DOWNLOADED=0"

    if %DOWNLOAD_GCP_DEPS% equ 0 (
        if exist "%SPARK_TARGET_FOLDER%\jars\%SPARK_BQ_JAR_NAME%" (
            echo - spark bq: OK
            set "SPARK_BQ_DOWNLOADED=0"
        ) else (
            echo - spark bq: KO
            set "SKIP_INSTALL=1"
        )
    ) else (
        set "SPARK_BQ_DOWNLOADED=0"
        echo - spark bq: skipped
    )

    if %DOWNLOAD_AZURE_DEPS% equ 0 (
        if exist "%SPARK_TARGET_FOLDER%\jars\%HADOOP_AZURE_JAR_NAME%" (
            echo - hadoop azure: OK
            set "HADOOP_AZURE_DOWNLOADED=0"
        ) else (
            echo - hadoop azure: KO
            set "SKIP_INSTALL=1"
        )

        if exist "%SPARK_TARGET_FOLDER%\jars\%AZURE_STORAGE_JAR_NAME%" (
            echo - azure storage: OK
            set "AZURE_STORAGE_DOWNLOADED=0"
        ) else (
            echo - azure storage: KO
            set "SKIP_INSTALL=1"
        )

        if exist "%SPARK_TARGET_FOLDER%\jars\%JETTY_UTIL_JAR_NAME%" (
            echo - jetty util: OK
            set "JETTY_UTIL_DOWNLOADED=0"
        ) else (
            echo - jetty util: KO
            set "SKIP_INSTALL=1"
        )

        if exist "%SPARK_TARGET_FOLDER%\jars\%JETTY_UTIL_AJAX_JAR_NAME%" (
            echo - jetty util ajax: OK
            set "JETTY_UTIL_AJAX_DOWNLOADED=0"
        ) else (
            echo - jetty util ajax: KO
            set "SKIP_INSTALL=1"
        )
    ) else (
        set "HADOOP_AZURE_DOWNLOADED=0"
        set "AZURE_STORAGE_DOWNLOADED=0"
        set "JETTY_UTIL_DOWNLOADED=0"
        set "JETTY_UTIL_AJAX_DOWNLOADED=0"
        echo - hadoop azure: skipped
        echo - azure storage: skipped
        echo - jetty util: skipped
        echo - jetty util ajax: skipped
    )

    if %DOWNLOAD_SNOWFLAKE_DEPS% equ 0 (
        if exist "%SPARK_TARGET_FOLDER%\jars\%SPARK_SNOWFLAKE_JAR_NAME%" (
            echo - spark snowflake: OK
            set "SPARK_SNOWFLAKE_DOWNLOADED=0"
        ) else (
            echo - spark snowflake: KO
            set "SKIP_INSTALL=1"
        )

        if exist "%SPARK_TARGET_FOLDER%\jars\%SNOWFLAKE_JDBC_JAR_NAME%" (
            echo - snowflake jdbc: OK
            set "SNOWFLAKE_JDBC_DOWNLOADED=0"
        ) else (
            echo - snowflake jdbc: KO
            set "SKIP_INSTALL=1"
        )
    ) else (
        set "SPARK_SNOWFLAKE_DOWNLOADED=0"
        set "SNOWFLAKE_JDBC_DOWNLOADED=0"
        echo - spark snowflake: skipped
        echo - snowflake jdbc: skipped
    )
) else (
    set "SKIP_INSTALL=1"
    echo - spark: KO

    if %DOWNLOAD_GCP_DEPS% equ 0 (
        echo - spark bq: KO
    ) else (
        set "SPARK_BQ_DOWNLOADED=0"
        echo - spark bq: skipped
    )

    if %DOWNLOAD_AZURE_DEPS% equ 0 (
        echo - hadoop azure: KO
        echo - azure storage: KO
        echo - jetty util: KO
        echo - jetty util ajax: KO
    ) else (
        set "HADOOP_AZURE_DOWNLOADED=0"
        set "AZURE_STORAGE_DOWNLOADED=0"
        set "JETTY_UTIL_DOWNLOADED=0"
        set "JETTY_UTIL_AJAX_DOWNLOADED=0"
        echo - hadoop azure: skipped
        echo - azure storage: skipped
        echo - jetty util: skipped
        echo - jetty util ajax: skipped
    )

    if %DOWNLOAD_SNOWFLAKE_DEPS% equ 0 (
        echo - spark snowflake: KO
        echo - snowflake jdbc: KO
    ) else (
        set "SPARK_SNOWFLAKE_DOWNLOADED=0"
        set "SNOWFLAKE_JDBC_DOWNLOADED=0"
        echo - spark snowflake: skipped
        echo - snowflake jdbc: skipped
    )
)
goto :eof

:clean_additional_jars
if exist "%SPARK_TARGET_FOLDER%\jars" (
    if "%SL_DOWNLOADED%"=="1" (
        del /q "%SPARK_TARGET_FOLDER%\jars\%SL_ARTIFACT_NAME%*" 2> nul
    )

    if "%DOWNLOAD_GCP_DEPS%"=="1" (
        del /q "%SPARK_TARGET_FOLDER%\jars\%SPARK_BQ_ARTIFACT_NAME%*" 2> nul
    )

    if "%SPARK_BQ_DOWNLOADED%"=="1" (
        del /q "%SPARK_TARGET_FOLDER%\jars\%SPARK_BQ_ARTIFACT_NAME%*" 2> nul
    )

    if "%DOWNLOAD_AZURE_DEPS%"=="1" (
        del /q "%SPARK_TARGET_FOLDER%\jars\%HADOOP_AZURE_ARTIFACT_NAME%*" 2> nul
        del /q "%SPARK_TARGET_FOLDER%\jars\%AZURE_STORAGE_ARTIFACT_NAME%*" 2> nul
        del /q "%SPARK_TARGET_FOLDER%\jars\%JETTY_UTIL_ARTIFACT_NAME%*" 2> nul
        del /q "%SPARK_TARGET_FOLDER%\jars\%JETTY_UTIL_AJAX_ARTIFACT_NAME%*" 2> nul
    )

    if "%HADOOP_AZURE_DOWNLOADED%"=="1" (
        del /q "%SPARK_TARGET_FOLDER%\jars\%HADOOP_AZURE_ARTIFACT_NAME%*" 2> nul
    )

    if "%AZURE_STORAGE_DOWNLOADED%"=="1" (
        del /q "%SPARK_TARGET_FOLDER%\jars\%AZURE_STORAGE_ARTIFACT_NAME%*" 2> nul
    )

    if "%JETTY_UTIL_DOWNLOADED%"=="1" (
        del /q "%SPARK_TARGET_FOLDER%\jars\%JETTY_UTIL_ARTIFACT_NAME%*" 2> nul
        
        if "%DOWNLOAD_AZURE_DEPS%"=="0" (
            if "%JETTY_UTIL_AJAX_DOWNLOADED%"=="0" (
                echo force jetty util ajax download
                set "JETTY_UTIL_AJAX_DOWNLOADED=1"
            )
        )
    )

    if "%JETTY_UTIL_AJAX_DOWNLOADED%"=="1" (
        del /q "%SPARK_TARGET_FOLDER%\jars\%JETTY_UTIL_AJAX_ARTIFACT_NAME%*" 2> nul
    )

    if "%DOWNLOAD_SNOWFLAKE_DEPS%"=="1" (
        del /q "%SPARK_TARGET_FOLDER%\jars\%SPARK_SNOWFLAKE_ARTIFACT_NAME%*" 2> nul
        del /q "%SPARK_TARGET_FOLDER%\jars\%SNOWFLAKE_JDBC_ARTIFACT_NAME%*" 2> nul
    )

    if "%SPARK_SNOWFLAKE_DOWNLOADED%"=="1" (
        del /q "%SPARK_TARGET_FOLDER%\jars\%SPARK_SNOWFLAKE_ARTIFACT_NAME%*" 2> nul
    )

    if "%SNOWFLAKE_JDBC_DOWNLOADED%"=="1" (
        del /q "%SPARK_TARGET_FOLDER%\jars\%SNOWFLAKE_JDBC_ARTIFACT_NAME%*" 2> nul
    )
)
goto :eof

:initEnv
echo.
echo -------------------
echo      Install
echo -------------------
echo.

if !HADOOP_DOWNLOADED! equ 1 (
    if not exist "%HADOOP_HOME%\bin" mkdir "%HADOOP_HOME%\bin"
    echo - hadoop: OK
) else (
    echo - hadoop: skipped
)

if !HADOOP_DLL_DOWNLOADED! equ 1 (
    echo - hadoop dll: downloading from %HADOOP_DLL_URL%
    powershell -command "Start-BitsTransfer -Source %HADOOP_DLL_URL% -Destination %HADOOP_HOME%\bin\hadoop.dll"
    echo - hadoop dll: OK
) else (
    echo - hadoop dll: skipped
)

if !HADOOP_WINUTILS_DOWNLOADED! equ 1 (
    echo - hadoop winutils: downloading from %WINUTILS_EXE_URL%
    powershell -command "Start-BitsTransfer -Source %WINUTILS_EXE_URL% -Destination %HADOOP_HOME%\bin\winutils.exe"
    echo - hadoop winutils: OK
) else (
    echo - hadoop winutils: skipped
)

if !SPARK_DOWNLOADED! equ 1 (
    if not exist "%SCRIPT_DIR%bin" mkdir "%SCRIPT_DIR%bin"
    echo - spark: downloading from %SPARK_TGZ_URL%
    powershell -command "Start-BitsTransfer -Source %SPARK_TGZ_URL% -Destination %SCRIPT_DIR%bin\%SPARK_TGZ_NAME%"
    @REM windows tar doesn't support quoting directory with -C arguments, that is why we jump into it before unarchive
    pushd .
    cd "%SCRIPT_DIR%bin\"
    tar zxf "%SCRIPT_DIR%bin\%SPARK_TGZ_NAME%" -C .
    popd
    del /q "%SCRIPT_DIR%bin\%SPARK_TGZ_NAME%"
    if not exist "%SPARK_TARGET_FOLDER%" mkdir "%SPARK_TARGET_FOLDER%"
    xcopy "%SCRIPT_DIR%bin\%SPARK_DIR_NAME%\*" "%SPARK_TARGET_FOLDER%\" /E /H /C /Y /Q > nul
    rmdir /s /q "%SCRIPT_DIR%bin\%SPARK_DIR_NAME%"
    del /q "%SPARK_TARGET_FOLDER%\conf\*.xml" 2>nul
    copy "%SPARK_TARGET_FOLDER%\conf\log4j2.properties.template" "%SPARK_TARGET_FOLDER%\conf\log4j2.properties" > nul
    echo - spark: OK
) else (
    echo - spark: skipped
)

if !SL_DOWNLOADED! equ 1 (
    echo - starlake: downloading from %SL_JAR_URL%
    powershell -command "Start-BitsTransfer -Source %SL_JAR_URL% -Destination %SPARK_TARGET_FOLDER%\jars\%SL_JAR_NAME%"
    echo - starlake: OK
) else (
    echo - starlake: skipped
)

if !SPARK_BQ_DOWNLOADED! equ 1 (
    echo - spark bq: downloading from %SPARK_BQ_URL%
    powershell -command "Start-BitsTransfer -Source %SPARK_BQ_URL% -Destination %SPARK_TARGET_FOLDER%\jars\%SPARK_BQ_JAR_NAME%"
    echo - spark bq: OK
) else (
    echo - spark bq: skipped
)

if !HADOOP_AZURE_DOWNLOADED! equ 1 (
    echo - hadoop azure: downloading from %HADOOP_AZURE_URL%
    powershell -command "Start-BitsTransfer -Source %HADOOP_AZURE_URL% -Destination %SPARK_TARGET_FOLDER%\jars\%HADOOP_AZURE_JAR_NAME%"
    echo - hadoop azure: OK
) else (
    echo - hadoop azure: skipped
)

if !AZURE_STORAGE_DOWNLOADED! equ 1 (
    echo - azure storage: downloading from %AZURE_STORAGE_URL%
    powershell -command "Start-BitsTransfer -Source %AZURE_STORAGE_URL% -Destination %SPARK_TARGET_FOLDER%\jars\%AZURE_STORAGE_JAR_NAME%"
    echo - azure storage: OK
) else (
    echo - azure storage: skipped
)

if !JETTY_UTIL_DOWNLOADED! equ 1 (
    echo - jetty util: downloading from %JETTY_UTIL_URL%
    powershell -command "Start-BitsTransfer -Source %JETTY_UTIL_URL% -Destination %SPARK_TARGET_FOLDER%\jars\%JETTY_UTIL_JAR_NAME%"
    echo - jetty util: OK
) else (
    echo - jetty util: skipped
)

if !JETTY_UTIL_AJAX_DOWNLOADED! equ 1 (
    echo - jetty util ajax: downloading from %JETTY_UTIL_AJAX_URL%
    powershell -command "Start-BitsTransfer -Source %JETTY_UTIL_AJAX_URL% -Destination %SPARK_TARGET_FOLDER%\jars\%JETTY_UTIL_AJAX_JAR_NAME%"
    echo - jetty util ajax: OK
) else (
    echo - jetty util ajax: skipped
)

if !SPARK_SNOWFLAKE_DOWNLOADED! equ 1 (
    echo - spark snowflake: downloading from %SPARK_SNOWFLAKE_URL%
    powershell -command "Start-BitsTransfer -Source %SPARK_SNOWFLAKE_URL% -Destination %SPARK_TARGET_FOLDER%\jars\%SPARK_SNOWFLAKE_JAR_NAME%"
    echo - spark snowflake: OK
) else (
    echo - spark snowflake: skipped
)

if !SNOWFLAKE_JDBC_DOWNLOADED! equ 1 (
    echo - snowflake jdbc: downloading from %SNOWFLAKE_JDBC_URL%
    powershell -command "Start-BitsTransfer -Source %SNOWFLAKE_JDBC_URL% -Destination %SPARK_TARGET_FOLDER%\jars\%SNOWFLAKE_JDBC_JAR_NAME%"
    echo - snowflake jdbc: OK
) else (
    echo - snowflake jdbc: skipped
)
goto :eof

:save_installed_versions
echo @echo off > "%SCRIPT_DIR%versions.cmd"
echo if "%%SL_VERSION%%"=="" set "SL_VERSION=!SL_VERSION!" >> "%SCRIPT_DIR%versions.cmd"
echo if "%%SPARK_VERSION%%"=="" set "SPARK_VERSION=!SPARK_VERSION!" >> "%SCRIPT_DIR%versions.cmd"
echo if "%%HADOOP_VERSION%%"=="" set "HADOOP_VERSION=!HADOOP_VERSION!" >> "%SCRIPT_DIR%versions.cmd"
echo if "%%DOWNLOAD_GCP_DEPS%%"=="" set "DOWNLOAD_GCP_DEPS=!DOWNLOAD_GCP_DEPS!" >> "%SCRIPT_DIR%versions.cmd"
if !DOWNLOAD_GCP_DEPS! equ 0 (
    echo if "%%SPARK_BQ_VERSION%%"=="" set "SPARK_BQ_VERSION=!SPARK_BQ_VERSION!" >> "%SCRIPT_DIR%versions.cmd"
)
echo if "%%DOWNLOAD_AZURE_DEPS%%"=="" set "DOWNLOAD_AZURE_DEPS=!DOWNLOAD_AZURE_DEPS!" >> "%SCRIPT_DIR%versions.cmd"
if !DOWNLOAD_AZURE_DEPS! equ 0 (
    echo if "%%HADOOP_AZURE_VERSION%%"=="" set "HADOOP_AZURE_VERSION=!HADOOP_AZURE_VERSION!" >> "%SCRIPT_DIR%versions.cmd"
    echo if "%%AZURE_STORAGE_VERSION%%"=="" set "AZURE_STORAGE_VERSION=!AZURE_STORAGE_VERSION!" >> "%SCRIPT_DIR%versions.cmd"
    echo if "%%JETTY_VERSION%%"=="" ( >> "%SCRIPT_DIR%versions.cmd"
    echo   if "%%JETTY_UTIL_VERSION%%"=="" set "JETTY_UTIL_VERSION=!JETTY_UTIL_VERSION!" >> "%SCRIPT_DIR%versions.cmd"
    echo   if "%%JETTY_UTIL_AJAX_VERSION%%"=="" set "JETTY_UTIL_AJAX_VERSION=!JETTY_UTIL_AJAX_VERSION!" >> "%SCRIPT_DIR%versions.cmd"
    echo ^) >> "%SCRIPT_DIR%versions.cmd"
)
echo if "%%DOWNLOAD_SNOWFLAKE_DEPS%%"=="" set "DOWNLOAD_SNOWFLAKE_DEPS=!DOWNLOAD_SNOWFLAKE_DEPS!" >> "%SCRIPT_DIR%versions.cmd"
if !DOWNLOAD_SNOWFLAKE_DEPS! equ 0 (
    echo if "%%SPARK_SNOWFLAKE_VERSION%%"=="" set "SPARK_SNOWFLAKE_VERSION=!SPARK_SNOWFLAKE_VERSION!" >> "%SCRIPT_DIR%versions.cmd"
    echo if "%%SNOWFLAKE_JDBC_VERSION%%"=="" set "SNOWFLAKE_JDBC_VERSION=!SNOWFLAKE_JDBC_VERSION!" >> "%SCRIPT_DIR%versions.cmd"
)
goto :eof

:launch_starlake
if exist "%SPARK_TARGET_FOLDER%\bin" (
    @REM Transform windows path to unix path for java
    set "SL_ROOT=!SL_ROOT:\=/!"
    set "SCRIPT_DIR=!SCRIPT_DIR:\=/!"
    set "UNIX_SPARK_TARGET_FOLDER=!SPARK_TARGET_FOLDER:\=/!"
    echo.
    echo Launching starlake.
    echo - HADOOP_HOME=!HADOOP_HOME!
    echo - JAVA_HOME=!JAVA_HOME!
    echo - SL_ROOT=!SL_ROOT!
    echo - SL_ENV=!SL_ENV!
    echo - SL_MAIN=!SL_MAIN!
    echo - SL_VALIDATE_ON_LOAD=!SL_VALIDATE_ON_LOAD!
    echo - SPARK_DRIVER_MEMORY=!SPARK_DRIVER_MEMORY!
    echo Make sure your java home path does not contain space

    PATH|FIND /i "!HADOOP_HOME!\bin"    >nul || SET PATH=!path!;!HADOOP_HOME!\bin

    if not defined SL_DEBUG (
        set "SPARK_DRIVER_OPTIONS=-Dlog4j.configuration=file://!SCRIPT_DIR!bin/spark/conf/log4j2.properties"
    ) else (
        set "SPARK_DRIVER_OPTIONS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Dlog4j.configuration=file://!SPARK_DIR!/conf/log4j2.properties"
    )

    if "%SL_DEFAULT_LOADER%" == "native" (
        set JAVA_OPTIONS=^
            --add-opens=java.base/java.lang=ALL-UNNAMED ^
            --add-opens=java.base/java.lang.invoke=ALL-UNNAMED ^
            --add-opens=java.base/java.lang.reflect=ALL-UNNAMED ^
            --add-opens=java.base/java.io=ALL-UNNAMED ^
            --add-opens=java.base/java.net=ALL-UNNAMED ^
            --add-opens=java.base/java.nio=ALL-UNNAMED ^
            --add-opens=java.base/java.util=ALL-UNNAMED ^
            --add-opens=java.base/java.util.concurrent=ALL-UNNAMED ^
            --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED ^
            --add-opens=java.base/sun.nio.ch=ALL-UNNAMED ^
            --add-opens=java.base/sun.nio.cs=ALL-UNNAMED ^
            --add-opens=java.base/sun.security.action=ALL-UNNAMED ^
            --add-opens=java.base/sun.util.calendar=ALL-UNNAMED ^
            --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED

        set "SPARK_OPTIONS=-Dlog4j.configurationFile="!SPARK_TARGET_FOLDER!\conf\log4j2.properties""
        rem Add any additional options you need for your Java application here
        set "JAVA_OPTIONS=!JAVA_OPTIONS! !SPARK_OPTIONS!"
        java !JAVA_OPTIONS! -cp "!UNIX_SPARK_TARGET_FOLDER!/jars/*" !SL_MAIN! %*
    ) else (
        set "SPARK_SUBMIT=%SPARK_TARGET_FOLDER%\bin\spark-submit.cmd"
        @REM spark-submit cmd handles windows path
        !SPARK_SUBMIT! !SPARK_EXTRA_PACKAGES! --driver-java-options "!SPARK_DRIVER_OPTIONS!" !SPARK_CONF_OPTIONS! --class !SL_MAIN! "!SPARK_TARGET_FOLDER!\jars\!SL_JAR_NAME!" %*
    )
) else (
    call :print_install_usage
)
goto :eof

:print_install_usage
echo Starlake is not installed yet. Please type 'starlake.cmd install'.
echo You can define the different env vars if you need to install specific versions.
echo.
echo SL_VERSION: Support stable and snapshot version. Default to latest stable version
echo SPARK_VERSION: default %SPARK_DEFAULT_VERSION%
echo HADOOP_VERSION: default %HADOOP_DEFAULT_VERSION%

:: GCP
echo.
echo DOWNLOAD_GCP_DEPS: enable or disable gcp dependencies ^(0 or 1^). Default 1  - disabled
echo - SPARK_BQ_VERSION: default %SPARK_BQ_DEFAULT_VERSION%

:: AZURE
echo.
echo DOWNLOAD_AZURE_DEPS: enable or disable azure dependencies ^(0 or 1^). Default 1  - disabled
echo - HADOOP_AZURE_VERSION: default %HADOOP_AZURE_DEFAULT_VERSION%
echo - AZURE_STORAGE_VERSION: default %AZURE_STORAGE_DEFAULT_VERSION%
echo - JETTY_VERSION: default %JETTY_DEFAULT_VERSION%
echo - JETTY_UTIL_VERSION: default to JETTY_VERSION
echo - JETTY_UTIL_AJAX_VERSION: default to JETTY_VERSION

:: SNOWFLAKE
echo.
echo DOWNLOAD_SNOWFLAKE_DEPS: enable or disable snowflake dependencies ^(0 or 1^). Default 1  - disabled
echo - SPARK_SNOWFLAKE_VERSION: default %SPARK_SNOWFLAKE_DEFAULT_VERSION%
echo - SNOWFLAKE_JDBC_VERSION: default %SNOWFLAKE_JDBC_DEFAULT_VERSION%
echo.
echo Example:
echo.
echo   set DOWNLOAD_GCP_DEPS=0
echo   starlake.cmd install
echo.
echo Once installed, 'versions.cmd' will be generated and pin dependencies' version.
echo.
goto :eof