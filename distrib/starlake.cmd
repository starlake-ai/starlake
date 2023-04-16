@echo off && setlocal EnableDelayedExpansion

SET SCRIPT_DIR=%~dp0
SET SCRIPT_DIR=%SCRIPT_DIR:~0,-1%

SET SCALA_VERSION=2.12
SET SL_ARTIFACT_NAME=starlake-spark3_%SCALA_VERSION%
SET SPARK_BQ_ARTIFACT_NAME=spark-bigquery-with-dependencies_%SCALA_VERSION%
SET HADOOP_DEFAULT_VERSION=3.2.2
SET HADOOP_DLL=https://github.com/cdarlint/winutils/raw/master/hadoop-%HADOOP_DEFAULT_VERSION%/bin/hadoop.dll
SET WINUTILS_EXE=https://github.com/cdarlint/winutils/raw/master/hadoop-%HADOOP_DEFAULT_VERSION%/bin/winutils.exe
SET SPARK_TARGET_FOLDER=%SCRIPT_DIR%\bin\spark
SET SPARK_SUBMIT=%SPARK_TARGET_FOLDER%\bin\spark-submit.cmd

SET HADOOP_DOWNLOADED=FALSE
SET HADOOP_DLL_DOWNLOADED=FALSE
SET HADOOP_WINUTILS_DOWNLOADED=FALSE
SET SPARK_DOWNLOADED=FALSE
SET SL_DOWNLOADED=FALSE
SET SKIP_INSTALL=TRUE

ECHO -------------------
ECHO    Current state
ECHO -------------------

if "x%HADOOP_HOME%"=="x" (
    SET HADOOP_HOME=%SCRIPT_DIR%\bin\hadoop
)

if exist %HADOOP_HOME% (
    ECHO - hadoop: OK
    SET HADOOP_DOWNLOADED=TRUE
) else (
    SET SKIP_INSTALL=FALSE
    ECHO - hadoop: KO
)

if exist %HADOOP_HOME%\bin\hadoop.dll (
    ECHO - hadoop dll: OK
    SET HADOOP_DLL_DOWNLOADED=TRUE
) else (
    SET SKIP_INSTALL=FALSE
    ECHO - hadoop dll: KO
)

if exist %HADOOP_HOME%\bin\winutils.exe (
    ECHO - hadoop winutils: OK
    SET HADOOP_WINUTILS_DOWNLOADED=TRUE
) else (
    SET SKIP_INSTALL=FALSE
    ECHO - hadoop winutils: KO
)

if exist %SPARK_TARGET_FOLDER%\jars (
    ECHO - spark: OK
    SET SPARK_DOWNLOADED=TRUE
    GOTO :FIND_SL_STAGE
) else (
    SET SKIP_INSTALL=FALSE
    ECHO - spark: KO
    ECHO - starlake: KO
    ECHO - spark bq: KO
    GOTO :SKIP_ARTIFACTS_SEARCH_STAGE
)

:FIND_SL_STAGE
for /f "tokens=*" %%f in ('dir %%SPARK_TARGET_FOLDER%%\jars /b /o-n /a-d') do (
    SET CURRENT_ARTIFACT=%%f
    Echo %%f | findstr /C:%SL_ARTIFACT_NAME%>nul && (
        ECHO - starlake: OK
        SET SL_DOWNLOADED=TRUE
        GOTO :SET_SL_JAR_FULL_NAME_STAGE
    )
)
SET SKIP_INSTALL=FALSE
ECHO - starlake: KO
GOTO :SKIP_SL_SEARCH_STAGE

:SET_SL_JAR_FULL_NAME_STAGE
SET SL_JAR_FULL_NAME=%SPARK_TARGET_FOLDER%\jars\%CURRENT_ARTIFACT%

:SKIP_SL_SEARCH_STAGE

:FIND_SPARK_BQ_STAGE
for /f "tokens=*" %%f in ('dir %%SPARK_TARGET_FOLDER%%\jars /b /o-n /a-d') do (
    SET CURRENT_ARTIFACT=%%f
    Echo %%f | findstr /C:%SPARK_BQ_ARTIFACT_NAME%>nul && (
        ECHO - spark bq: OK
        SET SPARK_BQ_DOWNLOADED=TRUE
        GOTO :SKIP_SPARKBQ_SEARCH_STAGE
    )
)
SET SKIP_INSTALL=FALSE
ECHO - spark bq: KO

:SKIP_SPARKBQ_SEARCH_STAGE
:SKIP_ARTIFACTS_SEARCH_STAGE

if "%SKIP_INSTALL%"=="TRUE" (
    goto :SKIP_INSTALL
)

ECHO.
ECHO -------------------
ECHO       Install
ECHO -------------------

if "x%HADOOP_VERSION%"=="x" (
    set HADOOP_VERSION=3
)

if "%HADOOP_DOWNLOADED%"=="TRUE" (
    echo - hadoop: skipped
    GOTO :SKIP_HADOOP_HOME
)

md %HADOOP_HOME%\bin
echo - hadoop: OK

:SKIP_HADOOP_HOME
if "%HADOOP_DLL_DOWNLOADED%" == "FALSE" (
    echo - hadoop dll: downloading from %HADOOP_DLL%
    powershell -command "Start-BitsTransfer -Source %HADOOP_DLL% -Destination %HADOOP_HOME%\bin\hadoop.dll"
    echo Hadoop dll version: %HADOOP_DEFAULT_VERSION% >> %SCRIPT_DIR%/version.info
    echo - hadoop dll: OK
) else (
    echo - hadoop dll: skipped
)

if "%HADOOP_WINUTILS_DOWNLOADED%" == "FALSE" (
    echo - hadoop winutils: downloading from %WINUTILS_EXE%
    powershell -command "Start-BitsTransfer -Source %WINUTILS_EXE% -Destination %HADOOP_HOME%\bin\winutils.exe"
    echo Hadoop winutils version: %HADOOP_DEFAULT_VERSION% >> %SCRIPT_DIR%/version.info
    echo - hadoop winutils: OK
) else (
    echo - hadoop winutils: skipped
)

if "x%SPARK_VERSION%"=="x" (
    set SPARK_VERSION=3.3.1
)

if "x%SPARK_BQ_VERSION%"=="x" (
    set SPARK_BQ_VERSION=0.27.1
)

if "%SPARK_DOWNLOADED%" == "TRUE" (
    echo - spark: skipped
    GOTO :SKIP_SPARK
)

SET SPARK_DIR_NAME=spark-%SPARK_VERSION%-bin-hadoop%HADOOP_VERSION%
SET SPARK_TGZ_NAME=%SPARK_DIR_NAME%.tgz
SET SPARK_TGZ_URL=https://archive.apache.org/dist/spark/spark-%SPARK_VERSION%/%SPARK_TGZ_NAME%
SET SPARK_DIR=%SCRIPT_DIR%\%SPARK_DIR_NAME%

if not exist %SPARK_TGZ_NAME% (
    echo - spark: downloading from %SPARK_TGZ_URL%
    powershell -command "Start-BitsTransfer -Source %SPARK_TGZ_URL% -Destination ./%SPARK_TGZ_NAME%"
)

tar zxf .\%SPARK_TGZ_NAME% -C .
copy %SPARK_DIR_NAME%\conf\log4j2.properties.template %SPARK_DIR_NAME%\conf\log4j2.properties > nul
md %SPARK_TARGET_FOLDER%
xcopy /e /v %SPARK_DIR_NAME% %SPARK_TARGET_FOLDER% > nul
del /F /S /Q %SPARK_DIR_NAME% > nul
rmdir /S /Q %SPARK_DIR_NAME%
del /F /Q %SPARK_TGZ_NAME%
echo Spark version: %SPARK_VERSION% >> %SCRIPT_DIR%/version.info
echo - spark: OK

:SKIP_SPARK

if "%SL_DOWNLOADED%" == "TRUE" (
    echo - starlake: skipped
    GOTO :SKIP_SL_DOWNLOAD
)

if "x%COMET_VERSION%"=="x" (
    for /f "usebackq delims=" %%a in (`powershell -Command "((((Invoke-WebRequest 'https://search.maven.org/solrsearch/select?q=g:ai.starlake AND a:%SL_ARTIFACT_NAME%&core=gav&start=0&rows=42&wt=json').content) | ConvertFrom-Json).response.docs.v | sort {[version] $_} -Descending)[0]"`) do (
        set COMET_VERSION=%%a
    )
)

SET SL_JAR_NAME=%SL_ARTIFACT_NAME%-%COMET_VERSION%-assembly.jar
SET SL_JAR_FULL_NAME=%SPARK_TARGET_FOLDER%\jars\%SL_JAR_NAME%
SET SL_JAR_URL=https://repo1.maven.org/maven2/ai/starlake/%SL_ARTIFACT_NAME%/%COMET_VERSION%/%SL_JAR_NAME%

if not x%COMET_VERSION:SNAPSHOT=%==x%COMET_VERSION% (
    SET SL_JAR_URL=https://oss.sonatype.org/content/repositories/snapshots/ai/starlake/%SL_ARTIFACT_NAME%/%COMET_VERSION%/%SL_JAR_NAME%
)

echo - starlake: downloading from %SL_JAR_URL%
powershell -command "Start-BitsTransfer -Source %SL_JAR_URL% -Destination %SPARK_TARGET_FOLDER%/jars/%SL_JAR_NAME%"
echo Starlake version: %COMET_VERSION% >> %SCRIPT_DIR%/version.info
echo - starlake: OK

:SKIP_SL_DOWNLOAD

if "%SPARK_BQ_DOWNLOADED%" == "TRUE" (
    echo - spark bq: skipped
    GOTO :SKIP_SPARK_BQ_DOWNLOAD
)

SET SPARK_BQ_JAR_NAME=%SPARK_BQ_ARTIFACT_NAME%-%SPARK_BQ_VERSION%.jar
SET SPARK_BQ_JAR_FULL_NAME=%SPARK_TARGET_FOLDER%\jars\%SPARK_BQ_JAR_NAME%
SET SPARK_BQ_JAR_URL=https://repo1.maven.org/maven2/com/google/cloud/spark/%SPARK_BQ_ARTIFACT_NAME%/%SPARK_BQ_VERSION%/%SPARK_BQ_JAR_NAME%

echo - spark bq: downloading from %SPARK_BQ_JAR_URL%
powershell -command "Start-BitsTransfer -Source %SPARK_BQ_JAR_URL% -Destination %SPARK_TARGET_FOLDER%/jars/%SPARK_BQ_JAR_NAME%"
echo Spark bq version: %SPARK_BQ_VERSION% >> %SCRIPT_DIR%/version.info
echo - spark bq: OK

:SKIP_SPARK_BQ_DOWNLOAD

:SKIP_INSTALL
if "x%COMET_ROOT%"=="x" (
    set COMET_ROOT=%cd%
)

if "x%COMET_ENV%"=="x" (
    set COMET_ENV=FS
)

if "x%COMET_FS%"=="x" (
    set COMET_FS=file://
)

if "x%COMET_MAIN%"=="x" (
    set COMET_MAIN=ai.starlake.job.Main
)

if "x%COMET_VALIDATE_ON_LOAD%"=="x" (
    set COMET_VALIDATE_ON_LOAD=false
)

if "x%SPARK_DRIVER_MEMORY%"=="x" (
    set SPARK_DRIVER_MEMORY=4G
)

ECHO.
ECHO Launching starlake.
ECHO - HADOOP_HOME=%HADOOP_HOME%
ECHO - JAVA_HOME=%JAVA_HOME%
ECHO - COMET_ROOT=%COMET_ROOT%
ECHO - COMET_ENV=%COMET_ENV%
ECHO - COMET_FS=%COMET_FS%
ECHO - COMET_MAIN=%COMET_MAIN%
ECHO - COMET_VALIDATE_ON_LOAD=%COMET_VALIDATE_ON_LOAD%
ECHO - SPARK_DRIVER_MEMORY=%SPARK_DRIVER_MEMORY%
ECHO Make sure your java home path does not contain space

PATH|FIND /i "%HADOOP_HOME%\bin"    >nul || SET PATH=%path%;%HADOOP_HOME%\bin

CALL %SPARK_SUBMIT% --driver-java-options "%SPARK_DRIVER_OPTIONS%" %SPARK_CONF_OPTIONS% --class ai.starlake.job.Main %SL_JAR_FULL_NAME% %*
