@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"

if "%SL_ROOT%"=="" (
    SET "SL_ROOT=%cd%"
)

if "%~1"=="upgrade" (
    if exist "%SCRIPT_DIR%bin\spark" rmdir /s /q "%SCRIPT_DIR%bin\spark"
) else (
 IF EXIST "%SCRIPT_DIR%versions.cmd" (
     call "%SCRIPT_DIR%versions.cmd"
 )
)



:: Internal variables
set "SL_ARTIFACT_NAME=starlake-core_%SCALA_VERSION%"
set "SPARK_DIR_NAME=spark-%SPARK_VERSION%-bin-hadoop%HADOOP_MAJOR_VERSION%"
set "SPARK_TARGET_FOLDER=%SCRIPT_DIR%bin\spark"
set "DEPS_EXTRA_LIB_FOLDER=%SCRIPT_DIR%bin\deps"
set "STARLAKE_EXTRA_LIB_FOLDER=%SCRIPT_DIR%bin\sl"
if "%SPARK_DRIVER_MEMORY%"=="" set "SPARK_DRIVER_MEMORY=4G"
set "SL_MAIN=ai.starlake.job.Main"
set "HADOOP_HOME=%SCRIPT_DIR%bin\hadoop"
:: set "SL_VALIDATE_ON_LOAD=false"
if not "%SL_VERSION%"=="" SET SL_JAR_NAME=%SL_ARTIFACT_NAME%-%SL_VERSION%-assembly.jar
:: End of internal variables

:: Check if Java is installed using JAVA_HOME env variable

if "%JAVA_HOME%"=="" (
    set "RUNNER=java"
) else (
    set "RUNNER=%JAVA_HOME%\bin\java"
)

IF /i "%language%"=="de" goto languageDE

SET SL_HTTP_HOST=127.0.0.1
if "%SL_HTTP_HOST%"=="" SET SL_HTTP_HOST=127.0.0.1
SET SL_SERVE_URI=http://%SL_HTTP_HOST%:%SL_HTTP_PORT%

if "%~1"=="--version" (
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
   ) else (
    if "%~1"=="install" (
        call :launch_setup %*
        echo Installation done. You're ready to enjoy Starlake!
        echo If any errors happen during installation. Please try to install again or open an issue.
    ) else (
        if "%~1"=="serve" (
            call :launch_starlake %*
        ) else (
            if "%SL_HTTP_PORT%"=="" (
                call :launch_starlake %*
            ) else (
                FOR %%x IN (validation run transform compile) DO DEL /F /Q %SL_ROOT\%out\%%x.log 2>NUL
                curl.exe  "%SL_SERVE_URI%?ROOT=%SL_ROOT%&PARAMS=%*"
                FOR %%x IN (validation run transform compile) DO TYPE  %SL_ROOT%\out\%%x.log 2>NUL
            )
        )
    )
)
goto :eof

:launch_setup
for /f tokens^=2-5^ delims^=.-_+^" %%j in ('"%RUNNER%" -fullversion 2^>^&1') do set "javaVersion=%%j%%k%%l%%m"

:: Check if Java is installed
if "%javaVersion%"=="" (
    echo Java is not installed. Please install Java 11 or above.
    exit /b 1
)

:: Check if Java version is less than 11
if "%javaVersion%" LSS "11000" (
    echo Java version %javaVersion% is not supported. Please install Java 11 or above.
    exit /b 1

)

set setup_url=https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/setup.jar
if defined https_proxy (
    set "PROXY=%https_proxy%"
) else if defined http_proxy (
    set "PROXY=%http_proxy%"
) else (
    set "PROXY="
)

if not "%PROXY%"=="" if defined SL_INSECURE (
    set "CURL_EXTRA=--insecure"
) else (
    set "CURL_EXTRA="
)

curl %CURL_EXTRA%  %PROXY% -s -o %SCRIPT_DIR%setup.jar %setup_url%

"%RUNNER%" -cp %SCRIPT_DIR%setup.jar Setup %SCRIPT_DIR%
goto :eof



:launch_starlake
PATH|FIND /i "%HADOOP_HOME%\bin"    >nul || SET PATH=%path%;%HADOOP_HOME%\bin
if exist %STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME% (
    @REM Transform windows path to unix path for java
    set SL_ROOT=!SL_ROOT:\=/!
    set SCRIPT_DIR=%SCRIPT_DIR:\=/%
    set UNIX_SPARK_TARGET_FOLDER=%SPARK_TARGET_FOLDER:\=/%
    set UNIX_DEPS_EXTRA_LIB_FOLDER=%DEPS_EXTRA_LIB_FOLDER:\=/%
    set UNIX_STARLAKE_EXTRA_LIB_FOLDER=%STARLAKE_EXTRA_LIB_FOLDER:\=/%
    echo.
    echo Launching starlake.
    echo - HADOOP_HOME=%HADOOP_HOME%
    echo - JAVA_HOME=%JAVA_HOME%
    echo - SL_ROOT=%SL_ROOT%
    echo - SL_ENV=%SL_ENV%
    echo - SL_ROOT=%SL_ROOT%


    if "%SL_DEBUG%" == "" (
        set SPARK_DRIVER_OPTIONS=-Dlog4j.configurationFile=file:///%SCRIPT_DIR%bin/spark/conf/log4j2.properties
        set SPARK_OPTIONS=-Dlog4j.configurationFile="%SPARK_TARGET_FOLDER%\conf\log4j2.properties"
    ) else (
        set SPARK_DRIVER_OPTIONS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Dlog4j.configurationFile=file:///%SPARK_DIR%/conf/log4j2.properties
        set SPARK_OPTIONS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Dlog4j.configurationFile="%SPARK_TARGET_FOLDER%\conf\log4j2.properties"
    )

    if "%SL_RUN_MODE%" == "main" (
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

        rem Add any additional options you need for your Java application here
        set JAVA_OPTIONS=!JAVA_OPTS! !JAVA_OPTIONS! !SPARK_OPTIONS!

    ) else (
        set EXTRA_CLASSPATH=%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%
        set extra_jars=%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%

        for %%F in ("%DEPS_EXTRA_LIB_FOLDER%\"*.jar) do (
            set "EXTRA_CLASSPATH=!EXTRA_CLASSPATH!;%%F"
            set "EXTRA_JARS=!EXTRA_JARS!,%%F"
        )
        set SPARK_SUBMIT=%SPARK_TARGET_FOLDER%\bin\spark-submit.cmd
        @REM spark-submit cmd handles windows path
        !SPARK_SUBMIT! %SPARK_EXTRA_PACKAGES% --driver-java-options "%JAVA_OPTS% %SPARK_DRIVER_OPTIONS%" %SPARK_CONF_OPTIONS% --driver-class-path "!EXTRA_CLASSPATH!" --class %SL_MAIN% --jars "!EXTRA_JARS!" "%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%" %*
    )
) else (
    echo Starlake jar %SL_JAR_NAME% do not exists. Please install it.
    EXIT /B 1
)
