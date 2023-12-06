@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"

if "%SL_ROOT%"=="" (
    SET "SL_ROOT=%cd%"
)

IF EXIST "%SCRIPT_DIR%versions.cmd" (
    call "%SCRIPT_DIR%versions.cmd"
) ELSE (
     echo %SCRIPT_DIR%versions.cmd not found.
     goto :eof
)


:: Internal variables
set "SL_ARTIFACT_NAME=starlake-spark3_%SCALA_VERSION%"
set "SPARK_DIR_NAME=spark-%SPARK_VERSION%-bin-hadoop%HADOOP_MAJOR_VERSION%"
set "SPARK_TARGET_FOLDER=%SCRIPT_DIR%bin\spark"
set "DEPS_EXTRA_LIB_FOLDER=%SCRIPT_DIR%bin\deps"
set "STARLAKE_EXTRA_LIB_FOLDER=%SCRIPT_DIR%bin\sl"
if "%SPARK_DRIVER_MEMORY%"=="" set "SPARK_DRIVER_MEMORY=4G"
set "SL_MAIN=ai.starlake.job.Main"
:: set "SL_VALIDATE_ON_LOAD=false"
if not "%SL_VERSION%"=="" SET SL_JAR_NAME=%SL_ARTIFACT_NAME%-%SL_VERSION%-assembly.jar
:: End of internal variables

:: Check if Java is installed using JAVA_HOME env variable

if "%JAVA_HOME%"=="" (
    set "RUNNER=java"
) else (
    set "RUNNER=%JAVA_HOME%\bin\java"
)


if "%~1"=="install" (
    call :launch_setup %*
    echo Installation done. You're ready to enjoy Starlake!
    echo If any errors happen during installation. Please try to install again or open an issue.
) else (
    call :launch_starlake %*
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
curl -s -o %SCRIPT_DIR%setup.jar %setup_url%
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
    echo - SL_MAIN=%SL_MAIN%
    echo - SL_VALIDATE_ON_LOAD=%SL_VALIDATE_ON_LOAD%
    echo - SPARK_DRIVER_MEMORY=%SPARK_DRIVER_MEMORY%
    echo - SL_ROOT=%SL_ROOT%


    if "%SL_DEBUG%" == "" (
        set SPARK_DRIVER_OPTIONS=-Dlog4j.configurationFile=file:///%SCRIPT_DIR%bin/spark/conf/log4j2.properties
        set SPARK_OPTIONS=-Dlog4j.configurationFile="%SPARK_TARGET_FOLDER%\conf\log4j2.properties"
    ) else (
        set SPARK_DRIVER_OPTIONS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Dlog4j.configurationFile=file:///%SPARK_DIR%/conf/log4j2.properties
        set SPARK_OPTIONS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Dlog4j.configurationFile="%SPARK_TARGET_FOLDER%\conf\log4j2.properties"
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
    echo "install starlake first using setup.cmd"
)
