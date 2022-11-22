rem @echo off

SET CURRENT_DIR=%~dp0
SET CURRENT_DIR=%CURRENT_DIR:~0,-1%
echo %CURRENT_DIR%

if "x%COMET_VERSION%"=="x" (
  set COMET_VERSION=0.5.2
)

if "x%COMET_ROOT%"=="x" (
  set COMET_ROOT=%CURRENT_DIR%
)

if "x%SPARK_VERSION%"=="x" (
  set SPARK_VERSION=3.3.1
)
set SPARK_VERSION=3.3.1

if "x%HADOOP_VERSION%"=="x" (
  set HADOOP_VERSION=3
)
set HADOOP_VERSION=3

SET COMET_JAR_NAME=starlake-spark3_2.12-%COMET_VERSION%-assembly.jar

SET COMET_JAR_FULL_NAME=%CURRENT_DIR%\spark\jars\%COMET_JAR_NAME%

echo COMET_VERSION=%COMET_VERSION%

echo SPARK_VERSION=%SPARK_VERSION%

echo HADOOP_VERSION=%HADOOP_VERSION%

SET COMET_JAR_URL=https://repo1.maven.org/maven2/ai/starlake/starlake-spark3_2.12/%COMET_VERSION%/%COMET_JAR_NAME%

if not x%COMET_VERSION:SNAPSHOT=%==x%COMET_VERSION% (
  SET COMET_JAR_URL=https://oss.sonatype.org/content/repositories/snapshots/ai/starlake/starlake-spark3_2.12/%COMET_VERSION%/%COMET_JAR_NAME%
)

setlocal EnableDelayedExpansion
SET HADOOP_DLL=https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.2/bin/hadoop.dll
SET WINUTILS_EXE=https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.2/bin/winutils.exe

if "x%HADOOP_HOME%"=="x" (
    mkdir hadoop
    mkdir hadoop\bin
    echo hadoop\bin created
    if not exist hadoop/bin/hadoop.dll (
        echo downloading %HADOOP_DLL%
        powershell -command "& { (New-Object Net.WebClient).DownloadFile('%HADOOP_DLL%', 'hadoop/bin/hadoop.dll') }"
    )
    if not exist hadoop/bin/winutils.exe (
        echo downloading %WINUTILS_EXE%
        powershell -command "& { (New-Object Net.WebClient).DownloadFile('%WINUTILS_EXE%', 'hadoop/bin/winutils.exe') }"
    )
) else (
    GOTO SKIP_HADOOP_PATH
)

:SKIP_HADOOP_PATH

SET HADOOP_HOME=%CURRENT_DIR%\hadoop
echo %HADOOP_HOME%

SET SPARK_TGZ_NAME=spark-%SPARK_VERSION%-bin-hadoop%HADOOP_VERSION%.tgz
SET SPARK_TGZ_URL=https://downloads.apache.org/spark/spark-%SPARK_VERSION%/%SPARK_TGZ_NAME%
SET SPARK_SUBMIT=%CURRENT_DIR%\spark\bin\spark-submit.cmd
SET SPARK_DIR=%CURRENT_DIR%\spark-%SPARK_VERSION%-bin-hadoop%HADOOP_VERSION%


if exist %SPARK_SUBMIT% (
    echo %SPARK_TGZ_NAME% found in spark\bin\
) else (
    echo downloading %SPARK_TGZ_NAME% from %SPARK_TGZ_URL%
    powershell -command "& { (New-Object Net.WebClient).DownloadFile('%SPARK_TGZ_URL%', './%SPARK_TGZ_NAME%') }"
    tar zxvf .\%SPARK_TGZ_NAME% -C .
    rename spark-%SPARK_VERSION%-bin-hadoop%HADOOP_VERSION% spark
)

if exist %COMET_JAR_FULL_NAME% (
    echo %COMET_JAR_FULL_NAME%
    echo %COMET_JAR_NAME% found in spark/bin
) ELSE (
    echo downloading %COMET_JAR_NAME% from %COMET_JAR_URL%
    powershell -command "& { (New-Object Net.WebClient).DownloadFile('%COMET_JAR_URL%', 'spark/jars/%COMET_JAR_NAME%') }"
)


rem DEL /F /Q spark\bin\%SPARK_DIR_NAME%\conf\*.xml

if exist %SPARK_SUBMIT% (
      echo %SPARK_SUBMIT% found in spark\bin
      echo Local env initialized correctly
) else (
    echo %SPARK_SUBMIT% not found !!!
    echo Failed to initialize environment
    EXIT /B 2
)


ECHO JAVA_HOME=%JAVA_HOME%
ECHO Make sure your java home path does not contain space

PATH|FIND /i "%HADOOP_HOME%\bin"    >nul || SET PATH=%path%;%HADOOP_HOME%\bin

CALL %SPARK_SUBMIT% --driver-java-options "%SPARK_DRIVER_OPTIONS%" %SPARK_CONF_OPTIONS% --class ai.starlake.job.Main %COMET_JAR_FULL_NAME% %*
