@echo off

if "x%COMET_VERSION%"=="x" (
  set COMET_VERSION=0.2.0
)
if "x%SPARK_VERSION%"=="x" (
  set SPARK_VERSION=3.1.1
)
if "x%HADOOP_VERSION%"=="x" (
  set HADOOP_VERSION=3.2
)

SET COMET_JAR_NAME=comet-spark3_2.12-%COMET_VERSION%-assembly.jar
SET COMET_JAR_FULL_NAME=..\bin\%COMET_JAR_NAME%

echo COMET_VERSION=%COMET_VERSION%
echo SPARK_VERSION=%SPARK_VERSION%
echo HADOOP_VERSION=%HADOOP_VERSION%

SET COMET_JAR_URL=https://repo1.maven.org/maven2/com/ebiznext/comet-spark3_2.12/%COMET_VERSION%/%COMET_JAR_NAME%

if not x%COMET_VERSION:SNAPSHOT=%==x%COMET_VERSION% (
  SET COMET_JAR_URL=https://oss.sonatype.org/content/repositories/snapshots/com/ebiznext/comet-spark3_2.12/%COMET_VERSION%/%COMET_JAR_NAME%
)
echo COMET_JAR_URL=%COMET_JAR_URL%
SET SPARK_DIR_NAME=spark-%SPARK_VERSION%-bin-hadoop%HADOOP_VERSION%
SET SPARK_TGZ_NAME=%SPARK_DIR_NAME%.tgz
SET SPARK_TGZ_URL=https://downloads.apache.org/spark/spark-%SPARK_VERSION%/%SPARK_TGZ_NAME%
SET SPARK_SUBMIT=..\bin\spark-%SPARK_VERSION%-bin-hadoop%HADOOP_VERSION%\bin\spark-submit.cmd
SET SPARK_DIR=%~dp0..\bin\spark-%SPARK_VERSION%-bin-hadoop%HADOOP_VERSION%


echo SPARK_DIR_NAME=%SPARK_DIR_NAME%
echo SPARK_TGZ_NAME=%SPARK_TGZ_NAME%
echo SPARK_TGZ_URL=%SPARK_TGZ_URL%
echo SPARK_SUBMIT=%SPARK_SUBMIT%
echo SPARK_DIR=%SPARK_DIR%


if "x%1" == "x" (
    GOTO :eof
) else (
    echo calling init env
)

IF exist quickstart (
    rmdir /S /Q quickstart
)

mkdir quickstart

IF exist ..\bin (
    echo ..\bin exists
) ELSE (
    mkdir ..\bin && echo ..\bin created
)

setlocal EnableDelayedExpansion
SET HADOOP_DLL=https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.1/bin/hadoop.dll
SET WINUTILS_EXE=https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.1/bin/winutils.exe
if "%HADOOP_HOME%"=="" (
    mkdir ..\hadoop
    mkdir ..\hadoop\bin
    echo ..\hadoop\bin created
    echo downloading %HADOOP_DLL%
    powershell -command "& { (New-Object Net.WebClient).DownloadFile('%HADOOP_DLL%', '../hadoop/bin/hadoop.dll') }"
    echo downloading %WINUTILS_EXE%
    powershell -command "& { (New-Object Net.WebClient).DownloadFile('%WINUTILS_EXE%', '../hadoop/bin/winutils.exe') }"
) else (
    GOTO SKIP_HADOOP_PATH
)
SET HADOOP_HOME=%~dp0..\..\hadoop
SET PATH=%PATH%;%HADOOP_HOME%\bin

:SKIP_HADOOP_PATH
echo HADOOP_HOME=%HADOOP_HOME%
echo PATH=%PATH%

pause
if exist %COMET_JAR_FULL_NAME% (
    echo %COMET_JAR_FULL_NAME%
    echo %COMET_JAR_NAME% found in ..\bin\
) ELSE (
    echo downloading %COMET_JAR_NAME% from %COMET_JAR_URL%
    powershell -command "& { (New-Object Net.WebClient).DownloadFile('%COMET_JAR_URL%', '../bin/%COMET_JAR_NAME%') }"
)


if exist ..\bin\%SPARK_TGZ_NAME% (
    echo %SPARK_TGZ_NAME% found in ..\bin\
) else (
    echo downloading %SPARK_TGZ_NAME% from %SPARK_TGZ_URL%
    powershell -command "& { (New-Object Net.WebClient).DownloadFile('%SPARK_TGZ_URL%', '../bin/%SPARK_TGZ_NAME%') }"
    tar zxvf ..\bin\%SPARK_TGZ_NAME% -C ..\bin
)


DEL /F /Q ..\bin\%SPARK_DIR_NAME%\conf\*.xml

XCOPY /E /I ..\quickstart-template quickstart

powershell -Command "(gc ../quickstart-template/metadata/env.comet.yml) -replace '__COMET_TEST_ROOT__', '%COMET_ROOT%' | Out-File -encoding ASCII quickstart/metadata/env.comet.yml"

if exist notebooks (
    echo notebooks directory found
) else (
    echo notebooks directory not found. creating ...
    mkdir notebooks
)

if exist %SPARK_SUBMIT% (
      echo %SPARK_SUBMIT% found in ..\bin
      echo Local env initialized correctly
) else (
    echo %SPARK_SUBMIT% not found !!!
    echo Failed to initialize environment
    EXIT /B 2
)

ECHO JAVA_HOME=%JAVA_HOME%
ECHO Make sure your java home path does not contain space
