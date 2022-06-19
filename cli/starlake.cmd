@echo off

if "%COMET_ROOT%"=="" (
    echo COMET_ROOT=%COMET_ROOT% undefined. set it to the starlake project folder
    pause
    exit
)

if "%SPARK_DIR%"=="" (
    echo SPARK_DIR undefined. set it to the Spark folder
    pause
    exit
)

if "%COMET_BIN%"=="" (
    echo COMET_BIN undefined. set it to the Starlake path
    pause
    exit
)



if exist %COMET_BIN% (
    echo COMET_BIN=%COMET_BIN%
) else (
  echo COMET_BIN not defined or does not reference the starlake assembly
    pause
  exit 1
)

if exist %SPARK_DIR% (
    echo SPARK_DIR=%SPARK_DIR%
) else (
  echo SPARK_DIR does not reference a valid folder
    pause
  exit 1
)

SET SPARK_SUBMIT=%SPARK_DIR%\bin\spark-submit.cmd

if exist %SPARK_SUBMIT% (
    echo %SPARK_SUBMIT% found
    echo SUCCESS: environment initialized correctly
) else (
  echo %SPARK_SUBMIT% not found
  echo FAILURE: Failed to initialize environment
    pause
  exit
)

if "%COMET_LOGLEVEL%"=="" (
  copy %SPARK_DIR%\conf\log4j.properties.template %SPARK_DIR%\conf\log4j.properties
) else (
  copy %SPARK_DIR%\conf\log4j.properties.template %SPARK_DIR%\conf\log4j.properties
  type %SPARK_DIR%\conf\log4j.properties.template | findstr /v log4j.rootCategory > %SPARK_DIR%\conf\log4j.properties
  echo log4j.rootCategory=%COMET_LOGLEVEL%, console >> %SPARK_DIR%\conf\log4j.properties
)

SET SPARK_DRIVER_MEMORY=4G
SET COMET_FS=file://
SET COMET_METRICS_ACTIVE=false
SET COMET_ASSERTIONS_ACTIVE=false
SET COMET_SINK_TO_FILE=true
SET COMET_ANALYZE=false
SET COMET_HIVE=false
SET COMET_GROUPED=false
SET COMET_MAIN=ai.starlake.job.Main


@echo on
%SPARK_SUBMIT% --driver-java-options "%SPARK_DRIVER_OPTIONS%" %SPARK_CONF_OPTIONS% --class %COMET_MAIN% %COMET_BIN% %*
