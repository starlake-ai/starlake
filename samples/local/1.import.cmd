call .\env.cmd init
echo %COMET_SCRIPT% import

echo %HADOOP_HOME%
echo %PATH%
%COMET_SCRIPT% import
