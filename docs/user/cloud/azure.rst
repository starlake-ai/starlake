.. _azure:

*********************************************
Microsoft Azure
*********************************************

Storage Accounts
################

Comet need to access ADFS. You need to provide the credentials in one of the three ways below :

* Through a core-site.xml file present in the classpath :

.. code-block:: xml

 <?xml version="1.0" encoding="UTF-8"?>
 <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
 <configuration>
     <property>
         <name>fs.azure.account.key.ebizcomet.dfs.core.windows.net</name>
         <value>*******==</value>
     </property>
     <property>
         <name>fs.default.name</name>
         <value>abfs://cometfs@ebizcomet.dfs.core.windows.net/</value>
     </property>
 </configuration>


* At cluster creation as specified `here <https://docs.microsoft.com/fr-fr/azure/databricks/data/data-sources/azure/azure-datalake-gen2#rdd-api>`_.


* Through a specific application.conf file in the comet.jar classpath.
  You must add the spark.hadoop. prefix to the corresponding Hadoop configuration keys to propagate them to the Hadoop configurations that are used used in the Comet Spark Job.

