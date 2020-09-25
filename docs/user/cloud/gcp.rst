.. _gcp:

*********************************************
Google Cloud Platform
*********************************************

Running Locally
----------------

When debugging your application, you may need to run your job locally against the remote GCP Project hosting your BigQuery datasets.
In that case, youo need a custom core-site.xml in your classpath as decribed below :

.. code-block:: xml

 <configuration>
     <property>
         <name>fs.gs.impl</name>
         <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
     </property>
     <property>
         <name>fs.AbstractFileSystem.gs.impl</name>
         <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
     </property>
     <property>
         <name>fs.gs.project.id</name>
         <value>myproject-1234</value>
     </property>
     <property>
         <name>google.cloud.auth.service.account.enable</name>
         <value>true</value>
     </property>
     <property>
         <name>google.cloud.auth.service.account.json.keyfile</name>
         <value>/Users/me/.gcloud/keys/myproject-1234.json</value>
     </property>
     <property>
         <name>fs.default.name</name>
         <value>gs://comet-app</value>
     </property>
     <property>
         <name>fs.defaultFS</name>
         <value>gs://comet-app</value>
     </property>
     <property>
         <name>fs.gs.system.bucket</name>
         <value>comet-app</value>
     </property>
 </configuration>

