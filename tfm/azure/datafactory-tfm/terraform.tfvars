# Toutes les variables du projet

env = "dev-ebiz-europe-west1"
domain = "ebiznext.com"
project_id = "ebiz-europe-west1"
region = "europe-west1"
zone = "europe-west1-b"
profile = [
  "all"]
destroy_artefacts = true
org_id = "1061590814097"
authorized_ips = [
  "78.193.32.254/32",
  "0.0.0.0/0"
]

composer = {
  machine_type = "c2-standard-8"
  machine_count = 3
  root_dir = "/tmp"
  #main_jar = "https://oss.sonatype.org/content/repositories/snapshots/com/ebiznext/comet-spark3_2.12/0.1.21/comet-spark3_2.12-0.1.21-assembly.jar"
  main_jar = "gs://dev-ebiz-europe-west1-ebiznext-com-artefacts/com.ebiznext/comet-spark3_2.12/0.1.26-SNAPSHOT/jars/comet-spark3_2.12-assembly.jar"
  #main_jar = "https://oss.sonatype.org/content/repositories/snapshots/com/ebiznext/comet-spark3_2.12/0.1.26-SNAPSHOT/comet-spark3_2.12-0.1.26-SNAPSHOT-assembly.jar"
  dataproc_machine_count = 3
  dataproc_master_machine_type = "n1-standard-4"
  dataproc_worker_machine_type = "n1-standard-4"
  dataproc_image_version = "preview"
}

dns_zone = [
  {
    name = "notebooks-googleapis",
    dns = "notebooks.googleapis.com.",
    description = "notebooks-googleapis"
  },
  {
    name = "notebooks-googleusercontent",
    dns = "notebooks.googleusercontent.com.",
    description = "notebooks-googleusercontent"
  },
  {
    name = "notebooks-cloud-google",
    dns = "notebooks.cloud.google.com.",
    description = "notebooks-cloud-google"
  },
  {
    name = "datalab-cloud-google",
    dns = "datalab.cloud.google.com.",
    description = "datalab-cloud-google"
  }
]


service_projects = [
  {
    id = "ebiz-europe-west1",
    number = "683153751995"
  }
#,
#  {
#    id = "ebiz-europe-west1",
#    number = "683153751995"
#  }
]

restricted_services = [
  "bigquery.googleapis.com",
  "bigquery.googleapis.com",
  "bigquerydatatransfer.googleapis.com",
  "bigtable.googleapis.com",
  "cloudbuild.googleapis.com",
  "cloudfunctions.googleapis.com",
  "cloudprofiler.googleapis.com",
  "cloudresourcemanager.googleapis.com",
  "cloudtrace.googleapis.com",
  "composer.googleapis.com",
  "compute.googleapis.com",
  "container.googleapis.com",
  "containeranalysis.googleapis.com",
  "containerregistry.googleapis.com",
  "datacatalog.googleapis.com",
  "dataflow.googleapis.com",
  "datafusion.googleapis.com",
  "dataproc.googleapis.com",
  "dlp.googleapis.com",
  "logging.googleapis.com",
  "metastore.googleapis.com",
  "ml.googleapis.com",
  "monitoring.googleapis.com",
  "notebooks.googleapis.com",
  "oslogin.googleapis.com",
  "pubsub.googleapis.com",
  "recommender.googleapis.com",
  "redis.googleapis.com",
  "servicecontrol.googleapis.com",
  "spanner.googleapis.com",
  "sqladmin.googleapis.com"
]
