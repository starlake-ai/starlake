# Il s'agit de variables en output. Elles seront affichées à l'écran
# On peut également les accéder dans le fichier terraform.tfstate.
# Dans ce premier exemple on va récupérer le bucket du composer qu'on a créé.
# Quand TFM a fini "l'apply", il va appeler lire la valeur de ces variables 
# dans le fichier terraform.tfstate
# Ce fichier contient toutes les variables définies pour le projet.
/*
output "composer_dag_gcs_bucket" {
  value = split("/", module.composer.composer_dag_gcs_prefix)[2]
}

output "composer_airflow_uri" {
  value = module.composer.composer_airflow_uri
}

output "composer_service_account" {
  value = module.composer.composer_service_account
}

output "dataproc_service_account" {
  value = module.composer.dataproc_service_account
}

output "path" {
  value = path.module
}

*/