data "google_project" "current" { }

# Ce ficheir est le programme principal du projet.
# On précise dans cette balise où est stocké l'état de la plateforme tel que c'est connu de terraform
# L'état sera stocké dans un bucket GCP gs://ebiz-tf-state-dev
# Cela permet de travailler en groupe. Un utilisateur peut faire "terraform apply" et l'autre peut faire "terraform destroy"
# Sinon l'état aurait été stocké en local et il n'aurait pas été possible à plusieurs utilisateurs dee travailler avec un état commun
terraform {
  required_version = ">= 0.12.24"
  #
  backend "azurerm" {
    resource_group_name  = "Hayssem-POC"
    storage_account_name = "ebizcomet2"
    container_name       = "tfstate"
    key                  = "prod.terraform.tfstate"
  }
}

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=2.46.0"
    }
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
}

# Create a resource group
resource "azurerm_resource_group" "example" {
  name     = "example-resources"
  location = "West Europe"
}

# ???
data "google_compute_default_service_account" "default" {}


module "composer" {
  source = "./modules/composer"
  env = var.env
  domain = var.domain
  region = var.region
  zone = var.zone
  config = var.composer
  data-bucket = module.data.data_bucket
}

module "ai" {
  source = "./modules/ai"
  dns_zone = var.dns_zone
}

/*
module "perimeter" {
  source = "./modules/perimeter"
  env = var.env
  region = var.region
  domain = var.domain
  zone = var.zone
  org_id = var.org_id
  authorized_ips = var.authorized_ips
  service_projects = var.service_projects
  restricted_services = var.restricted_services
  perimeter_depends_on = [module.composer.composer_airflow_uri]
}
*/

# Le projet est composé de 4 modules artefacts / composer / data / data2
# Notez comment chaque modulee est exécuté avec la liste des variables dont il a besoin en paramètre.
module "artefacts" {
  source = "./modules/artefacts"
  env = var.env
  region = var.region
  destroy_artefacts = var.destroy_artefacts
  domain = var.domain
  zone = var.zone
  config = var.composer
}

# Création d'un cluster GKE (issu d'un exemple sur le Web)

# Mis en commentaire car je préfère m'appuyer sur le projet Google suivant poru le faire.
# C'est un générateur de terraform (cf fichier gen/README.md dans ce projet)
# Le projet Google se trouve à cette url https://github.com/terraform-google-modules/terraform-google-lb-http
# Il permet à partir d'un fichier YML (gen/example.yml) et en lançant la commande gen/example.sh de créer tout le projet terraform


module "data" {
  source = "./modules/data"
  env = var.env
  region = var.region
  domain = var.domain
}

