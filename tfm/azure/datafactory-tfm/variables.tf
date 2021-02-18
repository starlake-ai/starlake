variable "env" {
  type = string
  description = "dev / test / prod"
}

variable "dns_zone" {
  type = list(object({
    name = string,
    dns = string,
    description = string
  }))
  description = "DNS Zones"
}

variable "region" {
  type = string
  description = "The region for the network. If the cluster is regional, this must be the same region. Otherwise, it should be the region of the zone."
}

variable "zone" {
  type = string
  description = "The zone where the cluster must be instantiated"
}

variable "restricted_services" {
  type = list(string)
  description = "restricted_services in VPC/SC"
}

variable "project_id" {
  type = string
  description = "project id"
}

variable "cluster_name" {
  description = "The name of the Kubernetes cluster."
  type = string
  default = "example-private-cluster"
}

variable "cluster_service_account_name" {
  description = "The name of the custom service account used for the GKE cluster. This parameter is limited to a maximum of 28 characters."
  type = string
  default = "example-private-cluster-sa"
}

variable "cluster_service_account_description" {
  description = "A description of the custom service account used for the GKE cluster."
  type = string
  default = "Example GKE Cluster Service Account managed by Terraform"
}


variable "profile" {
  description = "Only active profile will be installed"
  type = list(string)
}

variable "destroy_artefacts" {
  type = bool
}

variable "domain" {
  type = string
  description = "project domain"
}
variable "org_id" {
  type = string
  description = "project domain"
}

variable "authorized_ips" {
  type = list(string)
  description = "Authorized ips"
}

variable "service_projects" {
  type = list(object({
    id = string,
    number = string
  }))
  description = "Authorized ips"
}

variable "perimeter_depends_on" {
  type    = any
  default = null
}


variable "composer" {
  type = object({
    machine_type = string
    machine_count = number
    root_dir = string
    main_jar = string
    dataproc_machine_count = number
    dataproc_master_machine_type = string
    dataproc_worker_machine_type = string
    dataproc_image_version = string
  })
}

