variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for the Composer environment"
  type        = string
  default     = "us-central1"
}

variable "labels" {
  description = "Labels to apply"
  type        = map(string)
  default     = {}
}

variable "composer_name" {
  description = "Name of the Composer environment"
  type        = string
  default     = "composer-env"
}

variable "composer_environment_bucket" {
    description = "GCS bucket for Composer environment"
    type        = string
}

variable "composer_service_account" {
  description = "Service account for Composer workloads"
  type        = string
}

variable "image_version" {
  description = "Composer image version (e.g., composer-2.*)"
  type        = string
  default     = "composer-3-airflow-2.10.5-build.3"
}

variable "web_server_allowed_ip_range" {
  description = "CIDR block allowed to access the Airflow web UI"
  type        = string
}

variable "network" {
  type = string  
}

variable "subnetwork" {
  type = string 
}

variable "environment_size" {
  description = "Size of the Composer environment"
  type        = string
  default     = "ENVIRONMENT_SIZE_SMALL"
}