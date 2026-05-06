terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  required_version = ">= 1.3.0"
}

terraform {
  backend "gcs" {
    bucket  = "example-bucket" 
    prefix = "terraform/infra"
    impersonate_service_account = "sa-deployer" 
  }
}


provider "google" {
  project = var.project_id
  region  = var.region  
  impersonate_service_account = var.impersonate_service_account
}
