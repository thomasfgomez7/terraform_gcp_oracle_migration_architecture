resource "google_composer_environment" "composer" {
  name                               = var.composer_name
  region                             = var.region
  project                            = var.project_id
  
  labels                             = var.labels  

  storage_config {
    bucket = var.composer_environment_bucket
  }

  config {    

    node_config {
      network    = var.network
      subnetwork = var.subnetwork
      service_account = var.composer_service_account
    }

    software_config {
      image_version                  = var.image_version            

      airflow_config_overrides = {
        core-dags_are_paused_at_creation = "True"
      }      
    }

    environment_size = var.environment_size 
        
  }
}


