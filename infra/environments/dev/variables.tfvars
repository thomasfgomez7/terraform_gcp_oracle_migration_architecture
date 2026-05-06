# General Variables
location                                                     = "example"
region                                                       = "example"
project_id                                                   = "example"
labels = {  
  env     = "example"
  region  = "example"
  project = "example"
}

tfstate_bucket                                               = "example"
impersonate_service_account                                  = "example"

vpc_shared_link                                              = "example" 
psc_shared_link                                              = "example"      

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Secret Manager Variables
secrets_ids = {
  source_db_password_secret                                  = { secret_id = "example" }  
}

# Valores SENSIBLES: no commit
secrets_data = {
  source_db_password_secret                                  = "example" 
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Datastream Variables
source_port                                                  = "example"
psc_datastream                                               = "example"

# Oracle CRM QA connection profile variables
oracle_conn_profile_id_crm_qa                                = "example"
oracle_conn_profile_crm_qa_display_name                      = "example"
oracle_crm_qa_hostname                                       = "example"
oracle_crm_qa_username                                       = "example"
oracle_crm_qa_database_service                               = "example"

# Oracle CRM replica connection profile variables
oracle_conn_profile_id_crm_replica                           = "example"  
oracle_conn_profile_crm_replica_display_name                 = "example"
oracle_crm_replica_hostname                                  = "example"
oracle_crm_replica_username                                  = "example"
oracle_crm_replica_database_service                          = "example"
oracle_crm_replica_db_password                               = "example"

# BigQuery connection profile variables
bigquery_conn_profile_id                                     = "example"
bigquery_conn_profile_name                                   = "example"
bigquery_conn_profile_display_name                           = "example"

# Stream 01 Variables
crmqa_schema                                                 = "example"
stream_state                                                 = "example"
source_connection_profile_id                                 = "example"
destination_connection_profile_id                            = "example"
data_freshness                                               = 15
bq_dataset_id                                                = "example"
oracle_tables_list = [
  "example",
  "example",
  "example"
]

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# BigQuery Variables 
user_email                                                   = "example"

datasets = {
  bq_bronze_layer = {
    friendly_name = "example"
    description = "example"
    default_table_expiration_ms = 0 
    labels = {
      env     = "example"
      region  = "example"
      project = "example"      
    }
  }

  bq_silver_layer = {
    friendly_name = "example"
    description = "example"
    default_table_expiration_ms = 0 
    labels = {
      env     = "example"
      region  = "example"
      project = "example"      
    }    
  }

  bq_gold_layer = {
    friendly_name = "example"
    description = "example"
    default_table_expiration_ms = 0 
    labels = {
      env     = "example"
      region  = "example"
      project = "example"     
    }
  }

  bq_aseguramiento_layer = {
    friendly_name = "example"
    description = "example"
    default_table_expiration_ms = 0 
    labels = {
      env     = "example"
      region  = "example"
      project = "example"      
    }
  }
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Composer Variables
composer_name                                        = "example"
composer_environment_bucket                          = "example"
composer_service_account                             = "example"
web_server_allowed_ip_range                          = "example"
image_version                                        = "example"
environment_size                                     = "example"