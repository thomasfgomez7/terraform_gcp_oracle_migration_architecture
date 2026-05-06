# Secrets Module 
module "secrets" {
  source                                             = "../../modules/secrets"

  location                                           = var.location
  project_id                                         = var.project_id
  labels                                             = var.labels  
  secrets_ids                                        = var.secrets_ids
  secrets_data                                       = var.secrets_data
}

# BigQuery module
module "bigquery" {
    source                                            = "../../modules/bigquery"

    project_id                                        = var.project_id
    location                                          = var.location
    user_email                                        = var.user_email

    datasets                                          = var.datasets   
}

# Datastream Module
module "datastream" {
  source                                            = "../../modules/datastream"

  location                                          = var.location
  project_id                                        = var.project_id
  labels                                            = var.labels   

  oracle_port                                       = var.source_port
  psc_datastream                                    = var.psc_datastream

  # Oracle CRM QA connection profile
  oracle_conn_profile_id_crm_qa                     = var.oracle_conn_profile_id_crm_qa
  oracle_conn_profile_crm_qa_display_name           = var.oracle_conn_profile_crm_qa_display_name
  oracle_crm_qa_hostname                            = var.oracle_crm_qa_hostname
  oracle_crm_qa_username                            = var.oracle_crm_qa_username  
  oracle_crm_qa_password                            = var.secrets_data["oracle_crm_qa_db_password"]  #VARIABLE PARA SECRETOS CON FOR EACH    
  oracle_crm_qa_database_service                    = var.oracle_crm_qa_database_service
  
  # Oracle CRM replica connection profile
  oracle_conn_profile_id_crm_replica                = var.oracle_conn_profile_id_crm_replica
  oracle_conn_profile_crm_replica_display_name      = var.oracle_conn_profile_crm_replica_display_name
  oracle_crm_replica_hostname                       = var.oracle_crm_replica_hostname
  oracle_crm_replica_username                       = var.oracle_crm_replica_username
  oracle_crm_replica_password                       = var.secrets_data["oracle_crm_replica_db_password"] #VARIABLE PARA SECRETOS CON FOR EACH
  oracle_crm_replica_database_service               = var.oracle_crm_replica_database_service

  # BigQuery connection profile
  bigquery_conn_profile_id                          = var.bigquery_conn_profile_id
  bigquery_conn_profile_name                        = var.bigquery_conn_profile_name
  bigquery_conn_profile_display_name                = var.bigquery_conn_profile_display_name    

  # Stream 01
  crmqa_schema                                      = var.crmqa_schema
  stream_state                                      = var.stream_state
  source_connection_profile_id                      = var.source_connection_profile_id
  destination_connection_profile_id                 = var.destination_connection_profile_id
  data_freshness                                    = var.data_freshness
  bq_dataset_id                                     = var.bq_dataset_id    
  oracle_tables_list                                = var.oracle_tables_list
}

# Composer module
module "composer" {
  source                                              = "../../modules/composer"

  project_id                                          = var.project_id
  region                                              = var.region
  labels                                              = var.labels

  network                                             = var.vpc_shared_link
  subnetwork                                          = var.psc_shared_link

  composer_name                                       = var.composer_name
  composer_environment_bucket                         = var.composer_environment_bucket
  composer_service_account                            = var.composer_service_account
  image_version                                       = var.image_version
  environment_size                                    = var.environment_size
}

