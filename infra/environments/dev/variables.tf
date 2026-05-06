# General Variables
variable "region" {
    description = "The GCP region to deploy resources in."
    type        = string
}

variable "location" {
    description = "The GCP location to deploy resources in."
    type        = string
}

variable "project_id" {
    description = "The ID of the GCP project."
    type        = string
}

variable "labels" {
    description = "A map of labels to apply to resources."
    type        = map(string)
}

variable "vpc_shared_link" {
  type = string
}

variable "psc_shared_link" {
  type = string 
}

variable "tfstate_bucket" {
  description = "Bucket to store the tfstate on GCP"
  type = string
}

variable "impersonate_service_account" {
  description = "Service account to impersonate for Terraform operations"
  type        = string
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Secrets
variable "secrets_ids" {
    description = "value"
    type = map(object({ secret_id = string }))    
    default = {}
}

variable "secrets_data" {
    type = map(string)
    sensitive = true
    default = {}
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Datastream Variables
variable "source_port" {
    description = "The port number of the Oracle database."
    type        = number
}

variable "psc_datastream" {
    description = "Private Service Connect resource path for Datastream."
    type        = string
}

# Oracle CRM QA connection profile variables
variable "oracle_conn_profile_id_crm_qa" {
    description = "The ID of the Oracle connection profile."
    type        = string
}

variable "oracle_conn_profile_crm_qa_display_name" {
    description = "The name of the Oracle connection profile."
    type        = string
}

variable "oracle_crm_qa_hostname" {
    description = "The hostname of the Oracle database."
    type        = string
}

variable "oracle_crm_qa_username" {
    description = "The username to connect to the Oracle database."
    type        = string
}

variable "oracle_crm_qa_database_service" {
    description = "The database service name of the Oracle database."
    type        = string
}

# Oracle CRM replica connection profile variables
variable "oracle_conn_profile_id_crm_replica" {
    description = "The ID of the Oracle connection profile."
    type        = string
}

variable "oracle_conn_profile_crm_replica_display_name" {
    description = "The name of the Oracle connection profile."
    type        = string
}

variable "oracle_crm_replica_hostname" {
    description = "The hostname of the Oracle database."
    type        = string
}

variable "oracle_crm_replica_username" {
    description = "The username to connect to the Oracle database."
    type        = string
}

variable "oracle_crm_replica_db_password" {
    description = "Password to connect to the Oracle database."
    type        = string
}

variable "oracle_crm_replica_database_service" {
    description = "The database service name of the Oracle database."
    type        = string
}

# BigQuery connection profile variables
variable "bigquery_conn_profile_id" {
    description = "The ID of the BigQuery connection profile."
    type        = string
}

variable "bigquery_conn_profile_name" {
    description = "The name of the BigQuery connection profile."
    type        = string
}

variable "bigquery_conn_profile_display_name" {
    description = "The display name of the BigQuery connection profile."
    type        = string
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# BigQuery Variables
variable "user_email" {
    description = "The email address of the user with access to BigQuery."
    type        = string
}

variable "datasets" {
    description = "A map of datasets to create in BigQuery, including their properties."
    type        = map(object({
        friendly_name               = string
        description                 = string
        default_table_expiration_ms = number
        labels                      = map(string)
    }))
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Composer Variables
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

variable "web_server_allowed_ip_range" {
  description = "CIDR block allowed to access the Airflow web UI"
  type        = string
}

variable "image_version" {
  description = "Composer image version (e.g., composer-2.*)"
  type        = string
  default     = "composer-3-airflow-2.10.5-build.3"
}

variable "environment_size" {
  description = "Size of the Composer environment"
  type        = string
  default     = "ENVIRONMENT_SIZE_SMALL"
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Dataplex Variables
variable "lake_id" {
  description = "Identifier for the Dataplex Lake"
  type        = string
}

variable "lake_name" {
  description = "Name for the Dataplex Lake"
  type        = string
  default     = null
}

variable "lake_display_name" {
  description = "Display name for the Lake"
  type        = string
  default     = null
}

variable "lake_description" {
  description = "Description for the Lake"
  type        = string
  default     = null
}

variable "cleaning_enabled" {
  description = "Enable cleaning policy"
  type        = bool
  default     = false
}

variable "cleaning_delete_after_days" {
  description = "Days after which data is deleted"
  type        = number
  default     = 30
}

variable "zones" {
  description = "Map of Dataplex zones to create"
  type = map(object({
    display_name  = string
    type          = string        # RAW, CURATED, TRUSTED
    description   = string
    resource_type = string        # BIGQUERY, CLOUD_STORAGE, BIGTABLE, etc.
    location_type = string        # SINGLE_REGION, MULTI_REGION
    os_networks   = list(string)
    labels        = map(string)
  }))
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Streams variables
variable "crmqa_schema" {
    description = "Schema for tables migration from Oracle to BigQuery"
    type = string
}

variable "stream_state" {
    description = "Stream state"
    type = string
}

variable "source_connection_profile_id" {
    description = "ID source connection profile"
    type = string 
}

variable "temp_table_name" {
    description = "Table name for stream"
    type = string
}

variable "destination_connection_profile_id" {
    description = "BigQuery connection profile ID"
    type = string
}

variable "data_freshness" {
    description = "Update time to migrate data between source and destination"
    type = number
}

variable "bq_dataset_id" {
    description = "ID for BigQuery dataset"
    type = string
}

variable "oracle_tables_list" {
    description = "Lista de tablas a incluir en el stream"
    type = list(string)
}