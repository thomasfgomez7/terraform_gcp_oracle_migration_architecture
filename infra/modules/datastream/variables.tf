#Variables generales 
variable "project_id" {
    description = "The ID of the project in which the resource belongs."
    type        = string
}

variable "location" {
    description = "The location of the resource."
    type        = string
}

variable "labels" {
    description = "A map of labels to assign to the resource."
    type        = map(string)
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Oracle connection profile general variables

variable "oracle_port" {
    description = "The port of the Oracle database."
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

variable "oracle_crm_qa_password" {
  type = string
  description = "Secret Manager resource path to the Oracle password version"
}

variable "oracle_crm_qa_database_service" {
    description = "The database service name of the Oracle database."
    type        = string
}    

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

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

variable "oracle_crm_replica_password" {
  type = string
  description = "Secret Manager resource path to the Oracle password version"
}

variable "oracle_crm_replica_database_service" {
    description = "The database service name of the Oracle database."
    type        = string
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

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