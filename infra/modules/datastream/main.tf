# Oracle CRM QA Connection Profile
resource "google_datastream_connection_profile" "oracle_conn_profile_crm_qa" {
    connection_profile_id = var.oracle_conn_profile_id_crm_qa
    display_name          = var.oracle_conn_profile_crm_qa_display_name
    location              = var.location
    project               = var.project_id

    labels                = var.labels
    
    oracle_profile {
        hostname          = var.oracle_crm_qa_hostname
        port              = var.oracle_port
        username          = var.oracle_crm_qa_username
        password          = var.oracle_crm_qa_password
        database_service  = var.oracle_crm_qa_database_service        
    }
    
    # Private connection config to connect to the database.
    private_connectivity {
        private_connection = var.psc_datastream
    }
}

# Oracle CRM Replica Connection Profile
resource "google_datastream_connection_profile" "oracle_conn_profile_crm_replica" {
    connection_profile_id = var.oracle_conn_profile_id_crm_replica
    display_name          = var.oracle_conn_profile_crm_replica_display_name
    location              = var.location
    project               = var.project_id

    labels                = var.labels
    
    oracle_profile {
        hostname          = var.oracle_crm_replica_hostname
        port              = var.oracle_port
        username          = var.oracle_crm_replica_username
        password          = var.oracle_crm_replica_password
        database_service  = var.oracle_crm_replica_database_service        
    }
    
    # Private connection config to connect to the database.
    private_connectivity {
        private_connection = var.psc_datastream
    }
} 

# BigQuery Connection Profile
resource "google_datastream_connection_profile" "bigquery_conn_profile" {
    connection_profile_id = var.bigquery_conn_profile_id
    display_name          = var.bigquery_conn_profile_display_name
    location              = var.location
    project               = var.project_id
    labels                = var.labels
    
    # The connection profile uses the BigQuery connector to connect to the database.
    bigquery_profile {        
    }        
}




