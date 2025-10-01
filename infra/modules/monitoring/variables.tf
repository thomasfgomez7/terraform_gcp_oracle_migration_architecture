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

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# BIGQUERY VARIABLES 
variable "bq_notification_email" {
  description = "Email address to receive alert notifications"
  type        = string
}

variable "bq_1gb_query_alert_filter" {
  description = "Filter for BigQuery alert on queries larger than 1GB"
  type        = string
  default     = "metric.type=\"bigquery.googleapis.com/storage/uploaded_bytes_billed\" AND resource.type=\"bigquery_dataset"
}

variable "bq_1gb_query_alert_duration" {
  description = "Duration for alert evaluation"
  type        = string
}

variable "bq_1gb_query_alert_threshold_value" {
  description = "Query size in Bytes"
  type        = number
  }

variable bq_comparison {
  description = "Comparison operator for alert conditions"
  type        = string
}

variable "bq_aggregations_alignment_period" {
  description = "Alignment period for BigQuery metrics"
  type        = string  
}

variable "bq_aggregations_per_series_aligner" {
  description = "Aligner to use for per-series data"
  type        = string
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# COMPOSER VARIABLES 
variable "composer_notification_email" {
  description = "Email address to receive alert notifications"
  type        = string
}

variable composer_comparison {
  description = "Comparison operator for alert conditions"
  type        = string
}

variable "composer_dag_run_duration_filter" {
  description = "Filter for Composer DAG run duration alert (execution > 60 minutes)"
  type        = string
  default     = "metric.type=\"composer.googleapis.com/workflow/dag/run_duration\" AND resource.type=\"cloud_composer_environment\""
}

variable "composer_dag_run_duration_threshold_value" {
  description = "Threshold value in seconds for DAG run duration alert (e.g., 3600 for 60 minutes)"
  type        = number
}

variable "composer_dag_run_duration_duration" {
  description = "Duration period for the alert condition (e.g., '60s', '300s')"
  type        = string
}

variable "composer_aggregations_dag_run_duration_alignment_period" {
  description = "Alignment period for Composer aggregations (e.g., '60s')"
  type        = string
}

variable "composer_aggregations_dag_run_duration_per_series_aligner" {
  description = "Per-series aligner for the DAG run duration alert (e.g., 'ALIGN_MAX', 'ALIGN_MEAN')"
  type        = string 
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# DATAPROC VARIABLES 
variable "dataproc_notification_email" {
  description = "Email address to receive alert notifications"
  type        = string
}

variable "dataproc_comparison" {
    description = "Comparison operator for alert condition"
    type        = string
}

variable "dataproc_spark_executors_filter" {
    description = "Filter for Spark Executors metric"
    type        = string
    default     = "metric.type=\"dataproc.googleapis.com/batch/spark/executors\" AND resource.type=\"cloud_dataproc_batch\""
}

variable "dataproc_spark_executors_threshold_value" {
    description = "Threshold value for Spark Executors alert"
    type        = number
}

variable "dataproc_spark_executors_duration" {
    description = "Duration for which the condition must be met"
    type        = string
}

variable "dataproc_aggregations_spark_executors_alignment_period" {
    description = "Alignment period for Spark Executors metric"
    type        = string
}

variable "dataproc_aggregations_spark_executors_per_series_aligner" {
    description = "Per-series aligner for Spark Executors metric"
    type        = string
}

variable "dataproc_aggregations_spark_executors_cross_series_reducer" {
    description = "Cross-series reducer for Spark Executors metric"
    type        = string
}

variable "dataproc_aggregations_spark_executors_group_by_fields" {
    description = "Fields to group by for Spark Executors metric"
    type        = list(string)
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# DATASTREAM VARIABLES 

# Variables de Alertas
variable "datastream_notification_email" {
  description = "Email address to receive alert notifications"
  type        = string
}

variable "datastream_comparison" {
  description = "Comparison operator for DAG run duration alert (e.g., COMPARISON_GT, COMPARISON_LT)"
  type        = string
}

#Variables Unsupported events alert policy
variable "datastream_unsupported_events_filter" {
  description = "Filter for Composer queue backlog alert"
  type        = string
  default     = "metric.type=\"datastream.googleapis.com/stream/unsupported_event_count\" AND resource.type=\"datastream.googleapis.com/Stream\""
}

variable "datastream_unsupported_events_threshold_value" {
  description = "Threshold value for unsupported events alert (e.g., 0.0)"
  type        = number
}

variable "datastream_unsupported_events_duration" {
  description = "Duration period for the unsupported events alert (e.g., '86400s' for 24 hours)"
  type        = string
}

variable "datastream_aggregations_unsupported_events_alignment_period" {
  description = "Alignment period for the unsupported events alert (e.g., '3600s' for 1 hour)"
  type        = string
}

variable "datastream_aggregations_unsupported_events_per_series_aligner" {
  description = "Per-series aligner for the unsupported events alert (e.g., 'ALIGN_RATE', 'ALIGN_MAX')"
  type        = string
}

variable "datastream_aggregations_unsupported_events_cross_series_reducer" {
  description = "Cross-series reducer for the unsupported events alert (e.g., 'REDUCE_SUM', 'REDUCE_MEAN')"
  type        = string
}

variable "datastream_aggregations_unsupported_events_group_by_fields" {
  description = "Group by fields for the unsupported events alert (e.g., ['resource.labels.stream'])"
  type        = list(string)
}

#Variables Stream freshness alert policy
variable "datastream_stream_freshness_filter" {
  description = "Filter for Datastream freshness lag alert (lag > 5 minutes)"
  type        = string
  default     = "metric.type=\"datastream.googleapis.com/stream/freshness\" AND resource.type=\"datastream.googleapis.com/Stream\""
}

variable "datastream_stream_freshness_threshold_value" {
  description = "Threshold value in seconds for DAG run duration alert (e.g., 3600 for 60 minutes)"
  type        = number
}

variable "datastream_stream_freshness_duration" {
  description = "Duration period for the alert condition (e.g., '60s', '300s')"
  type        = string
}

variable "datastream_aggregations_stream_freshness_alignment_period" {
  description = "Alignment period for the DAG run duration alert (e.g., '60s')"
  type        = string
}

variable "datastream_aggregations_stream_freshness_per_series_aligner" {
  description = "Per-series aligner for the DAG run duration alert (e.g., 'ALIGN_MAX', 'ALIGN_MEAN')"
  type        = string
}
