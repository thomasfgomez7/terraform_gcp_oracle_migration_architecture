# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# BIGQUERY ALERTS

resource "google_monitoring_notification_channel" "email" {
  display_name = "Alerta de monitoreo Bigquery"
  type         = "email"

  labels = {
    email_address = var.bq_notification_email
  }
}

resource "google_monitoring_alert_policy" "bq_1gb_query_alert" {
  display_name = "BigQuery Alerta - Query > 1GB"

  combiner = "OR"

  conditions {
    display_name = "BigQuery Query Bytes Cobrados > 1GB"

    condition_threshold {
      filter          = var.bq_1gb_query_alert_filter
      comparison      = var.bq_comparison
      threshold_value = var.bq_1gb_query_alert_threshold_value
      duration        = var.bq_1gb_query_alert_duration

      aggregations {
        alignment_period   = var.bq_aggregations_alignment_period 
        per_series_aligner = var.bq_aggregations_per_series_aligner
        }
    }
  }  

  documentation {
    content   = "Se ha detectado una consulta de BigQuery que superó los 1 GB escaneados. Revisa el ID del job y evita consultas costosas innecesarias."
    mime_type = "text/markdown"
  }

  enabled = true

  notification_channels = [google_monitoring_notification_channel.email.name]
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# COMPOSER ALERTS

# notification channel
resource "google_monitoring_notification_channel" "email" {
  display_name = "Alerta de monitoreo Cloud Composer"
  type         = "email"

  labels = {
    email_address = var.composer_notification_email
  }
}

# composer_dag_run_duration

resource "google_monitoring_alert_policy" "composer_dag_run_duration" {
  display_name = "Composer - DAG Run Duration > 180min"
  combiner     = "OR"

  conditions {
    display_name = "DAG run duration exceeds 180 minutes"

    condition_threshold {
      filter          = var.composer_dag_run_duration_filter
      comparison      = var.composer_comparison
      threshold_value = var.composer_dag_run_duration_threshold_value
      duration        = var.composer_dag_run_duration_duration

      aggregations {
        alignment_period   = var.composer_aggregations_dag_run_duration_alignment_period
        per_series_aligner = var.composer_aggregations_dag_run_duration_per_series_aligner
      }
    }
  }

  documentation {
    content   = "Duración del DAG excedió 180 minutos. Revisar logs."
    mime_type = "text/markdown"
  }

  user_labels = {
    environment = "tlc-prd-data-01-4e80-composer-environment"
  }
  
  enabled = true

  notification_channels = [google_monitoring_notification_channel.email.name]
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# DATAPROC ALERTS

resource "google_monitoring_notification_channel" "email" {
  display_name = "Alerta de monitoreo Dataproc"
  type         = "email"

  labels = {
    email_address = var.dataproc_notification_email
  }
}

resource "google_monitoring_alert_policy" "dataproc_batch_spark_executors" {
  display_name = "Dataproc Alerta - Uso elevado de ejecutores Spark (posible costo elevado)"

  combiner = "OR"

  conditions {
    display_name = "Ejecutores activos > 100 por más de 10 minutos"

    condition_threshold {
      filter = var.dataproc_spark_executors_filter

      comparison      = var.dataproc_comparison
      threshold_value = var.dataproc_spark_executors_threshold_value
      
      duration        = var.dataproc_spark_executors_duration

      aggregations {
        alignment_period     = var.dataproc_aggregations_spark_executors_alignment_period
        per_series_aligner   = var.dataproc_aggregations_spark_executors_per_series_aligner
        cross_series_reducer = var.dataproc_aggregations_spark_executors_cross_series_reducer
        group_by_fields      = var.dataproc_aggregations_spark_executors_group_by_fields
      }
    }
  }

  documentation {
    content = "El número de ejecutores activos ha superado el umbral de 100 durante más de 10 minutos. Esto podría indicar una configuración ineficiente que genera costos innecesarios."
    mime_type = "text/markdown"
  }

  enabled = true
  
  notification_channels = [google_monitoring_notification_channel.email.id]
}

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# DATASTREAM ALERTS 

resource "google_monitoring_notification_channel" "email" {
  display_name = "Alerta de monitoreo Datastream"
  type         = "email"

  labels = {
    email_address = var.datastream_notification_email
  }
}

resource "google_monitoring_alert_policy" "datastream_stream_freshness" {
  display_name = "Datastream - Lag > 5 min"
  combiner     = "OR"
  enabled      = true

  conditions {
    display_name = "Freshness Lag > 300s"

    condition_threshold {
      filter          = var.datastream_stream_freshness_filter
      comparison      = var.datastream_comparison
      threshold_value = var.datastream_stream_freshness_threshold_value
      duration        = var.datastream_stream_freshness_duration

      aggregations {
        alignment_period   = var.datastream_aggregations_stream_freshness_alignment_period
        per_series_aligner = var.datastream_aggregations_stream_freshness_per_series_aligner
      }
    }
  }

  documentation {
    content   = "El lag de frescura superó 5 minutos. Revisar Datastream y logs."
    mime_type = "text/markdown"
  }

  notification_channels = [google_monitoring_notification_channel.email.name]
}

resource "google_monitoring_alert_policy" "datastream_unsupported_events" {
  display_name = "Datastream Eventos no admitidos"

  combiner = "OR"

  conditions {
    display_name = "Eventos no admitidos en las últimas 24hs."

    condition_threshold {
      filter          = var.datastream_unsupported_events_filter
      comparison      = var.datastream_comparison
      threshold_value = var.datastream_unsupported_events_threshold_value
      duration        = var.datastream_unsupported_events_duration

      trigger {
        count = 1
      }

      aggregations {
        alignment_period     = var.datastream_aggregations_unsupported_events_alignment_period
        per_series_aligner   = var.datastream_aggregations_unsupported_events_per_series_aligner
        cross_series_reducer = var.datastream_aggregations_unsupported_events_cross_series_reducer
        group_by_fields      = var.datastream_aggregations_unsupported_events_group_by_fields
      }
    }
  }

  documentation {
    content   = "Se detectaron eventos no compatibles (descartados) en uno o más streams de Datastream en las últimas 24 horas."
    mime_type = "text/markdown"
  }

  enabled = true
  
  notification_channels = [google_monitoring_notification_channel.email.name]
}



