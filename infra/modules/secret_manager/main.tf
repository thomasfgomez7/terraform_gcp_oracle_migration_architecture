resource "google_secret_manager_secret" "secrets" {  
  for_each = var.secrets_ids

  secret_id = each.value.secret_id
  project   = var.project_id
  labels = var.labels

  replication {
    auto {           
    }
  }
}

resource "google_secret_manager_secret_version" "secrets_data" {  
  for_each = var.secrets_ids

  secret      = google_secret_manager_secret.secrets[each.key].id
  secret_data = var.secrets_data[each.key]
}