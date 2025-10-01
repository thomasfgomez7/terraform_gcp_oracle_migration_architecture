resource "google_bigquery_dataset" "datasets" {
  for_each                        = var.datasets

  dataset_id                      = each.key
  project                         = var.project_id
  location                        = var.location
  friendly_name                   = each.value.friendly_name
  description                     = each.value.description
  labels                          = each.value.labels

  access {
    role                          = "OWNER"
    user_by_email                 = var.user_email
  }

  lifecycle {
    prevent_destroy               = true
  }
}
