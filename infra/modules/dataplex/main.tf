resource "google_dataplex_lake" "lake" {
  provider       = google
  project        = var.project_id
  location       = var.location
  name           = var.lake_name
    
  display_name   = var.lake_display_name
  description    = var.lake_description
  labels         = var.labels
}

resource "google_dataplex_zone" "zones" {
  for_each         = var.zones

  project          = var.project_id
  location         = var.location
  lake             = google_dataplex_lake.lake.name

  name             = each.key
  type             = each.value.type
  display_name     = each.value.display_name
  description      = each.value.description

  labels           = each.value.labels
  
  resource_spec {    
    location_type = each.value.location_type
  }
  
  discovery_spec {
    enabled = true
  }
}
