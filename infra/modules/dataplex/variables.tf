variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "location" {
  description = "Region for Dataplex resources"
  type        = string
}

variable "lake_id" {
  description = "Identifier for the Dataplex Lake"
  type        = string
}

variable "lake_name" {
  description = "Name for the Dataplex Lake"
  type        = string
}

variable "lake_display_name" {
  description = "Display name for the Lake"
  type        = string
}

variable "lake_description" {
  description = "Description for the Lake"
  type        = string
}

variable "labels" {
  description = "Labels for the Lake"
  type        = map(string)
}

variable "cleaning_enabled" {
  description = "Enable cleaning policy"
  type        = bool
  default     = false
}

variable "cleaning_delete_after_days" {
  description = "Days after which data is deleted"
  type        = number
}

variable "zones" {
  description = "Map of Dataplex zones to create"
  type = map(object({
    display_name  = string
    type          = string        
    description   = string
    resource_type = string        
    location_type = string        
    os_networks   = list(string)
    labels        = map(string)
  }))
}