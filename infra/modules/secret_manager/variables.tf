variable "location" {
    description = "The GCP region where resources will be created."
    type        = string
}

variable "project_id" {
    description = "The GCP project ID where resources will be managed."
    type        = string
}

variable "labels" {
    description = "A map of labels to assign to resources."
    type        = map(string)
}

variable "secrets_ids" {
    description = "Map of secrets metadata to create. Key = logical name. Example: { db = {secret_id = \"my-db-pass\"} }"
    type = map(object({
        secret_id = string
    }))
    default = {}
}

variable "secrets_data" {
description = "Map of secret values. Keys must match secrets_map keys. This variable is sensitive and should not be committed."
type = map(string)
sensitive = true
default = {}
}