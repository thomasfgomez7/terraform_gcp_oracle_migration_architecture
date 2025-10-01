variable "project_id" {
  type = string
}

variable "location" {
  type = string
}

variable "user_email" {
  type = string
  description = "Email for the user with OWNER al dataset"
}

variable "datasets" {
  description = "Mapping Datasets to create"
  type = map(object({
    friendly_name               = string
    description                 = string
    labels                      = map(string)
  }))
}
