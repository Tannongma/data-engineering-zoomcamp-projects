variable "project_id" {
  type        = string
  description = "ID of the Google Project"
  default     = "bike-sharing-472017"
}

variable "region" {
  type        = string
  description = "Default Region"
  default     = "europe-west1"
}

variable "zone" {
  type        = string
  description = "Default Zone"
  default     = "europe-west1-b"
}

variable "server_name" {
  type        = string
  description = "Name of server"
  default     = "bike-sharing-server"
}

variable "machine_type" {
  type        = string
  description = "Machine Type"
  default     = "e2-micro"
}

variable "credentials_file_path" {
  type        = string
  description = "Path to GCP credentials file"
  default     = "/home/bonaventure/gcp-keys.json"
}