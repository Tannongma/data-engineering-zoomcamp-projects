terraform {
required_version = "~> 1.13"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.40"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  zone        = var.zone
  credentials = file(var.credentials_file_path)
}