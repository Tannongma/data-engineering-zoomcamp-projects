terraform {
  backend "gcs" {
    bucket = "bikesharing-terraform-state-bucket"
    prefix = "bike-sharing/backend"
  }
}
