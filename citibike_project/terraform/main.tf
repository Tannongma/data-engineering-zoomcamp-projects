resource "google_compute_instance" "dev-vm" {
  provider     = google
  count        = 1
  name         = "${var.server_name}-${count.index}"
  machine_type = var.machine_type
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
    }
  }
  network_interface {
    network = "default"
    access_config {
      // Ephemeral public IP
    }
  }
  metadata_startup_script = file("startup.sh")

  tags = ["dev-vm-server"]
}


resource "google_storage_bucket" "citibike-static" {
 name          = "gcs-bucket-${var.project_id}-static"
 location      = "EU"
 storage_class = "STANDARD"

 uniform_bucket_level_access = true
}