terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project     = var.project
  region      = var.location
  credentials = file("/home/andrewgallagher/.google/credentials/boston-blue-bikes-065bd2f0e8da.json")
}

# Data Lake Bucket
# ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "boston_blue_bikes_data_lake"
  location      = var.location
  force_destroy = true

  storage_class               = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }
}

# Data Warehouses - Raw
# ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "raw_data" {
  dataset_id = "raw_data"
  project    = var.project
  location   = var.location
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = "staging"
  project    = var.project
  location   = var.location
}

resource "google_bigquery_dataset" "production" {
  dataset_id = "production"
  project    = var.project
  location   = var.location
}
