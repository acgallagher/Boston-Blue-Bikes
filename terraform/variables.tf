variable "project" {
  description = "Google Cloud Platform project id"
  default     = "boston-blue-bikes"
}

variable "location" {
  description = "Location for GCP resources"
  default     = "US"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for the data lake bucket"
  default     = "STANDARD"
}

