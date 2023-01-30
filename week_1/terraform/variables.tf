locals {
  data_lake_bucket = "zoomcamp_data_lake"
}

variable "gcp_credential_path" {
  description = "Path to GCP credential file"
  type = string
  default = "/home/user/zoomcamp-de-375610-2d5da5be475f.json"
}

variable "gcp_project_id" {
  description = "GCP project id"
  type = string
  default = "zoomcamp-de-375610"
}

variable "gcp_region" {
  description = "Region for GCP resources https://cloud.google.com/about/locations"
  type = string
  default = "europe-west6"
}

variable "bigquery_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "ny_taxi_data"
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}
