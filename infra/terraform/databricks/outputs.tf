output "job_id" {
  value = databricks_job.tri.id
}

output "job_url" {
  value = "${var.workspace_url}/jobs/${databricks_job.tri.id}"
}

output "catalog_name" {
  value = databricks_catalog.tri.name
}

output "workspace_code_folder" {
  value = local.ws_folder
}
