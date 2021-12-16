terraform {
  required_providers {
    databricks = {
      source = "databrickslabs/databricks"
      version = "0.4.0"
    }
  }
}

provider "databricks" {
  host  = var.HOST
  token = var.TOKEN
}

# Data sources for use with resources
data "databricks_current_user" "me" {}
data "databricks_spark_version" "latest_lts_dbr" {
    long_term_support = true
}
data "databricks_node_type" "smallest_node" {
  min_cores  = 8
  local_disk = true
  category   = "Compute Optimized"
}

# Define framework resources
resource "databricks_notebook" "ddl" {
  source = "${path.root}/job-template/autoloader_stream_bronze.py"
  path = "${data.databricks_current_user.me.home}/terraform/autoloader_stream_bronze.py"
}

# Generate parameterized autoloader jobs
module "autoloader_deploy" {

  for_each = toset(var.output_table_names)
  source = "./modules/autoloader_deploy/"

  autoloader_notebook = databricks_notebook.ddl

  output_table_name = "${each.value}"
  current_user      = data.databricks_current_user.me
  latest_lts_dbr_id = data.databricks_spark_version.latest_lts_dbr.id
  smallest_node_id  = data.databricks_node_type.smallest_node.id
}