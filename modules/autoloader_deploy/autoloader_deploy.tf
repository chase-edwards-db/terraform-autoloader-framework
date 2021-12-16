variable "output_table_name" {}
variable "current_user" {}
variable "latest_lts_dbr_id" {}
variable "smallest_node_id" {}
variable "autoloader_notebook" {
  description = "The generalized framework notebook used to create each job."
}

terraform {
  required_providers {
    databricks = {
      source = "databrickslabs/databricks"
      version = "0.4.0"
    }
  }
}

# Generate databricks jobs
resource "databricks_job" "this" {
  name = "Terraform Autoloader ${var.output_table_name} (${var.current_user.alphanumeric})"

  new_cluster {
    num_workers   = 4
    spark_version = var.latest_lts_dbr_id
    node_type_id  = var.smallest_node_id
    # cluster_pool_id = databricks_instance_pool.smallest_nodes.id
  }

  notebook_task {
    notebook_path = var.autoloader_notebook.path

    base_parameters = {
        static_sync_id  = 100000001
        base_table_name = var.output_table_name
    }
  }
}