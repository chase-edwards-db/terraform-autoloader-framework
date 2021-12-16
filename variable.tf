variable "HOST" {
    type        = string
    description = "Databricks host, extracted from environment variable."
}

variable "TOKEN" {
    type        = string
    description = "Databricks workspace token, extracted from environment variable."
}

variable "output_table_names" {
    type        = list(string)
    description = "List of output table names mapped to Autoloader jobs."
}