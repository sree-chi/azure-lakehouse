variable "subscription_id" {
  description = "Azure subscription ID (az account show --query id -o tsv)."
  type        = string
}

variable "location" {
  description = "Azure region. Azure for Students restricts regions by policy; see README step 2."
  type        = string
  default     = "centralus"
}

variable "prefix" {
  description = "Short lowercase prefix for resource names (letters/numbers, max 6 chars)."
  type        = string
  default     = "trilh"

  validation {
    condition     = can(regex("^[a-z0-9]{2,6}$", var.prefix))
    error_message = "prefix must be 2-6 lowercase letters or digits."
  }
}

variable "databricks_sku" {
  description = "Databricks tier. 'trial' = 14 days of free Premium DBUs, then bills as Premium. Standard tier can no longer be created."
  type        = string
  default     = "trial"

  validation {
    condition     = contains(["trial", "premium"], var.databricks_sku)
    error_message = "databricks_sku must be 'trial' or 'premium'."
  }
}

variable "sql_database_name" {
  description = "Name of the Azure SQL database (created later by Bicep; Terraform only needs the name for outputs)."
  type        = string
  default     = "tri_gold"
}

variable "client_ip" {
  description = "Your public IP, so Power BI Desktop / sqlcmd on your laptop can reach Azure SQL. Empty = no rule."
  type        = string
  default     = ""
}

variable "budget_amount" {
  description = "Monthly USD budget for the resource group. Alerts at 50/80/100%."
  type        = number
  default     = 20
}

variable "alert_email" {
  description = "Email for budget alerts. Empty = no budget created."
  type        = string
  default     = ""
}
