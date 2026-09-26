variable "aws_region" {
  description = "AWS Region to deploy the infrastructure"
  type        = string
  default     = "us-east-1"
}

variable "vpc_id" {
  description = "VPC ID where the cluster will be deployed"
  type        = string
}

variable "subnet_ids" {
  description = "List of 3 Subnet IDs across different AZs for cluster nodes"
  type        = list(string)

  validation {
    condition     = length(var.subnet_ids) == 3
    error_message = "Exactly 3 subnet IDs across 3 Availability Zones are required."
  }
}

variable "environment" {
  description = "Deployment environment name"
  type        = string
  default     = "production"
}

variable "instance_type" {
  description = "EC2 Instance Type for Patroni nodes"
  type        = string
  default     = "r6i.xlarge"
}

variable "key_name" {
  description = "EC2 SSH Key Pair name"
  type        = string
}

variable "root_volume_size" {
  description = "Root Volume Size in GB"
  type        = number
  default     = 50
}

variable "data_volume_size" {
  description = "PostgreSQL Data Volume Size in GB (gp3)"
  type        = number
  default     = 200
}

variable "allowed_client_cidr_blocks" {
  description = "CIDR blocks allowed to access PostgreSQL (Port 5432) and PgBouncer (Port 6432)"
  type        = list(string)
  default     = ["10.0.0.0/16"]
}
