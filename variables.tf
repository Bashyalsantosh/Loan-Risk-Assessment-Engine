variable "aws_region" {
  description = "Target AWS Region for infrastructure deployment"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment identifier (e.g., staging, production)"
  type        = string
  default     = "production"
}

variable "project_name" {
  description = "Project name tag and naming prefix"
  type        = string
  default     = "loan-risk-engine"
}

# Network Topology Variables
variable "vpc_cidr" {
  description = "CIDR block for the dedicated VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "List of Availability Zones for high-availability multi-AZ deployment"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

# EKS Cluster Variables
variable "eks_cluster_version" {
  description = "Kubernetes control plane version"
  type        = string
  default     = "1.30"
}

variable "eks_node_instance_types" {
  description = "Worker node EC2 instance types"
  type        = list(string)
  default     = ["t3.xlarge"]
}

variable "eks_min_capacity" {
  description = "Minimum number of worker nodes"
  type        = number
  default     = 3
}

variable "eks_max_capacity" {
  description = "Maximum number of worker nodes for autoscaling"
  type        = number
  default     = 10
}

variable "eks_desired_capacity" {
  description = "Initial desired number of worker nodes"
  type        = number
  default     = 3
}

# Kafka MSK Variables
variable "kafka_version" {
  description = "Apache Kafka cluster version"
  type        = string
  default     = "3.6.0"
}

variable "kafka_broker_instance_type" {
  description = "Kafka broker node instance type"
  type        = string
  default     = "kafka.m5.large"
}

variable "kafka_ebs_volume_size" {
  description = "EBS storage volume size per broker (in GB)"
  type        = number
  default     = 100
}
