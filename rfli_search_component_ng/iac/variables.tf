# AWS Region
variable "aws_region" {
  type = string
  default = "us-east-1"
}

# Environment
variable "environment" {
  type = string
  default = "dev"
}

# CloudFront Id
variable "cloudfront_id" {
  type = string
  default = "EFIXMJFX1EAZA"
}

#---------------------------------------------------------------
# Component
variable "component" {
  type = string
  default = "isin-search"
}

#---------------------------------------------------------------
# Conteniner Repository

variable "container_repo_name" {
  type = string
  default = "Contenedor_mf_intradia_js"
}

variable "container_repo_url" {
  type = string
  default = "https://git-codecommit.us-east-1.amazonaws.com/v1/repos/Contenedor_mf_intradia_js"
}

variable "container_repo_branch" {
  type = string
  default = "developer"
}