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

# VPC's
variable "vpc_app" {
  type = list(string)
  default = ["vpc_app_1", "vpc_app_2"]
}

# Subnets
variable "subnet_app" {
  type = list(string)
  default = ["subnet-0d5ac5d3b65e2950e", "subnet-029ff558e3c118433"]
}

# Security group
variable "security_group" {
  type = string
  default = "sg-0591b309a73f15dd4"
}

#---------------------------------------------------------------
# Component
variable "component" {
  type = string
  default = "isin-search"
}

#---------------------------------------------------------------
# Secret data

variable "secret_mail_map" {  
  default = {    
    "smtp_server": "dummy",
    "smtp_port": "dummy",
    "smtp_user": "dummy",
    "smtp_password": "dummy"
  }
  type = map(string)
  sensitive = true
}

variable "secret_db_map" { 
    default = {
    "username" = "dummy",
    "password" = "dummy",
    "host" = "rds-dev-back-8.cwejzmqeyiuq.us-east-1.rds.amazonaws.com",    
    "port" = "dummy"
  }  
  type = map(string)
  sensitive = true
}

variable "secrets_kms_key" {
  type    = string
  default = "dummy"
  sensitive = true
}

#---------------------------------------------------------------
# Mails

variable "mail_from"{
  type = string
  default = "isin-lookup-component-dev@precia.co"  
}

variable "mail_to"{
  type = string
  default = "flash2-dev@precia.co"  
}

#---------------------------------------------------------------
# Bakcup

variable "backup_folder"{
  type = string
  default = "backup"  
}
