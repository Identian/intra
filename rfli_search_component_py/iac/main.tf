
################################################################################################################################
# Account_id
################################################################################################################################
data "aws_caller_identity" "AccountID" {}

################################################################################################################################
# Data sources
################################################################################################################################

#------------------------------------------------------------------------------------------------------------------------------
# Zip files

data "archive_file" "trigger_intra" {
  type = "zip"
  source_file = format("../code/lbd-rfli-trigger-%s-intra/lambda_function.py", var.component)
  output_path = format("lbd-rfli-trigger-%s-intra.zip", var.component)
}

data "archive_file" "trigger_eod" {
  type = "zip"
  source_file = format("../code/lbd-rfli-trigger-%s-eod/lambda_function.py", var.component)
  output_path = format("lbd-rfli-trigger-%s-eod.zip", var.component)
}

data "archive_file" "layer_titles" {
  type = "zip"
  source_file = format("../code/lbd-rfli-layer-%s-titles/lambda_function.py", var.component)
  output_path = format("lbd-rfli-layer-%s-titles.zip", var.component)
}

data "archive_file" "layer_issuers" {
  type = "zip"
  source_file = format("../code/lbd-rfli-layer-%s-issuers/lambda_function.py", var.component)
  output_path = format("lbd-rfli-layer-%s-issuers.zip", var.component)
}

data "archive_file" "layer_isines" {
  type = "zip"
  source_file = format("../code/lbd-rfli-layer-%s-isines/lambda_function.py", var.component)
  output_path = format("lbd-rfli-layer-%s-isines.zip", var.component)
}

#------------------------------------------------------------------------------------------------------------------------------
# SSM Parameter

data "aws_ssm_parameter" "eod_rate_time"{
  name = format("ps-%s-rfli-eod-rate-time", var.environment)
}

data "aws_ssm_parameter" "prelim_eod_time"{
  name = format("ps-%s-rfli-prelim-eod-time", var.environment)
}

data "aws_ssm_parameter" "market_close_time"{
  name = format("ps-%s-rfli-market-close-time", var.environment)
}

data "aws_ssm_parameter" "final_eod_time"{
  name = format("ps-%s-rfli-final-eod-time", var.environment)
}

data "aws_ssm_parameter" "market_open_time"{
  name = format("ps-%s-rfli-market-open-time", var.environment)
}

data "aws_ssm_parameter" "intra_rate_time"{
  name = format("ps-%s-rfli-intra-rate-time", var.environment)
}

#------------------------------------------------------------------------------------------------------------------------------
# DynamoDB tables

data "aws_dynamodb_table" "data_version_intra"{
  name = "dnb-rfli-data-version-intra"
}

#------------------------------------------------------------------------------------------------------------------------------
# API Gateway

data "aws_api_gateway_rest_api" "intradiapp"{
  name = format("ag-%s-rfli-intradiapp", var.environment)
}

data "aws_api_gateway_authorizers" "intradiapp" {
  rest_api_id = data.aws_api_gateway_rest_api.intradiapp.id
}

data "aws_api_gateway_authorizer" "intradiapp" {
  rest_api_id   = data.aws_api_gateway_rest_api.intradiapp.id
  authorizer_id = data.aws_api_gateway_authorizers.intradiapp.ids[0]
}

#------------------------------------------------------------------------------------------------------------------------------
# SNS

data "aws_sns_topic" "trigger_all_intra" {
  name = format("sns-%s-rfli-trigger-all-intra", var.environment)
}

data "aws_sns_topic" "trigger_all_eod" {
  name = format("sns-%s-rfli-trigger-all-eod", var.environment)
}

#------------------------------------------------------------------------------------------------------------------------------
# KMS

data "aws_kms_key" "secrets" {
  key_id = var.secrets_kms_key
}

#------------------------------------------------------------------------------------------------------------------------------
# Lambda

data "aws_lambda_function" "trigger_init" {
  function_name = format("lbd-%s-rfli-trigger-init-data-version", var.environment)
}

#------------------------------------------------------------------------------------------------------------------------------
# Role

data "aws_iam_role" "trigger_init" {
  name = format("rol-%s-lbd-rfli-trigger-init-data-version", var.environment)
}

################################################################################################################################
# Secrets
################################################################################################################################

#------------------------------------------------------------------------------------------------------------------------------
# mail

resource "aws_secretsmanager_secret" "mail" {
  name = format("sm-%s-rfli-%s-mail", var.environment, var.component)
  description = format("Secreto con las credenciales de mail para el componente %s", var.component)
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "mail_value" {
  secret_id     = aws_secretsmanager_secret.mail.id
  secret_string = jsonencode(var.secret_mail_map)
}

#------------------------------------------------------------------------------------------------------------------------------
# DB

resource "aws_secretsmanager_secret" "db" {
  name = format("sm-%s-rfli-%s-db", var.environment, var.component)
  description = format("Secreto con las credenciales de db para el componente %s", var.component)
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "db_value" {
  secret_id     = aws_secretsmanager_secret.db.id
  secret_string = jsonencode(var.secret_db_map)
}

################################################################################################################################
# DynamoDB Tables
################################################################################################################################

resource "aws_dynamodb_table" "all_isines" {
  name             = format("dnb-rfli-%s-all-isines", var.component)
  hash_key         = "isin"
  billing_mode     = "PAY_PER_REQUEST"
  attribute {
    name = "isin"
    type = "S"
  }
}

resource "aws_dynamodb_table" "issuers" {
  name             = format("dnb-rfli-%s-issuers", var.component)
  hash_key         = "issuer"
  billing_mode     = "PAY_PER_REQUEST"
  attribute {
    name = "issuer"
    type = "S"
  }
}

################################################################################################################################
# S3 to code
################################################################################################################################

resource "aws_s3_bucket" "component_code" {
  bucket = format("s3-%s-rfli-%s-code", var.environment, var.component)
  force_destroy = true
}

################################################################################################################################
# General policies
################################################################################################################################

resource "aws_iam_policy" "log" {
  name = format("policy-rfli-%s-log", var.component)
  description = format("Politica para acceder a log del componente %s", var.component)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = [
              "logs:CreateLogGroup",
              "logs:CreateLogStream",
              "logs:PutLogEvents",
              "logs:DescribeLogStreams"
            ]
            Effect = "Allow"
            Resource = ["*"]
        }
    ]
  })
}

resource "aws_iam_policy" "vpc" {
  name = format("policy-rfli-%s-vpc", var.component)
  description = format("Politica para acceder a VPC del componente %s", var.component)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = [
              "ec2:DescribeVpcEndpoints",
              "ec2:DescribeRouteTables",
              "ec2:CreateNetworkInterface",
              "ec2:CreateTags",
              "ec2:DeleteNetworkInterface",
              "ec2:DescribeNetworkInterfaces",
              "ec2:DescribeSecurityGroups",
              "ec2:DescribeSubnets",
              "ec2:DescribeVpcAttribute"
            ]
            Effect = "Allow"
            Resource = ["*"]
        }
    ]
  })
}

resource "aws_iam_policy" "code_bucket" {
  name = format("policy-rfli-%s-bucket-code", var.component)
  description = format("Politica para acceder al S3 bucket con el codigo del componente %s", var.component)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = [
              "s3:GetObject",
              "s3:ListBucket"
            ]
            Effect = "Allow"
            Resource = [
              "${aws_s3_bucket.component_code.arn}",
              "${aws_s3_bucket.component_code.arn}/*"
            ]
        }
    ]
  })
}

resource "aws_iam_policy" "secrets" {
  name = format("policy-rfli-%s-secrets", var.component)
  description = format("Politica para acceder a los secretos del componente %s", var.component)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = ["secretsmanager:GetSecretValue"]
            Effect = "Allow"
            Resource = [
              "${aws_secretsmanager_secret.mail.arn}",
              "${aws_secretsmanager_secret.db.arn}"
            ]
        },
        {
            Action = ["kms:Decrypt"]
            Effect = "Allow"
            Resource = [
              "${data.aws_kms_key.secrets.arn}"
            ]
        }
    ]
  })
}

################################################################################################################################
# Glue Jobs
################################################################################################################################

#------------------------------------------------------------------------------------------------------------------------------
# glue-*-rfli-etl-component-intra

# ROL:
resource "aws_iam_role" "etl_intra" {
  name = format("rol-%s-glue-rfli-etl-%s-intra", var.environment, var.component)

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = "sts:AssumeRole"
            Effect = "Allow"
            Sid    = ""
            Principal = {
                Service = "glue.amazonaws.com"
            } 
        },
    ]
  })
}

# Code to bucket
resource "aws_s3_object" "etl_intra_code" {
  bucket = aws_s3_bucket.component_code.bucket
  key    = format("glue-rfli-etl-%s-intra.py", var.component)
  source = format("../code/glue-rfli-etl-%s-intra.py", var.component)
  force_destroy = true
}

#Glue
resource "aws_glue_job" "etl_intra" {
  name     = format("glue-%s-rfli-etl-%s-intra", var.environment, var.component)
  role_arn = "${aws_iam_role.etl_intra.arn}"
  max_capacity = 1
  glue_version = "3.0"
  execution_class = "STANDARD"
  max_retries = 0
  timeout = 10
  connections = var.vpc_app
  command {
    name = "pythonshell"
    script_location = "s3://${aws_s3_object.etl_intra_code.bucket}/${aws_s3_object.etl_intra_code.key}"
    python_version = "3.9"
  }
  execution_property {
    max_concurrent_runs = 1
  }
  default_arguments = {
    "library-set" = "analytics"
    "--job-language" = "python"
    "--DB_QUERY_TIMEOUT" = "300"
    "--DESTINATION_MAIL" = "${var.mail_to}"
    "--FLASH_ORIGIN_DB" = "${aws_secretsmanager_secret.db.name}"
    "--INTRA_RATE_TIME" = "${data.aws_ssm_parameter.intra_rate_time.name}"
    "--MARKET_CLOSE_TIME" = "${data.aws_ssm_parameter.market_close_time.name}"
    "--MARKET_OPEN_TIME" = "${data.aws_ssm_parameter.market_open_time.name}"
    "--ORIGIN_MAIL" = "${var.mail_from}"
    "--PRE_EOD_TIME" = "${data.aws_ssm_parameter.prelim_eod_time.name}"
    "--SMTP_CREDENTIALS" = "${aws_secretsmanager_secret.mail.name}"
  }
}

# Policy
resource "aws_iam_policy" "specify_etl_intra" {
  name = format("policy-rfli-etl-%s-intra", var.component)
  description = format("Politicas especificas para glue-*-rfli-etl-%s-intra", var.component)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = [
              "glue:GetConnections",
              "glue:GetConnection"
            ]
            Effect = "Allow"
            Resource = [
              "*"
            ]
        },
        {
            Action = [
              "ssm:GetParameters",
              "ssm:GetParameter"
            ]
            Effect = "Allow"
            Resource = [
              "${data.aws_ssm_parameter.prelim_eod_time.arn}",
              "${data.aws_ssm_parameter.market_close_time.arn}",
              "${data.aws_ssm_parameter.market_open_time.arn}",
              "${data.aws_ssm_parameter.intra_rate_time.arn}"
            ]
        },
        {
            "Action"= [
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:BatchWriteItem",
                "dynamodb:Scan"
            ],
            "Effect"= "Allow",
            "Resource"= [
              "${data.aws_dynamodb_table.data_version_intra.arn}",
              "${aws_dynamodb_table.all_isines.arn}"             
            ]
        }
    ]
  })
}

# Add policies
resource "aws_iam_role_policy_attachment" "etl_intra_rol_with_log_policy" {
  role       = aws_iam_role.etl_intra.name
  policy_arn = aws_iam_policy.log.arn
}

resource "aws_iam_role_policy_attachment" "etl_intra_rol_with_vpc_policy" {
  role       = aws_iam_role.etl_intra.name
  policy_arn = aws_iam_policy.vpc.arn
}

resource "aws_iam_role_policy_attachment" "etl_intra_rol_with_code_bucket_policy" {
  role       = aws_iam_role.etl_intra.name
  policy_arn = aws_iam_policy.code_bucket.arn
}

resource "aws_iam_role_policy_attachment" "etl_intra_rol_with_secrets_policy" {
  role       = aws_iam_role.etl_intra.name
  policy_arn = aws_iam_policy.secrets.arn
}

resource "aws_iam_role_policy_attachment" "etl_intra_rol_with_specify_policy" {
  role       = aws_iam_role.etl_intra.name
  policy_arn = aws_iam_policy.specify_etl_intra.arn
}

#------------------------------------------------------------------------------------------------------------------------------
# glue-*-rfli-etl-component-eod

# ROL:
resource "aws_iam_role" "etl_eod" {
  name = format("rol-%s-glue-rfli-etl-%s-eod", var.environment, var.component)

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = "sts:AssumeRole"
            Effect = "Allow"
            Sid    = ""
            Principal = {
                Service = "glue.amazonaws.com"
            } 
        },
    ]
  })
}

# Code to bucket
resource "aws_s3_object" "etl_eod_code" {
  bucket = aws_s3_bucket.component_code.bucket
  key    = format("glue-rfli-etl-%s-eod.py", var.component)
  source = format("../code/glue-rfli-etl-%s-eod.py", var.component)
  force_destroy = true
}

#Glue
resource "aws_glue_job" "etl_eod" {
  name     = format("glue-%s-rfli-etl-%s-eod", var.environment, var.component)
  role_arn = "${aws_iam_role.etl_eod.arn}"
  max_capacity = 1
  glue_version = "3.0"
  execution_class = "STANDARD"
  max_retries = 0
  timeout = 10
  connections = var.vpc_app
  command {
    name = "pythonshell"
    script_location = "s3://${aws_s3_object.etl_eod_code.bucket}/${aws_s3_object.etl_eod_code.key}"
    python_version = "3.9"
  }
  execution_property {
    max_concurrent_runs = 1
  }
  default_arguments = {
    "library-set" = "analytics"
    "--job-language" = "python"
    "--DB_QUERY_TIMEOUT" = "300"
    "--DESTINATION_MAIL" = "${var.mail_to}"
    "--FLASH_ORIGIN_DB" = "${aws_secretsmanager_secret.db.name}"
    "--FINAL_EOD_TIME" = "${data.aws_ssm_parameter.final_eod_time.name}"
    "--ORIGIN_MAIL" = "${var.mail_from}"
    "--SMTP_CREDENTIALS" = "${aws_secretsmanager_secret.mail.name}"
  }
}

# Policy
resource "aws_iam_policy" "specify_etl_eod" {
  name = format("policy-rfli-etl-%s-eod", var.component)
  description = format("Politicas especificas para glue-*-rfli-etl-%s-eod", var.component)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = [
              "glue:GetConnections",
              "glue:GetConnection"
            ]
            Effect = "Allow"
            Resource = [
              "*"
            ]
        },
        {
            Action = [
              "ssm:GetParameters",
              "ssm:GetParameter"
            ]
            Effect = "Allow"
            Resource = [
              "${data.aws_ssm_parameter.final_eod_time.arn}"
            ]
        },
        {
            "Action"= [
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:BatchWriteItem",
                "dynamodb:Scan"
            ],
            "Effect"= "Allow",
            "Resource"= [
              "${data.aws_dynamodb_table.data_version_intra.arn}",
              "${aws_dynamodb_table.all_isines.arn}"             
            ]
        }
    ]
  })
}

# Add policies
resource "aws_iam_role_policy_attachment" "etl_eod_rol_with_log_policy" {
  role       = aws_iam_role.etl_eod.name
  policy_arn = aws_iam_policy.log.arn
}

resource "aws_iam_role_policy_attachment" "etl_eod_rol_with_vpc_policy" {
  role       = aws_iam_role.etl_eod.name
  policy_arn = aws_iam_policy.vpc.arn
}

resource "aws_iam_role_policy_attachment" "etl_eod_rol_with_code_bucket_policy" {
  role       = aws_iam_role.etl_eod.name
  policy_arn = aws_iam_policy.code_bucket.arn
}

resource "aws_iam_role_policy_attachment" "etl_eod_rol_with_secrets_policy" {
  role       = aws_iam_role.etl_eod.name
  policy_arn = aws_iam_policy.secrets.arn
}

resource "aws_iam_role_policy_attachment" "etl_eod_rol_with_specify_policy" {
  role       = aws_iam_role.etl_eod.name
  policy_arn = aws_iam_policy.specify_etl_eod.arn
}

#------------------------------------------------------------------------------------------------------------------------------
# glue-*-rfli-init-component

# ROL:
resource "aws_iam_role" "etl_init" {
  name = format("role-%s-glue-rfli-init-%s", var.environment, var.component)

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = "sts:AssumeRole"
            Effect = "Allow"
            Sid    = ""
            Principal = {
                Service = "glue.amazonaws.com"
            } 
        },
    ]
  })
}

# Code to bucket
resource "aws_s3_object" "etl_init_code" {
  bucket = aws_s3_bucket.component_code.bucket
  key    = format("glue-rfli-init-%s.py", var.component)
  source = format("../code/glue-rfli-init-%s.py", var.component)
  force_destroy = true
}

#Glue
resource "aws_glue_job" "etl_init" {
  name     = format("glue-%s-rfli-init-%s", var.environment, var.component)
  role_arn = "${aws_iam_role.etl_init.arn}"
  max_capacity = 1
  glue_version = "3.0"
  execution_class = "STANDARD"
  max_retries = 0
  timeout = 10
  connections = var.vpc_app
  command {
    name = "pythonshell"
    script_location = "s3://${aws_s3_object.etl_init_code.bucket}/${aws_s3_object.etl_init_code.key}"
    python_version = "3.9"
  }
  execution_property {
    max_concurrent_runs = 1
  }
  default_arguments = {
    "library-set" = "analytics"
    "--job-language" = "python"
    "--DB_QUERY_TIMEOUT" = "300"
    "--DESTINATION_MAIL" = "${var.mail_to}"
    "--FLASH_ORIGIN_DB" = "${aws_secretsmanager_secret.db.name}"
    "--ORIGIN_MAIL" = "${var.mail_from}"
    "--SMTP_CREDENTIALS" = "${aws_secretsmanager_secret.mail.name}"
  }
}

# Policy
resource "aws_iam_policy" "specify_etl_init" {
  name = format("policy-rfli-init-%s", var.component)
  description = format("Politicas especificas para glue-*-rfli-init-%s", var.component)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = [
              "glue:GetConnections",
              "glue:GetConnection"
            ]
            Effect = "Allow"
            Resource = [
              "*"
            ]
        },
        {
            "Action"= [
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:DeleteItem",
                "dynamodb:BatchWriteItem",
                "dynamodb:Scan",
                "dynamodb:DescribeTable"
            ],
            "Effect"= "Allow",
            "Resource"= [
              "${aws_dynamodb_table.issuers.arn}"  ,
              "${aws_dynamodb_table.all_isines.arn}"             
            ]
        }
    ]
  })
}

# Add policies
resource "aws_iam_role_policy_attachment" "etl_init_rol_with_log_policy" {
  role       = aws_iam_role.etl_init.name
  policy_arn = aws_iam_policy.log.arn
}

resource "aws_iam_role_policy_attachment" "etl_init_rol_with_vpc_policy" {
  role       = aws_iam_role.etl_init.name
  policy_arn = aws_iam_policy.vpc.arn
}

resource "aws_iam_role_policy_attachment" "etl_init_rol_with_code_bucket_policy" {
  role       = aws_iam_role.etl_init.name
  policy_arn = aws_iam_policy.code_bucket.arn
}

resource "aws_iam_role_policy_attachment" "etl_init_rol_with_secrets_policy" {
  role       = aws_iam_role.etl_init.name
  policy_arn = aws_iam_policy.secrets.arn
}

resource "aws_iam_role_policy_attachment" "etl_init_rol_with_specify_policy" {
  role       = aws_iam_role.etl_init.name
  policy_arn = aws_iam_policy.specify_etl_init.arn
}

################################################################################################################################
# Lambdas trigger
################################################################################################################################

#------------------------------------------------------------------------------------------------------------------------------
# lbd-*-rfli-trigger-component-intra

# ROL:
resource "aws_iam_role" "trigger_intra" {
  name = format("rol-%s-lbd-rfli-trigger-%s-intra", var.environment, var.component)

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = "sts:AssumeRole"
            Effect = "Allow"
            Sid    = ""
            Principal = {
                Service = "lambda.amazonaws.com"
            } 
        },
    ]
  })
}

# Lambda
resource "aws_lambda_function" "trigger_intra" {
  filename      = data.archive_file.trigger_intra.output_path
  source_code_hash = data.archive_file.trigger_intra.output_base64sha256
  function_name = format("lbd-%s-rfli-trigger-%s-intra", var.environment, var.component)
  role          = aws_iam_role.trigger_intra.arn
  handler       = "lambda_function.lambda_handler"  
  runtime       = "python3.9"

   vpc_config {    
    subnet_ids         = var.subnet_app
    security_group_ids = [var.security_group]
  }  

  environment{
    variables = {      
      JOB_NAME = aws_glue_job.etl_intra.name
    }
  }

  depends_on = [aws_iam_role.trigger_intra]
}

# Policy
resource "aws_iam_policy" "specify_trigger_intra" {
  name = format("policy-rfli-trigger-%s-intra", var.component)
  description = format("Politicas especificas para lbd-*-rfli-trigger-%s-intra", var.component)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
          "Action": [
              "glue:StartJobRun"
          ],
          "Effect": "Allow",
          "Resource": [
            "${aws_glue_job.etl_intra.arn}"
          ]
      }
    ]
  })
}

# Add policies
resource "aws_iam_role_policy_attachment" "trigger_intra_rol_with_log_policy" {
  role       = aws_iam_role.trigger_intra.name
  policy_arn = aws_iam_policy.log.arn
}

resource "aws_iam_role_policy_attachment" "trigger_intra_rol_with_vpc_policy" {
  role       = aws_iam_role.trigger_intra.name
  policy_arn = aws_iam_policy.vpc.arn
}

resource "aws_iam_role_policy_attachment" "trigger_intra_with_specify_policy" {
  role       = aws_iam_role.trigger_intra.name
  policy_arn = aws_iam_policy.specify_trigger_intra.arn
}

# Add to topic
resource "aws_lambda_permission" "trigger_all_intra" {
  statement_id  = "AllowExecutionFromSNS"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_intra.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = data.aws_sns_topic.trigger_all_intra.arn
}

resource "aws_sns_topic_subscription" "trigger_all_intra" {
  topic_arn = data.aws_sns_topic.trigger_all_intra.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.trigger_intra.arn

  depends_on = [ aws_lambda_permission.trigger_all_intra ]
}

#------------------------------------------------------------------------------------------------------------------------------
# lbd-*-rfli-trigger-component-eod

# ROL:
resource "aws_iam_role" "trigger_eod" {
  name = format("rol-%s-lbd-rfli-trigger-%s-eod", var.environment, var.component)

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = "sts:AssumeRole"
            Effect = "Allow"
            Sid    = ""
            Principal = {
                Service = "lambda.amazonaws.com"
            } 
        },
    ]
  })
}

# Lambda
resource "aws_lambda_function" "trigger_eod" {
  filename      = data.archive_file.trigger_eod.output_path
  source_code_hash = data.archive_file.trigger_eod.output_base64sha256
  function_name = format("lbd-%s-rfli-trigger-%s-eod", var.environment, var.component)
  role          = aws_iam_role.trigger_eod.arn
  handler       = "lambda_function.lambda_handler"  
  runtime       = "python3.9"

   vpc_config {    
    subnet_ids         = var.subnet_app
    security_group_ids = [var.security_group]
  }  

  environment{
    variables = {      
      JOB_NAME = aws_glue_job.etl_eod.name
    }
  }

  depends_on = [
    aws_iam_role.trigger_eod,
    data.archive_file.trigger_eod
  ]
}

# Policy
resource "aws_iam_policy" "specify_trigger_eod" {
  name = format("policy-rfli-trigger-%s-eod", var.component)
  description = format("Politicas especificas para lbd-*-rfli-trigger-%s-eod", var.component)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
          "Action": [
              "glue:StartJobRun"
          ],
          "Effect": "Allow",
          "Resource": [
            "${aws_glue_job.etl_eod.arn}"
          ]
      }
    ]
  })
}

# Add policies
resource "aws_iam_role_policy_attachment" "trigger_eod_rol_with_log_policy" {
  role       = aws_iam_role.trigger_eod.name
  policy_arn = aws_iam_policy.log.arn
}

resource "aws_iam_role_policy_attachment" "trigger_eod_rol_with_vpc_policy" {
  role       = aws_iam_role.trigger_eod.name
  policy_arn = aws_iam_policy.vpc.arn
}

resource "aws_iam_role_policy_attachment" "trigger_eod_with_specify_policy" {
  role       = aws_iam_role.trigger_eod.name
  policy_arn = aws_iam_policy.specify_trigger_eod.arn
}

# Add to topic
resource "aws_lambda_permission" "trigger_all_eod" {
  statement_id  = "AllowExecutionFromSNS"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_eod.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = data.aws_sns_topic.trigger_all_eod.arn
}

resource "aws_sns_topic_subscription" "trigger_all_eod" {
  topic_arn = data.aws_sns_topic.trigger_all_eod.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.trigger_eod.arn

  depends_on = [ aws_lambda_permission.trigger_all_eod ]
}

################################################################################################################################
# Lambdas layer
################################################################################################################################

#------------------------------------------------------------------------------------------------------------------------------
# lbd-*-rfli-layer-component-issuers

# ROL:
resource "aws_iam_role" "layer_issuers" {
  name = format("rol-%s-lbd-layer-rfli-%s-issuers", var.environment, var.component)

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = "sts:AssumeRole"
            Effect = "Allow"
            Sid    = ""
            Principal = {
                Service = "lambda.amazonaws.com"
            } 
        },
    ]
  })
}

# Lambda
resource "aws_lambda_function" "layer_issuers" {
  filename      = data.archive_file.layer_issuers.output_path
  source_code_hash = data.archive_file.layer_issuers.output_base64sha256
  function_name = format("lbd-%s-rfli-layer-%s-issuers", var.environment, var.component)
  role          = aws_iam_role.layer_issuers.arn
  handler       = "lambda_function.lambda_handler"  
  runtime       = "python3.9"

   vpc_config {    
    subnet_ids         = var.subnet_app
    security_group_ids = [var.security_group]
  }  

  depends_on = [aws_iam_role.layer_issuers]
}

# Policy
resource "aws_iam_policy" "specify_layer_issuers" {
  name = format("policy-rfli-layer-%s-issuers", var.component)
  description = format("Politicas especificas para lbd-*-rfli-layer-%s-issuers", var.component)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        "Action"= [
            "dynamodb:Scan"
        ],
        "Effect"= "Allow",
        "Resource"= [
          "${aws_dynamodb_table.issuers.arn}"             
        ]
      }
    ]
  })
}

# Add policies
resource "aws_iam_role_policy_attachment" "layer_issuers_rol_with_log_policy" {
  role       = aws_iam_role.layer_issuers.name
  policy_arn = aws_iam_policy.log.arn
}

resource "aws_iam_role_policy_attachment" "layer_issuers_rol_with_vpc_policy" {
  role       = aws_iam_role.layer_issuers.name
  policy_arn = aws_iam_policy.vpc.arn
}

resource "aws_iam_role_policy_attachment" "layer_issuers_with_specify_policy" {
  role       = aws_iam_role.layer_issuers.name
  policy_arn = aws_iam_policy.specify_layer_issuers.arn
}

#------------------------------------------------------------------------------------------------------------------------------------------

# lbd-*-rfli-layer-component-isines

# ROL:
resource "aws_iam_role" "layer_isines" {
  name = format("rol-%s-lbd-layer-rfli-%s-isines", var.environment, var.component)

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = "sts:AssumeRole"
            Effect = "Allow"
            Sid    = ""
            Principal = {
                Service = "lambda.amazonaws.com"
            } 
        },
    ]
  })
}

# Lambda
resource "aws_lambda_function" "layer_isines" {
  filename      = data.archive_file.layer_isines.output_path
  source_code_hash = data.archive_file.layer_isines.output_base64sha256
  function_name = format("lbd-%s-rfli-layer-%s-isines", var.environment, var.component)
  role          = aws_iam_role.layer_isines.arn
  handler       = "lambda_function.lambda_handler"  
  runtime       = "python3.9"

   vpc_config {    
    subnet_ids         = var.subnet_app
    security_group_ids = [var.security_group]
  }  

  environment{
    variables = {      
      MARKET_RATE = data.aws_ssm_parameter.eod_rate_time.name
    }
  }

  depends_on = [aws_iam_role.layer_isines]
}

# Policy
resource "aws_iam_policy" "specify_layer_isines" {
  name = format("policy-rfli-layer-%s-isines", var.component)
  description = format("Politicas especificas para lbd-*-rfli-layer-%s-isines", var.component)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "ssm:GetParameters",
          "ssm:GetParameter"
        ]
        Effect = "Allow"
        Resource = [
          "${data.aws_ssm_parameter.eod_rate_time.arn}"
        ]
      },
      {
        "Action"= [
            "dynamodb:GetItem"
        ],
        "Effect"= "Allow",
        "Resource"= [
          "${aws_dynamodb_table.all_isines.arn}",
          "${data.aws_dynamodb_table.data_version_intra.arn}"           
        ]
      }
    ]
  })
}

# Add policies
resource "aws_iam_role_policy_attachment" "layer_isines_rol_with_log_policy" {
  role       = aws_iam_role.layer_isines.name
  policy_arn = aws_iam_policy.log.arn
}

resource "aws_iam_role_policy_attachment" "layer_isines_rol_with_vpc_policy" {
  role       = aws_iam_role.layer_isines.name
  policy_arn = aws_iam_policy.vpc.arn
}

resource "aws_iam_role_policy_attachment" "layer_isines_with_specify_policy" {
  role       = aws_iam_role.layer_isines.name
  policy_arn = aws_iam_policy.specify_layer_isines.arn
}

#------------------------------------------------------------------------------------------------------------------------------------------
# lbd-*-rfli-layer-component-titles

# ROL:
resource "aws_iam_role" "layer_titles" {
  name = format("rol-%s-lbd-layer-rfli-%s-titles", var.environment, var.component)

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Action = "sts:AssumeRole"
            Effect = "Allow"
            Sid    = ""
            Principal = {
                Service = "lambda.amazonaws.com"
            } 
        },
    ]
  })
}

# Lambda
resource "aws_lambda_function" "layer_titles" {
  filename      = data.archive_file.layer_titles.output_path
  source_code_hash = data.archive_file.layer_titles.output_base64sha256
  function_name = format("lbd-%s-rfli-layer-%s-titles", var.environment, var.component)
  role          = aws_iam_role.layer_titles.arn
  handler       = "lambda_function.lambda_handler"  
  runtime       = "python3.9"
  timeout       = 10

   vpc_config {    
    subnet_ids         = var.subnet_app
    security_group_ids = [var.security_group]
  }  

  environment{
    variables = {      
      MARKET_RATE = data.aws_ssm_parameter.eod_rate_time.name
    }
  }

  depends_on = [aws_iam_role.layer_titles]
}

# Policy
resource "aws_iam_policy" "specify_layer_titles" {
  name = format("policy-rfli-layer-%s-titles", var.component)
  description = format("Politicas especificas para lbd-*-rfli-layer-%s-titles", var.component)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "ssm:GetParameters",
          "ssm:GetParameter"
        ]
        Effect = "Allow"
        Resource = [
          "${data.aws_ssm_parameter.eod_rate_time.arn}"
        ]
      },
      {
        "Action"= [
          "dynamodb:GetItem",
          "dynamodb:Scan"
        ],
        "Effect"= "Allow",
        "Resource"= [
          "${aws_dynamodb_table.all_isines.arn}",
          "${data.aws_dynamodb_table.data_version_intra.arn}"           
        ]
      }
    ]
  })
}

# Add policies
resource "aws_iam_role_policy_attachment" "layer_titles_rol_with_log_policy" {
  role       = aws_iam_role.layer_titles.name
  policy_arn = aws_iam_policy.log.arn
}

resource "aws_iam_role_policy_attachment" "layer_titles_rol_with_vpc_policy" {
  role       = aws_iam_role.layer_titles.name
  policy_arn = aws_iam_policy.vpc.arn
}

resource "aws_iam_role_policy_attachment" "layer_titles_with_specify_policy" {
  role       = aws_iam_role.layer_titles.name
  policy_arn = aws_iam_policy.specify_layer_titles.arn
}

################################################################################################################################
# API Gateway
################################################################################################################################

# Parent resource
resource "aws_api_gateway_resource" "component" {
  rest_api_id = data.aws_api_gateway_rest_api.intradiapp.id
  parent_id   = data.aws_api_gateway_rest_api.intradiapp.root_resource_id
  path_part   = "${var.component}"
}

#------------------------------------------------------------------------------------------------------------------------------
# Isines path

# Child resource: isines
resource "aws_api_gateway_resource" "isines" {
  rest_api_id = data.aws_api_gateway_rest_api.intradiapp.id
  parent_id   = aws_api_gateway_resource.component.id
  path_part   = "isines"
}

# Add get method to isines
resource "aws_api_gateway_method" "isines_get_method" {
  rest_api_id = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id = aws_api_gateway_resource.isines.id
  http_method = "GET"
  authorization = "CUSTOM"
  authorizer_id = data.aws_api_gateway_authorizer.intradiapp.id
}

# Add api permission to invoke lambda
resource "aws_lambda_permission" "isines_lambda_permission" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.layer_isines.function_name
  principal     = "apigateway.amazonaws.com"
}

# Integrate lambda to api
resource "aws_api_gateway_integration" "isines_lambda_integration" {
  rest_api_id             = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id             = aws_api_gateway_resource.isines.id
  http_method             = aws_api_gateway_method.isines_get_method.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.layer_isines.invoke_arn
}

resource "aws_api_gateway_method_response" "isines_get_response_200" {
  rest_api_id = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id = aws_api_gateway_resource.isines.id
  http_method = aws_api_gateway_method.isines_get_method.http_method  
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true,
    "method.response.header.Access-Control-Allow-Methods" = true,
    "method.response.header.Access-Control-Allow-Origin" = true
  }

  response_models = {
    "application/json" = "Empty"
  }
}

resource "aws_api_gateway_integration_response" "isines_get_response_200" {
  rest_api_id             = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id             = aws_api_gateway_resource.isines.id
  http_method             = aws_api_gateway_method.isines_get_method.http_method
  status_code             = aws_api_gateway_method_response.isines_get_response_200.status_code
  response_templates      = {
    "application/json" = ""
  }

  depends_on = [
    aws_api_gateway_integration.isines_lambda_integration
  ]
}

# Add options method to isines
resource "aws_api_gateway_method" "isines_options_method" {
  rest_api_id   = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id   = aws_api_gateway_resource.isines.id
  http_method   = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "isines_options_integration" {
  rest_api_id             = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id             = aws_api_gateway_resource.isines.id
  http_method             = aws_api_gateway_method.isines_options_method.http_method
  integration_http_method = "OPTIONS"
  type                    = "MOCK"
}

resource "aws_api_gateway_method_response" "isines_options_response_200" {
  rest_api_id = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id = aws_api_gateway_resource.isines.id
  http_method = aws_api_gateway_method.isines_options_method.http_method
  status_code = "200"

  response_models = {
    "application/json" = "Empty"
  }

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true,
    "method.response.header.Access-Control-Allow-Origin" = true
    "method.response.header.Access-Control-Allow-Methods" = true
  }
}

resource "aws_api_gateway_integration_response" "isines_options_response_200" {
  rest_api_id             = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id             = aws_api_gateway_resource.isines.id
  http_method             = aws_api_gateway_method.isines_options_method.http_method
  status_code             = aws_api_gateway_method_response.isines_options_response_200.status_code
  response_parameters     = {
     "method.response.header.Access-Control-Allow-Headers" = "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
    "method.response.header.Access-Control-Allow-Origin" = "'*'"
    "method.response.header.Access-Control-Allow-Methods" = "'GET,OPTIONS'"
  }
  response_templates      = {
    "application/json" = ""
  }

  depends_on = [
    aws_api_gateway_integration.isines_options_integration
  ]
}

#------------------------------------------------------------------------------------------------------------------------------
# issuers path

# Child resource: issuers
resource "aws_api_gateway_resource" "issuers" {
  rest_api_id = data.aws_api_gateway_rest_api.intradiapp.id
  parent_id   = aws_api_gateway_resource.component.id
  path_part   = "issuers"
}

# Add get method to issuers
resource "aws_api_gateway_method" "issuers_get_method" {
  rest_api_id = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id = aws_api_gateway_resource.issuers.id
  http_method = "GET"
  authorization = "CUSTOM"
  authorizer_id = data.aws_api_gateway_authorizer.intradiapp.id
}

# Add api permission to invoke lambda
resource "aws_lambda_permission" "issuers_lambda_permission" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.layer_issuers.function_name
  principal     = "apigateway.amazonaws.com"
}

# Integrate lambda to api
resource "aws_api_gateway_integration" "issuers_lambda_integration" {
  rest_api_id             = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id             = aws_api_gateway_resource.issuers.id
  http_method             = aws_api_gateway_method.issuers_get_method.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.layer_issuers.invoke_arn
}

resource "aws_api_gateway_method_response" "issuers_get_response_200" {
  rest_api_id = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id = aws_api_gateway_resource.issuers.id
  http_method = aws_api_gateway_method.issuers_get_method.http_method  
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true,
    "method.response.header.Access-Control-Allow-Methods" = true,
    "method.response.header.Access-Control-Allow-Origin" = true
  }
}

resource "aws_api_gateway_integration_response" "issuers_get_response_200" {
  rest_api_id             = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id             = aws_api_gateway_resource.issuers.id
  http_method             = aws_api_gateway_method.issuers_get_method.http_method
  status_code             = aws_api_gateway_method_response.issuers_get_response_200.status_code
  response_templates      = {
    "application/json" = ""
  }

  depends_on = [
    aws_api_gateway_integration.issuers_lambda_integration
  ]
}

# Add options method to issuers
resource "aws_api_gateway_method" "issuers_options_method" {
  rest_api_id   = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id   = aws_api_gateway_resource.issuers.id
  http_method   = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "issuers_options_integration" {
  rest_api_id             = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id             = aws_api_gateway_resource.issuers.id
  http_method             = aws_api_gateway_method.issuers_options_method.http_method
  integration_http_method = "OPTIONS"
  type                    = "MOCK"

}

resource "aws_api_gateway_method_response" "issuers_options_response_200" {
  rest_api_id = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id = aws_api_gateway_resource.issuers.id
  http_method = aws_api_gateway_method.issuers_options_method.http_method
  status_code = "200"

  response_models = {
    "application/json" = "Empty"
  }

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true,
    "method.response.header.Access-Control-Allow-Origin" = true
    "method.response.header.Access-Control-Allow-Methods" = true
  }
}

resource "aws_api_gateway_integration_response" "issuers_options_response_200" {
  rest_api_id             = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id             = aws_api_gateway_resource.issuers.id
  http_method             = aws_api_gateway_method.issuers_options_method.http_method
  status_code             = aws_api_gateway_method_response.issuers_options_response_200.status_code
  response_parameters     =  {
    "method.response.header.Access-Control-Allow-Headers" = "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
    "method.response.header.Access-Control-Allow-Methods" = "'GET,OPTIONS,POST,PUT'",
    "method.response.header.Access-Control-Allow-Origin" = "'*'"
  }
  response_templates      = {
    "application/json" = ""
  }

  depends_on = [
    aws_api_gateway_integration.issuers_options_integration
  ]
}
#------------------------------------------------------------------------------------------------------------------------------
# titles path

# Child resource: titles
resource "aws_api_gateway_resource" "titles" {
  rest_api_id = data.aws_api_gateway_rest_api.intradiapp.id
  parent_id   = aws_api_gateway_resource.component.id
  path_part   = "titles"
}

# Add post method to titles
resource "aws_api_gateway_method" "titles_post_method" {
  rest_api_id = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id = aws_api_gateway_resource.titles.id
  http_method = "POST"
  authorization = "CUSTOM"
  authorizer_id = data.aws_api_gateway_authorizer.intradiapp.id
}

# Add api permission to invoke lambda
resource "aws_lambda_permission" "titles_lambda_permission" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.layer_titles.function_name
  principal     = "apigateway.amazonaws.com"
}

# Integrate lambda to api
resource "aws_api_gateway_integration" "titles_lambda_integration" {
  rest_api_id             = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id             = aws_api_gateway_resource.titles.id
  http_method             = aws_api_gateway_method.titles_post_method.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.layer_titles.invoke_arn
}

resource "aws_api_gateway_method_response" "titles_post_response_200" {
  rest_api_id = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id = aws_api_gateway_resource.titles.id
  http_method = aws_api_gateway_method.titles_post_method.http_method  
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true,
    "method.response.header.Access-Control-Allow-Methods" = true,
    "method.response.header.Access-Control-Allow-Origin" = true
  }

  response_models = {
    "application/json" = "Empty"
  }
}

resource "aws_api_gateway_integration_response" "titles_post_response_200" {
  rest_api_id             = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id             = aws_api_gateway_resource.titles.id
  http_method             = aws_api_gateway_method.titles_post_method.http_method
  status_code             = aws_api_gateway_method_response.titles_post_response_200.status_code
  response_templates      = {
    "application/json" = ""
  }

  depends_on = [
    aws_api_gateway_integration.titles_lambda_integration
  ]
}

# Add options method to titles
resource "aws_api_gateway_method" "titles_options_method" {
  rest_api_id   = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id   = aws_api_gateway_resource.titles.id
  http_method   = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "titles_options_integration" {
  rest_api_id             = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id             = aws_api_gateway_resource.titles.id
  http_method             = aws_api_gateway_method.titles_options_method.http_method
  integration_http_method = "OPTIONS"
  type                    = "MOCK"
}

resource "aws_api_gateway_method_response" "titles_options_response_200" {
  rest_api_id = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id = aws_api_gateway_resource.titles.id
  http_method = aws_api_gateway_method.titles_options_method.http_method
  status_code = "200"

  response_models = {
    "application/json" = "Empty"
  }

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true,
    "method.response.header.Access-Control-Allow-Origin" = true
    "method.response.header.Access-Control-Allow-Methods" = true
  }
}

resource "aws_api_gateway_integration_response" "titles_options_response_200" {
  rest_api_id             = data.aws_api_gateway_rest_api.intradiapp.id
  resource_id             = aws_api_gateway_resource.titles.id
  http_method             = aws_api_gateway_method.titles_options_method.http_method
  status_code             = aws_api_gateway_method_response.titles_options_response_200.status_code
  response_parameters     = {
    "method.response.header.Access-Control-Allow-Origin" = "'*'"
    "method.response.header.Access-Control-Allow-Methods" = "'POST,OPTIONS'"
  }
  response_templates      = {
    "application/json" = ""
  }

  depends_on = [
    aws_api_gateway_integration.titles_options_integration
  ]
}

################################################################################################################################
# CLI UPDATE
################################################################################################################################

locals {
  etl_init_version_uri = "s3://s3-${var.environment}-rfli-curve-compare-code/glue-rfli-init-data-version.py"
  backup_folder = "${var.backup_folder}_${formatdate("YYYYMMDD_hhmmss", timestamp())}"
  code_backup = "${replace(data.aws_lambda_function.trigger_init.function_name, "-${var.environment}", "")}"
  lbd_trigger = "${data.aws_lambda_function.trigger_init.function_name}"
  lbd_trigger_code = "${replace(data.aws_lambda_function.trigger_init.function_name, "-${var.environment}", "")}"
  lbd_trigger_env = "{INIT_DATA_JOB_NAME=glue-${var.environment}-rfli-init-data-version,ISIN_TRACK_INIT_JOB_NAME=glue-${var.environment}-rfli-init-isin-track,PORTFOLIO_TRACK_INIT_JOB_NAME=glue-${var.environment}-rfli-init-portfolio-track,ISIN_SEARCH_INIT_JOB_NAME=${aws_glue_job.etl_init.name}}"

}

resource "null_resource" "cli_commands" {
  # Deploy Api Gateway Stage
  provisioner "local-exec" {
    command = "aws apigateway create-deployment --rest-api-id ${data.aws_api_gateway_rest_api.intradiapp.id} --stage-name ${var.environment}"
  }

  # Create folder backup
  provisioner "local-exec" {
    command = "mkdir ${local.backup_folder}"
  }

  # Backup glue-rfli-init-data-version: Code
  provisioner "local-exec" {
    command = "aws s3 cp ${local.etl_init_version_uri} ${local.backup_folder}"
  }

  # Update glue-rfli-init-data-version: Code
  provisioner "local-exec" {
    command = "aws s3 cp glue-rfli-init-data-version.py ${local.etl_init_version_uri}"
    working_dir = "../code"
  }

 # Backup lbd-rfli-trigger-init-data-version: Code & config
 # provisioner "local-exec" {
 #  command = "curl -o ${local.backup_folder}/${local.code_backup}.zip $(aws lambda get-function --function-name ${local.lbd_trigger} | jq -r '.Code.Location'); aws lambda get-function --function-name ${local.lbd_trigger} > ${local.backup_folder}/${local.code_backup}.json"
 #
 # }

  # Update lbd-rfli-trigger-init-data-version: Code
  provisioner "local-exec" {
    command = "aws lambda update-function-code --function-name ${local.lbd_trigger} --zip-file fileb://${local.lbd_trigger_code}.zip"

    working_dir = "../code"
  }

  # Update lbd-rfli-trigger-init-data-version: Variables
  provisioner "local-exec" {
    command = "aws lambda update-function-configuration --function-name ${local.lbd_trigger} --environment Variables=${local.lbd_trigger_env}"
  }

  depends_on = [
    aws_api_gateway_integration_response.isines_get_response_200,
    aws_api_gateway_integration_response.isines_options_response_200,
    aws_api_gateway_integration_response.issuers_get_response_200,
    aws_api_gateway_integration_response.issuers_options_response_200,
    aws_api_gateway_integration_response.titles_post_response_200,
    aws_api_gateway_integration_response.titles_options_response_200,
  ]  
}

################################################################################################################################
# Update Resource
################################################################################################################################

#------------------------------------------------------------------------------------------------------------------------------
# lbd-rfli-trigger-init-data-version: Policy

# Policy
resource "aws_iam_policy" "trigger_etl_init" {
  name = format("policy-rfli-trigger-glue-init-%s", var.component)
  description = format("Politicas especificas para trigger glue-*-rfli-init-%s", var.component)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
          "Action": [
              "glue:StartJobRun"
          ],
          "Effect": "Allow",
          "Resource": [
            "${aws_glue_job.etl_init.arn}"
          ]
      }
    ]
  })
}

# Add policy
resource "aws_iam_role_policy_attachment" "trigger_init_rol_with_etl_init_policy" {
  role       = data.aws_iam_role.trigger_init.name
  policy_arn = aws_iam_policy.trigger_etl_init.arn
}
