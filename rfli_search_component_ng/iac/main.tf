
################################################################################################################################
# Account_id
################################################################################################################################
data "aws_caller_identity" "AccountID" {}

################################################################################################################################
# Data sources
################################################################################################################################

#------------------------------------------------------------------------------------------------------------------------------
# S3 Bucket
data "aws_s3_bucket" "angular_code" {
  bucket = format("s3-%s-rfli-angular-files", var.environment)
}

################################################################################################################################
# CLI
################################################################################################################################

resource "null_resource" "update_by_cli" {

# Sync Site: Component
  provisioner "local-exec" {
    command = "aws s3 cp main.js s3://${data.aws_s3_bucket.angular_code.id}/${var.component}/main.js"    

    working_dir = "../dist/${var.component}"
  }

  # Clone container repo
  provisioner "local-exec" {
    command = "git clone ${var.container_repo_url} -b ${var.container_repo_branch}"    

    working_dir = "../.."
  }

  # Sync Site: Container
  provisioner "local-exec" {
    command = "aws s3 sync ./${var.container_repo_name}/dist s3://${data.aws_s3_bucket.angular_code.id}/none-root-config"    

    working_dir = "../.."
  }

# cloudfront cache invalidation
  provisioner "local-exec" {
    command = "aws cloudfront create-invalidation --distribution-id ${var.cloudfront_id} --paths ${var.path}"    

  }

}

