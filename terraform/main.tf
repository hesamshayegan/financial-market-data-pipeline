terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

# aws provider
provider "aws" {
  region = "us-west-1"
}

# security group
resource "aws_security_group" "allow_sg" {
  name        = "financial_sg"
  description = "allow on port 8080"


  tags = {
    Name = "financial_security_group"
  }
}

# Allow inbound traffic on port 8080 (for TLS)
resource "aws_vpc_security_group_ingress_rule" "allow_tls_ipv4" {
  security_group_id = aws_security_group.allow_sg.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 8080
  ip_protocol       = "tcp"
  to_port           = 8080
}

# Allow inbound traffic on port 22 (SSH)
resource "aws_vpc_security_group_ingress_rule" "allow_ssh_ipv4" {
  security_group_id = aws_security_group.allow_sg.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 22
  ip_protocol       = "tcp"
  to_port           = 22
}

# Allow all outbound traffic
resource "aws_vpc_security_group_egress_rule" "allow_all_traffic_ipv4" {
  security_group_id = aws_security_group.allow_sg.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1" # Allows all outbound traffic
}

# get the most recent ami (it's not building a resource)
data "aws_ami" "financial-pipeline" {
  most_recent = true
  name_regex  = "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-20240927"
  owners      = ["099720109477"]
}

# import existing key pair
resource "aws_key_pair" "existing_key" {
  key_name   = "fin-key-pair"
  public_key = file("./keys/zx.pub")  
}

# ami tells that which os will be used: here is Ubuntu
# sg reads the id after sg is created above
resource "aws_instance" "financial-piepeline-instance" {
  instance_type          = "t2.small"
  ami                    = data.aws_ami.financial-pipeline.id
  vpc_security_group_ids = [aws_security_group.allow_sg.id]
  key_name      = aws_key_pair.existing_key.key_name
  
  tags = { 
      Name = "financial-pipeline-instance" 
  } 
}

# s3 bucket
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "bucket" {
  bucket = "financial-s3-bucket-${random_id.bucket_suffix.hex}"

  tags = {
    Name        = "financial-pipeline"
    Environment = "Dev"
  }
}

resource "aws_s3_object" "file" {
  bucket = aws_s3_bucket.bucket.id
  key    = "test.txt"
  source = "/Users/hesam/Desktop/codes/terraform/test.txt"
}


# RDS instance
# Data block to get the default VPC
data "aws_vpc" "default" {
  default = true
}

resource "aws_security_group" "postgres-sg" {
  vpc_id      = data.aws_vpc.default.id # Uses the default VPC ID
  name        = "financial-sg-postgres"
  description = "Allow all inbound for Postgres"

  ingress {
    from_port   = var.postgres_port
    to_port     = var.postgres_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1" # Allows all outbound traffic
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "default" {
  allocated_storage      = 20
  storage_type           = "gp2"
  db_name                = "financialdb"
  engine                 = "postgres"
  engine_version         = "14.13"
  instance_class         = "db.t3.micro"
  identifier             = var.postgres_identifier
  username               = var.postgres_db_user_name
  password               = var.postgres_db_password
  parameter_group_name   = "default.postgres14"
  vpc_security_group_ids = [aws_security_group.postgres-sg.id]
  publicly_accessible    = true
  skip_final_snapshot    = true

  tags = {
    Name = "PostgreSQL RDS instance"
  }

}

# 1.1-create s3 access policy from rds
resource "aws_iam_policy" "rds-s3-import-policy" {
  name        = "S3AccessPolicy"
  description = "Policy to allow access to the financial S3 bucket"

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Effect   = "Allow"
        Resource = [
          aws_s3_bucket.bucket.arn,
          "${aws_s3_bucket.bucket.arn}/*"
        ]
      },
    ]
  })
}

# 1.2-create an iam role and attach the policy
resource "aws_iam_role" "rds_s3_role" {
  name = "RDS_S3_Role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "rds.amazonaws.com"
        }
      },
    ]
  })
}

# 1.3-attach the policy to the role
resource "aws_iam_role_policy_attachment" "attach_s3_policy" {
  role       = aws_iam_role.rds_s3_role.name
  policy_arn = aws_iam_policy.rds-s3-import-policy.arn
}