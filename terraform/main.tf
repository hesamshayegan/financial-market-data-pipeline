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
resource "aws_security_group" "allow_tls" {
  name        = "financial_sg"
  description = "allow on port 8080"


  tags = {
    Name = "financial_security_group"
  }
}

# Allow inbound traffic on port 8080 (for TLS)
resource "aws_vpc_security_group_ingress_rule" "allow_tls_ipv4" {
  security_group_id = aws_security_group.allow_tls.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 8080
  ip_protocol       = "tcp"
  to_port           = 8080
}

# Allow all outbound traffic
resource "aws_vpc_security_group_egress_rule" "allow_all_traffic_ipv4" {
  security_group_id = aws_security_group.allow_tls.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1" # Allows all outbound traffic
}


# get the most recent ami (it's not building a resource)
data "aws_ami" "financial-pipeline" {
  most_recent = true
  name_regex  = "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-20240927"
  owners      = ["099720109477"]
}

# ami tells that which os will be used: here is Ubuntu
# sg reads the id after sg is created above
resource "aws_instance" "financial-piepeline-instance" {
  instance_type          = "t2.micro"
  ami                    = data.aws_ami.financial-pipeline.id
  vpc_security_group_ids = [aws_security_group.allow_tls.id]
  
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
  db_name                = "postgresdb"
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