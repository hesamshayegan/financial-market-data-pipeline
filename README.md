# Financial Market Pipeline

The **Financial Market Pipeline** is a data pipeline designed to streamline the ingestion, processing, and storage of financial market data. This pipeline leverages Apache Airflow for orchestration, AWS RDS for structured data storage, and AWS S3 for scalable storage solutions. The system is configured to handle data efficiently while ensuring reliability and scalability for financial data workflows.

This guide will walk you through setting up the development environment, configuring Airflow, and connecting to essential resources like AWS RDS and S3.

## Setting Up the Environment
To get started, follow the steps below to prepare your infrastructure and Airflow environment.


## Prerequisites
Ensure you have the following installed on your system before starting:

- Python 3.12+
- pip
- Terraform
- AWS CLI configured with your credentials

## Setting Up Development Variables
Define the following development variables in a `dev.tfvars` file:

```terraform
postgres_identifier   = "your-postgres-instance"
postgres_db_user_name = "postgres"
postgres_db_password  = "your-password"
postgres_port         = 5432
```

## Initializing and Applying Terraform
Navigate to the Terraform directory and initialize Terraform:

```bash
terraform init
```

Apply the Terraform plan with the development variables:

```bash
terraform apply -var-file=dev.tfvars
```

## Setting Up the AWS Instance
After connecting to your AWS instance, update the package manager and install Python dependencies:

```bash
sudo apt update
sudo apt install python3-pip
sudo apt install python3.12-venv
```

Create a virtual environment for Airflow:

```bash
python3 -m venv airflow_venv
source airflow_venv/bin/activate
mkdir airflow
```

## Installing Airflow Dependencies
Copy the `requirements.txt` file to the `airflow` directory and install the dependencies:

```bash
pip install -r requirements.txt
```

## Configuring Airflow
Navigate to the `airflow` directory and set up the Airflow database:

```bash
export AIRFLOW_HOME=/home/ubuntu/airflow
echo $AIRFLOW_HOME
airflow db init
```

### Update Airflow Configuration
Edit the `airflow.cfg` file to set the following:

```text
executor = LocalExecutor
sql_alchemy_conn = postgresql+psycopg2://{your_rds_user}:{your_rds_password}@{your_rds_host}:5432/{your_rds_database}
```

Replace the placeholders with your PostgreSQL RDS instance details.

## Creating an Airflow Admin User
In the AWS CLI, create an admin user for Airflow:

```bash
airflow users create --username airflow --firstname firstname --lastname lastname --role Admin --email airflow@domainairflow.com --password airflow
```

## Adding Airflow Connections
### PostgreSQL Connection
Configure the PostgreSQL connection in Airflow:

```bash
airflow connections add 'postgres_conn' \
    --conn-type 'postgres' \
    --conn-host 'rds-host-address' \
    --conn-login 'postgres' \
    --conn-password 'your-password' \
    --conn-port '5432' \
    --conn-schema 'financialdb'
```

### S3 Connection
Define the following environmental variables to connect to your S3 bucket:

```bash
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"
export AWS_REGION="your-region"
```

Update the S3 bucket path in the `duckdb.queries.py` file:

```python
OUTPUT_PATH_BASE = "s3://financial-s3-bucket-xxxxxxxx"
```

## Running Airflow
Start the Airflow webserver and scheduler:

```bash
airflow webserver &
airflow scheduler
```

## Accessing the Airflow UI
Open your browser and navigate to the following URL:

```
[your Public IPv4 DNS]:8080
```

Log in using the admin credentials you created earlier.

## Notes
- Ensure the AWS instance security group allows inbound traffic on port `8080` for accessing the Airflow UI.
- Replace all placeholder values (`your-access-key-id`, `your-region`, etc.) with your actual configuration details.