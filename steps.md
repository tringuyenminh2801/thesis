# INITIAL STEPS

1. Make sure that your RDS database is private and you can SSH to the EC2 publicly via port 22.
2. Create a custom parameter group for your Postgres database, set the `rds.logical_replication` setting to 1. Detail for setup CDC for Postgres visit [here](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html)

# SETUP EC2 AND POSTGRES

1. Update
```bash
sudo dnf update -y
```

2. Install `psql`

```bash
sudo dnf install postgresql15
```

3. Connect to PostgreSQL    

```bash
psql --host=host_name --port=5432 --dbname=db_name --username=username
```

4. Adding permissions to the database user

```bash
create user repluser password 'replpass';
grant rds_replication to repluser;
```

5. Create tables for Change Data Capture


6. Add publication for all tables

```sql
create publication cdc for all tables;
```

7. Set replica identity for all tables so that any update or delete will be pushed to replication slots

```sql
alter table table_name replica identity full;
```
# SETUP LAKEHOUSE IN S3

Create the following folders:
1. raw - to store raw data from Kinesis Data Firehose
2. silver - to store transformed data
3. config - to hold the "Last query date" file, where we use it for delta load. See Delta load definition [here](https://dataengineering.wiki/Concepts/Delta+Load)

After creating these folders, add the file [load_config.json](/kinesis2lakehouse/load_config.json) to the folder "config".


# INGESTION CODE
Ingestion code can be found [here](/csv2pg/)
These scripts are used to ingest data into RDS. If you want the changes in RDS to be pushed to Kinesis Data Stream, do the steps:

1. [Setup EC2 and Postgres](#setup-ec2-and-postgres)
2. [Setup Kinesis Data Stream](#setup-kinesis-stream)
3. [Setup Kinesis Data Firehose](#create-kinesis-data-firehose)
4. [Run CDC scripts in EC2](#setup-change-data-capture-code)
5. Run the ingestion code locally

There are two scripts:

1. Initial ingest, where we ingest 1000 lines per batch to RDS. It ingest data from data from [this file](/data/frt_init.csv). [Scripts here](/csv2pg/ingest.py)
2. Incremental ingest, where we ingest row-by-row to RDS. It ingest data from data from [this file](/data/frt_incremental.csv). [Scripts here](/csv2pg/incremental_ingest.py)

Before try running these code, fill in the information in the `config.yml` file first. The file can be found [here](/config.yml)

# SETUP CHANGE DATA CAPTURE CODE
1. Setup Python inside EC2

Update `yum`
```bash
sudo yum update -y
```

Install Python
```bash
sudo yum install python3 -y
```

Install Git
```bash
sudo yum install git
```

2. Create virtual environment

```bash
python3 -m venv pg2kinesis
```

Activate it 

```bash
source pg2kinesis/bin/activate
```

3. Clone the repo

```bash
git clone https://github.com/tringuyenminh2801/thesis
```

4. Run the script to listen to changes in RDS
```bash
cd thesis
python pg2kinesis/app.py
```

# SETUP KINESIS STREAM
1. Go to AWS Console, create the Kinesis stream named `kns-stream-name`
2. Set "Provisioned" and number of shard to 1

# SETUP ROLES FOR FIREHOSE
1. Grant access for Firehose to access AWS Glue for Data Format Conversion
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "glue:GetTableVersion",
                "glue:GetSchemaVersion",
                "glue:GetTables",
                "glue:GetTableVersions",
                "glue:GetTable"
            ],
            "Resource": "*"
        }
    ]
}
```

2. Grant access for Firehose to access S3 
```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "VisualEditor0",
			"Effect": "Allow",
			"Action": [
				"s3:ListBucketMultipartUploads",
				"kms:Decrypt",
				"lambda:InvokeFunction",
				"kinesis:ListShards",
				"kinesis:GetShardIterator",
				"lambda:GetFunctionConfiguration",
				"kinesis:DescribeStream",
				"s3:ListBucket",
				"logs:PutLogEvents",
				"s3:PutObject",
				"s3:GetObject",
				"s3:AbortMultipartUpload",
				"kms:GenerateDataKey",
				"kinesis:GetRecords",
				"s3:GetBucketLocation"
			],
			"Resource": "*"
		}
	]
}
```

# SETUP POLICY AND ROLE FOR GLUE

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:*",
                "glue:*"
            ],
            "Resource": "*"
        }
    ]
}
```

# CREATE KINESIS DATA FIREHOSE
## SETTINGS

Use the below settings while creating kinesis data firehose

```json
{
    "formatConversion" : "false",
    "s3Bucket" : "s3://lakehouse-location",
    "dynamicPartitioning" : "true",
    "dynamicPartitionKeys" : {
        "table" : ".table",
        "year" : ".event_timestamp| strftime("%Y")",
        "month" : ".event_timestamp| strftime("%m")",
        "day" : ".event_timestamp| strftime("%d")",
        "hour" : ".event_timestamp| strftime("%H")",
    },
    "s3BucketPrefix" : "raw/!{partitionKeyFromQuery:table}/year=!{partitionKeyFromQuery:year}/month=!{partitionKeyFromQuery:month}/day=!{partitionKeyFromQuery:day}/hour=!{partitionKeyFromQuery:hour}/",
    "s3BucketErrorOutputPrefix" : "raw/error/",
    "fileExtensionFormat" : ".json.gz",
    "iamRole" : "firehose-to-s3",
    
}
```

# SETUP GLUE SCRIPT FOR TRANSFORMATION
- Before creating Glue script, add tables to AWS Glue Data Catalog using the schema. Visit [here](/kinesis2lakehouse/schema/)
- The Glue script can be found [here](/kinesis2lakehouse/scripts/)
- Create 3 Glue scripts for
1. Updating dimension tables: [Business Unit Dimension](/kinesis2lakehouse/scripts/d_bu.py) and [Product Dimension](/kinesis2lakehouse/scripts/d_product.py)
2. Updating fact table: [Fact Sales](/kinesis2lakehouse/scripts/f_sales.py)
- Setup Workflow to update dimension tables first, then fact tables later.
