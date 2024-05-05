# THESIS

## INTRODUCTION
In an era dominated by the relentless growth of Big Data, enterprises find themselves at the crossroads of a pivotal challenge – the efficient management and extraction of meaningful insights from expansive and diverse datasets. As organizations grapple with this monumental task, the demand for innovative strategies becomes imperative. This thesis undertakes an investigation of a hybrid methodology, skillfully merging the advantages of data lakes and data warehouses into a revolutionary structure called a data lakehouse architecture. This endeavor aims to furnish businesses with a comprehensive guide to constructing a robust and scalable data warehouse, empowered by the dynamic capabilities of Amazon Web Services (AWS).
### Data Architecture: 
![Data Architecture](/assets/images/data_architecture.png)
1. The operation running on EC2 sends data to RDS 24/7. To mimic their business, Python script is used to write CSV file data to RDS by accessing EC2 via Secure Shell (SSH) and connect EC2 with RDS via PSQL.

2. Logical replication is set up to track new data coming to the RDS. In theory, the logical replication mechanism in PostgreSQL follows the publisher and subscriber model, with one or more subscribers subscribing to one or more publications on a publisher node. We set up an EC2 instance to connect to the RDS, run a Python script listening to the changes made, and send it to KDS.

3. We use Amazon Data Firehose to read the data inside Kinesis Data Streams. Firehose will deliver the data inside Kinesis Data Streams to an S3 bucket.

4. We use AWS Glue to transform the raw data into usable data, store them in the Apache Iceberg tables.

5. Amazon Athena is used to query data inside cleaned Iceberg tables inside S3. We can use the result to build an interactive dashboard on Amazon Quicksight.


## SETUP
### EC2 AND POSTGRES

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

5. Add publication for all tables

```sql
create publication cdc for all tables;
```

6. Set replica identity so that any update or delete will be pushed to replication slots

```sql
alter table table_name replica identity full;
```

### SETUP CHANGE DATA CAPTURE CODE
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

4. Run the script
```bash
python pg2kinesis/app.py
```

### SETUP KINESIS STREAM
1. Go to AWS Console, create the Kinesis stream named `kns-stream-name`
2. Set "Provisioned" and number of shard to 1

### SETUP ROLES FOR FIREHOSE
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

### SETUP POLICY AND ROLE FOR GLUE

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

### CREATE KINESIS DATA FIREHOSE

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