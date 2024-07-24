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


### IMPLEMENTATION STEPS

Please navigate to [this file](/steps.md) for more details.