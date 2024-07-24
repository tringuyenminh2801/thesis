import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node TGT DIM BU
df_tgt = glueContext.create_data_frame.from_catalog(
    database="thesis", table_name="d_bu")
dynf_tgt = DynamicFrame.fromDF(
    df_tgt, glueContext, "dynf_tgt_dimbu")

# Script generated for node Amazon S3
df_src = glueContext.create_data_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://trinm-lakehouse/raw/sales/"],
        "recurse": True
    },
    transformation_ctx="dynf_src"
)


# Get Last Query Time
df_lstQ = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://trinm-lakehouse/config/load_config.json"],
        "recurse": True
    },
    transformation_ctx="dynf_lstQ")

# Rename for joining
dynf_renamed_tgt_dimbu = ApplyMapping.apply(
    frame=dynf_tgt,
    mappings=[("id", "int", "id", "int"),
              ("business_unit", "string", "right_business_unit", "string"),
              ("province_lv1", "string", "right_province_lv1", "string"),
              ("province_lv2", "string", "right_province_lv2", "string")],
    transformation_ctx="dynf_renamed_tgt_dimbu")

# Script generated for node Delta Load
SqlQuery4 = '''
select distinct
    business_unit,
    province_lv1,
    province_lv2
from
    myDataSource
where
    event_timestamp >= (
        select
            sales.dim_bu.last_read
        from
            lstQ
    )
'''
dynf_deltaLoad = sparkSqlQuery(
    glueContext,
    query=SqlQuery4,
    mapping={
        "myDataSource": df_src,
        "lstQ": df_lstQ
    },
    transformation_ctx="dynf_deltaLoad")

# Script generated for node Get new data
df_deltaLoad = dynf_deltaLoad.toDF()
dynf_renamed_tgt_dimbu = dynf_renamed_tgt_dimbu.toDF()
dynf_newDimRecord = DynamicFrame.fromDF(
    df_deltaLoad.join(dynf_renamed_tgt_dimbu, (df_deltaLoad['business_unit'] == dynf_renamed_tgt_dimbu['right_business_unit']) & (
        df_deltaLoad['province_lv1'] == dynf_renamed_tgt_dimbu['right_province_lv1']) & (df_deltaLoad['province_lv2'] == dynf_renamed_tgt_dimbu['right_province_lv2']), "leftanti"), glueContext, "dynf_newDimRecord")

# Script generated for node Generate ID
SqlQuery3 = '''
select
    ROW_NUMBER() OVER (
        ORDER BY
            business_unit
    ) + (
        SELECT
            ifnull (max(id), 0)
        FROM
            mid
    ) id,
    business_unit,
    province_lv1,
    province_lv2
from
    src
'''
dynf_genID = sparkSqlQuery(
    glueContext, 
    query=SqlQuery3, 
    mapping={
        "src" : dynf_newDimRecord, 
        "mid" : dynf_tgt
    }, 
    transformation_ctx="dynf_genID")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1713692901617 = glueContext.write_data_frame.from_catalog(
    frame=dynf_genID.toDF(), 
    database="thesis", 
    table_name="d_bu", 
    additional_options={}
)

job.commit()
