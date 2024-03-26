import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1711462857829 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="telegram", transformation_ctx="AWSGlueDataCatalog_node1711462857829")

# Script generated for node SQL Query
SqlQuery0 = '''
MSCK REPAIR TABLE telegram;
'''
SQLQuery_node1711462894912 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AWSGlueDataCatalog_node171116288888}, transformation_ctx = "SQLQuery_node1711162888888") # substituir os dados pelos verdadeiros.

job.commit()