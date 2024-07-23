import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import regexp_replace, concat_ws, collect_list, when, lit, to_date,col
from pyspark.sql import Window
from awsglue.dynamicframe import DynamicFrame

# Initialize the Glue context
glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)

# Get the arguments passed to the Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SecretName', 'TempDir', 'RedshiftSchema', 'RedshiftCluster','RedshiftDatabase'])

# Start the Glue job
job.init(args['JOB_NAME'], args)

# Read the CSV files from S3 using the respective crawlers
customer_df = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce-shop",
    table_name="customer_csv",
    transformation_ctx="customer_df",
    options={
        "quoteChar": "\"",
        "escaper": "\"",
        "withHeader": True,
        "separator": ",",
        "AllowQuotedRecordDelimiter": True
    }
).toDF()

# Handle inconsistent phone number format in customer.csv
customer_df = customer_df.withColumn("customer_phone", regexp_replace("customer_phone", r"[^0-9]", ""))

inventory_df = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce-shop",
    table_name="inventory_csv",
    transformation_ctx="inventory_df",
    options={
        "quoteChar": "\"",
        "escaper": "\"",
        "withHeader": True,
        "separator": ",",
        "AllowQuotedRecordDelimiter": True
    }
).toDF()

order_df = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce-shop",
    table_name="order_csv",
    transformation_ctx="order_df",
    options={
        "quoteChar": "\"",
        "escaper": "\"",
        "withHeader": True,
        "separator": ",",
        "AllowQuotedRecordDelimiter": True
    }
).toDF()

# Define a window specification to order by order_id and collect lines
window_spec = Window.partitionBy("order_id").orderBy("order_id")

# Concatenate multiline addresses for shipping and billing addresses
order_df = order_df.withColumn(
    "order_shipping_address",
    concat_ws(" ", collect_list("order_shipping_address").over(window_spec))
).withColumn(
    "order_billing_address",
    concat_ws(" ", collect_list("order_billing_address").over(window_spec))
)

# Clean up address fields to remove newlines and unnecessary spaces
order_df = order_df.withColumn("order_shipping_address", regexp_replace("order_shipping_address", r"\s*,\s*", ", "))
order_df = order_df.withColumn("order_billing_address", regexp_replace("order_billing_address", r"\s*,\s*", ", "))

# Handle empty columns
for col in order_df.columns:
    order_df = order_df.withColumn(col, when(col(col).isNull(), lit('')).otherwise(col(col)))

# Convert specific date columns to YYYY-MM-DD format
date_columns = ["order_date", "order_payment_date", "order_shipping_date"]
for col_name in date_columns:
    order_df = order_df.withColumn(col_name, to_date(col(col_name), 'M/d/yyyy'))

order_item_df = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce-shop",
    table_name="order_item_csv",
    transformation_ctx="order_item_df",
    options={
        "quoteChar": "\"",
        "escaper": "\"",
        "withHeader": True,
        "separator": ",",
        "AllowQuotedRecordDelimiter": True
    }
).toDF()

product_df = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce-shop",
    table_name="product_csv",
    transformation_ctx="product_df",
    options={
        "quoteChar": "\"",
        "escaper": "\"",
        "withHeader": True,
        "separator": ",",
        "AllowQuotedRecordDelimiter": True
    }
).toDF()

# Get the Redshift password from Secrets Manager
secret_name = args['SecretName']
secrets_manager = boto3.client('secretsmanager')
response = secrets_manager.get_secret_value(SecretId=secret_name)
secret = json.loads(response['SecretString'])
redshift_password = secret['password']

# Redshift connection options
redshift_connection_options = {
    "url": "jdbc:redshift://{args['RedshiftCluster'].c3ykinhb5kxx.us-west-2.redshift.amazonaws.com:5439/{args['RedshiftDatabase']}",
    "user": "ecommerce",
    "password": redshift_password,
    "redshiftTmpDir": args['TempDir'],
    "table": "",
    "Schema": args['RedshiftSchema']
}


# Define the table names
table_names = {
    "customer_df": "customer",
    "inventory_df": "inventory",
    "order_df": "order",
    "order_item_df": "order_item",
    "product_df": "product"
}

# Load the data into separate Redshift tables
for df_name, table_name in table_names.items():
    df = locals()[df_name]
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, df_name)
    redshift_connection_options["table"] = table_name
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift",
        connection_options=redshift_connection_options,
        transformation_ctx=f"redshift_sink_{table_name}"
    )

# Commit the Glue job
job.commit()
