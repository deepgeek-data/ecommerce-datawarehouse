# ecommerce-datawarehouse

1. docs directory
Regarding this requirement entire pipeline steps has been implemented under docs directory.
2. glue directory
This directory contain one job etl.py which is used for transformation related and finally load data into data warehouse redshift as per requirment.
3. lambda
This directory contain lambda function data_pipeline_trigger.py which is used to automate this pipeline ,it will trigger the Glue Crawler and also Glue job.
4. redshift
This directory contain required sql query that we have implemented in Redshift Data Warehouse.
5. requirements.txt
If u want to install dependency for related boto3,pyspark then install using pip command
pip install -r requirements.txt


