import sys
import logging
import boto3
import time

S3_BUCKET          = 'grab-transaction' #Your S3 Bucket Name
TEMP_FILE_PREFIX   = 'redshift_data_upload' #Temporary file prefix
REDSHIFT_WORKGROUP = 'grab-transaction'
REDSHIFT_DATABASE  = 'dev' #default "dev"
MAX_WAIT_CYCLES    = 10

def run_redshift_statement(sql_statement):
     """
     Generic function to handle redshift statements (DDL, SQL..),
     it retries for the maximum MAX_WAIT_CYCLES.
     Returns the result set if the statement return results.
     """
     res = client.execute_statement(
     Database=REDSHIFT_DATABASE,
     WorkgroupName=REDSHIFT_WORKGROUP,
     Sql=sql_statement
     )

     # DDL statements such as CREATE TABLE doesn't have result set.
     has_result_set = False
     data = None
     done = False
     attempts = 0

     while not done and attempts < MAX_WAIT_CYCLES:
          attempts += 1
          time.sleep(1)

     desc = client.describe_statement(Id=res['Id'])
     query_status = desc['Status']

     if query_status == "FAILED":
          raise Exception('SQL query failed: ' + desc["Error"])
     elif query_status == "FINISHED":
          done = True
          has_result_set = desc['HasResultSet']
     else:
          logging.info("Current working... query status is: {} ".format(query_status))

     if not done and attempts >= MAX_WAIT_CYCLES:
          raise Exception('Maximum of ' + str(attempts) + ' attempts reached.')

     if has_result_set:
          data = client.get_statement_result(Id=res['Id'])
     return data

def create_redshift_table():
     create_table_ddl = """
     DROP TABLE IF EXISTS staging_items;
     CREATE TABLE staging_items (
          quantity INTEGER,
          product_name VARCHAR(255),
          price INTEGER,
          order_id VARCHAR(255),
          note VARCHAR(255)
     );
     """

     run_redshift_statement(create_table_ddl)
     logging.info('Table created successfully.')

def import_s3_file(file_name):
     """
     Loads the content of the S3 temporary file into the Redshift table.
     """
     load_data_ddl = f"""
     COPY staging_items 
     FROM 's3://{S3_BUCKET}/{file_name}' 
     DELIMITER ';'
     IGNOREHEADER as 1
     REGION 'ap-southeast-2'
     IAM_ROLE default
     timeformat 'auto'
     FILLRECORD;
     """

     run_redshift_statement(load_data_ddl)
     logging.info('Imported S3 file to Redshift.')

def query_redshift_table():
     # You can use your own SQL to fetch data.
     select_sql = 'SELECT * FROM staging_items;'
     data = run_redshift_statement(select_sql)
     print(data['Records'])

client = boto3.client('redshift-data', region_name='ap-southeast-2')

if __name__ == "__main__":
     logging.basicConfig(level=logging.INFO)
     logging.info('Process started')
     
     temp_file_name = 'all_item.csv'

     create_redshift_table()
     import_s3_file(temp_file_name)
     query_redshift_table()
     logging.info('Process finished')