import json
import urllib.parse
import boto3
import psycopg2
import os
import csv
import pandas as pd
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype


#AWS services clients
s3 = boto3.client('s3')
sns = boto3.client('sns')

#Psycopg2 Connection Parameters
db_host = os.environ["db_host"]
db_user = os.environ["db_user"]
db_pw = os.environ["db_pw"]
db_port = os.environ["db_port"]
db_database = os.environ["db_database"]
db_sslmode = os.environ["db_sslmode"]
connection = None

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    aws_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    bucketcsvfile = s3.get_object(Bucket=bucket, Key=aws_key)
    csvfile = pd.read_csv(bucketcsvfile['Body'])

    #column headers validation
    validation_headers_list = ['id','first_name','last_name','salary','department']
    file_headers_list = list(csvfile.columns.values)
    headers_comparison = (validation_headers_list == file_headers_list)
    data_type_validation = {}

    #data type validation
    if (headers_comparison):
        data_type_validation['id'] = is_numeric_dtype(csvfile['id'])
        data_type_validation['first_name'] = is_string_dtype(csvfile['first_name'])
        data_type_validation['last_name'] = is_string_dtype(csvfile['last_name'])
        data_type_validation['salary'] = is_numeric_dtype(csvfile['salary'])
        data_type_validation['department'] = is_string_dtype(csvfile['department'])
        print(data_type_validation)

    if (headers_comparison and all(data_type_validation.values())):
        print("Validated Headers and data types in File!")

        try:
            connection_rds = psycopg2.connect("dbname={} user={} host={} password={} port={} sslmode={}".format(db_database, db_user, db_host, db_pw, db_port, db_sslmode))
            cursor_rds = connection_rds.cursor()
            print("Connection to DB successful") 
            
            temp_table_query = "create temporary table employee_staging ( like employees ) on commit drop"
            cursor_rds.execute(temp_table_query)
            
            upload_s3_file_query = "select * from fn_load_s3_file('{}');".format(aws_key)
            cursor_rds.execute(upload_s3_file_query)
            
            record_processing_query = """insert into employees (id, first_name, last_name, salary, department)
                                         select id, first_name, last_name, salary, department
                                         from employee_staging
                                         on conflict (id)
                                         do update set first_name = excluded.first_name
                                                        ,last_name = excluded.last_name
                                                        ,salary = excluded.salary
                                                        ,department = excluded.department""" 
                                                        
            cursor_rds.execute(record_processing_query)
    
        except :
            connection_rds.rollback()
            raise
        
        else: 
            connection_rds.commit()
            
        finally:
            connection_rds.close()
            message = message = {"Processed_file": aws_key}
            response = sns.publish(
                TargetArn=os.environ["sns_topic"],
                Message=json.dumps(message)
            )
        return {'processed_file': aws_key}
    
    else:
        if (validation_headers_list != file_headers_list):
            print("Please check columns in file. File headers order should be: id,first_name,last_name,salary,department")
        elif (all(data_type_validation.values()) == False):
            columns_to_check = []
            for key in data_type_validation.keys():
                if data_type_validation[key] is False:
                    columns_to_check.append(key)
            print("Please check the data in columns: " + str(columns_to_check))
        return {'Error': "File Error"}