import pandas as pd
import boto3
import json 
import uuid
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import awswrangler as wr
from configparser import ConfigParser
import os
from DataopsUtil import DataopsUtilss
from datetime import datetime
from LoggingUtils import logging_utils


JOB_NAME = 'COURT_HISTORIC'
env = None
config = None



def get_config_file_loc():
    return os.environ['CONFIG_LOC']

def get_environment():
    global env
    try:
        env = os.environ['ENV']
        logger.info("Found the env in the environment variables")
    except:
        logger.error("Did not found the env in the environment variables")
        env = 'dev'
def initialize_config():
    global config
    s3_boto = boto3.client('s3')
    config = ConfigParser()
    configuration_file_bucket = get_config_file_loc()
    configuration_file_key = "afjaa/dev/config.ini"
    obj = s3_boto.get_object(Bucket=configuration_file_bucket, Key=configuration_file_key)
    config.read_string(obj['Body'].read().decode())
def get_common(parameter):
    if not env:
        get_environment()
    if not config:
        initialize_config()
    try:
        section_name = 'COMMON' + '_' + env.upper()
        value = config.get(section_name, parameter)
        logger.info(f"Found {parameter} values in config ini")
    except Exception as e:
        value = 'value not found in configuration file'
        logger.error(f"Value Did not found {parameter} in config.ini")
    return value
def get_job_specific_parameters(parameter):
    if not env:
        get_environment()
    if not config:
        initialize_config()
    try:
        section = JOB_NAME + '_' + env.upper()
        value = config.get(section, parameter)
        logger.info(f"Found {parameter} values in config ini")
        
    except Exception as e:
        value = 'value not found in configuration file'
        logger.error(f"Value Did not found {parameter} in config.ini")
    return value


def csv_to_df(loc):
    out_df = pd.DataFrame()
    try:
        out_df = pd.read_csv(loc)
        out_df['modified'] = datetime.now()
        logger.info("Found the csv file")
    except Exception as errormsg:
        logger.error("Value Did not found the csv file-->",errormsg)
    return out_df
    
        

def create_court_catalog(df,COURT_OUTPUT_PARQUET_LOC,DB_NAME,COURT_TABLE):
    try:
        wr.s3.to_parquet(
            df=df,
            path=COURT_OUTPUT_PARQUET_LOC,
            dataset=True,
            database=DB_NAME,
            table=COURT_TABLE,
        )
        logger.info("Court table updates")
    except Exception as e:
        logger.error("Cannot create court catlog-->",errormsg)

        
def upload_es(df,es,ES_INDEX,ES_DOC):
    jsonData=df.to_json(orient ='index')
    records=json.loads(jsonData)
    try:
        for record in records:
            dataBody=records[record]
            x=es.index(index=ES_INDEX,doc_type=ES_DOC,id=dataBody['id'],body=dataBody)
        logger.info(f"Index sync in Elastic search with {len(records)}")
    except Exception as errormsg:
        logger.error("Cannot uploaded to elastic search",errormsg)


def main():

    HOST= get_common('HOST')
    REGION_NAME = get_common('REGION_NAME')
    COURT_CSV_LOC = get_job_specific_parameters('COURT_CSV_LOC')
    COURT_OUTPUT_PARQUET_LOC = get_job_specific_parameters('COURT_OUTPUT_PARQUET_LOC')
    COURT_TABLE = get_job_specific_parameters('COURT_TABLE')
    ES_DOC=get_job_specific_parameters('ES_DOC')
    ES_INDEX = get_job_specific_parameters('ES_INDEX')
    DB_NAME = get_common('DATABASE')
    ATHENA_QUERY_RESULTS = get_job_specific_parameters('ATHENA_QUERY_RESULTS')
    client = boto3.client('s3', region_name=REGION_NAME)
    athena_client = boto3.client('athena')
    credentials = boto3.Session().get_credentials()
    AWSAUTH = AWS4Auth(credentials.access_key, credentials.secret_key, REGION_NAME, "es", session_token=credentials.token)
    es = Elasticsearch(hosts = [{'host': HOST, 'port': 443}], http_auth = AWSAUTH, use_ssl = True, verify_certs = True, connection_class = RequestsHttpConnection)
    CREATE_VIEW_QUERY ='''CREATE OR REPLACE VIEW court_view 
    AS
    SELECT * 
    FROM court_registry c1
    WHERE modified = (SELECT MAX(modified) FROM court_registry c2 WHERE c1.id = c2.id)'''
    out_df = csv_to_df(COURT_CSV_LOC)
    out_df['ID'] = out_df['ID'].astype(str)
    create_court_catalog(out_df,COURT_OUTPUT_PARQUET_LOC,DB_NAME,COURT_TABLE) 
    response = athena_client.start_query_execution(QueryString=CREATE_VIEW_QUERY,QueryExecutionContext={'Database': DB_NAME},ResultConfiguration={'OutputLocation': ATHENA_QUERY_RESULTS})
    upload_es(out_df,es,ES_INDEX,ES_DOC)
    


def lambda_handler(event, context):
    global logger
    logger = logging_utils(context.function_name,context.aws_request_id).logging()
    Start = datetime.now()
    try: 
        main()
        status = 'SUCCESS'
        logger.info("Job run SUCCESS")
    except Exception as errormsg:
        status = 'FAILED'
        logger.error("Cannot run the main function",errormsg)
    End = datetime.now()
    # DataopsUtilss().insertDataInJobLog(JobId=context.aws_request_id,fkJobName=context.function_name, FileName=COURT_CSV_LOC, Size=None, NumberofComponents=0,NumberofFiles=1, Start=Start,End=End,LookupJobStatus=status) 
    




