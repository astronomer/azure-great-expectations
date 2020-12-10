from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from hooks.azure_data_factory_hook import AzureDataFactoryHook
from airflow.contrib.hooks.wasb_hook import WasbHook
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults
from great_expectations.data_context import BaseDataContext


#Get yesterday's date, in the correct format
yesterday_date = '{{ yesterday_ds_nodash }}'

#Define Great Expectations file paths
data_dir = '/usr/local/airflow/include/data/'
data_file = '/usr/local/airflow/include/data/or_20201208.csv'
ge_root_dir = '/usr/local/airflow/include/great_expectations'

#Define Great Expectations contexts
data_context_config = DataContextConfig(
    datasources={
        "data__dir": DatasourceConfig(
            class_name="PandasDatasource",
            batch_kwargs_generators={
                "subdir_reader": {
                    "class_name": "SubdirReaderBatchKwargsGenerator",
                    "base_directory": data_dir,
                }
            },
        )
    },
    store_backend_defaults=FilesystemStoreBackendDefaults(root_directory=ge_root_dir)
)
data_context = BaseDataContext(project_config=data_context_config, context_root_dir=ge_root_dir)


def run_adf_pipeline(pipeline_name, date):
    '''Runs an Azure Data Factory pipeline using the AzureDataFactoryHook and passes in a date parameter
    '''
    
    #Create a dictionary with date parameter 
    params = {}
    params["date"] = date

    #Make connection to ADF, and run pipeline with parameter
    hook = AzureDataFactoryHook('azure_data_factory_conn')
    hook.run_pipeline(pipeline_name, parameters=params)


def get_azure_blob_files():
    '''Downloads file from Azure blob storage
    '''
    azure = WasbHook(wasb_conn_id='azure_blob')
    azure.get_file(data_file, container_name='covid-data', blob_name='or/20201208.csv')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


with DAG('adf_great_expectations',
         start_date=datetime(2019, 1, 1),
         max_active_runs=1,
         schedule_interval='@daily', 
         default_args=default_args,
         catchup=False
         ) as dag:

         opr_run_pipeline = PythonOperator(
            task_id='run_pipeline',
            python_callable=run_adf_pipeline,
            op_kwargs={'pipeline_name': 'pipeline1', 'date': yesterday_date}
         )

         opr_download_data = PythonOperator(
            task_id='download_data',
            python_callable=get_azure_blob_files
         )

         opr_ge_check = GreatExpectationsOperator(
            task_id='ge_check',
            expectation_suite_name='azure.demo',
            checkpoint_name="azure.pass.chk",
            data_context=data_context
         )

         opr_send_email = EmailOperator(
            task_id='send_email',
            to='noreply@astronomer.io',
            subject='Covid to S3 DAG',
            html_content='<p>The great expectations checks passed successfully. <p>'
        )

         opr_run_pipeline >> opr_download_data >> opr_ge_check >> opr_send_email
          
