import logging

from datetime import datetime
from airflow import DAG, settings
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.postgres_operator import PostgresOperator

from dags.kpi_processing_operator import KPIProcessingOperator


logger = logging.getLogger(__name__)


def prepare_data_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    inner_dag_name = f'{parent_dag_name}.{child_dag_name}'
    inner_dag = DAG(inner_dag_name,
                    schedule_interval=schedule_interval,
                    start_date=start_date)
    clean_xcoms = PostgresOperator(
        task_id='clean_xcoms',
        postgres_conn_id='airflow_db',
        sql=f"delete from xcom where dag_id='{inner_dag_name}'",
        dag=inner_dag)

    prepare_data_op = PythonOperator(task_id='prepare_data', python_callable=prepare_data, dag=inner_dag)
    clean_xcoms >> prepare_data_op

    return inner_dag


def processing_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval, **kwargs):
    parent_dag = kwargs.get('parent_dag')
    inner_dag = DAG(
        f'{parent_dag_name}.{child_dag_name}',
        schedule_interval=schedule_interval,
        start_date=start_date,
    )
    logger.info(f"generating dags {parent_dag_name}.{child_dag_name}")
    # # debug
    # from IPython import embed
    # embed()
    # import pdb
    # pdb.set_trace()
    active_runs = parent_dag.get_active_runs()
    logger.info(f"exist runs {active_runs}")
    if len(active_runs) > 0:
        parent_task = parent_dag.get_task_instances(session=settings.Session, start_date=active_runs[-1])[-1]
        logger.info(f"parent task {parent_task}")
        task_data = parent_task.xcom_pull(dag_id=f'{parent_dag_name}.{PREPARE_DATA_DAG_NAME}', task_ids='prepare_data')
        logger.info(f"pulled data {task_data}")
        if task_data:
            #from airflow.utils.helpers import chain
            #tasks = []
            for country, region in task_data:
                logger.info(f"creating task for {country}, {region}")
                # KPIProcessingOperator(task_id=f'process_{country}_{region}',
                #                       dag=inner_dag,
                #                       country=country,
                #                       region=region)

                def run_some_task(*args, **kwargs):
                    task_data = f'processed: {args} {kwargs}'
                    logger.warning(task_data)
                    return task_data

                PythonOperator(
                    task_id=f'process_{country}_{region}',
                    python_callable=run_some_task,
                    op_kwargs={'country': country, 'region': region},
                    dag=inner_dag)
                logger.info(f"task for {country}, {region} has been created")
           # chain(tasks)
    logger.info("finish dag")
    return inner_dag


def prepare_data():
    return [('UA', 1), ('US', 2), ('UK', 3)]


MAIN_DAG_NAME = 'MyLovelyMainDag'
PREPARE_DATA_DAG_NAME = 'PreparingDataForProcessing'
PROCESS_DATA_DAG_NAME = 'ProcessingDagN'

main_dag = DAG(MAIN_DAG_NAME, description='creating dag. it should generate many child tasks and '
                                          'pass data between them',
               schedule_interval=None,
               start_date=datetime(2017, 3, 20), catchup=False)

prepare_data_sdug = SubDagOperator(subdag=prepare_data_sub_dag(parent_dag_name=MAIN_DAG_NAME,
                                                               child_dag_name=PREPARE_DATA_DAG_NAME,
                                                               start_date=main_dag.start_date,
                                                               schedule_interval=main_dag.schedule_interval),
                                   task_id=PREPARE_DATA_DAG_NAME, dag=main_dag)


process_data_sdug = SubDagOperator(subdag=processing_sub_dag(parent_dag_name=MAIN_DAG_NAME,
                                                             child_dag_name=PROCESS_DATA_DAG_NAME,
                                                             start_date=main_dag.start_date,
                                                             schedule_interval=main_dag.schedule_interval,
                                                             parent_dag=main_dag),
                                   task_id=PROCESS_DATA_DAG_NAME, dag=main_dag)

done = DummyOperator(task_id='the_end', dag=main_dag)


prepare_data_sdug >> process_data_sdug >> done
