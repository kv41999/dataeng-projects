from airflow import DAG
from datetime import datetime
from custom.data_fetch_op import FormulaOneDataFetchOperator
from custom.data_TL_op import FormulaOneDataTransformer
from custom.data_fetch_sensor import FormulaOneDataFetchSensor

with DAG(
    dag_id = "FormulaOneETL4",
    start_date= datetime(2010,1,1),
    end_date= datetime(2020,1,1),
    schedule_interval= "@yearly",
    catchup=False
) as dag:
    race_data_fetch = FormulaOneDataFetchOperator(
        task_id = "Race_data_fetch",
        output_path= "/mnt/c/bigdata/formula_one_data/race/",
        url_query= "results",
        start_date= "{{ ds }}"
    )

    quali_data_fetch = FormulaOneDataFetchOperator(
        task_id = "Quali_data_fetch",
        output_path= "/mnt/c/bigdata/formula_one_data/quali/",
        url_query= "qualifying",
        start_date= "{{ ds }}"
    )

    data_sensor = FormulaOneDataFetchSensor(
        task_id = "Data_fetch_sensor",
        race_input_path = "/mnt/c/bigdata/formula_one_data/race",
        quali_input_path = "/mnt/c/bigdata/formula_one_data/quali"
    )

    transform_load = FormulaOneDataTransformer(
        task_id = "Transform_Load",
        race_input_path = "/mnt/c/bigdata/formula_one_data/race",
        quali_input_path = "/mnt/c/bigdata/formula_one_data/quali"
    )

    race_data_fetch >> data_sensor
    quali_data_fetch >> data_sensor
    data_sensor >> transform_load

    #2