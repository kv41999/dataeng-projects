from airflow import DAG
from datetime import datetime
from custom.data_fetch_op import FormulaOneDataFetchOperator
from custom.data_TL_op import FormulaOneDataTransformer

with DAG(
    dag_id = "FormulaOneETL2",
    start_date= datetime(2010,1,1),
    end_date= datetime(2020,1,1),
    schedule_interval= "@yearly",
    catchup=False
) as dag:
    data_fetch = FormulaOneDataFetchOperator(
        task_id = "Data_fetch",
        race_output_path= "/mnt/c/bigdata/formula_one_data/race/",
        quali_output_path= "/mnt/c/bigdata/formula_one_data/quali/",
        start_date= "{{ ds }}"
    )

    transform_load = FormulaOneDataTransformer(
        task_id = "Transform_Load",
        race_input_path = "/mnt/c/bigdata/formula_one_data/race",
        quali_input_path = "/mnt/c/bigdata/formula_one_data/quali"
    )

    data_fetch >> transform_load

    #2