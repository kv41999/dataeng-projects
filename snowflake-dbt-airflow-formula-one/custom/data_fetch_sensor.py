from airflow.utils.decorators import apply_defaults
from airflow.sensors.base import BaseSensorOperator
import os

class FormulaOneDataFetchSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, race_input_path, quali_input_path,**kwargs):
        super().__init__(**kwargs)
        self.race_input_path = race_input_path
        self.quali_input_path = quali_input_path

    def poke(self,context):

        ti = context["ti"]
        start_year = ti.xcom_pull("Race_data_fetch","FormulaOneETL4","year")

        if os.path.exists(f"/mnt/c/bigdata/formula_one_data/race/race_{start_year}.json") and os.path.exists(f"/mnt/c/bigdata/formula_one_data/quali/quali_{start_year}.json"):
            return True
        else:
            return False
