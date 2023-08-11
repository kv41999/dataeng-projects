from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import requests
import os

class FormulaOneDataFetchOperator(BaseOperator):
    template_fields = ("_start_date")
    
    @apply_defaults
    def __init__(self, race_output_path, quali_output_path, start_date = "{{ ds }}", **kwargs):
        super(FormulaOneDataFetchOperator,self).__init__(**kwargs)
        self._start_date = start_date
        self.race_output_path = race_output_path
        self.quali_output_path = quali_output_path
    
    def execute(self, context):
        start_year = int(self._start_date[:4])

        offset = 0
        response = requests.get(f"http://ergast.com/api/f1/{start_year}/results.json", params={"offset":offset})
        resp = response.json()
        final = []
        limit = int(resp["MRData"]["limit"])
        total = int(resp["MRData"]["total"])
        
        while offset < total:
            response = requests.get(f"http://ergast.com/api/f1/{start_year}/results.json", params={"offset":offset})
            resp = response.json()
            resp = resp["MRData"]["RaceTable"]["Races"]
            final += resp
            offset += limit
        
        final = {"data":final}
        race_output_dir = os.path.dirname(self.race_output_path)
        os.makedirs(race_output_dir, exist_ok=True)

        with open(race_output_dir + f"/race_{start_year}.json", "w") as f:
            f.write(json.dumps(final))
        
        offset = 0
        response = requests.get(f"http://ergast.com/api/f1/{start_year}/qualifying.json", params={"offset":offset})
        resp = response.json()
        final = []
        limit = int(resp["MRData"]["limit"])
        total = int(resp["MRData"]["total"])
        
        while offset < total:
            response = requests.get(f"http://ergast.com/api/f1/{start_year}/qualifying.json", params={"offset":offset})
            resp = response.json()
            resp = resp["MRData"]["RaceTable"]["Races"]
            final += resp
            offset += limit
        
        final = {"data":final}
        quali_output_dir = os.path.dirname(self.quali_output_path)
        os.makedirs(quali_output_dir, exist_ok=True)

        with open(quali_output_dir + f"/quali_{start_year}.json", "w") as f:
            f.write(json.dumps(final))
        
        ti = context["ti"]
        ti.xcom_push(key="year",value=start_year)