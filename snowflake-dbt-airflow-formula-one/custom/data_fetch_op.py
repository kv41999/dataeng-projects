from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import requests
import os

class FormulaOneDataFetchOperator(BaseOperator):
    template_fields = ("_start_date")
    
    @apply_defaults
    def __init__(self, output_path, url_query, start_date = "{{ ds }}", **kwargs):
        super(FormulaOneDataFetchOperator,self).__init__(**kwargs)
        self._start_date = start_date
        self.output_path = output_path
        self.url_query = url_query
    
    def execute(self, context):
        start_year = int(self._start_date[:4])

        offset = 0
        response = requests.get(f"http://ergast.com/api/f1/{start_year}/{self.url_query}.json", params={"offset":offset})
        resp = response.json()
        final = []
        limit = int(resp["MRData"]["limit"])
        total = int(resp["MRData"]["total"])
        
        while offset < total:
            response = requests.get(f"http://ergast.com/api/f1/{start_year}/{self.url_query}.json", params={"offset":offset})
            resp = response.json()
            resp = resp["MRData"]["RaceTable"]["Races"]
            final += resp
            offset += limit
        
        final = {"data":final}
        output_dir = os.path.dirname(self.output_path)
        os.makedirs(output_dir, exist_ok=True)

        file_name = self.output_path.split("/")[-2]

        with open(output_dir + f"/{file_name}_{start_year}.json", "w") as f:
            f.write(json.dumps(final))
        
        ti = context["ti"]
        ti.xcom_push(key="year",value=start_year)