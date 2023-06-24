import os
import json
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom.hooks import MovielensHook

class MovielensFetchRatingsOperator(BaseOperator):
    template_fields = ("_start_date","_end_date","_output_path")
    @apply_defaults
    def __init__(self, conn_id, output_path, start_date="{{ds}}", end_date = "{{next_ds}}",**kwargs):
        super(MovielensFetchRatingsOperator, self).__init__(**kwargs)
        self._conn_id = conn_id
        self._output_path = output_path
        self._start_date = start_date
        self._end_date = end_date

    def execute(self, context):
        hook = MovielensHook(conn_id=self._conn_id)
        try:
            self.log.info(f"Fetching ratings from {self._start_date} to {self._end_date}.")
            ratings = list(hook.get_ratings(
            start_date = self._start_date,
            end_date = self._end_date,
            ))

            self.log.info(f"Fetched {len(ratings)} ratings.")
        finally:
            hook.close()

        self.log.info(f"Writing ratings to {self._output_path}")

        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)

        with open(self._output_path, "w") as file_:
            json.dump(ratings, fp=file_)
