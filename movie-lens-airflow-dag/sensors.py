from airflow.utils.decorators import apply_defaults
from airflow.sensors.base import BaseSensorOperator
from custom.hooks import MovielensHook

class MovielensRatingsSensor(BaseSensorOperator):
    template_fields = ("_start_date","_end_date")

    @apply_defaults
    def __init__(self, conn_id, start_date = "{{ds}}", end_date = "{{next_ds}}", **kwargs):
        super().__init__(**kwargs)
        self._conn_id = conn_id
        self._start_date = start_date
        self._end_date = end_date

    def poke(self, context):
        hook  = MovielensHook(conn_id=self._conn_id)

        try:
            next(
                hook.get_ratings(
                    start_date=self._start_date,
                    end_date=self._end_date,
                    batch_size=1
                )
            )
            self.log.info(f"Found ratings for {self._start_date} to {self._end_date}")
            return True
        except StopIteration:
            self.log.info(f"Didn't find any ratings for {self._start_date} to {self._end_date}")
            return False
        finally:
            hook.close()