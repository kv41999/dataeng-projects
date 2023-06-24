import requests
from airflow.hooks.base import BaseHook

class MovielensHook(BaseHook):
    DEFAULT_HOST = "localhost"
    DEFAULT_SCHEMA = "http"
    DEFAULT_PORT = 5000
    
    def __init__(self,conn_id):
        super().__init__()
        self._conn_id = conn_id
        self._session = None
        self._base_url = None
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def close(self):
        """Closes any active session."""
        if self._session:
            self._session.close()
        self._session = None
        self._base_url = None
    
    def get_conn(self):
        if self._session is None:    
            config = self.get_connection(conn_id=self._conn_id)
            schema = config.schema or self.DEFAULT_SCHEMA
            host = config.host or self.DEFAULT_HOST
            port = config.port or self.DEFAULT_PORT

            self._base_url = f"{schema}://{host}:{port}"
            self._session = requests.Session()
            self.log.info(f"{self._session}")

            if config.login:
                self._session.auth = (config.login, config.password)

        return self._session, self._base_url

    def _get_with_pagination(self, endpoint, params, batch_size = 100):
        session, base_url = self.get_conn()
        url = base_url + endpoint

        offset = 0
        total = None
        while total is None or offset<total:
            response = session.get(
                url,
                params = {
                    **params,
                    **{"offset":offset, "limit": batch_size}
                }
            )
            response.raise_for_status()
            response_json = response.json()

            yield from response_json["result"]

            offset += batch_size
            total = response_json["total"]
        
    def get_ratings(self, start_date=None, end_date=None, batch_size=100):
        yield from self._get_with_pagination(
            endpoint="/ratings",
            params={
                "start_date":start_date,
                "end_date": end_date
            },
            batch_size=batch_size
        )    

    