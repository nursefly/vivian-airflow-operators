import json
import requests
import logging

from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults


class SecureHttpHook(HttpHook):
    @apply_defaults
    def __init__(self, http_conn_id: str=None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if http_conn_id is None:
            raise AirflowException('http_conn_id is required')
        
        self.http_conn_id = http_conn_id
    
    def _get_credentials(self):
        conn = self.get_conn()

        logging.info(vars(conn))

        self.password = conn.password

    def _get_response(self, url: str, method: str, headers: dict[str, str]) -> dict:
        response = requests.request(method, url, headers=headers)
        response.raise_for_status()
        response_data = response.text
        dict_data = json.loads(response_data)

        return dict_data
