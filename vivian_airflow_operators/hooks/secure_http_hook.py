import json
import requests

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults


class SecureHttpHook(BaseHook):
    @apply_defaults
    def __init__(self, http_conn_id: str=None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if http_conn_id is None:
            raise AirflowException('http_conn_id is required')
        
        self.http_conn_id = http_conn_id
    
    def _get_credentials(self):
        conn = BaseHook(None).get_connection(conn_id=self.http_conn_id)
        self.password = conn.password

    def _get_response(self, url: str, method: str, headers: dict[str, str]) -> dict:
        response = requests.request(method, url, headers=headers)
        response.raise_for_status()
        response_data = response.text
        dict_data = json.loads(response_data)

        return dict_data
