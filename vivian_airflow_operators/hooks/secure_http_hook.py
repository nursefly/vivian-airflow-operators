import json
import requests

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults


class SecureHttpHook(BaseHook):
    @apply_defaults
    def __init__(self, connection_id: str=None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if connection_id is None:
            raise AirflowException('connection_id is required')
        
        self.connection_id = connection_id
    
    def _get_credentials(self):
        conn = BaseHook(None).get_connection(conn_id=self.connection_id)
        self.password = conn.password

    def _get_response(self, url: str, method: str, headers: dict[str, str]) -> dict:
        response = requests.request(method, url, headers=headers)
        response.raise_for_status()
        response_data = response.text
        dict_data = json.loads(response_data)

        return dict_data
