import logging
from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from ..hooks.stitch_hook import StitchHook


class StitchRunSourceOperator(BaseOperator): 
    ui_color = '#00cdcd'
    
    @apply_defaults
    def __init__(self, source_id: str=None, client_id: str=None, http_conn_id: str=None, 
                 api_version: str='v4', *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if source_id is None:
            raise AirflowException('source_id is required')
        if client_id is None:
            raise AirflowException('client_id is required')
        if http_conn_id is None:
            raise AirflowException('http_conn_id is required')

        self.source_id = source_id
        self.client_id = client_id
        self.http_conn_id = http_conn_id
        self.base_url = f'https://api.stitchdata.com/{api_version}'

    def execute(self, context):
        self.stitch_hook = StitchHook(http_conn_id=self.http_conn_id)
        self.stitch_hook._get_credentials()
        self.headers = {
            'Authorization': 'Bearer ' + self.stitch_hook.password,
            'Content-type': 'application/json'
        }
        logging.info(f'Starting extraction: source_id = {self.source_id}')
        self.stitch_hook._trigger_extraction(source_id=self.source_id, client_id=self.client_id, 
                                             base_url=self.base_url, headers=self.headers)


class StitchRunAndMonitorSourceOperator(StitchRunSourceOperator): 
    @apply_defaults
    def __init__(self, sleep_time: int=None, timeout: int=None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if sleep_time is None:
            self.sleep_time = 300
        if timeout is None:
            self.timeout = 86400

    def execute(self, context):
        tic = datetime.now()

        super().execute(context)

        logging.info(f'Monitoring source: source_id = {self.source_id}')
        self.stitch_hook._monitor_extraction(sleep_time=self.sleep_time, timeout=self.timeout, 
                                             source_id=self.source_id, client_id=self.client_id, 
                                             base_url=self.base_url, headers=self.headers, start_time=tic)
