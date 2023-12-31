import logging
from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from ..hooks.stitch_hook import StitchHook


class StitchRunSourceOperator(BaseOperator): 
    ui_color = '#00cdcd'
    
    @apply_defaults
    def __init__(self, source_id: str=None, client_id: str=None, conn_id: str=None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if source_id is None:
            raise AirflowException('source_id is required')
        if client_id is None:
            raise AirflowException('client_id is required')
        if conn_id is None:
            raise AirflowException('conn_id is required')

        self.source_id = source_id
        self.client_id = client_id
        self.conn_id = conn_id

    def execute(self, context):
        self.stitch_hook = StitchHook(conn_id=self.conn_id)
        self.stitch_hook._get_credentials()
        logging.info(f'Starting extraction: source_id = {self.source_id}')
        self.stitch_hook._trigger_extraction(source_id=self.source_id, client_id=self.client_id)


class StitchRunAndMonitorSourceOperator(StitchRunSourceOperator): 
    @apply_defaults
    def __init__(self, sleep_time=300, timeout=86400, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.sleep_time = sleep_time
        self.timeout = timeout

    def execute(self, context):
        tic = datetime.now()

        super().execute(context)

        logging.info(f'Monitoring source: source_id = {self.source_id}')
        self.stitch_hook._monitor_extraction(sleep_time=self.sleep_time, timeout=self.timeout, source_id=self.source_id, client_id=self.client_id, start_time=tic)
