import logging
from datetime import datetime

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from ..hooks.stitch_hook import StitchHook


class StitchMonitorSourceSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, source_id: str=None, client_id: str=None, connection_id: str=None, 
                 api_version: str='v4', sleep_time: int=300, timeout: int=86400, 
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if source_id is None:
            raise AirflowException('source_id is required')
        if client_id is None:
            raise AirflowException('client_id is required')
        if connection_id is None:
            raise AirflowException('connection_id is required')

        self.source_id = source_id
        self.client_id = client_id
        self.connection_id = connection_id
        self.base_url = f'https://api.stitchdata.com/{api_version}'
        self.sleep_time = sleep_time
        self.timeout = timeout

    def _get_password(self, context):
        self.stitch_hook = StitchHook(connection_id=self.connection_id)
        self.stitch_hook._get_credentials()

    def poke(self, context):
        tic = datetime.now()

        self.stitch_hook = StitchHook(connection_id=self.connection_id)
        self.stitch_hook._get_credentials()
        self.headers = {
            'Authorization': 'Bearer ' + self.stitch_hook.password,
            'Content-type': 'application/json'
        }

        logging.info(f'Monitoring source: source_id = {self.source_id}')
        self.stitch_hook._monitor_extraction(sleep_time=self.sleep_time, timeout=self.timeout, 
                                             source_id=self.source_id, client_id=self.client_id, 
                                             base_url=self.base_url, headers=self.headers, start_time=tic)
