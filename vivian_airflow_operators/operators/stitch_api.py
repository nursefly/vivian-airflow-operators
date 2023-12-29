import logging
import requests
import json
import time
from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class StitchRunSourceOperator(BaseOperator): 
    ui_color = '#00cdcd'
    
    @apply_defaults
    def __init__(self, source_id: str = None, client_id: str = None, connection_id: str = None, api_version: str = 'v4', *args, **kwargs) -> None:
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
    
    def _get_credentials(self):
        conn = BaseHook(None).get_connection(conn_id=self.connection_id)
        api_key = conn.password
        self.headers = {
            'Authorization': api_key,
            'Content-type': 'application/json'
        }

    def _get_response(self, url: str, method: str) -> dict:
        response = requests.request(method, url, headers=self.headers)
        response.raise_for_status()
        response_data = response.text
        dict_data = json.loads(response_data)

        return dict_data
    
    def _trigger_extraction(self):
        url = (f'{self.base_url}/sources/{self.source_id}/sync')
        dict_data = self._get_response(url, 'POST')
        error = dict_data.get('error', None)
        job_name = dict_data.get('job_name', None)

        # check if process is already running (still returns 200 so need separate check)
        if error is not None:
            raise AirflowException(f'Error: type = {error.get("type", None)}, message = {error.get("message", None)}')
        
        if job_name is None:
            raise AirflowException(f'Error: the API did not return a job_name, response: {dict_data}')
        else:
            logging.info(f'Extraction triggered: source_id = {self.source_id}, job_name = {job_name}, integration url = https://app.stitchdata.com/client/{self.client_id}/pipeline/v2/sources/{self.source_id}/')

        return job_name

    def execute(self, context):
        self._get_credentials()
        logging.info(f'Starting extraction: source_id = {self.source_id}')
        self.job_name = self._trigger_extraction()


class StitchRunAndMonitorSourceOperator(StitchRunSourceOperator): 
    @apply_defaults
    def __init__(self, sleep_time: int = None, timeout: int = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if sleep_time is None:
            self.sleep_time = 300
        if timeout is None:
            self.timeout = 86400

    def _monitor_extraction(self, start_time: datetime):
        time.sleep(30) # let the job actually trigger
        url = (f'{self.base_url}/{self.client_id}/extractions')
        id_found = False

        while (datetime.now() - start_time).seconds < self.timeout:
            dict_data = self._get_response(url, 'GET')

            for item in dict_data['data']:
                if str(item['source_id']) == self.source_id:
                    logging.info(f'Extraction status object: {item}')
                    id_found = True
                    last_extraction_completion_time = datetime.strptime(item['completion_time'],'%Y-%m-%dT%H:%M:%SZ')

                    if last_extraction_completion_time < start_time:
                        logging.info(f'Waiting for all extractions to complete: source_id = {self.source_id}')
                        time.sleep(self.sleep_time)
                        url = (f'{self.base_url}/{self.client_id}/extractions')   
                    elif item['tap_exit_status'] != 0 and item['tap_exit_status'] is not None:
                        raise AirflowException(f'Error: source_id = {self.source_id} extraction failed')
                    else:
                        exec_time = (datetime.now() - start_time).seconds
                        logging.info(f'Extraction succeeded in {exec_time} seconds')
                        return    
                    break

            if 'next' in dict_data['links'] and not id_found:
                next = dict_data['links']['next']
                url = (f'https://api.stitchdata.com{next}')
            elif not id_found:
                raise AirflowException(f'Error: source_id = {self.source_id} not found in response')
        
        raise AirflowException(f'Error: source_id = {self.source_id} timed out')

    def execute(self, context):
        tic = datetime.now()

        super().execute(context)

        logging.info(f'Monitoring source: source_id = {self.source_id}')
        self._monitor_extraction(tic)
