import logging
import time
from datetime import datetime

from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

from .secure_http_hook import SecureHttpHook


class StitchHook(SecureHttpHook):
    @apply_defaults
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
    
    def _trigger_extraction(self, source_id: str, client_id: str, base_url: str, headers: dict[str, str]) -> dict:
        url = (f'{base_url}/sources/{source_id}/sync')
        dict_data = self._get_response('POST', url, headers=headers)
        error = dict_data.get('error', None)
        job_name = dict_data.get('job_name', None)

        # check if process is already running (still returns 200 so need separate check)
        if error is not None:
            raise AirflowException(f'Error: type = {error.get("type", None)}, message = {error.get("message", None)}')
        
        if job_name is None:
            raise AirflowException(f'Error: the API did not return a job_name, response: {dict_data}')
        else:
            logging.info(f'Extraction triggered: source_id = {source_id}, job_name = {job_name}, integration url = https://app.stitchdata.com/client/{client_id}/pipeline/v2/sources/{source_id}/')

    def _monitor_extraction(self, source_id: str, client_id: str, base_url: str, headers: dict[str, str], 
                            sleep_time: int, timeout: int, start_time: datetime=datetime.now()) -> None:
        time.sleep(30) # let the job actually trigger
        url = (f'{base_url}/{client_id}/extractions')
        id_found = False

        while (datetime.now() - start_time).seconds < timeout:
            dict_data = self._get_response(url, 'GET', headers)

            for item in dict_data['data']:
                if str(item['source_id']) == source_id:
                    logging.info(f'Extraction status object: {item}')
                    id_found = True
                    last_extraction_completion_time = datetime.strptime(item['completion_time'],'%Y-%m-%dT%H:%M:%SZ')

                    if last_extraction_completion_time < start_time:
                        logging.info(f'Waiting for all extractions to complete: source_id = {source_id}')
                        time.sleep(sleep_time)
                        url = (f'{base_url}/{client_id}/extractions')   
                    elif item['tap_exit_status'] != 0 and item['tap_exit_status'] is not None:
                        raise AirflowException(f'Error: source_id = {source_id} extraction failed')
                    else:
                        exec_time = (datetime.now() - start_time).seconds
                        logging.info(f'Extraction succeeded in {exec_time} seconds')
                        return    
                    break

            if 'next' in dict_data['links'] and not id_found:
                next = dict_data['links']['next']
                url = (f'https://api.stitchdata.com{next}')
            elif not id_found:
                raise AirflowException(f'Error: source_id = {source_id} not found in response')
        
        raise AirflowException(f'Error: source_id = {source_id} timed out')
