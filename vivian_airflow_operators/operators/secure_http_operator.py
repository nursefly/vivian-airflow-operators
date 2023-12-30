from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from ..hooks.secure_http_hook import SecureHttpHook


class SecureHttpOperator(SimpleHttpOperator):  
    @apply_defaults
    def __init__(self, connection_id: str=None, authorization_type: str='Basic', *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if authorization_type not in ('Bearer', 'Basic'):
            raise AirflowException('authorization_type must be either Bearer or Basic')
        if connection_id is None:
            raise AirflowException('connection_id is required')

        self.authorization_type = authorization_type
        self.connection_id = connection_id   

    def execute(self, context):
        self.secure_http_hook = SecureHttpHook(connection_id=self.connection_id)
        self.secure_http_hook._get_credentials()
        self.headers['Authorization'] = self.authorization_type + ' ' + self.secure_http_hook.password
        return super().execute(context)