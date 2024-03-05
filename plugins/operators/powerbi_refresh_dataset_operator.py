import time
from airflow.models import BaseOperator
import requests
from airflow.models.variable import Variable
from hooks.powerbi_hook import PowerBIHook
from urllib.parse import urlencode

class PowerBIDatasetRefreshOperator(BaseOperator):
    """
    Refreshes a PowerBI Dataset

    """

    def __init__(
        self,
        dataset_id: str,
        group_id: str = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.dataset_id = dataset_id
        self.group_id = group_id
        self.hook = None


    def execute(self, context):
        # Get Hook class
        # if not self.hook:
        #     self.hook = PowerBIHook(client_id = self.client_id)

        # self.hook.dataset_refresh(dataset_id=self.dataset_id, group_id=self.group_id)

        # call Dataset Refresh
        client_id=Variable.get("client_id", default_var=None)
        client_secret = Variable.get("client_secret", default_var=None)
        tenant_id = Variable.get("tenant_id", default_var=None)
        resource='https://analysis.windows.net/powerbi/api'
        scope='https://api.powerbi.com'
        username=Variable.get("username", default_var=None)
        password = Variable.get("password", default_var=None)

        url = f'https://login.windows.net/common/oauth2/token'
        data = {
            'grant_type': 'password',
            'client_id': client_id,
            'client_secret': client_secret,
            'resource': resource,
            'username': username,
            'password': password,
            'scope': scope
        }

        r = requests.post(self.auth_url, data=data)
        r.raise_for_status()

        print(r.json().get('access_token'))
        return
