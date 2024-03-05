import time
from airflow.models import BaseOperator
import json
import urllib3
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

        url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
        data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'resource': resource
        }
        encoded_data = urlencode(data).encode('utf-8')

        http = urllib3.PoolManager()
        response = http.request(
            'POST',
            url,
            body=encoded_data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        response_data = json.loads(response.data)

        if response.status == 200 and 'access_token' in response_data:
            self.access_token = response_data['access_token']
        else:
            raise Exception(f"Failed to obtain Power BI access token. Response: {response_data}")

        print(self.access_token)
        return
