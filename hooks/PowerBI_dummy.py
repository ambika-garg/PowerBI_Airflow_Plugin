import json
import urllib3
from airflow.hooks.base import BaseHook
from urllib.parse import urlencode

class PowerBIClientCredentialsHook(BaseHook):
    """
    Custom Airflow Hook to obtain Power BI access token using Client Credentials Flow (client_id and client_secret).
    """
    def __init__(self, client_id, client_secret, tenant_id='common', resource='https://analysis.windows.net/powerbi/api'):
        """
        Initialize the hook.

        :param client_id: The client ID (Application ID) of the registered application in Azure AD.
        :type client_id: str
        :param client_secret: The client secret (Application Secret) of the registered application in Azure AD.
        :type client_secret: str
        :param tenant_id: The tenant ID of the Azure AD tenant. Default is 'common'.
        :type tenant_id: str
        :param resource: The resource for which the access token is requested. Default is Power BI API resource.
        :type resource: str
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.resource = resource
        self.access_token = None

    def get_access_token(self):
        if not self.access_token:
            url = f'https://login.microsoftonline.com/{self.tenant_id}/oauth2/token'
            data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'resource': self.resource
            }
            encoded_data = urlencode(data).encode('utf-8')

            print(f"Getting URL: {url} - data: {data} - encoded_data: {encoded_data}")

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

        return self.access_token