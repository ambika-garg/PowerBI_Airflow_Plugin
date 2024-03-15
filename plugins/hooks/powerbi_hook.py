from airflow.hooks.base import BaseHook
from airflow.models import Variable
from azure.identity import ClientSecretCredential
from airflow.exceptions import AirflowException
import requests

class PowerBIHook(BaseHook):

    resource = "https://analysis.windows.net/powerbi/api"

    def __init__(
        self,
        dataset_id: str,
        group_id: str = None
    ):
        self.client_id = dataset_id,
        self.group_id = group_id

    def dataset_refresh(self, dataset_id: str, group_id: str = None) -> None:
        """
        Triggers a refresh for the specified dataset from "My Workspace" if
        no `group id` is specified or from the specified workspace when
        `group id` is specified.

        :param dataset_key: The dataset id.
        :param group_id: The workspace id.
        """
        api_version = "v1.0"

        url = f'https://api.powerbi.com/{api_version}/myorg'

        # add the group id if it is specified
        if group_id:
            url += f'/groups/{group_id}'

        # add the dataset key
        url += f'/datasets/{dataset_id}/refreshes'

        self._send_request('POST', url=url)



    def _get_token(self) -> str:
        """
        Retrieve the access token used to authenticate against the API.
        """

        client_id=Variable.get("client_id", default_var=None)
        client_secret = Variable.get("client_secret", default_var=None)
        # resource='https://analysis.windows.net/powerbi/api'
        # scope='https://api.powerbi.com'
        # username=Variable.get("username", default_var=None)
        # password = Variable.get("password", default_var=None)

        credential = ClientSecretCredential(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id="98c45f19-7cac-4002-8702-97d943a5ccb4"
        )

        token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")

        print("Client_secret_credential", token)

        # url = f'https://login.windows.net/common/oauth2/token'
        # data = {
        #     'grant_type': 'password',
        #     'client_id': client_id,
        #     'client_secret': client_secret,
        #     'resource': resource,
        #     'username': username,
        #     'password': password,
        #     'scope': scope
        # }

        # r = requests.post(url, data=data)
        # r.raise_for_status()

        # access_token = r.json().get('access_token')
        return ""


    def _send_request(
        self,
        request_type: str,
        url: str,
        **kwargs
    ) -> requests.Response:
        """
        Send a request to the Power BI REST API.

        This method checks to see if authorisation token has been retrieved and
        the request `header` has been built using it. If not then it will
        establish the connection to perform this action on the first call. It
        is important to NOT have this connection established as part of the
        initialisation of the hook to prevent a Power BI API call each time
        the Airflow scheduler refreshes the DAGS.


        :param request_type: Request type (GET, POST, PUT etc.).
        :param url: The URL against which the request needs to be made.
        :return: requests.Response
        """
        self.header = {'Authorization': f'Bearer {self._get_token()}'}

        request_funcs = {
            'GET': requests.get,
            'POST': requests.post
        }

        func = request_funcs.get(request_type.upper())

        if not func:
            raise AirflowException(
                f'Request type of {request_type.upper()} not supported.'
            )

        r = func(url=url, headers=self.header, **kwargs)
        r.raise_for_status()
        return r

