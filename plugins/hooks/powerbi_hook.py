"""Standard Library imports"""
from typing import Dict, Union
import time
import requests # type: ignore

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from azure.identity import ClientSecretCredential


class PowerBIDatasetRefreshStatus:
    """Power BI refresh dataset statuses"""

    # If the completion state is unknown or a refresh is in progress.
    IN_PROGRESS = "Unknown"
    FAILED = "Failed"
    COMPLETED = "Completed"
    DISABLED = "Disabled"

    TERMINAL_STATUSES = {FAILED, COMPLETED}


class PowerBIDatasetRefreshException(AirflowException):
    """An exception that indicates a dataset refresh failed to complete."""

class PowerBIHook(BaseHook):
    """
    A hook to interact with Power BI.

    :param dataset_id: The dataset id.
    :param group_id: The workspace id.
    """

    hook_name: str = "Power BI"


    def __init__(
        self,
        *,
        dataset_id: str,
        group_id: str,
    ):
        self.dataset_id = dataset_id
        self.group_id = group_id
        self.header = None
        self._api_version = "v1.0"
        self._base_url = "https://api.powerbi.com"
        super().__init__()

    def refresh_dataset(self, dataset_id: str, group_id: str) -> None:
        """
        Triggers a refresh for the specified dataset from the given group Id.

        :param dataset_id: The dataset id.
        :param group_id: The workspace id.
        """
        url = f"{self._base_url}/{self._api_version}/myorg"

        # add the group id if it is specified
        url += f"/groups/{group_id}"

        # add the dataset key
        url += f"/datasets/{dataset_id}/refreshes"

        response = self._send_request("POST", url=url)
        print(response.headers)

    def _get_token(self) -> str:
        """
        Retrieve the access token used to authenticate against the API.
        """

        client_id = Variable.get("client_id", None)
        client_secret = Variable.get("client_secret", None)
        tenant = Variable.get("tenant_id", None)

        if not client_id or not client_secret:
            raise ValueError("A Client ID and Secret is required to authenticate with Power BI.")

        if not tenant:
            raise ValueError("A Tenant ID is required when authenticating with Client ID and Secret.")

        credential = ClientSecretCredential(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant
        )

        resource = "https://analysis.windows.net/powerbi/api"

        access_token = credential.get_token(f"{resource}/.default")

        # TODO: Check if token is generated else throw Airflow exception

        return access_token.token

    def get_refresh_history(
        self,
        dataset_id: str,
        group_id: str,
    ) -> dict:
        """
        Returns the refresh history of the specified dataset from the given group Id.

        :param dataset_id: The dataset id.
        :param group_id: The workspace id.

        :return: dict object.
        """
        url = f"{self._base_url}/{self._api_version}/myorg"

        # add the group id
        url += f"/groups/{group_id}"

        # add the dataset id
        url += f"/datasets/{dataset_id}/refreshes"

        r = self._send_request("GET", url=url)
        return r.json()

    def get_latest_refresh_details(self) -> Union[Dict[str, str], None]:
        """
        Get the refresh details of the most recent dataset refresh in the
        refresh history of the data source.

        :return: Dictionary containing refresh status and end time if refresh history exists,
        otherwise None.
        :rtype: dict or None
        """
        history = self.get_refresh_history(dataset_id=self.dataset_id, group_id=self.group_id)

        if history is None or not history.get("value"):
            return None

        latest_refresh = history.get("value")[0]

        return {
            "requestId": latest_refresh.get("requestId"),
            "status": latest_refresh.get("status"),
            "end_time": latest_refresh.get("endTime"),
            "error": latest_refresh.get("serviceExceptionJson")
        }

    def get_refresh_details_by_request_id(self, request_id) -> Union[Dict[str, str], None]:
        """
        Get the refresh details of the given request Id.

        :param request_id: Request Id of the Dataset refresh.
        """
        history = self.get_refresh_history(dataset_id=self.dataset_id, group_id=self.group_id)

        if history is None or not history.get("value"):
            return None

        refresh_histories = history.get("value")

        request_ids = [refresh_history.get("requestId") for refresh_history in refresh_histories]

        if request_id not in request_ids:
            return None

        request_id_index = request_ids.index(request_id)
        refresh_details_by_refresh_id = refresh_histories[request_id_index]

        return {
            "requestId": refresh_details_by_refresh_id.get("requestId"),
            "status": refresh_details_by_refresh_id.get("status"),
            "end_time": refresh_details_by_refresh_id.get("endTime"),
            "error": refresh_details_by_refresh_id.get("serviceExceptionJson")
        }


    def wait_for_dataset_refresh_status(
        self,
        expected_status: str,
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
    ) -> bool:
        """
        Wait until the dataset refresh has reached the expected status.

        :param expected_status: The desired status to check against a dataset refresh's current status.
        :param check_interval: Time in seconds to check on a dataset refresh's status.
        :param timeout: Time in seconds to wait for a dataset to reach a terminal status or the expected status.
        :return: Boolean indicating if the dataset refresh has reached the ``expected_status``.
        """
        dataset_refresh_details = self.get_latest_refresh_details()
        dataset_refresh_status = dataset_refresh_details.get("status")

        start_time = time.monotonic()

        while (
            dataset_refresh_status not in PowerBIDatasetRefreshStatus.TERMINAL_STATUSES
            and dataset_refresh_status not in expected_status
        ):
            # Check if the dataset-refresh duration has exceeded the ``timeout`` configured.
            if start_time + timeout < time.monotonic():
                raise PowerBIDatasetRefreshException(
                    f"Dataset refresh has not reached a terminal status after {timeout} seconds"
                )

            time.sleep(check_interval)

            dataset_refresh_details = self.get_latest_refresh_details()
            dataset_refresh_status = dataset_refresh_details.get("status")

        return dataset_refresh_status in expected_status

    def trigger_dataset_refresh(self, wait_for_termination: bool):
        """
        Triggers the Power BI dataset refresh.

        :param wait_for_termination: Wait until the refresh completes before exiting.
        """
        # Start dataset refresh
        self.log.info("Starting dataset refresh.")
        self.refresh_dataset(dataset_id=self.dataset_id, group_id=self.group_id)

        if wait_for_termination:
            self.log.info("Waiting for dataset refresh to terminate.")
            if self.wait_for_dataset_refresh_status(expected_status=PowerBIDatasetRefreshStatus.COMPLETED):
                self.log.info("Dataset refresh has completed successfully")
            else:
                raise PowerBIDatasetRefreshException("Dataset refresh has failed or has been cancelled.")

    def _send_request(self, request_type: str, url: str, **kwargs) -> requests.Response:
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
        self.header = {"Authorization": f"Bearer {self._get_token()}"}

        request_funcs = {"GET": requests.get, "POST": requests.post}

        func = request_funcs.get(request_type.upper())

        if not func:
            raise AirflowException(f"Request type of {request_type.upper()} not supported.")

        response = func(url=url, headers=self.header, **kwargs)

        response.raise_for_status()
        return response
