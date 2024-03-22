"""Standard imports"""
import time
import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.models import BaseOperatorLink, XCom

from hooks.powerbi_hook import PowerBIHook

logger = logging.getLogger(__name__)


class PowerBILink(BaseOperatorLink):
    """
    Construct a link to monitor a dataset in Power BI.
    """

    name = "Power BI"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
        group_id = XCom.get_value(key="group_id", ti_key=ti_key) or ""
        dataset_id = XCom.get_value(key="dataset_id", ti_key=ti_key)

        # group_id = operator.group_id
        # dataset_id = operator.dataset_id

        return f"https://app.powerbi.com/groups/{group_id}/datasets/{dataset_id}/details?experience=power-bi"


class PowerBIDatasetRefreshOperator(BaseOperator):
    """
    Refreshes a Power BI dataset.

    If no `group id` is specified then the dataset from "My Workspace" will
    be refreshed.

    By default the operator will wait until the refresh has completed before
    exiting. The refresh status is checked every 60 seconds as a default. This
    can be changed by specifying a new value for `recheck_delay`.

    :param client_id: Power BI App ID used to identify the application
        registered to have access to the REST API.
    :param dataset_id: The dataset id.
    :param group_id: The workspace id.
    :param wait_for_completion: Wait until the refresh completes before exiting.
    :param recheck_delay: Number of seconds to wait before rechecking the
        refresh status.
    :param force_refresh: Force refresh if pre-existing refresh found.
    :param powerbi_conn_id: Airflow Connection ID that contains the connection
        information for the Power BI account used for authentication.
    """
    template_fields = []
    template_ext = []
    operator_extra_links = (PowerBILink(),)

    @apply_defaults
    def __init__(self,
                 dataset_id: str,
                 group_id: str = None,
                 wait_for_completion: bool = True,
                 recheck_delay: int = 60,
                 force_refresh: bool = True,
                 powerbi_conn_id: str = "powerbi_default",
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_id = dataset_id
        self.group_id = group_id
        self.wait_for_completion = wait_for_completion
        self.recheck_delay = recheck_delay
        self.powerbi_conn_id = powerbi_conn_id
        self.force_refresh = force_refresh
        self.hook = None

    # def hook(self) -> PowerBIHook:
    #     """Create and return an PowerBIHook"""
    #     return PowerBIHook(dataset_id=self.dataset_id, group_id=self.group_id)

    def get_refresh_details(self) -> dict:
        """
        Get the refresh details of the most recent dataset refresh in the
        refresh history of the data source.

        :return: dict object of refresh status and end time.
        """
        history = self.hook.get_refresh_history(dataset_id=self.dataset_id,
                                                group_id=self.group_id,
                                                top=1)

        value = history.get("value")

        if not value:
            return "NoHistory"
        else:
            return {
                "status": value[0].get("status"),
                "end_time": value[0].get("end_time")
            }

    def wait_on_completion(self) -> None:
        """
        Wait until the dataset refresh has completed.
        """
        self.log.info("Waiting for Completion")
        while True:
            time.sleep(self.recheck_delay)
            refresh_details = self.get_refresh_details()
            status = refresh_details.get("status")
            self.log.info(f"Checking refresh status. Status is `{status}`")
            if status == "Completed":
                break

    def trigger_refresh_dataset(self):
        """
        Triggers the Power BI dataset refresh.
        """
        # Start dataset refresh
        self.log.info("Starting refresh.")
        self.hook.refresh_dataset(dataset_id=self.dataset_id,
                                  group_id=self.group_id)

        if self.wait_for_completion:
            self.wait_on_completion()

    def execute(self, context):
        """
        Refresh the Power BI Dataset
        """
        if not self.hook:
            self.hook = PowerBIHook(
                dataset_id=self.dataset_id,
                group_id=self.group_id,
                powerbi_conn_id=self.powerbi_conn_id
            )

        # Check to see if a refresh is already in progress
        # Power BI only allows one refresh to take place and will raise
        # an exception if the API is called when a refresh is already in
        # progress. We wait until any existing refreshes are completed
        # before starting a new one.
        self.log.info("Check if a refresh is already in progress.")
        refresh_details = self.get_refresh_details()
        status = refresh_details.get("status")

        if status == "Unknown":
            self.log.info(
                "Found pre-existing refresh."
            )

            if self.wait_for_completion:
                self.wait_on_completion()

            self.log.info("Pre-existing refresh completed.")

            if self.force_refresh:
                self.trigger_refresh_dataset()
        else:
            self.log.info("No Found pre-existing refresh.")

            self.trigger_refresh_dataset()

        refresh_details = self.get_refresh_details()
        status = refresh_details.get("status")
        end_time = refresh_details.get("end_time")

        # Xcom Integration
        context["ti"].xcom_push(
            key="powerbi_dataset_refresh_status", value=status)
        context["ti"].xcom_push(
            key="powerbi_dataset_refresh_end_time", value=end_time)
        context["ti"].xcom_push(key="dataset_id", value=self.dataset_id)
        context["ti"].xcom_push(key="group_id", value=self.group_id)
