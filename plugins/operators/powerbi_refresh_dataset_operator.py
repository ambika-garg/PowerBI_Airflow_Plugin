"""Standard imports"""
from functools import cached_property
from typing import Sequence

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.models import BaseOperatorLink  # type: ignore
from airflow.utils.context import Context
from hooks.powerbi_hook import (
    PowerBIDatasetRefreshException,
    PowerBIDatasetRefreshStatus,
    PowerBIHook
)


class PowerBILink(BaseOperatorLink):
    """
    Construct a link to monitor a dataset in Power BI.
    """
    name = "Monitor PowerBI Dataset"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
        """
        Returns the link to redirect to the PowerBI.
        """
        url = (
            f"https://app.powerbi.com"
            f"/groups/{operator.group_id}/datasets/{operator.dataset_id}"
            f"/details?experience=power-bi"
        )

        return url


class PowerBIDatasetRefreshOperator(BaseOperator):
    """
    Refreshes a Power BI dataset.

    By default the operator will wait until the refresh has completed before
    exiting. The refresh status is checked every 60 seconds as a default. This
    can be changed by specifying a new value for `check_interval`.

    :param dataset_id: The dataset id.
    :param group_id: The workspace id.
    :param wait_for_termination: Wait until the pre-existing or current triggered refresh completes before exiting.
    :param force_refresh: Force refresh if pre-existing refresh found.
    :param timeout: Time in seconds to wait for a dataset to reach a terminal status for non-asynchronous waits. Used only if ``wait_for_termination`` is True.
    :param check_interval: Number of seconds to wait before rechecking the
        refresh status.
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "group_id",
    )
    template_fields_renderers = {"parameters": "json"}

    operator_extra_links = (PowerBILink(),)

    def __init__(
        self,
        # Indicates all the following parameters must be specified using keyword arguments.
        *,
        dataset_id: str,
        group_id: str,
        wait_for_termination: bool = True,
        force_refresh: bool = True,
        timeout: int = 60 * 60 * 24 * 7,
        check_interval: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.group_id = group_id
        self.wait_for_termination = wait_for_termination
        self.force_refresh = force_refresh
        self.timeout = timeout
        self.check_interval = check_interval

    @cached_property
    def hook(self) -> PowerBIHook:
        """
        Create and return an PowerBIHook (cached).
        """
        return PowerBIHook(
            dataset_id=self.dataset_id, group_id=self.group_id
        )

    def execute(self, context: Context):
        """
        Refresh the Power BI Dataset
        """
        self.log.info("Check if a refresh is already in progress.")
        refresh_details = self.hook.get_latest_refresh_details()

        if (
            refresh_details is None
            or refresh_details.get("status") != PowerBIDatasetRefreshStatus.IN_PROGRESS
        ):
            self.log.info("No pre-existing refresh Found.")
            self.hook.trigger_dataset_refresh(self.wait_for_termination)
        else:
            if refresh_details.get("status") == PowerBIDatasetRefreshStatus.IN_PROGRESS:
                self.log.info("Found pre-existing refresh.")

                if self.force_refresh or self.wait_for_termination:
                    self.log.info("Waiting for dataset refresh to terminate.")
                    if self.hook.wait_for_dataset_refresh_status(
                        expected_status=PowerBIDatasetRefreshStatus.COMPLETED
                    ):
                        self.log.info(
                            "Pre-existing Dataset refresh has completed successfully")

                        if self.force_refresh:
                            self.log.info("Starting forced refresh.")
                            self.hook.trigger_dataset_refresh(
                                self.wait_for_termination)
                    else:
                        raise PowerBIDatasetRefreshException(
                            "Dataset refresh has failed or has been cancelled."
                        )

        # Retrieve refresh details after triggering refresh
        refresh_details = self.hook.get_latest_refresh_details()
        status = refresh_details.get("status")
        end_time = refresh_details.get("end_time")

        # Xcom Integration
        context["ti"].xcom_push(
            key="powerbi_dataset_refresh_status", value=status)
        context["ti"].xcom_push(
            key="powerbi_dataset_refresh_end_time", value=end_time)
