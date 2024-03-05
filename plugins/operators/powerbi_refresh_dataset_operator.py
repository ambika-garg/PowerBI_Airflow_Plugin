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
        if not self.hook:
            self.hook = PowerBIHook(dataset_id = self.dataset_id, group_id = self.group_id)

        self.hook.dataset_refresh(dataset_id=self.dataset_id, group_id=self.group_id)

        return
