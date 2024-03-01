import time
from airflow.models import BaseOperator

from powerbi_hook import PowerBIHook

class PowerBIDatasetRefreshOperator(BaseOperator):
    """
    Refreshes a PowerBI Dataset

    """

    def __init__(
        self,
        client_id: str,
        dataset_id: str,
        task_id: str,
        group_id: str = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.client_id = client_id
        self.task_id = task_id
        self.dataset_id = dataset_id
        self.group_id = group_id
        self.hook = None


    def execute(self, context):
        # Get Hook class
        # if not self.hook:
        #     self.hook = PowerBIHook(client_id = self.client_id)

        # self.hook.dataset_refresh(dataset_id=self.dataset_id, group_id=self.group_id)

        # call Dataset Refresh
        print(self.client_id)
        print(self.group_id)
        print(self.dataset_id)
