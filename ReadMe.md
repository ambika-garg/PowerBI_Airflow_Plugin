# Apache Airflow Plugin for Power BI Dataset Refresh. 🚀

## Introduction

Get ready to enhance your Apache Airflow workflows with a new plugin designed for refreshing Power BI datasets! The plugin contains the custom operator to seamlessly handle dataset refresh and it supports SPN authentication. Additionally, the operator checks for existing refreshes before triggering the new one.

## How to Use

### Getting Started

### Install the Plugin

Pypi package: https://pypi.org/project/airflow-powerbi-plugin/

```bash
pip install airflow-powerbi-plugin
```

### Authentication

Before diving in,

- The plugin supports the <strong>SPN (Service Principal) authentication</strong> with the Power BI. You need to add your service prinicpal as the <strong>Contributor</strong> in your Power BI workspace.

Since custom connection forms aren't feasible in Apache Airflow plugins, use can use `Generic` connection type. Here's what you need to store:

1. `Connection Id`: Name of the connection Id
2. `Connection Type`: Generic
3. `Login`: The Client ID of your service principal.
4. `Password`: The Client Secret of your service principal.
5. `Extra`: {
   "tenantId": The Tenant Id of your service principal.
   }

## Operators

### PowerBIDatasetRefreshOperator

This operator composes the logic for this plugin. It triggers the Power BI dataset refresh and pushes the details in Xcom. It can accept the following parameters:

- `dataset_id`: The dataset Id.
- `group_id`: The workspace Id.
- `powerbi_conn_id`: The connection Id to connect to PowerBI dataset.
- `wait_for_termination`: (Default value: True) Wait until the pre-existing or current triggered refresh completes before exiting.
- `force_refresh`: When enabled, it will force refresh the dataset again, after pre-existing ongoing refresh request is terminated.
- `timeout`: Time in seconds to wait for a dataset to reach a terminal status for non-asynchronous waits. Used only if `wait_for_termination` is True.
- `check_interval`: Number of seconds to wait before rechecking the refresh status.

## Features

- #### Xcom Integration: The Power BI Dataset refresh operator enriches the Xcom with essential fields for downstream tasks:

1. `refresh_id`: Request Id of the semantic model refresh.
2. `refresh_status`: Refresh Status.
   - `Unknown`: Refresh state is unknown or a refresh is in progress.
   - `Completed`: Refresh successfully completed.
   - `Failed`: Refresh failed (details in `refresh_error`).
   - `Disabled`: Refresh is disabled by a selective refresh.
3. `refresh_end_time`: The end date and time of the refresh (may be None if a refresh is in progress)
4. `refresh_error`: Failure error code in JSON format (None if no error)

- #### External Monitoring link: The operator conveniently provides a redirect link to the Power BI UI for monitoring refreshes.

Configure the package as Airflow plugin in the plugins folder. Copy the following code

```python
"""Airflow Imports"""
from airflow.plugins_manager import AirflowPlugin

from airflow_powerbi_plugin.hooks.powerbi import PowerBIHook
from airflow_powerbi_plugin.operators.powerbi import PowerBILink

class AirflowExtraLinkPlugin(AirflowPlugin):
    """
    PowerBI plugin.
    """

    name = "powerbi_link_plugin"
    operator_extra_links = [
        PowerBILink(),
    ]
    hooks= [
        PowerBIHook,
    ]
```

## Sample DAG to use the plugin.

Ready to give it a spin? Check out the sample DAG code below:

```python
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow_powerbi_plugin.operators.powerbi import PowerBIDatasetRefreshOperator

with DAG(
        dag_id='refresh_dataset_powerbi',
        schedule_interval=None,
        start_date=datetime(2023, 8, 7),
        catchup=False,
        concurrency=20,
        tags=['powerbi', 'dataset', 'refresh']
) as dag:

    refresh_in_given_workspace = PowerBIDatasetRefreshOperator(
        task_id="refresh_in_given_workspace",
        powerbi_conn_id="powerbi_default",
        dataset_id="<dataset_id>",
        group_id="<workspace_id>",
        force_refresh = False,
        wait_for_termination = False
    )

    refresh_in_given_workspace

```

Feel free to tweak and tailor this DAG to suit your needs!

## Contributing

We welcome any contributions:

- Report all enhancements, bugs, and tasks as [GitHub issues](https://github.com/ambika-garg/PowerBI_Airflow_Plugin/issues)
- Provide fixes or enhancements by opening pull requests in GitHub.
