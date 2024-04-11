"""Airflow Imports"""
from airflow.plugins_manager import AirflowPlugin

from operators.powerbi_refresh_dataset_operator import PowerBILink
# from hooks.powerbi_hook import PowerBIHook

# Defining the plugin class
class AirflowExtraLinkPlugin(AirflowPlugin):
    """
    PowerBI plugin.
    """

    name = "powerbi_link_plugin"
    operator_extra_links = [
        PowerBILink(),
    ]
    # hooks= [
    #     PowerBIHook,
    # ]
