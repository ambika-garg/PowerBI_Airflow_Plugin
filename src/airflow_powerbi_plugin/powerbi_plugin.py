"""Airflow Imports"""
from airflow.plugins_manager import AirflowPlugin

from airflow_powerbi_plugin.hooks.powerbi import PowerBIHook
from airflow_powerbi_plugin.operators.powerbi import PowerBILink

# Defining the plugin class
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
