from airflow.plugins_manager import AirflowPlugin

from operators.powerbi_refresh_dataset_operator import PowerBILink
from plugins.hooks.powerbi_hook import PowerBIHook

# Defining the plugin class
class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "powerbi_link_plugin"
    operator_extra_links = [
        PowerBILink(),
    ]
    hooks= [
        PowerBIHook(),
    ]