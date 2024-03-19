from airflow.plugins_manager import AirflowPlugin

from operators.powerbi_refresh_dataset_operator import GoogleLink

# Defining the plugin class
class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "powerbi_link_plugin"
    operator_extra_links = [
        GoogleLink(),
    ]