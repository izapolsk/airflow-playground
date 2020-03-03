import logging

from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)


class KPIProcessingOperator(BaseOperator):
    @apply_defaults
    def __init__(self, country, region, *args, **kwargs):
        super(KPIProcessingOperator, self).__init__(*args, **kwargs)
        self.country = country
        self.region = region

    def execute(self, context):
        logger.info(f"Starting Task for {self.country}, {self.region}")


class KPIProcessingPlugin(AirflowPlugin):
    name = "kpi_processing_plugin"
    operators = [KPIProcessingOperator]
