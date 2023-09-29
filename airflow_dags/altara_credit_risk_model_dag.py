from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from kedro.framework.session import KedroSession
from kedro.framework.project import configure_project


class KedroOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        package_name: str,
        pipeline_name: str,
        node_name: str,
        project_path: str | Path,
        env: str,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.package_name = package_name
        self.pipeline_name = pipeline_name
        self.node_name = node_name
        self.project_path = project_path
        self.env = env

    def execute(self, context):
        configure_project(self.package_name)
        with KedroSession.create(self.package_name,
                                 self.project_path,
                                 env=self.env) as session:
            session.run(self.pipeline_name, node_names=[self.node_name])


# Kedro settings required to run your pipeline
env = "local"
pipeline_name = "__default__"
project_path = Path.cwd()
package_name = "altara_credit_risk_model"

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(
    dag_id="altara-credit-risk-model",
    start_date=datetime(2023,1,1),
    max_active_runs=3,
    # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    schedule_interval=timedelta(hours=1),
    catchup=False,
    # Default settings applied to all tasks
    default_args=dict(
        owner="airflow",
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5)
    )
) as dag:
    tasks = {
        "data-engineering-data-node": KedroOperator(
            task_id="data-engineering-data-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="data_engineering.data_node",
            project_path=project_path,
            env=env,
        ),
        "data-engineering-processed-data-node": KedroOperator(
            task_id="data-engineering-processed-data-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="data_engineering.processed_data_node",
            project_path=project_path,
            env=env,
        ),
    }

