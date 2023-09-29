"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.18.12
"""

from kedro.pipeline import Pipeline, node, pipeline
from altara_credit_risk_model.pipelines.data_engineering.nodes import fetch_raw_data, preprocess

def create_pipeline(**kwargs) -> Pipeline:
    pipeline_instance =  pipeline(
        [node(
            func=fetch_raw_data,
            inputs="my_sql_data_source",
            outputs=None,
            name="data_node"
        ),
        node(
            func=preprocess,
            inputs="fetched_data",
            outputs="cleaned_data",
            name="processed_data_node"
         )
         ]
        )
     
    data_engineering = pipeline(
        pipe=pipeline_instance,
        inputs= ["my_sql_data_source","fetched_data"],
        namespace = "data_engineering",
        outputs = ["cleaned_data"]
        )
    return data_engineering