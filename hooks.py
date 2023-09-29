# hooks.py

from kedro.pipeline import Pipeline
from altara_credit_risk_model.pipelines.data_engineering import create_pipeline


def register_pipelines(pipelines_dict):
    pipelines_dict["data_engineering"] = create_pipeline()
    return pipelines_dict

