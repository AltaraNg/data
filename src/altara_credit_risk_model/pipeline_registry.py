"""Project pipelines."""
from __future__ import annotations

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from typing import Dict


from altara_credit_risk_model.pipelines import data_engineering as de
from altara_credit_risk_model.pipelines import data_science as ds


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.

    """
    data_engineering = de.create_pipeline()
    data_science = ds.create_pipeline()
   

    return {"__default__": data_engineering+data_science,
            "data_engineering": data_engineering,
            "data_science": data_science,
            }