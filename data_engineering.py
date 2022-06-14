from prefect import task, Flow, Parameter
from typing import Any, Dict, List
import pandas as pd

@task
def load_data(path: str) -> pd.DataFrame:
    ...


@task
def get_classes(data: pd.DataFrame, target_col: str) -> List[str]:
    """Task for getting the classes from the Iris data set."""
    ...


@task
def encode_categorical_columns(data: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """Task for encoding the categorical columns in the Iris data set."""
    ...

@task
def split_data(data: pd.DataFrame, test_data_ratio: float, classes: list) -> Dict[str, Any]:
    """Task for splitting the classical Iris data set into training and test
    sets, each split into features and labels.
    """
    ...

with Flow("data-engineer") as flow:
    
    # Define parameters
    target_col = 'Species'
    test_data_ratio = Parameter("test_data_ratio", default=0.2)
    
    # Define tasks
    data = load_data(path="data/iris.csv")
    classes = get_classes(data=data, target_col=target_col) 
    categorical_columns = encode_categorical_columns(data=data, target_col=target_col)
    train_test_dict = split_data(data=categorical_columns, test_data_ratio=test_data_ratio, classes=classes)

flow.run(parameters={'test_data_ratio': 0.3})

# flow.visualize()

flow.register(project_name="Iris Project")