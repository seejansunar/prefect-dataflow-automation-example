from prefect import task, Flow, Parameter
from typing import Any, Dict, List
import pandas as pd
from sklearn.model_selection import train_test_split
from prefect.engine.results import LocalResult

@task
def load_data(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


@task(
    target="{date:%a_%b_%d_%Y_%H-%M-%S}/{task_name}_output",
    result=LocalResult(dir="data/processed"))
def get_classes(data: pd.DataFrame, target_col: str) -> List[str]:
    """Task for getting the classes from the Iris data set."""
    return sorted(data[target_col].unique())


@task
def encode_categorical_columns(data: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """Task for encoding the categorical columns in the Iris data set."""
    return pd.get_dummies(data, columns=[target_col], prefix="", prefix_sep="")

@task(target="{date:%a_%b_%d_%Y_%H:%M:%S}/{task_name}_output",
      result = LocalResult(dir='data/processed'))
def split_data(data: pd.DataFrame, test_data_ratio: float, classes: list) -> Dict[str, Any]:
    """Task for splitting the classical Iris data set into training and test
    sets, each split into features and labels.
    """
    print(f"Splitting data into training and test sets with ratio {test_data_ratio}")
    X, y = data.drop(columns=classes), data[classes]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_data_ratio)

    # When returning many variables, it is a good practice to give them names:
    return dict(
        train_x=X_train,
        train_y=y_train,
        test_x=X_test,
        test_y=y_test,
    )

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