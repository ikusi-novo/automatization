from typing import Generator
import pandas as pd

BATCH_SIZE = 50000

def format_money(value: float) -> str:
    
    return f"{value:.2f}"

def batch_sql(iterable: list[str]) -> Generator[list[str], None, None]:
    
    for i in range(0, len(iterable), BATCH_SIZE):
        yield iterable[i:i+BATCH_SIZE]

def map_columns(column_mapping: list[str], df: pd.DataFrame) -> pd.DataFrame:
    
    for mapping in column_mapping:
        exec(mapping)
    
    return df
    
def validate_params(params: dict, expected_params: list[str]) -> None:
    
    if list(sorted(params.keys())) != sorted(expected_params):
        raise ValueError(f"Missing / Additional parameters: {params.keys().difference(expected_params)}")
