from typing import Generator
from typing import Literal
import pandas as pd
import fcntl
import json
import csv
import re

from constants import PATH_FILE_HEADERS, PATH_CHECKLIST_CONTROL_OPERATIVO

BATCH_SIZE = 50000

def format_money(value: float) -> str:
    
    return f"{value:.2f}"

def batch_sql(iterable: list[str]) -> Generator[list[str], None, None]:
    
    for i in range(0, len(iterable), BATCH_SIZE):
        yield iterable[i:i+BATCH_SIZE]

def add_columns(df: pd.DataFrame, column_mapping: list[str]) -> pd.DataFrame:
    
    for mapping in column_mapping:
        exec(mapping)
    
    return df
    
def validate_params(params: dict, expected_params: list[str]) -> None:
    
    if not set(expected_params).issubset(params.keys()):
        missing = set(expected_params) - params.keys()
        raise ValueError(f"Missing required parameters: {missing}")

def read_csv_helper(filename: str, delimiter: str, skiprows: int, names: list[str], column_format: list[list[str, Literal['number'] | list[Literal['date'], str]]], column_ops: list[tuple[str, str]]) -> pd.DataFrame:
    
    """
    
    Reads a csv-like file and returns a pandas DataFrame.
    
    Args:
        filename (str): The path to the csv-like file.
        delimiter (str): The delimiter used in the csv-like file.
        names (list[str]): The names of the columns in the csv-like file. Use None if the file has headers in the first row.
        column_format (dict[str, Literal['number', 'date']]): The format of the columns in the csv-like file. Default is 'text'.
        column_ops (dict[str, str]): The operations to perform on the columns in the csv-like file. 
        
    Returns:
        pd.DataFrame: The pandas DataFrame.
    
    """
    
    df = pd.read_csv(
        filename, 
        delimiter=delimiter,
        skiprows=skiprows,
        names=names, 
        dtype=str,
        encoding='latin-1', 
        na_values=['', 'null'],
        quoting=csv.QUOTE_MINIMAL,
    )

    for column, format in column_format:

        if column not in df.columns:
            raise ValueError(f"Column {column} not found in dataframe")

        if format == 'number':
            df[column] = df[column].astype(float)
        elif format[0] == 'date':
            df[column] = pd.to_datetime(df[column], format=format[1])

    col = r"\'.+\'"
    num_lit = r"-?\d+(\.\d+)?"
    date_lit = r"'[^']*%[YmdHMSf][^']*'"

    regex_ops = {
        'mul': [rf'({col}) mul ({col})', rf'({col}) mul ({num_lit})', rf'({num_lit}) mul ({col})', rf'({num_lit}) mul ({num_lit})'],
        'div': [rf'({col}) div ({col})', rf'({col}) div ({num_lit})', rf'({num_lit}) div ({col})', rf'({num_lit}) div ({num_lit})'],
        'add': [rf'({col}) add ({col})', rf'({col}) add ({num_lit})', rf'({num_lit}) add ({col})', rf'({num_lit}) add ({num_lit})'],
        'sub': [rf'({col}) sub ({col})', rf'({col}) sub ({num_lit})', rf'({num_lit}) sub ({col})', rf'({num_lit}) sub ({num_lit})'],
        'as': [rf'({col}) as ({date_lit})'] 
    }

    for column, expr in column_ops:

        found = False
        
        for op, patterns in regex_ops.items():            

            for i, pattern in enumerate(patterns):
                
                match = re.match(pattern, expr)

                if not match:
                    continue

                if op in ['mul', 'div', 'add', 'sub']:

                    if i == 0:
                        
                        left_col_name = match.group(1)[1:-1]
                        if left_col_name not in df.columns:
                            raise ValueError(f"Column {left_col_name} not found in dataframe")
                        left = df[left_col_name]
                        
                        right_col_name = match.group(2)[1:-1]
                        if right_col_name not in df.columns:
                            raise ValueError(f"Column {right_col_name} not found in dataframe")
                        right = df[right_col_name]

                    elif i == 1:
                        
                        left_col_name = match.group(1)[1:-1]
                        if left_col_name not in df.columns:
                            raise ValueError(f"Column {left_col_name} not found in dataframe")
                        left = df[left_col_name]
                        
                        right = float(match.group(2))

                    elif i == 2:
                        
                        left = float(match.group(1))
                        
                        right_col_name = match.group(2)[1:-1]
                        if right_col_name not in df.columns:
                            raise ValueError(f"Column {right_col_name} not found in dataframe")
                        right = df[right_col_name]

                    elif i == 3:
                        
                        left = float(match.group(1))
                        right = float(match.group(2))

                else:

                    left_col_name = match.group(1)[1:-1]
                    if left_col_name not in df.columns:
                        raise ValueError(f"Column {left_col_name} not found in dataframe")
                    left = df[left_col_name]
                    
                    right = str(match.group(2)[1:-1])

                if op == 'mul':
                    df[column] = left * right
                elif op == 'div':
                    df[column] = left / right
                elif op == 'add':
                    df[column] = left + right
                elif op == 'sub':
                    df[column] = left - right
                elif op == 'as':
                    df[column] = left.dt.strftime(right)

                found = True
                
                break
            
            if found:
                break

        if not found:
            raise ValueError(f"Invalid operation: {expr}")    

    return df

def read_csv(client: str, document_type: str, filename: str) -> pd.DataFrame:

    with open(PATH_FILE_HEADERS, 'r') as f:
        definitions = json.load(f)

    definitions = definitions[client][document_type]

    skiprows = definitions.get("skiprows", 0)
    
    df = read_csv_helper(
        filename=filename, 
        delimiter=definitions['delimiter'], 
        skiprows=skiprows,
        names=definitions['names'], 
        column_format=definitions['column_format'], 
        column_ops=definitions['column_ops']
    )

    return df

def read_csv_processed(client: str, document_type: str, filename: str) -> pd.DataFrame:

    with open(PATH_FILE_HEADERS, 'r') as f:
        definitions = json.load(f)

    definitions = definitions[client][document_type]

    df = pd.read_csv(
        filepath_or_buffer=filename,
        sep=definitions['delimiter'],
        header=None
    )

    return df

def update_checklist_control_operativo(client: str, fecha: str, checklist_updates: dict):
    
    with open(PATH_CHECKLIST_CONTROL_OPERATIVO, "r+") as f:

        fcntl.flock(f, fcntl.LOCK_EX)
        
        try:

            data = json.load(f)

            client_data = data[client]
            default_checklist = client_data["checklist"].copy()

            for key in default_checklist.keys():
                default_checklist[key] = ""

            fecha_curr = client_data["fecha"]
            
            if fecha != fecha_curr:

                client_data["fecha"] = fecha
                client_data["checklist"] = default_checklist

            for key, value in checklist_updates.items():
                
                if key not in default_checklist:
                    raise RuntimeError("Invalid key")
                
                client_data["checklist"][key] = value

            data[client] = client_data

            print(all(client_data["checklist"].values())) # Prints True if ready to start SQL process
            
            f.seek(0)
            f.truncate()
            json.dump(data, f, indent=2)

        finally:

            fcntl.flock(f, fcntl.LOCK_UN)
