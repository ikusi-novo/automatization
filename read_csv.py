from typing import Literal
import pandas as pd
import json
import csv
import re

def read_csv_helper(file_path: str, delimiter: str, names: list[str], column_format: list[list[str, Literal['number'] | list[Literal['date'], str]]], column_ops: list[tuple[str, str]]) -> pd.DataFrame:
    
    """
    
    Reads a csv-like file and returns a pandas DataFrame.
    
    Args:
        file_path (str): The path to the csv-like file.
        delimiter (str): The delimiter used in the csv-like file.
        names (list[str]): The names of the columns in the csv-like file. Use None if the file has headers in the first row.
        column_format (dict[str, Literal['number', 'date']]): The format of the columns in the csv-like file. Default is 'text'.
        column_ops (dict[str, str]): The operations to perform on the columns in the csv-like file. 
        
    Returns:
        pd.DataFrame: The pandas DataFrame.
    
    """
    
    df = pd.read_csv(
        file_path, 
        delimiter=delimiter, 
        names=names, 
        dtype=str,
        encoding='latin-1', 
        na_values=['', 'null'],
        quoting=csv.QUOTE_MINIMAL
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

def read_csv(client: str, document: str, file_path: str) -> pd.DataFrame:

    with open('read_csv_definitions.json', 'r') as f:
        definitions = json.load(f)

    definitions = definitions[client][document]

    names = definitions['names']

    names = None if names == [] else names
    
    df = read_csv_helper(
        file_path=file_path, 
        delimiter=definitions['delimiter'], 
        names=names, 
        column_format=definitions['column_format'], 
        column_ops=definitions['column_ops']
    )

    return df