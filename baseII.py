import pandas as pd
import json

from utils import validate_params, map_columns, format_money
from read_csv import read_csv

PATH = './baseII'

def tabla_baseII(client: str, fecha: str, df: pd.DataFrame):

    cantidad = df.shape[0]
    monto = df['f030f5088e744d224fc4f886ba963ceda38ffdec2c85504d741a105871123a54'].sum()

    # --- WRITE SQL OUTPUT ---

    with open(f"{PATH}/{client}_tabla_baseII.sql", "w") as file:
        file.write(f"""
            INSERT INTO {client}.baseII (fecha, cantidad, monto) 
                VALUES ('{fecha}', {cantidad}, {format_money(monto)});
        """)

def workflow_one(client: str, fecha: str, df: pd.DataFrame, params: dict):

    # --- VALIDATE PARAMETERS ---

    validate_params(params, ['column_mapping'])

    # --- APPLY COLUMN MAPPING ---

    df = map_columns(params['column_mapping'], df)

    # --- EXECUTE WORKFLOW ---

    tabla_baseII(client, fecha, df)

if __name__ == '__main__':

    client = 'zinli'
    fecha = '2025-06-18'

    with open('baseII_definitions.json', 'r') as f:
        workflows = json.load(f)

    workflow_type = workflows[client]['workflow_type']
    params = workflows[client]['params']

    df = read_csv(client, 'baseII', './documents/NP-MFTECH-485046BASE168.txt')

    if workflow_type == '1':

        workflow_one(client, fecha, df, params)