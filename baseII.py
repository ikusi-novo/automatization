import pandas as pd
import json

from constants import PATH_SQL_BASEII, PATH_DEFINITION_BASEII
from utils import format_money, add_columns, read_csv, update_checklist_control_operativo, validate_params

def tabla_baseII(client: str, fecha: str, df: pd.DataFrame):

    cantidad = df.shape[0]
    monto = df['f030f5088e744d224fc4f886ba963ceda38ffdec2c85504d741a105871123a54'].sum()

    # --- WRITE SQL OUTPUT ---

    with open(f"{PATH_SQL_BASEII}/{client}_tabla_baseII.sql", "w") as file:
        file.write(f"""
            CREATE SCHEMA IF NOT EXISTS {client};

            CREATE TABLE IF NOT EXISTS {client}.baseII (
                fecha DATE PRIMARY KEY DEFAULT CURRENT_DATE,
                cantidad INTEGER DEFAULT 0,
                monto NUMERIC(15,2) DEFAULT 0
            );

            INSERT INTO {client}.baseII (fecha, cantidad, monto) 
                VALUES ('{fecha}', {cantidad}, {format_money(monto)});
        """)

def workflow_one(client: str, fecha: str, df: pd.DataFrame, params: dict):

    # --- VALIDATE PARAMETERS ---

    validate_params(params, ['column_mapping'])

    # --- APPLY COLUMN MAPPING ---

    df = add_columns(df, params['column_mapping'])

    # --- EXECUTE WORKFLOW ---

    tabla_baseII(client, fecha, df)

def baseII(client, filename, fecha):

    with open(PATH_DEFINITION_BASEII, 'r') as f:
        workflows = json.load(f)

    workflow_type = workflows[client]['workflow_type']
    params = workflows[client]['params']

    df = read_csv(client, 'baseII', filename)

    if workflow_type == '1':

        workflow_one(client, fecha, df, params)

    update_checklist_control_operativo(client, fecha, { "baseII": filename })
