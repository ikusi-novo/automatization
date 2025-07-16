import pandas as pd
import json

from constants import PATH_SQL_LIBERACIONES, PATH_DEFINITION_LIBERACIONES
from utils import format_money, add_columns, read_csv, update_checklist_control_operativo, validate_params

def tabla_liberaciones(client: str, fecha: str, df: pd.DataFrame):

    cantidad = df.shape[0]
    monto = df['f030f5088e744d224fc4f886ba963ceda38ffdec2c85504d741a105871123a54'].sum()

    # --- WRITE SQL OUTPUT ---

    with open(f"{PATH_SQL_LIBERACIONES}/{client}_tabla_liberaciones.sql", "w") as file:
        file.write(f"""
            CREATE SCHEMA IF NOT EXISTS {client};

            CREATE TABLE IF NOT EXISTS {client}.liberaciones (
                fecha DATE PRIMARY KEY DEFAULT CURRENT_DATE,
                cantidad NUMERIC(15,2) DEFAULT 0,
                monto INTEGER DEFAULT 0
            );

            INSERT INTO {client}.liberaciones (fecha, cantidad, monto) 
                VALUES ('{fecha}', {cantidad}, {format_money(monto)});
        """)

def workflow_one(client: str, fecha: str, df: pd.DataFrame, params: dict) -> pd.DataFrame:

    # --- VALIDATE PARAMETERS ---

    validate_params(params, ['column_mapping'])

    # --- APPLY COLUMN MAPPING ---

    df = add_columns(df, params['column_mapping'])

    # --- EXECUTE WORKFLOW ---

    tabla_liberaciones(client, fecha, df)

def liberaciones(client, filename, fecha):

    with open(PATH_DEFINITION_LIBERACIONES, 'r') as f:
        workflows = json.load(f)

    workflow_type = workflows[client]['workflow_type']
    params = workflows[client]['params']

    df = read_csv(client, 'liberaciones', filename)

    if workflow_type == '1':
        workflow_one(client, fecha, df, params)

    update_checklist_control_operativo(client, fecha, { "liberaciones": filename })
