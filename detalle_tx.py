import pandas as pd
import numpy as np
import json

from constants import PATH_SQL_DETALLE_TX, PATH_DEFINITION_DETALLE_TX
from utils import batch_sql, format_money, add_columns, read_csv, update_checklist_control_operativo, validate_params

# --- NP SELECT HELPERS ---

def eval_np_select(df: pd.DataFrame, params: list[str]) -> list[pd.Series]:

    return [pd.eval(param, target=df) for param in params]
    
def execute_np_select(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    
    condlist = params['condlist']
    choicelist = params['choicelist']
    default = params['default']

    condlist_eval = eval_np_select(df, condlist)
    choicelist_eval = eval_np_select(df, choicelist)

    return np.select(condlist_eval, choicelist_eval, default)

# --- PROCESS HELPERS ---

def detalle_tx_helper(df: pd.DataFrame, params: dict) -> pd.DataFrame:

    # --- EXTRACT PARAMETERS ---

    estado = params['estado']
    debito = params['debito']
    credito = params['credito']

    # --- VALIDATE PARAMETERS ---

    validate_params(estado, ['condlist', 'choicelist', 'default'])
    validate_params(debito, ['condlist', 'choicelist', 'default'])
    validate_params(credito, ['condlist', 'choicelist', 'default'])

    # --- EVALUATE CONDITIONS ---

    df['estado'] = execute_np_select(df, estado)
    df['debito'] = execute_np_select(df, debito)
    df['credito'] = execute_np_select(df, credito)

    return df

def posteos_helper(df: pd.DataFrame, params: dict) -> pd.DataFrame:

    df['posteo'] = execute_np_select(df, params)

    return df

# --- TABLE FUNCTIONS ---

def tabla_detalle_tx(client: str, fecha: str, df: pd.DataFrame, detalle_tx_params: dict) -> pd.DataFrame:

    # --- VALIDATE PARAMETERS ---

    validate_params(detalle_tx_params, ['estado', 'debito', 'credito'])

    # --- EXECUTE WORKFLOW ---

    df = detalle_tx_helper(df, detalle_tx_params)

    debito = df['debito'].sum()
    credito = df['credito'].sum()

    with open(f"{PATH_SQL_DETALLE_TX}/{client}_tabla_detalle_tx.sql", "w") as file:
        file.write(f"""
            CREATE SCHEMA IF NOT EXISTS {client};

            CREATE TABLE IF NOT EXISTS {client}.detalle_tx (
                fecha DATE PRIMARY KEY DEFAULT CURRENT_DATE,
                debito NUMERIC(15,2) DEFAULT 0,
                credito NUMERIC(15,2) DEFAULT 0,
                total NUMERIC(15,2) DEFAULT 0,
                saldo_calculado NUMERIC(15,2) DEFAULT 0
            );

            INSERT INTO {client}.detalle_tx (fecha, debito, credito) 
                VALUES ('{fecha}', {format_money(debito)}, {format_money(credito)});
        """)

    return df

def tabla_posteos(client: str, fecha: str, df: pd.DataFrame, posteos_params: dict):

    # --- VALIDATE PARAMETERS ---

    validate_params(posteos_params, ['condlist', 'choicelist', 'default'])

    # --- EXECUTE WORKFLOW ---

    df = posteos_helper(df, posteos_params)

    cantidad = df[df['posteo'] == 'POSTEADO'].shape[0]
    monto = df[df['posteo'] == 'POSTEADO']['f030f5088e744d224fc4f886ba963ceda38ffdec2c85504d741a105871123a54'].sum()

    with open(f"{PATH_SQL_DETALLE_TX}/{client}_tabla_posteos.sql", "w") as file:
        file.write(f"""
            CREATE SCHEMA IF NOT EXISTS {client};

            CREATE TABLE IF NOT EXISTS {client}.posteos (
                fecha DATE PRIMARY KEY DEFAULT CURRENT_DATE,
                cantidad INTEGER DEFAULT 0,
                monto NUMERIC(15,2) DEFAULT 0
            );

            INSERT INTO {client}.posteos (fecha, cantidad, monto) 
                VALUES ('{fecha}', {cantidad}, {format_money(monto)});
        """)

def table_control_operativo_detalle_tx(client: str, df: pd.DataFrame):

    df['debito'] = -1 * df['debito']

    df_debito = df[["6f2f3abd5cd439acc192a74f731e74c18a0a1eb8ee49cd2a6ffe13cda2b66114", "debito"]]
    df_debito = df_debito.groupby("6f2f3abd5cd439acc192a74f731e74c18a0a1eb8ee49cd2a6ffe13cda2b66114").sum().reset_index()
    df_debito["account_id"] = df_debito["6f2f3abd5cd439acc192a74f731e74c18a0a1eb8ee49cd2a6ffe13cda2b66114"]

    df_credito = df[["b44f59c11425e5aedad4d9b44c72d4f4b855173d0463e741d667fb1cbf66f51f", "credito"]]
    df_credito = df_credito.groupby("b44f59c11425e5aedad4d9b44c72d4f4b855173d0463e741d667fb1cbf66f51f").sum().reset_index()
    df_credito["account_id"] = df_credito["b44f59c11425e5aedad4d9b44c72d4f4b855173d0463e741d667fb1cbf66f51f"]

    df_control = pd.merge(df_debito, df_credito, on="account_id", how="outer").infer_objects().fillna(0)
    df_control['total'] = df_control['debito'] + df_control['credito']

    rows = [
        f"('{row['account_id']}', {format_money(row['debito'])}, {format_money(row['credito'])}, {format_money(row['total'])})" 
        for _, row in df_control.iterrows()
    ]

    batches = batch_sql(rows)

    command = """
    CREATE SCHEMA IF NOT EXISTS {client};

    CREATE TABLE IF NOT EXISTS {client}.control_operativo_detalle_tx (
        account_id TEXT PRIMARY KEY,
        debito NUMERIC(15,2) DEFAULT 0,
        credito NUMERIC(15,2) DEFAULT 0,
        total NUMERIC(15,2) DEFAULT 0
    );

    INSERT INTO {client}.control_operativo_detalle_tx (account_id, debito, credito, total)
        VALUES {values};
    """

    for i, batch in enumerate(batches):
        with open(f"{PATH_SQL_DETALLE_TX}/{client}_tabla_control_operativo_detalle_tx_{i}.sql", "w") as file:
            file.write(command.format(client=client, values=",\n".join(batch)))

# --- WORKFLOW FUNCTIONS ---

def workflow_one(client: str, fecha: str, df: pd.DataFrame, params: dict) -> pd.DataFrame:

    # --- VALIDATE PARAMETERS ---

    validate_params(params, ['column_mapping', 'detalle_tx', 'posteos'])

    # --- APPLY COLUMN MAPPING ---

    df = add_columns(df, params['column_mapping'])
    
    # --- EXTRACT PARAMETERS ---

    detalle_tx_params = params['detalle_tx']
    posteos_params = params['posteos']

    # --- EXECUTE WORKFLOW ---

    df = tabla_detalle_tx(client, fecha, df, detalle_tx_params)
    tabla_posteos(client, fecha, df, posteos_params) 

    table_control_operativo_detalle_tx(client, df)

def workflow_two(client: str, fecha: str, df: pd.DataFrame, params: dict) -> pd.DataFrame:
    # --- VALIDATE PARAMETERS ---

    validate_params(params, ['column_mapping', 'detalle_tx'])

    # --- APPLY COLUMN MAPPING ---

    df = add_columns(df, params['column_mapping'])
    
    # --- EXTRACT PARAMETERS ---

    detalle_tx_params = params['detalle_tx']

    # --- EXECUTE WORKFLOW ---

    df = tabla_detalle_tx(client, fecha, df, detalle_tx_params)

    table_control_operativo_detalle_tx(client, df)

def detalle_tx(client, filename, fecha):

    with open(PATH_DEFINITION_DETALLE_TX, 'r') as f:
        workflows = json.load(f)

    workflow_type = workflows[client]['workflow_type']
    params = workflows[client]['params']

    df = read_csv(client, 'detalle_tx', filename)

    if workflow_type == '1':
        workflow_one(client, fecha, df, params)

    elif workflow_type == '2':
        workflow_two(client, fecha, df, params)

    update_checklist_control_operativo(client, fecha, { "detalle_tx": filename })
