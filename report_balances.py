import pandas as pd
import json

from constants import PATH_SQL_REPORT_BALANCES, PATH_DEFINITION_REPORT_BALANCES
from utils import batch_sql, format_money, add_columns, read_csv, update_checklist_control_operativo, validate_params

def tabla_report_balances(client: str, fecha: str, df: pd.DataFrame):

    df['sobregiro'] = df['6f2f3abd5cd439acc192a74f731e74c18a0a1eb8ee49cd2a6ffe13cda2b66114'] < 0

    numero_cuentas = df.shape[0]
    ledger_balance = df['6f2f3abd5cd439acc192a74f731e74c18a0a1eb8ee49cd2a6ffe13cda2b66114'].sum()
    available_balance = df['b44f59c11425e5aedad4d9b44c72d4f4b855173d0463e741d667fb1cbf66f51f'].sum()

    numero_cuentas_sobregiro = df[df['sobregiro']].shape[0]
    monto_sobregiro = df[df['sobregiro']]['6f2f3abd5cd439acc192a74f731e74c18a0a1eb8ee49cd2a6ffe13cda2b66114'].sum()

    # --- WRITE SQL OUTPUT ---

    with open(f"{PATH_SQL_REPORT_BALANCES}/{client}_tabla_report_balances.sql", "w") as file:
        file.write(f"""
            CREATE SCHEMA IF NOT EXISTS {client};

            CREATE TABLE IF NOT EXISTS {client}.report_balances (
                fecha DATE PRIMARY KEY DEFAULT CURRENT_DATE,
                numero_cuentas INTEGER DEFAULT 0,
                ledger_balance NUMERIC(15,2) DEFAULT 0,
                available_balance NUMERIC(15,2) DEFAULT 0,
                numero_cuentas_sobregiro INTEGER DEFAULT 0,
                monto_sobregiro NUMERIC(15,2) DEFAULT 0,
                hold NUMERIC(15,2) DEFAULT 0,
                dif_ledger_balance NUMERIC(15,2) DEFAULT 0,
                dif_available_balance NUMERIC(15,2) DEFAULT 0,
                dif_hold NUMERIC(15,2) DEFAULT 0
            );

            INSERT INTO {client}.report_balances (fecha, numero_cuentas, ledger_balance, available_balance, numero_cuentas_sobregiro, monto_sobregiro) 
                VALUES ('{fecha}', {numero_cuentas}, {format_money(ledger_balance)}, {format_money(available_balance)}, {numero_cuentas_sobregiro}, {format_money(monto_sobregiro)});
        """)

def tabla_control_operativo_report_balances(client: str, df: pd.DataFrame):

    rows = [
        f"('{row['f030f5088e744d224fc4f886ba963ceda38ffdec2c85504d741a105871123a54']}', {format_money(row['6f2f3abd5cd439acc192a74f731e74c18a0a1eb8ee49cd2a6ffe13cda2b66114'])}, {format_money(row['6f2f3abd5cd439acc192a74f731e74c18a0a1eb8ee49cd2a6ffe13cda2b66114'])})"
        for _, row in df.iterrows()
    ]

    batches = batch_sql(rows)

    command = """
    CREATE SCHEMA IF NOT EXISTS {client};

    CREATE TABLE IF NOT EXISTS {client}.control_operativo_report_balances (
        account_id TEXT PRIMARY KEY,
        ledger_balance_curr NUMERIC(15,2) DEFAULT 0,
        ledger_balance_prev NUMERIC(15,2) DEFAULT 0,
        dif_ledger_balance NUMERIC(15,2) DEFAULT 0
    );

    INSERT INTO {client}.control_operativo_report_balances (account_id, ledger_balance_curr, dif_ledger_balance)
        VALUES {values}
    ON CONFLICT (account_id) DO UPDATE SET
        ledger_balance_curr = EXCLUDED.ledger_balance_curr,
        ledger_balance_prev = control_operativo_report_balances.ledger_balance_curr,
        dif_ledger_balance = EXCLUDED.ledger_balance_curr - control_operativo_report_balances.ledger_balance_curr;
    """

    for i, batch in enumerate(batches):
        with open(f"{PATH_SQL_REPORT_BALANCES}/{client}_tabla_control_operativo_report_balances_{i}.sql", "w") as file:
            file.write(command.format(client=client, values=",\n".join(batch)))

def workflow_one(client: str, fecha: str, df: pd.DataFrame, params: dict) -> pd.DataFrame:

    # --- VALIDATE PARAMETERS ---

    validate_params(params, ['column_mapping'])

    # --- APPLY COLUMN MAPPING ---

    df = add_columns(df, params['column_mapping'])

    # --- EXECUTE WORKFLOW ---

    tabla_report_balances(client, fecha, df)
    tabla_control_operativo_report_balances(client, df)

def report_balances(client, filename, fecha):

    with open(PATH_DEFINITION_REPORT_BALANCES, 'r') as f:
        workflows = json.load(f)

    workflow_type = workflows[client]['workflow_type']
    params = workflows[client]['params']

    df = read_csv(client, 'report_balances', filename)

    if workflow_type == '1':

        workflow_one(client, fecha, df, params)

    update_checklist_control_operativo(client, fecha, { "report_balances": filename })
