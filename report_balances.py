from utils import format_money, batch_sql, map_columns, validate_params
from read_csv import read_csv

import pandas as pd
import json

PATH = './report_balances'

def tabla_report_balances(client: str, fecha: str, df: pd.DataFrame):

    df['sobregiro'] = df['6f2f3abd5cd439acc192a74f731e74c18a0a1eb8ee49cd2a6ffe13cda2b66114'] < 0

    numero_cuentas = df.shape[0]
    ledger_balance = df['6f2f3abd5cd439acc192a74f731e74c18a0a1eb8ee49cd2a6ffe13cda2b66114'].sum()
    available_balance = df['b44f59c11425e5aedad4d9b44c72d4f4b855173d0463e741d667fb1cbf66f51f'].sum()

    numero_cuentas_sobregiro = df[df['sobregiro']].shape[0]
    monto_sobregiro = df[df['sobregiro']]['6f2f3abd5cd439acc192a74f731e74c18a0a1eb8ee49cd2a6ffe13cda2b66114'].sum()

    # --- WRITE SQL OUTPUT ---

    with open(f"{PATH}/{client}_tabla_report_balances.sql", "w") as file:
        file.write(f"""
            INSERT INTO report_balances (fecha, numero_cuentas, ledger_balance, available_balance, numero_cuentas_sobregiro, monto_sobregiro) 
                VALUES ('{fecha}', {numero_cuentas}, {format_money(ledger_balance)}, {format_money(available_balance)}, {numero_cuentas_sobregiro}, {format_money(monto_sobregiro)});
        """)

def tabla_control_operativo_report_balances(client: str, df: pd.DataFrame):

    rows = [
        f"('{row['f030f5088e744d224fc4f886ba963ceda38ffdec2c85504d741a105871123a54']}', {format_money(row['6f2f3abd5cd439acc192a74f731e74c18a0a1eb8ee49cd2a6ffe13cda2b66114'])}, {format_money(row['b44f59c11425e5aedad4d9b44c72d4f4b855173d0463e741d667fb1cbf66f51f'])})"
        for _, row in df.iterrows()
    ]

    batches = batch_sql(rows)

    command = """
    INSERT INTO {client}.control_operativo_report_balances (account_id, ledger_balance, available_balance)
        VALUES {values}
    ON CONFLICT (account_id) DO UPDATE SET
        ledger_balance_curr = EXCLUDED.ledger_balance_curr,
        ledger_balance_prev = control_operativo_report_balances.ledger_balance_curr,
        dif_ledger_balance = EXCLUDED.ledger_balance_curr - control_operativo_report_balances.ledger_balance_curr;
    """

    for i, batch in enumerate(batches):
        with open(f"{PATH}/{client}_tabla_control_operativo_report_balances_{i}.sql", "w") as file:
            file.write(command.format(client=client, values=",\n".join(batch)))

def workflow_one(client: str, fecha: str, df: pd.DataFrame, params: dict) -> pd.DataFrame:

    # --- VALIDATE PARAMETERS ---

    validate_params(params, ['column_mapping'])

    # --- APPLY COLUMN MAPPING ---

    df = map_columns(params['column_mapping'], df)

    # --- EXECUTE WORKFLOW ---

    tabla_report_balances(client, fecha, df)
    tabla_control_operativo_report_balances(client, df)

if __name__ == '__main__':

    client = 'banco_coopcentral'
    fecha = '2025-06-17'

    with open('report_balances_definitions.json', 'r') as f:
        workflows = json.load(f)

    workflow_type = workflows[client]['workflow_type']
    params = workflows[client]['params']

    df = read_csv(client, 'report_balances', './documents/ReportBalances_Coopcentral_20250617.txt')

    if workflow_type == '1':

        workflow_one(client, fecha, df, params)