from collections import defaultdict, deque
import pandas as pd
import argparse
import json

from constants import PATH_DEFINITION_DETALLE_TX, PATH_DEFINITION_BASEII, PATH_DEFINTION_RECONCILIACION, PATH_FILE_HEADERS, PATH_CHECKLIST_CONTROL_OPERATIVO, PATH_BASEII_NO_CONCILIADA, PATH_DETALLE_NO_CONCILIADA
from utils import add_columns, read_csv, validate_params
from detalle_tx import detalle_tx_helper, posteos_helper

def add_key(key_mapping, df):

    key, key_params = key_mapping
    
    key_builder = ""
    for key_param in key_params:
        key_builder += df[key_param].astype(str)
    
    df[key] = key_builder

    return df

def add_keys(key_mappings, df):

    for key_mapping in key_mappings:
        df = add_key(key_mapping, df)

    return df

def split(debit, credit, df):

    debit_col, debit_vals = debit

    debit_df = df[df[debit_col].isin(debit_vals)].copy()

    credit_col, credit_vals = credit

    credit_df = df[df[credit_col].isin(credit_vals)].copy()

    return debit_df, credit_df

def get_keys(key_col, df1, df2):

    df1_key_counts = df1[key_col].value_counts()
    df2_key_counts = df2[key_col].value_counts()

    common_keys = df1_key_counts.index.intersection(df2_key_counts.index)

    keys = [key for key in common_keys if df1_key_counts[key] == df2_key_counts[key]]

    return keys

def match_on_key(key_col, df1, df2):
    
    keys = get_keys(key_col, df1, df2)
    
    df1_valid = df1[df1[key_col].isin(keys)].copy()
    df2_valid = df2[df2[key_col].isin(keys)].copy()

    df2_queues = defaultdict(deque)
    for idx, key in df2_valid[key_col].items():
        df2_queues[key].append(idx)

    df1_match = []
    for idx, key in df1_valid[key_col].items():
        if df2_queues[key]:
            match_idx = df2_queues[key].popleft()
            df1_match.append((idx, match_idx))
        else:
            df1_match.append((idx, None))

    df1_match_series = pd.Series(index=df1.index, dtype=object)
    df2_match_series = pd.Series(index=df2.index, dtype=object)

    for df1_idx, df2_idx in df1_match:
        if df2_idx is not None:
            df1_match_series.at[df1_idx] = df2_idx
            df2_match_series.at[df2_idx] = df1_idx

    return df1_match_series, df2_match_series

def match_on_keys(df1, df2):

    llave1_df1_match, llave1_df2_match = match_on_key("llave1", df1, df2)
    df1["baseII"] = llave1_df1_match
    df2["detalle"] = llave1_df2_match

    unmatched_df1 = df1[df1["baseII"].isna()]
    unmatched_df2 = df2[df2["detalle"].isna()]

    llave2_df1_match, llave2_df2_match = match_on_key("llave2", unmatched_df1, unmatched_df2)
    df1.loc[llave2_df1_match.index, "baseII"] = llave2_df1_match
    df2.loc[llave2_df2_match.index, "detalle"] = llave2_df2_match

    unmatched_df1 = df1[df1["baseII"].isna()]
    unmatched_df2 = df2[df2["detalle"].isna()]

    llave3_df1_match, llave3_df2_match = match_on_key("llave3", unmatched_df1, unmatched_df2)
    df1.loc[llave3_df1_match.index, "baseII"] = llave3_df1_match
    df2.loc[llave3_df2_match.index, "detalle"] = llave3_df2_match

    return df1, df2

def load_detalle_tx(client: str, filename: str, params_reconciliacion: dict[str, list[str, list[str]]]) -> pd.DataFrame:
    
    validate_params(params_reconciliacion, ["llaves", "debit", "credit"])

    with open(PATH_DEFINITION_DETALLE_TX, 'r') as f:
        workflows = json.load(f)

    params = workflows[client]['params']

    validate_params(params, ['column_mapping', 'detalle_tx', 'posteos'])

    df = read_csv(client, 'detalle_tx', filename)

    df = add_columns(df, params['column_mapping'])

    detalle_tx_params = params['detalle_tx']
    posteos_params = params['posteos']

    df = detalle_tx_helper(df, detalle_tx_params)
    df = posteos_helper(df, posteos_params)

    filtered_df = df[df['posteo'].isin(["POSTEADO", "POSTEADO MANUAL"]) & df['dbcfe2aa5556d97663084daa29bf901f0796eda081b91a2e059a15a34b1ff84b'].isin(["0200", "0220"])].copy().reset_index(drop=True)

    key_mappings = params_reconciliacion["llaves"]

    filtered_df = add_keys(key_mappings, filtered_df)
 
    filtered_df["id"] = filtered_df.index
    filtered_df["baseII"] = None

    debit = params_reconciliacion["debit"]
    credit = params_reconciliacion["credit"]

    filtered_debit_df, filtered_credit_df = split(debit, credit, filtered_df)

    return filtered_debit_df, filtered_credit_df

def load_baseII(client: str, filename: str, params_reconciliacion: dict[str, list[str, list[str]]]) -> pd.DataFrame:

    validate_params(params_reconciliacion, ["llaves", "debit", "credit"])
    
    with open(PATH_DEFINITION_BASEII, 'r') as f:
        workflows = json.load(f)

    params = workflows[client]['params']

    validate_params(params, ['column_mapping'])

    df = read_csv(client, 'baseII', filename)

    df = add_columns(df, params['column_mapping'])
    
    key_mappings = params_reconciliacion["llaves"]

    df = add_keys(key_mappings, df)

    df["id"] = df.index
    df["detalle"] = None

    debit = params_reconciliacion["debit"]
    credit = params_reconciliacion["credit"]

    debit_df, credit_df = split(debit, credit, df)

    return debit_df, credit_df

def cleanup_df(df: pd.DataFrame, columns_to_keep: list[str]) -> pd.DataFrame:

    return df[columns_to_keep]

def get_columns_to_keep(names: list[str], column_ops: list[list[str]]):

    column_ops_names = list(set([column_op[0] for column_op in column_ops if column_op[0] not in names]))

    return names + column_ops_names

def cleanup(client: str, detalle_tx_df: pd.DataFrame, baseII_df: pd.DataFrame):
    
    with open(PATH_FILE_HEADERS) as f:
        data = json.load(f)

    client_data = data[client]

    validate_params(client_data, ["detalle_tx", "baseII"])

    detalle_tx = client_data["detalle_tx"]
    baseII = client_data["baseII"]

    validate_params(detalle_tx, ["names", "column_ops"])
    validate_params(baseII, ["names", "column_ops"])

    detalle_tx_columns_to_keep = get_columns_to_keep(detalle_tx["names"], detalle_tx["column_ops"]) + ["id", "baseII", "estado"]
    baseII_columns_to_keep = get_columns_to_keep(baseII["names"], baseII["column_ops"]) + ["id", "detalle", "estado"]

    detalle_tx_df = cleanup_df(detalle_tx_df, detalle_tx_columns_to_keep)
    baseII_df = cleanup_df(baseII_df, baseII_columns_to_keep)

    return detalle_tx_df, baseII_df

if __name__ == "__main__":

    # Args

    parser = argparse.ArgumentParser()
    parser.add_argument("client")

    args = parser.parse_args()

    client = args.client

    with open(PATH_CHECKLIST_CONTROL_OPERATIVO) as f:
        
        checklists = json.load(f)
        checklist_client = checklists[client]["checklist"]

        detalle_tx_filename = checklist_client["detalle_tx"]
        baseII_filename = checklist_client["baseII"]

    # Reconciliacion

    with open(PATH_DEFINTION_RECONCILIACION) as f:
        key_mappings = json.load(f)

    client_key_mappings = key_mappings[client]

    validate_params(client_key_mappings, ["detalle_tx", "baseII"])

    detalle_tx_params_reconciliacion = client_key_mappings["detalle_tx"]
    baseII_params_reconciliacion = client_key_mappings["baseII"]

    # Load
    
    debit_detalle_tx_df, credit_detalle_tx_df = load_detalle_tx("mio", detalle_tx_filename, detalle_tx_params_reconciliacion)
    debit_baseII_df, credit_baseII_df = load_baseII("mio", baseII_filename, baseII_params_reconciliacion)

    # Reconciliacion

    debit_detalle_tx_df, debit_baseII_df = match_on_keys(debit_detalle_tx_df, debit_baseII_df)
    credit_detalle_tx_df, credit_baseII_df = match_on_keys(credit_detalle_tx_df, credit_baseII_df)

    # Concat + Clean

    detalle_tx_df = pd.concat([debit_detalle_tx_df, credit_detalle_tx_df], ignore_index=True)
    baseII_df = pd.concat([debit_baseII_df, credit_baseII_df], ignore_index=True)

    detalle_tx_df.loc[detalle_tx_df["baseII"].notna(), "estado"] = "COMPENSA"
    detalle_tx_df.loc[detalle_tx_df["baseII"].isna(), "estado"] = "NO CONCILIADA"

    baseII_df.loc[baseII_df["detalle"].notna(), "estado"] = "COMPENSA"
    baseII_df.loc[baseII_df["detalle"].isna(), "estado"] = "NO CONCILIADA"
    
    # detalle_tx_df, detalle_tx_columns, baseII_df, baseII_columns = cleanup(client, detalle_tx_df, baseII_df)

    detalle_tx_no_conciliada_df = detalle_tx_df[detalle_tx_df["estado"] == "NO CONCILIADA"].drop(columns=["id", "baseII", "estado"])
    baseII_no_conciliada_df = baseII_df[baseII_df["estado"] == "NO CONCILIADA"].drop(columns=["id", "detalle", "estado"])

    detalle_tx_no_conciliada_df.to_csv("/data/shared/scripts/mem/detalle_no_conciliada.csv", index=False, sep=";")
    baseII_no_conciliada_df.to_csv("/data/shared/scripts/mem/baseII_no_conciliada.csv", index=False, sep="|")

    # Output

    # Guardar Entradas NO CONCILIADAs

    # detalle_tx_no_conciliada_df = detalle_tx_df[detalle_tx_df["estado"] == "NO CONCILIADA"].copy().drop(columns=["id", "baseII", "estado"])
    # baseII_no_conciliada_df = baseII_df[baseII_df["estado"] == "NO CONCILIADA"].copy().drop(columns=["id", "detalle", "estado"])

    # detalle_tx_no_conciliada_df.to_csv(PATH_DETALLE_NO_CONCILIADA, index=False, columns=detalle_tx_columns, sep=";")
    # baseII_no_conciliada_df.to_csv(PATH_BASEII_NO_CONCILIADA, index=False, columns=baseII_columns, sep="|")
