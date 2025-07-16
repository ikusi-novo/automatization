from collections import defaultdict, deque
import pandas as pd
import argparse
import json

from constants import PATH_DEFINITION_DETALLE_TX, PATH_DEFINITION_BASEII, PATH_DEFINTION_RECONCILIACION, PATH_FILE_HEADERS, PATH_CHECKLIST_CONTROL_OPERATIVO, PATH_BASEII_NO_CONCILIADA, PATH_DETALLE_NO_CONCILIADA
from utils import add_columns, read_csv, read_csv_processed, validate_params
from detalle_tx import detalle_tx_helper, posteos_helper

def get_client():

    parser = argparse.ArgumentParser()
    parser.add_argument("client")

    args = parser.parse_args()

    client = args.client

    return client

def get_files(client):

    with open(PATH_CHECKLIST_CONTROL_OPERATIVO) as f:
        
        checklists = json.load(f)
    
    client_checklist = checklists[client]["checklist"]

    detalle_filename = client_checklist["detalle_tx"]
    baseII_filename = client_checklist["baseII"]

    return detalle_filename, baseII_filename

def add_key(df, key_mapping):

    key, key_params = key_mapping
    
    key_builder = ""
    for key_param in key_params:
        key_builder += df[key_param].astype(str)
    
    df[key] = key_builder

    return df

def add_keys(df, key_mappings):

    for key_mapping in key_mappings:
        df = add_key(key_mapping, df)

    return df

# Detalle


def load_detalle(client, filename) -> pd.DataFrame:

    with open(PATH_DEFINITION_DETALLE_TX, 'r') as f:
        workflows = json.load(f)

    params = workflows[client]['params']

    validate_params(params, ["column_mapping", "detalle_tx", "posteos"])

    df = read_csv(client, 'detalle_tx', filename)
    df = add_columns(df, params["column_mapping"])

    df = detalle_tx_helper(df, params["detalle_tx"])
    df = posteos_helper(df, params["posteos"])

    return df

def filter_detalle(df) -> pd.DataFrame:

    return df[df['posteo'].isin(["POSTEADO", "POSTEADO MANUAL"]) & df['dbcfe2aa5556d97663084daa29bf901f0796eda081b91a2e059a15a34b1ff84b'].isin(["0200", "0220"])].copy().reset_index(drop=True)

def add_keys_detalle(df) -> pd.DataFrame:

    with open(PATH_DEFINTION_RECONCILIACION) as f:
        key_mappings = json.load(f)

    client_key_mappings = key_mappings[client]

    llaves_detalle = client_key_mappings["detalle_tx"]["llaves"]

    df = add_keys(df, llaves_detalle)

    return df

if __name__ == "__main__":

    client = get_client()

    detalle_filename, baseII_filename = get_files(client)
    
    df = read_csv_processed(client, "detalle_tx", PATH_DETALLE_NO_CONCILIADA)

    df = load_detalle(client, detalle_filename)
    df = filter_detalle(df)
    df = add_keys_detalle(df)

    print(df)
