import argparse
import json
import re

from report_balances import report_balances
from liberaciones import liberaciones
from detalle_tx import detalle_tx
from baseII import baseII

from constants import PATH_FILE_MAPPINGS

def get_filename_and_fecha():

    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument("fecha")

    args = parser.parse_args()

    filename = filename.split("/")[-1]
    fecha = args.fecha

    return filename, fecha

def get_client_and_document(filename):

    with open(PATH_FILE_MAPPINGS, "r") as f:
        file_mappings = json.load(f)

    found = False

    for regex_pattern, [client, document] in file_mappings.items():
        if re.match(regex_pattern, filename):
            found = True
            break
    
    if not found:
        raise RuntimeError("No matching workflow found")

    return client, document

if __name__ == "__main__":

    filename, fecha = get_filename_and_fecha()
    
    client, document = get_client_and_document(filename)

    # Next node in n8n workflow reads stdout to determine where the .sql files were stored

    print(client)
    print(document) 
    
    # Choose workflow

    if document == "report_balances":
        report_balances(client, filename, fecha)
    elif document == "detalle_tx":
        detalle_tx(client, filename, fecha)
    elif document == "baseII":
        baseII(client, filename, fecha)
    elif document == "liberaciones":
        liberaciones(client, filename, fecha)
    else:
        raise RuntimeError("Invalid document workflow")
