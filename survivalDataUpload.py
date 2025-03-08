import pandas as pd
from pymongo import MongoClient
import re

mongo_uri = "yourMongoUrl"
mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client["tcga"]
clinical_collection = mongo_db["clinical_survival_data"]
gene_expression_collection = mongo_db["gene_expression"]
combined_collection = mongo_db["combined_clinical_gene_expression_data"]

def normalize_patient_id(patient_id):
    return patient_id[:-3]

file_path = 'TCGA_clinical_survival_data.tsv'
df = pd.read_csv(file_path, sep='\t')

columns = ['bcr_patient_barcode', 'DSS', 'OS', 'clinical_stage']
df_selected = df[columns]

data = df_selected.to_dict(orient='records')

for record in data:
    patient_barcode = record['bcr_patient_barcode']

    gene_expression_records = gene_expression_collection.find({}, {'_id': 0})
    for gene_expression_record in gene_expression_records:
        if 'patient_id' not in gene_expression_record:
            continue

        patient_id = gene_expression_record['patient_id']
        normalized_patient_id = normalize_patient_id(patient_id)
        if normalized_patient_id == patient_barcode:
            combined_record = {**record, **gene_expression_record}

            combined_collection.insert_one(combined_record)
            break

print("Data insertion process completed.")