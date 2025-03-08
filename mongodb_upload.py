import pandas as pd
from io import BytesIO
from minio import Minio
from pymongo import MongoClient

class MinioToMongoUploader:
    def __init__(self, minio_endpoint, access_key, secret_key, mongo_uri, mongo_db, mongo_collection):
        self.minio_client = Minio(
            minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        self.mongo_client = MongoClient(mongo_uri)
        self.mongo_db = self.mongo_client[mongo_db]
        self.mongo_collection = self.mongo_db[mongo_collection]

    def list_objects_in_bucket(self, bucket_name):
        objects = self.minio_client.list_objects(bucket_name)
        return [obj.object_name for obj in objects]

    def download_tsv_from_minio(self, bucket_name, object_name):
        response = self.minio_client.get_object(bucket_name, object_name)
        return BytesIO(response.read())

    def transform_data(self, data, cancer_cohort):
        df = pd.read_csv(data, sep='\t')
        df_transposed = df.transpose()
        df_transposed.columns = df_transposed.iloc[0]
        df_transposed = df_transposed.iloc[1:]
        df_transposed['patient_id'] = df_transposed.index
        df_transposed['cancer_cohort'] = cancer_cohort

        genes_of_interest = [
            'C6orf150', 'CCL5', 'CXCL10', 'TMEM173',
            'CXCL9', 'CXCL11', 'NFKB1', 'IKBKE', 'IRF3',
            'TREX1', 'ATM', 'IL6', 'IL8'
        ]
        genes_found = [gene for gene in genes_of_interest if gene in df_transposed.columns]
        if not genes_found:
            return []

        return df_transposed[['patient_id', 'cancer_cohort'] + genes_found].to_dict(orient='records')

    def upload_to_mongo(self, records):
        if records:
            self.mongo_collection.insert_many(records)

    def process_and_upload_all(self, bucket_name):
        object_names = self.list_objects_in_bucket(bucket_name)
        for object_name in object_names:
            tsv_data = self.download_tsv_from_minio(bucket_name, object_name)
            cancer_cohort = object_name.split('_')[0]
            records = self.transform_data(tsv_data, cancer_cohort)
            self.upload_to_mongo(records)
            print(f"Successfully uploaded {object_name} to MongoDB")

# Example usage
if __name__ == "__main__":
    minio_endpoint = "localhost:9000"
    access_key = "yourMinioAccessKey"
    secret_key = "yourMinioPassword"
    mongo_uri = "yourMongoUrl"
    mongo_db = "tcga"
    mongo_collection = "gene_expression"

    uploader = MinioToMongoUploader(minio_endpoint, access_key, secret_key, mongo_uri, mongo_db, mongo_collection)
    uploader.process_and_upload_all("tcga-data")