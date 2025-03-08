from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient

app = Flask(__name__)
CORS(app)

mongo_uri = "yourMongoUrl"
mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client["tcga"]
mongo_collection = mongo_db["gene_expression"]

@app.route('/gene_expression', methods=['GET'])
def get_gene_expression():
    patient_id = request.args.get('patient_id')
    if not patient_id:
        return jsonify({"error": "patient_id is required"}), 400

    query = {"patient_id": patient_id}
    projection = {"_id": 0, "patient_id": 1, "cancer_cohort": 1, "C6orf150": 1, "CCL5": 1, "CXCL10": 1,
                  "TMEM173": 1, "CXCL9": 1, "CXCL11": 1, "NFKB1": 1, "IKBKE": 1, "IRF3": 1, "TREX1": 1, "ATM": 1, "IL6": 1, "IL8": 1}
    result = mongo_collection.find_one(query, projection)

    if not result:
        return jsonify({"error": "No data found for the given patient_id"}), 404

    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)