import os
import json
import shutil
from flask import Flask, request, jsonify
from fastavro import writer, parse_schema

AUTH_TOKEN = os.getenv("AUTH_TOKEN")
BASE_DIR = os.getenv("BASE_DIR", ".")

if not AUTH_TOKEN:
    raise ValueError("AUTH_TOKEN environment variable must be set")

app = Flask(__name__)

def clean_directory(directory: str):
    """Clears the contents of the specified directory."""
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory)

def convert_json_to_avro(json_path: str, avro_path: str, schema: dict):
    """Converts a list of JSON records to Avro format based on the given schema."""
    with open(json_path, 'r') as json_file:
        data = json.load(json_file)

    # Ensure the Avro file directory exists
    os.makedirs(os.path.dirname(avro_path), exist_ok=True)
    
    with open(avro_path, 'wb') as out_file:
        writer(out_file, schema, data)


@app.route('/', methods=['POST'])
def main():
    """A controller that accepts commands via HTTP and runs the transformation logic."""
    input_data = request.json
    raw_dir = input_data.get('raw_dir')
    stg_dir = input_data.get('stg_dir')
    
    if not raw_dir or not stg_dir:
        return jsonify(message="raw_dir or stg_dir parameter missed"), 400
    
    raw_path = os.path.join(BASE_DIR, raw_dir)
    stg_path = os.path.join(BASE_DIR, stg_dir)
    
    # Reading a schematic from a file
    with open('schema.json', 'r') as f:
        schema = json.load(f)
        
    avro_schema = parse_schema(schema)
    
    # Convert all JSON files in the raw directory to Avro format and save them in the stg directory
    for file_name in os.listdir(raw_path):
        json_file_path = os.path.join(raw_path, file_name)
        avro_file_name = file_name.replace('.json', '.avro')
        avro_file_path = os.path.join(stg_path, avro_file_name)
        
        convert_json_to_avro(json_file_path, avro_file_path, avro_schema)
    
    return jsonify(message="Files converted successfully"), 201

if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)
