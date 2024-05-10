import os
import requests
import shutil
from flask import Flask, request, jsonify


AUTH_TOKEN = os.getenv("AUTH_TOKEN")
BASE_DIR = os.getenv("BASE_DIR", ".")
API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'

if not AUTH_TOKEN:
    raise ValueError("AUTH_TOKEN environment variable must be set")

app = Flask(__name__)

def clean_directory(directory: str):
    """Clears the contents of the specified directory."""
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory)

def fetch_and_save_data(date: str, raw_dir: str, page: int = 1):
    """Requests data from the API and saves it to disk."""
    full_path = os.path.join(BASE_DIR, raw_dir)
    
    # Check if the directory exists, otherwise create
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    
    clean_directory(full_path)
    
    response = requests.get(
        url=API_URL,
        params={'date': date, 'page': page},
        headers={'Authorization': AUTH_TOKEN}
    )
    
    if response.status_code == 200:
        # Saving data to a file
        file_name = f"sales_{date}_page_{page}.json"
        file_path = os.path.join(full_path, file_name)
        with open(file_path, 'w') as file:
            file.write(response.text)
        print(f"Data saved to {file_path}")
    else:
        print(f"Failed to fetch data: {response.status_code}")
        if response.status_code == 400:
            print("Response JSON:", response.json())

@app.route('/', methods=['POST'])
def main():
    """A controller that accepts commands via HTTP and runs business logic."""
    input_data = request.json
    date = input_data.get('date')
    raw_dir = input_data.get('raw_dir')
    
    if not date or not raw_dir:
        return jsonify(message="date or raw_dir parameter missed"), 400
    
    fetch_and_save_data(date=date, raw_dir=raw_dir)
    return jsonify(message="Data retrieved successfully from API"), 201

if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8081)
