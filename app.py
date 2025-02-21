from flask import Flask, request, jsonify
import pandas as pd
import io

app = Flask(__name__)

@app.route('/process_csv', methods=['POST'])
def process_csv():
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']
    df = pd.read_csv(file)

    # Extract headers and data types
    headers = df.columns.tolist()
    data_types = df.dtypes.astype(str).tolist()

    return jsonify({
        "headers": headers,
        "data_types": data_types
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
