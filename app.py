from flask import Flask, request, jsonify
import pandas as pd
import io

app = Flask(__name__)

@app.route('/process_file', methods=['POST'])
def process_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']
    filename = file.filename.lower()

    # Determine file type
    try:
        if filename.endswith('.csv'):
            df = pd.read_csv(file)
        elif filename.endswith('.xls') or filename.endswith('.xlsx'):
            df = pd.read_excel(file, engine='openpyxl')  # Use openpyxl for .xlsx
        else:
            return jsonify({"error": "Unsupported file format. Please upload a CSV or Excel file."}), 400

        # Extract headers and data types
        headers = df.columns.tolist()
        data_types = df.dtypes.astype(str).tolist()

        return jsonify({
            "filename": filename,
            "headers": headers,
            "data_types": data_types
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

