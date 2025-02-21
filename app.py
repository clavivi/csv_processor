from flask import Flask, request, jsonify
import pandas as pd
import numpy as np
import io

app = Flask(__name__)

def convert_types(obj):
    """Convert numpy types to standard Python types for JSON serialization."""
    if isinstance(obj, np.int64) or isinstance(obj, np.int32):
        return int(obj)
    elif isinstance(obj, np.float64) or isinstance(obj, np.float32):
        return float(obj)
    elif isinstance(obj, list):
        return [convert_types(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_types(v) for k, v in obj.items()}
    return obj

@app.route('/process_file', methods=['POST'])
def process_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']
    filename = file.filename.lower()

    try:
        # Read file into DataFrame
        if filename.endswith('.csv'):
            df = pd.read_csv(file, dtype=str)  # Read everything as text to prevent misinterpretation
        elif filename.endswith('.xls') or filename.endswith('.xlsx'):
            df = pd.read_excel(file, dtype=str, engine='openpyxl')
        else:
            return jsonify({"error": "Unsupported file format. Please upload a CSV or Excel file."}), 400

        # Basic file info
        num_rows, num_cols = df.shape
        missing_values = df.isnull().sum().sum()
        missing_percentage = round((missing_values / (num_rows * num_cols)) * 100, 2)

        # Column-Level Metadata
        column_metadata = []
        for col in df.columns:
            unique_values = df[col].nunique()
            missing = df[col].isnull().sum()
            most_frequent = df[col].mode()[0] if not df[col].mode().empty else None
            example_values = df[col].dropna().sample(min(5, len(df[col].dropna())), random_state=1).tolist() if len(df[col].dropna()) > 0 else []

            # Detect potential column standard names based on keywords
            column_mappings = {
                "gift": ["Gift Amount", "gift_amount", "Amount", "Gift Value"],
                "date": ["Date of Gift", "gift_date", "Transaction Date"],
                "donor": ["Donor ID", "donor_id", "Supporter ID"],
                "fund": ["Fund", "Fund Name"],
                "campaign": ["Campaign", "Appeal Code"],
                "email": ["Email", "Email Address"],
                "state": ["State", "Province"],
                "city": ["City"],
                "zip": ["Zip", "Postal Code"]
            }
            suggested_name = None
            for standard_name, variations in column_mappings.items():
                if any(variation.lower() in col.lower() for variation in variations):
                    suggested_name = standard_name
                    break

            # Infer data type
            inferred_type = "Text"
            if df[col].str.match(r'^\d+(\.\d+)?$').sum() > 0.8 * len(df[col].dropna()):  # If 80%+ of values look like numbers
                inferred_type = "Numeric"
            elif df[col].str.match(r'\d{4}-\d{2}-\d{2}').sum() > 0.8 * len(df[col].dropna()):  # YYYY-MM-DD format detection
                inferred_type = "DateTime"
            elif unique_values < 20:
                inferred_type = "Categorical"

            column_metadata.append({
                "original_name": col,
                "suggested_name": suggested_name,
                "inferred_type": inferred_type,
                "unique_values": unique_values,
                "missing_values": missing,
                "missing_percentage": round((missing / num_rows) * 100, 2),
                "most_frequent_value": most_frequent,
                "example_values": example_values
            })

        # Prepare JSON response
        response = {
            "filename": filename,
            "file_info": {
                "num_rows": num_rows,
                "num_columns": num_cols,
                "missing_percentage": missing_percentage
            },
            "columns": column_metadata
        }

        return jsonify(convert_types(response))  # Convert non-serializable types before returning JSON

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
