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

    try:
        # Read file into DataFrame
        if filename.endswith('.csv'):
            df = pd.read_csv(file)
        elif filename.endswith('.xls') or filename.endswith('.xlsx'):
            df = pd.read_excel(file, engine='openpyxl')
        else:
            return jsonify({"error": "Unsupported file format. Please upload a CSV or Excel file."}), 400

        # Basic file info
        num_rows, num_cols = df.shape
        missing_values = df.isnull().sum().sum()
        missing_percentage = round((missing_values / (num_rows * num_cols)) * 100, 2)

        # Standardized column names
        column_mappings = {
            "Date of Gift": "gift_date",
            "Gift Amount": "gift_amount",
            "Donor ID": "donor_id",
            "Gift Frequency": "gift_frequency",
            "Gift Type": "gift_type",
            "Campaign": "campaign",
            "Fund": "fund",
            "Channel Gift came in": "gift_channel",
            "State": "state",
            "City": "city",
            "Zip": "postal_code",
            "Email": "email"
        }
        
        df.rename(columns=column_mappings, inplace=True)

        # Column-Level Metadata
        column_metadata = []
        for col in df.columns:
            unique_values = df[col].nunique()
            missing = df[col].isnull().sum()
            most_frequent = df[col].mode()[0] if not df[col].mode().empty else None
            dtype = str(df[col].dtype)

            # Convert dtype to AI-friendly types
            if "int" in dtype or "float" in dtype:
                inferred_type = "Numeric"
            elif "datetime" in dtype:
                inferred_type = "DateTime"
            elif df[col].nunique() < 20:
                inferred_type = "Categorical"
            else:
                inferred_type = "Text"

            stats = None
            if inferred_type == "Numeric":
                stats = {
                    "min": df[col].min(),
                    "max": df[col].max(),
                    "mean": df[col].mean(),
                    "std_dev": df[col].std()
                }

            column_metadata.append({
                "name": col,
                "data_type": dtype,
                "inferred_type": inferred_type,
                "unique_values": unique_values,
                "missing_values": missing,
                "missing_percentage": round((missing / num_rows) * 100, 2),
                "most_frequent_value": most_frequent,
                "statistics": stats
            })

        # Donor-Level Aggregation for RFM Analysis
        df["gift_date"] = pd.to_datetime(df["gift_date"], errors='coerce')
        rfm_summary = df.groupby("donor_id").agg(
            last_gift=("gift_date", "max"),
            gift_count=("gift_amount", "count"),
            total_given=("gift_amount", "sum"),
            avg_gift=("gift_amount", "mean")
        ).reset_index()

        # Compute Recency, Frequency, Monetary Metrics
        today = pd.Timestamp.today()
        rfm_summary["recency"] = (today - rfm_summary["last_gift"]).dt.days
        rfm_summary["frequency"] = rfm_summary["gift_count"]
        rfm_summary["monetary"] = rfm_summary["total_given"]

        # Assign RFM Scores
        rfm_summary["R_score"] = pd.qcut(rfm_summary["recency"], 5, labels=[5, 4, 3, 2, 1]).astype(int)
        rfm_summary["F_score"] = pd.qcut(rfm_summary["frequency"], 5, labels=[1, 2, 3, 4, 5]).astype(int)
        rfm_summary["M_score"] = pd.qcut(rfm_summary["monetary"], 5, labels=[1, 2, 3, 4, 5]).astype(int)
        rfm_summary["RFM_Score"] = rfm_summary["R_score"].astype(str) + rfm_summary["F_score"].astype(str) + rfm_summary["M_score"].astype(str)

        # Detect potential data issues
        duplicate_rows = df.duplicated().sum()
        inconsistent_formats = sum(df[col].apply(lambda x: isinstance(x, str) and x.isnumeric()).sum() for col in df.select_dtypes(include=["object"]).columns)

        return jsonify({
            "filename": filename,
            "file_info": {
                "num_rows": num_rows,
                "num_columns": num_cols,
                "missing_percentage": missing_percentage,
                "duplicate_rows": duplicate_rows,
                "inconsistent_format_warnings": inconsistent_formats
            },
            "columns": column_metadata,
            "rfm_summary": rfm_summary.to_dict(orient="records")
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

