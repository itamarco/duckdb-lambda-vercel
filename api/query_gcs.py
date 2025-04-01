import duckdb
import json

def handler(event, context):
    # Only accept POST requests
    if event.get('httpMethod') != 'POST':
        return {
            "statusCode": 405,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Method not allowed. Only POST requests are accepted."})
        }
    
    # Get parameters from request body
    try:
        params = json.loads(event.get('body', '{}'))
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Invalid JSON in request body"})
        }

    gcs_key = params.get("gcs_key", "")
    gcs_secret = params.get("gcs_secret", "")
    parquet_path = params.get("parquet_path", "")
    query = params.get("query", "")

    if not all([gcs_key, gcs_secret, parquet_path, query]):
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Missing required parameters"})
        }

    query = query.replace("{table}", f"read_parquet('{parquet_path}')")

    duckdb.sql(f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_region='auto';
        SET s3_endpoint='storage.googleapis.com';
        SET s3_access_key_id='{gcs_key}';
        SET s3_secret_access_key='{gcs_secret}';
    """)

    result = duckdb.sql(query).df().to_json(orient='records')

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(result)
    }
