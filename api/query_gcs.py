import duckdb
import json
from http.server import BaseHTTPRequestHandler
import tempfile
import os

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            params = json.loads(post_data.decode('utf-8'))
        except json.JSONDecodeError:
            self.send_error(400, "Invalid JSON in request body")
            return

        gcs_key = params.get("gcs_key", "")
        gcs_secret = params.get("gcs_secret", "")
        parquet_path = params.get("parquet_path", "")
        query = params.get("query", "")

        if not all([gcs_key, gcs_secret, parquet_path, query]):
            self.send_error(400, "Missing required parameters")
            return

        query = query.replace("{table}", f"read_parquet('{parquet_path}')")

        try:
            # Create a temporary directory for DuckDB
            with tempfile.TemporaryDirectory() as temp_dir:
                # Set DuckDB home directory
                duckdb.sql(f"SET home_directory='{temp_dir}';")
                
                # Install and configure httpfs
                duckdb.sql(f"""
                    INSTALL httpfs;
                    LOAD httpfs;
                    SET s3_region='auto';
                    SET s3_endpoint='storage.googleapis.com';
                    SET s3_access_key_id='{gcs_key}';
                    SET s3_secret_access_key='{gcs_secret}';
                """)

                result = duckdb.sql(query).df().to_json(orient='records')

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(result.encode('utf-8'))
        except Exception as e:
            self.send_error(500, str(e))

    def do_GET(self):
        self.send_error(405, "Method not allowed. Only POST requests are accepted.")
