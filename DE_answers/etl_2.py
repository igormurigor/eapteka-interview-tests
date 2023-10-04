import os
import psycopg2
import boto3
from io import BytesIO
from dotenv import load_dotenv
load_dotenv()


def connect_to_postgres(db_params):
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None


def fetch_data_from_postgres(conn, sql_query):
    try:
        cursor = conn.cursor()
        output_stream = BytesIO()
        cursor.copy_expert(sql_query, output_stream)
        output_stream.seek(0)
        return output_stream.getvalue()
    except psycopg2.Error as e:
        print(f"Error at fetch_data_from_postgres: {e}")
        return None


def upload_data_to_s3(data, s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key, endpoint_url):
    try:
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                          endpoint_url=endpoint_url
                          )
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=data)
        print(f"Data successfully uploaded to S3 at s3://{s3_bucket}/{s3_key}")
    except Exception as e:
        print(f"Error at upload_data_to_s3: {e}")


def main():
    # Connection parameters
    db_params = {
        'database': os.environ.get("EA_DBNAME"),
        'user': os.environ.get("EA_USER"),
        'password': os.environ.get("EA_PASSWORD"),
        'host': os.environ.get("EA_HOST"),
        'port': os.environ.get("EA_PORT"),
    }

    # S3 parameters
    s3_bucket = 'bucketeas3test'
    s3_key = 'output4.csv'
    aws_access_key_id = os.environ.get("aws_access_key_id")
    aws_secret_access_key = os.environ.get("aws_secret_access_key")
    endpoint_url = 'https://storage.yandexcloud.net'

    # SQL query
    sql_query = """
        COPY (select client_id, max(datetime) as last_operation_date from public.sales s group by client_id)
        TO STDOUT WITH CSV HEADER
    """

    conn = connect_to_postgres(db_params)
    if conn:
        data = fetch_data_from_postgres(conn, sql_query)
        if data:
            upload_data_to_s3(data, s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key, endpoint_url)
        conn.close()


if __name__ == "__main__":
    main()
