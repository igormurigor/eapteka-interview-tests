import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd

# Database connection parameters
load_dotenv()
dbname = os.environ.get("EA_DBNAME")
user = os.environ.get("EA_USER")
password = os.environ.get("EA_PASSWORD")
host = os.environ.get("EA_HOST")
port = os.environ.get("EA_PORT")
commit_interval = 10
table_name = "sales"
csv_file = "../DE/csv/sales.csv"

# Connection to the PostgreSQL database
try:
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    print("Connected to the database!")

# Create a cursor object
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS public.{table_name} (
                datetime timestamp NULL,
                client_id int4 NULL,
                order_id int4 NULL,
                item_id int4 NULL,
                amount int4 NULL,
                "cost" numeric NULL,
                vat numeric NULL,
                return_flag varchar NULL
        )"""
    cursor.execute(create_table_sql)
    conn.commit()
    print(f"Table '{table_name}' created or already exists.")

    # Use Pandas to read the CSV file in chunks
    chunk_size = 10
    for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
        # Convert the Pandas DataFrame to a list of tuples
        data = [tuple(row) for row in chunk.values]

        # Insert data into the PostgreSQL table
        insert_sql = f"""
            INSERT INTO {table_name} (datetime, client_id, order_id, item_id, amount, cost, vat, return_flag)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_sql, data)

        # Commit the changes every 'commit_interval' rows
        if cursor.rowcount >= commit_interval:
            conn.commit()
            print(f"Committed {cursor.rowcount} rows.")

    # Commit any remaining changes
    conn.commit()
    print(f"Committed the remaining rows.")

    # Close the cursor and connection
    cursor.close()
    conn.close()

except psycopg2.Error as e:
    print("Error connecting to the database:", e)