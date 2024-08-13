import csv
import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL connection parameters
db_params = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT")
}

# CSV file path
csv_file = "./"

script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the full path to the CSV file
full_path = os.path.join(script_dir, csv_file)

# Connect to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Read CSV file and insert data into the table
with open(full_path, 'r') as f:
    csv_reader = csv.reader(f, delimiter=';')
    next(csv_reader)  # Skip the header row
    
    # Prepare data for bulk insert
    data = []
    for row in csv_reader:
        # Convert empty strings to None
        row = [None if cell == '' else cell for cell in row]
        
        # Convert date string to proper format
        row[1] = row[1].strip('"')  # Remove quotes from date string
        
        data.append(tuple(row))

    # Bulk insert data
    insert_query = """
    INSERT INTO protest_events VALUES %s
    ON CONFLICT (event_id_cnty) DO NOTHING
    """
    execute_values(cursor, insert_query, data)

# Commit the transaction and close the connection
conn.commit()
cursor.close()
conn.close()

print("Data successfully loaded into the PostgreSQL table.")