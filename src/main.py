
"""
main.py - Produkcijska verzija generisana iz Jupyter notebooka
"""

import sys

def main():
    #!/usr/bin/env python
    # coding: utf-8
    
    # In[1]:
    
    
    import requests
    import pandas as pd
    import json
    import logging
    from datetime import datetime
    
    # Configure logging
    logging.basicConfig(
        filename="parser_errors.log",
        level=logging.ERROR,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # Load configuration
    try:
        with open("config.json", "r", encoding="utf-8") as config_file:
            config = json.load(config_file)
            url = config.get("url")
            token = config.get("token")
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        print("Error loading 'config.json'.")
        exit()
    
    # HTTP headers
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    # Fetch data from server
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        print("Data successfully retrieved from server.")
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP request error: {e}")
        print("Error retrieving data.")
        exit()
    
    # Parse JSON content directly from memory
    try:
        data = response.json()
    except Exception as e:
        logging.error(f"Error parsing JSON response: {e}")
        print("Error parsing JSON response.")
        exit()
    
    # Convert to DataFrame
    try:
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.json_normalize(data)
        else:
            raise Exception("Unsupported data format.")
    except Exception as e:
        logging.error(f"Error converting to DataFrame: {e}")
        print("Unsupported data format.")
        exit()
    
    # Display first 5 rows
    print("\nFirst 5 rows of data:")
    print(df.head(5))
    
    # Save to CSV
    try:
        df.to_csv("dataset.csv", index=False, encoding="utf-8")
        print("\nData saved as 'dataset.csv'.")
    except Exception as e:
        logging.error(f"Error saving CSV file: {e}")
        print("Error saving CSV file.")
    
    # Identify complex columns
    def is_complex_type(elem):
        return isinstance(elem, (dict, list))
    
    def is_list(elem):
        return isinstance(elem, list)
    
    try:
        complex_columns = [col for col in df.columns if df[col].map(is_complex_type).any()]
    except Exception as e:
        logging.error(f"Error identifying complex columns: {e}")
        complex_columns = []
    
    # Unpack complex columns
    if complex_columns:
        print("\nFound complex columns:", complex_columns)
    
        for col in complex_columns:
            try:
                print(f"\nUnpacking column: {col}")
                col_non_null = df[col].dropna()
                if col_non_null.map(is_list).all():
                    # Explode if all values are lists
                    exploded = df[[col]].explode(col)
                    nested_df = pd.json_normalize(exploded[col])
                else:
                    # Otherwise assume dicts
                    nested_df = pd.json_normalize(col_non_null)
    
                nested_df.to_csv(f"{col}_table.csv", index=False, encoding="utf-8")
                print(f"Column '{col}' unpacked and saved as '{col}_table.csv'.")
            except Exception as e:
                logging.error(f"Error processing column '{col}': {e}")
                print(f"Error processing column: '{col}'. See 'parser_errors.log'.")
    else:
        print("\nNo complex columns found. No unpacking needed.")
    
    print("\nData is prepared and ready for further processing.")
    
    
    # In[2]:
    
    
    import pyodbc
    import pandas as pd
    import json
    
    server = "localhost\\MSSQLSERVER01"
    database = "bank"
    conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("Successfully connected to the database!")
    
        # Ensure the table exists, otherwise create it
        create_table_query = """
        IF OBJECT_ID('dbo.transactions', 'U') IS NULL
        CREATE TABLE transactions (
            id INT IDENTITY(1,1) PRIMARY KEY,
            city NVARCHAR(255),
            date DATE,
            card_type NVARCHAR(50),
            exp_type NVARCHAR(50),
            gender CHAR(1),
            amount INT
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
    
        # Truncate the table to remove old data
        cursor.execute("TRUNCATE TABLE dbo.transactions;")
        conn.commit()
        print("Table 'transactions' has been cleared.")
    
        # Prepare insert query
        insert_query = """
        INSERT INTO transactions (city, date, card_type, exp_type, gender, amount)
        VALUES (?, ?, ?, ?, ?, ?)
        """
    
        # Read CSV in chunks
        chunks = pd.read_csv("dataset.csv", sep=",", encoding="utf-8-sig", chunksize=1000)
    
        for chunk in chunks:
            # Optional JSON unpacking (if needed)
            if "json" in chunk.columns:
                print("Detected JSON in 'json' column. Unpacking first row only for simplicity.")
                unpacked = json.loads(chunk["json"].iloc[0])
                chunk = pd.DataFrame(unpacked)
    
            # Normalize column names
            chunk.columns = chunk.columns.str.strip().str.lower()
    
            # Convert data types
            chunk["date"] = chunk["date"].astype(str)
            chunk["amount"] = chunk["amount"].astype(int)
    
            # Prepare values directly from chunk (generator)
            values = zip(
                chunk["city"],
                chunk["date"],
                chunk["card type"],
                chunk["exp type"],
                chunk["gender"],
                chunk["amount"]
            )
    
            cursor.executemany(insert_query, list(values))  # or just values if pyodbc supports it
            conn.commit()
            print("Inserted 1000 rows...")
    
        cursor.close()
        conn.close()
        print("All data inserted successfully.")
    
    except Exception as e:
        print("Database connection error:", e)
    
    
    # In[3]:
    
    
    import pyodbc
    import pandas as pd
    
    # Connection config
    conn = pyodbc.connect("DRIVER={SQL Server};SERVER=localhost\\MSSQLSERVER01;DATABASE=bank;Trusted_Connection=yes;")
    cursor = conn.cursor()
    
    # Read original data
    df = pd.read_sql("SELECT * FROM transactions", conn)
    
    # Split 'city' into 'city' and 'country'
    df[['city', 'country']] = df['city'].str.split(', ', expand=True)
    
    # Ensure new table exists, otherwise create it
    create_table_query = """
    IF OBJECT_ID('dbo.new_transactions', 'U') IS NULL
    CREATE TABLE new_transactions (
        id INT IDENTITY(1,1) PRIMARY KEY,
        city NVARCHAR(255),
        country NVARCHAR(255),
        date DATE,
        card_type NVARCHAR(50),
        exp_type NVARCHAR(50),
        gender CHAR(1),
        amount INT
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    
    # Truncate the table to keep structure but remove old data
    cursor.execute("TRUNCATE TABLE new_transactions;")
    conn.commit()
    print("Table 'new_transactions' has been cleared using TRUNCATE.")
    
    # Prepare insert query
    insert_query = """
    INSERT INTO new_transactions (city, country, date, card_type, exp_type, gender, amount)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    
    # Prepare data for batch insert
    values = list(zip(
        df['city'],
        df['country'],
        df['date'],
        df['card_type'],
        df['exp_type'],
        df['gender'],
        df['amount']
    ))
    
    cursor.executemany(insert_query, values)
    conn.commit()
    print("Data successfully inserted into 'new_transactions'.")
    
    # Cleanup
    cursor.close()
    conn.close()
    

if __name__ == "__main__":
    main()
