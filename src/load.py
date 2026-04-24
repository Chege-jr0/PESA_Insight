import pandas as pd
import numpy as np
import os
import sqlite3
from transform import transform_all

DATABASE_PATH = "database/finance.db"

# Creating a connection to the database
def get_connection():
    """
    Create connection to SQLite database.
    """
    os.makedirs("database", exist_ok=True)
    conn = sqlite3.connect(DATABASE_PATH)
    print("Database Connection Established")
    return conn


# Loading County data to database
def load_county_data(conn, df):
    """
    Load county inclusion data.
    """
    print("Loading county data...")
    df["inclusion_severity"] = df["inclusion_severity"].astype(str)
    df.to_sql("county_inclusion", conn, if_exists="replace", index=False)
    count = pd.read_sql("SELECT COUNT (*) AS total FROM county_inclusion", conn)
    print(f"County data loaded! {count['total'][0]} records saved to database")

# Loading mpesa data to database  
def load_mpesa_data(conn, df):
    """
    Loading M-pesa trend data.
    """
    print("Loading M-pesa data...")
    df.to_sql("mpesa_trends", conn, if_exists="replace", index=False)
    count = pd.read_sql("SELECT COUNT(*) AS total FROM mpesa_trends", conn)
    print(f"M-pesa data loaded {count['total'][0]} records loaded to database")

# Loading demographics data to database
def load_demographics(conn, df):
    """
    Load demographics data.
    """ 
    print("Loading demographics data...")
    df.to_sql("demogarphics", conn, if_exists="replace", index=False) 
    count = pd.read_sql("SELECT COUNT(*) as total FROM demographics", conn)
    print(f"Demographics data loaded {count['total'][0]} records saved to database") 

# Loading Barriers  data to database
def load_barriers_data(conn, df):
    """
    Loading barriers data.
    """
    print("Loading Barriers data....")
    df["severity"] = df["severity"].astype(str)
    df.to_sql("barriers", conn, if_exists = "replace", index=False)
    count = pd.read_sql("SELECT COUNT(*) as total FROM demographics", conn)
    print(f"Barriers data loaded {count['total'][0]} records saved to database")

# Loading Products data to database
def load_products(conn, df):
    """
    Loading Financial Products data
    """ 
    print("Loading products data...")
    df.to_sql("Financial Products", conn, if_exists="replace", index= False)
    count = pd.read_sql(
        "SELECT COUNT(*) as total FROM financial_products", conn
    )   
    print(f"Products data loaded {count['total'][0]} records saved to database")

def verify_database(conn):
    """Check all tables exist and show record counts."""
    print("\n📊 Database Summary:")
    print("=" * 45)

    tables = [
        "county_inclusion",
        "mpesa_trends",
        "demographics",
        "barriers",
        "financial_products"
    ]

    all_good = True
    for table in tables:
        try:
            count = pd.read_sql(
                f"SELECT COUNT(*) as total FROM {table}", conn
            )
            records = int(count["total"][0])
            status = "✅" if records > 0 else "❌"
            print(f"  {status} {table}: {records} records")
            if records == 0:
                all_good = False
        except Exception as e:
            print(f"  ❌ {table}: Error — {e}")
            all_good = False

    print("=" * 45)
    if all_good:
        print("✅ All tables loaded successfully!")
    else:
        print("❌ Some tables have issues — check pipeline!")    


def load_all():
    """
    Running the complete ETL load step
    """
    print("Starting data load")

    # Transform data
    data = transform_all()

    #Connect to database
    conn = get_connection()

    #Load each table
    load_county_data(conn, data["county"])
    load_mpesa_data(conn, data["mpesa"])
    load_demographics(conn, data["demographics"])
    load_barriers_data(conn, data["barriers"])
    load_products(conn, data["products"])

    #Close connection
    print("Database connection closed")
    print("ETL Pipeline complete")

if __name__ == "__main__":
    load_all()
    