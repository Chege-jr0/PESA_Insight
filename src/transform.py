import pandas as pd
import numpy as np
import os

RAW_DATA_PATH = "data/raw"

print("Tranform module loaded")

def transform_county_data():
    """
    Clean and enrich county data
    """
    print("Transforming county data")

    df = pd.read_csv(f"{RAW_DATA_PATH}/county_inclusion_data.csv")

    #Basic Cleaning
    df = df.dropna()
    df = df.drop_duplicates()
    df["year"] = df["year"].astype(int)

    # Round all rate columns
    rate_cols = [
       "inclusion_rate", "exclusion_rate", "mpesa_adoption",
       "bank_account_rate", "sacco_membership",
       "financial_health", "credit_uptake", "savings_rate"
    ]

    for col in rate_cols:
        df[col] =df[col].astype(float).round(1)

    
    #Severity classification
    df["inclusion_severity"] = pd.cut(
        df["inclusion_rate"], 
        bins = [0, 65, 78, 85, 100],
        labels = ["Critical", "Low", "Medium", "High"]
    )
     
    # Opportunity score - counties with low inclusion
    # But improving trend value have high opportunity 

    df = df.sort_values(["county", "year"])
    df["inclusion_change"] = df.groupby("county")[
        "inclusion_rate"
    ].diff().round(1)

    # Financial Inclusion Index (0-100)
    # Composite of Inclusion, health, credit and savings
    df["inclusion_index"] = round(
        (df["inclusion_rate"] * 0.40) + (df["financial_health"] * 0.25) + 
        (df["credit_uptake"] * 0.20) + (df["savings_rate"] * 0.51), 1)
    
    # Mobile Money GAP - Difference between inclusion and M-pesa adoption
    df["mpesa_gap"] = round(
        df["inclusion_rate"] - df["mpesa_adoption"], 1
    )

    print(f"County data transformed {len(df)} records transformed.")

    return df


# Transforming M-pesa Trends

def transform_mpesa_data():
    """
    Clean and enrich M-pesa trend data ...
    """
    print("Transforming M-pesa data...")

    df = pd.read_csv(f"{RAW_DATA_PATH}/mpesa_trends.csv")

    # Basic Cleaning an setting the years as an interger
    df = df.dropna()
    df = df.drop_duplicates()
    df["year"] = df["year"].astype(int)
    df = df.sort_values("year")

    # Year on year subscriber growth
    df["subscriber_growth"] = df["subscribers_millions"].diff().round(2)
    df["growth_rate_pct"] = (
        df["subscribers_millions"].pct_change() * 100
    ).round(1)

    #Revenue per subscriber
    df["revenue_per_subscriber_kes"] = round(
        (df["revenue_billions_kes"] * 1000000000)/ (df["subscribers_millions"] * 1000000), 0
    )

    # Transaction volume per subscriber 
    df["transactions_per_subscriber)"] = round(
        (df["transactions_billions_kes"] * 1000000000)/
        (df["subscribers_millions"] * 1000000), 0
    )
   
    # Transaction volume per subscriber
    df["transactions_per_subscriber"] = round(
        (df["transactions_billions_kes"] * 1000000000) /
        (df["subscribers_millions"] * 1000000), 0
    )

    print(f"M-pesa data transformed {len(df)} records transformed.")


def transform_demographics_data():
    """
    Clean and enrich demographic data
    """

    df = pd.read_csv(f"{RAW_DATA_PATH}/demographics.csv")

    #Basic Cleaning
    df = df.dropna()
    df = df.drop_duplicates()
    df["year"] = df["year"].astype(int)

    # Rounding off the float datatype
    rate_cols = [
        "male_inclusion", "female_inclusion", "gender_gap",
        "urban_inclusion", "rural_inclusion",
        "urban_rural_gap", "national_avg"
    ]

    for col in rate_cols:
        df[col] = df[col].astype(float).round(1)

    # Gender parity score - 100 means perfect quality
    df["gender_parity_score"] = round(
        (df["female_inclusion"] / df["male_inclusion"]) * 100, 1
    )

    # Digital Divide Index
    df["digital_divide"] = round(
        df["urban_inclusion"] - df["rural_inclusion"], 1
    )

    print(f"Demographics data transformed {len(df)} records transformed")
    return df

def transform_barriers_data():
    """
    Clean and enrich barriers data..
    """
    print("Transforming barriers data ...")

    df = pd.read_csv(f"{RAW_DATA_PATH}/barriers.csv")

    # Basic Cleaning
    df = df.dropna()
    df = df.drop_duplicates()
    df["year"] = df["year"].astype(int)
    df["percentage"] = df["percentage"].astype(float).round(1)


    # Barrier severity classification
    df["severity"] = pd.cut(
        df["percentage"],
        bins = [0, 15, 30, 50, 100],
        labels= ["Low", "Medium", "High", "Critical"]
    )

    # Year on year Improvement
    df = df.sort_values(["barrier", "year"])
    df["improvement"] = df.groupby("barrier")["percentage"].diff()
    df["improvement"] = (df["improvement"] * -1).round(1)

    print(f"Barriers data transformed! {len(df)} records transformed")

def transform_products_data():
    """
    Clean and enriich financial products data.
    """

    df = pd.read_csv(f"{RAW_DATA_PATH}/products.csv")

    #Basic Cleaning
    df = df.dropna()
    df = df.drop_duplicates()
    df["year"] = df["year"].astype(int)
    df["uptake_rate"] = df["uptake_rate"].astype(float).round(1)

    #Growth since launch
    df = df.sort_values(["product", "year"])
    df["growth_from_launch"] = df.groupby(
        "product"
    )["uptake_rate"].transform(
        lambda x:x - x[x > 0].iloc[0] if (x > 0).any() else 0
    )
    df["growth_from_launch"] = df["growth_from_launch"].round(1)

    #Digital vs Traditional label
    df["product_type"] = df["is_digital"].map(
        {1: "Digital", 0: "Traditional"}
    )

    print(f"Products data transformed! {len(df)} records transformed")

    return df

def transform_all():
    """
    Run all transformation steps
    """

    print("Starting data transformation")

    county = transform_county_data()
    mpesa = transform_mpesa_data()
    demographics = transform_demographics_data()
    barriers = transform_barriers_data()
    products = transform_products_data()

    print("All data transformed Successfully")

    return{
        "County": county,
        "Mpesa": mpesa,
        "demographics": demographics,
        "barriers": barriers,
        "products": products
    }

if __name__ == "__main__":
    data = transform_all()
