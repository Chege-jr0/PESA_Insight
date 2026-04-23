import pandas as pd
import numpy as np
import requests
import os

RAW_DATA_PATH = "data/raw"

# Think of this of like setting up a single source of truth according to the last 2024 FinAccess household data in Kenya.
REAL_DATA = {
    "national_inclusion_rate": 84.8,
    "national_exclusion_rate": 9.9,
    "mpesa_usage": 52.6,
    "credit_uptake": 64.0,
    "savings_rate": 68.1,
    "financial_health": 18.3,
    "financial_literacy": 42.1,
    "urban_inclusion": 91.3,
    "rural_inclusion": 80.2,
    "male_inclusion": 85.7,
    "female_inclusion": 84.1,
    "barrier_no_phone": 64.1,
    "barrier_no_id": 51.5

}

# Setting up folders
def setup_folders():
    """ Create project folders if they don't exist.
    """
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    os.makedirs("database", exist_ok=True)
    print("Folders Ready")

def extract_county_inclusion():
    """
    Generate county-level financial inclusion data anchored to real FinAcess survey Statistics
    """    
    print("Generating county inclusion data...")

    np.random.seed(42)

    counties = { "Kiambu": 94.0, "Nairobi": 93.7, "Kirinyaga": 92.8,
                "Nyeri": 91.6, "Isiolo": 91.5, "Kisumu": 91.2,
                "Embu": 90.9, "Taita-Taveta": 90.5, "Uasin-Gishu": 90.2,
                "Mandera": 89.7, "Lamu": 88.8, "Nandi": 88.3,
                "Machakos": 88.2, "Nyandarua": 87.9, "Mombasa": 87.6,
                "Garissa": 87.1, "Kisii": 86.7, "Murang'a": 86.6,
                "Laikipia": 86.4, "Kajiado": 86.1, "Tharaka-Nithi": 85.9,
                "Wajir": 85.6, "Nyamira": 85.4, "Elgeyo-Marakwet": 84.7,
                "Nakuru": 84.5, "Meru": 84.0, "Homabay": 83.3,
                "Bomet": 82.7, "Makueni": 81.9, "Siaya": 81.9,
                "Kitui": 81.2, "Samburu": 80.5, "Kwale": 79.7,
                "Kakamega": 79.6, "Marsabit": 79.1, "Vihiga": 79.1,
                "Kericho": 79.0, "Bungoma": 78.5, "Kilifi": 77.9,
                "Trans-Nzoia": 77.5, "Tana-River": 77.4, "Busia": 76.9,
                "Baringo": 76.3, "Narok": 73.1, "Migori": 72.9,
                "Turkana": 65.9, "West-Pokot": 48.5

    }

    years = [2006, 2009, 2013, 2016, 2019, 2021, 2024]

    #National growth_rates according to Google
    national_history = {
    2006: 26.7, 2009: 40.5, 2013: 66.7,
    2016: 75.3, 2019: 82.9, 2021: 83.7, 2024: 84.8
    }   

    records = []
    for county, inclusion_2024 in counties.items():
        for year in years:
            national_rate = national_history[year]
            scale = national_rate / 84.8
            base = inclusion_2024 * scale
            noise = np.random.uniform(-1.5, 1.5)
            inclusion = round(min(99, max(20, base + noise)), 1)
            exclusion = round(max(0, 100 - inclusion - np.random.uniform(3, 8)), 1)

            records.append({
                "county": county,
                "year": year,
                "inclusion_rate": inclusion,
                "exclusion_rate": exclusion,
                "mpesa_adoption": round(inclusion * np.random.uniform(0.55, 0.70), 1),
                "bank_account_rate": round(inclusion * np.random.uniform(0.35, 0.50), 1),
                "sacco_membership": round(inclusion * np.random.uniform(0.10, 0.15), 1),
                "financial_health": round(REAL_DATA["financial_heath"] * (inclusion / 84.8) + np.random.uniform(-2, 2), 1),
                "credit_uptake": round(REAL_DATA["credit_uptake"] * (inclusion / 84.8) + np.random.uniform(-3, 3),1),
                "savings_rate": round(REAL_DATA["savings_rate"] * (inclusion / 84.8) + np.ramdom.uniform(-3, 3), 1)
            })

    df = pd.DataFrame(records)
    filepath = f"{RAW_DATA_PATH}/county_inclusion_data.csv"
    df.to_csv(filepath, index=False)
    print(f"County data generated, {len(df)} records saved.")
    return df

#Simulating M-pesa data from the real_world
def extract_mpesa_trends():
    """
    Generate M-pesa adoption trend data since launch in 2007
    Anchored to real Safaricom annual report figures.
    """
    print("Generating M-pesa trend data...")
    np.random.seed(7)

    # Real M-pesa subscriber dara from Safaricom annual reports
    real_subscribers = {
        2007: 1.0,  2008: 5.0,  2009: 9.5,  2010: 13.8,
        2011: 17.3, 2012: 19.0, 2013: 21.8, 2014: 24.1,
        2015: 25.6, 2016: 26.0, 2017: 29.5, 2018: 33.0,
        2019: 37.1, 2020: 41.5, 2021: 48.3, 2022: 52.4,
        2023: 60.7, 2024: 66.2
    }

    records = []
    for year, subscribers_millions in real_subscribers.items():
        noise = np.random.uniform(-0.3, 0.3)
        records.append({
            "year": year,
            "subscribers_millions": round(subscribers_millions + noise, 1),
            "active_users_millions": round(subscribers_millions * 0.65 + np.random.uniform(-0.5, 0.5),1),
            "agents_thousands": round(subscribers_millions * 5.2 + np.random.uniform(-10, 10), 0),
            "transcations_billion_kes": round(subscribers_millions * 2.1 + np.random.uniform(-5, 5), 1),
            "avg_transaction_kes": round(1420 * (year / 2024) * 0.85 + np.random.uniform(-50, 50), 0),
            "revenue_billions_kes": round(subscribers_millions * 2.1 + np.random.uniform(-2, 2), 1)
        })

        df = pd.DataFrame(records)
        filepath = f"{RAW_DATA_PATH}/mpesa_trends.csv"
        df.to_csv(filepath, index=False)
        print(f"Mpesa trend data generate{len(df)} records saved.")
        return df 

# Simulating Demographics data of financial inclusion

def extract_demographics():
    """
    Generate demographic breakdown of financial inclusion,
    Gender, age group and urban/rural anchored to real data.
    This data examines how often financial services are utilized. 
    """

    print("Generating demographics data ...")
    np.random.seed(15)

    years = [2006, 2009, 2013, 2016, 2019, 2021, 2024]

    real_gender = {
       2006: {"male": 33.2, "female": 20.5},
        2009: {"male": 45.8, "female": 35.2},
        2013: {"male": 70.1, "female": 63.8},
        2016: {"male": 78.4, "female": 72.6},
        2019: {"male": 84.1, "female": 81.7},
        2021: {"male": 85.2, "female": 82.3},
        2024: {"male": 85.7, "female": 84.1} 
    }

    real_urban_rural ={
        2006: {"urban": 35.5, "rural": 23.8},
        2009: {"urban": 52.3, "rural": 36.4},
        2013: {"urban": 74.5, "rural": 62.1},
        2016: {"urban": 82.1, "rural": 70.4},
        2019: {"urban": 88.7, "rural": 78.9},
        2021: {"urban": 90.2, "rural": 79.4},
        2024: {"urban": 91.3, "rural": 80.2}
    }

    age_groups = ["15-24", "25-34", "35-44", "45-54", "55+ "]

    age_multipliers = {
        "15-24": 0.78, "25-34": 1.05,
        "35-44": 1.08, "45-54": 1.02, "55+": 0.85
    }

    records = []
    for year in years:
        male_rate = real_gender[year]["male"]
        female_rate = real_gender[year]["female"]
        urban_rate = real_urban_rural[year]["urban"]
        rural_rate = real_urban_rural[year]["rural"]
        national = (male_rate + female_rate)/2

        for age in age_groups:
            multiplier = age_multipliers[age]
            records.append({
                "year": year,
                "age_group" : age,
                "male_inclusion": round(male_rate * multiplier + np.random.uniform(-1, 1), 1),
                "female_inclusion": round(female_rate * multiplier + np.random.uniform(-1, 1),1),
                "gender_gap": round((male_rate - female_rate) * multiplier, 1),
                "urban_inclusion": round(urban_rate * multiplier + np.ramndom.uniform(-1, 1),1),
                "rural_inclusion": round(rural_rate * multiplier + np.random.uniform(-1, 1),1),
                "urban_rural_gap": round((urban_rate - rural_rate) * multiplier,1),
                "national_avg": round(national * multiplier, 1)
            })
        df = pd.DataFrame(records)
        filepath = f"{RAW_DATA_PATH}/demographics.csv"
        df.to_csv(filepath, index=False)
        print(f"Demographics data generated{len(df)} records saved.")
        return df     


# Simulating Financial Exclusion Barrier Data

def extract_barriers():
    """
    Generate financial exclusion barrier data anchored to real 2024 FinAccess Findings

    """

    print("Generating barriers data...")
    np.random.seed(20)

    barriers = {
        "No Mobile Phone": 64.1,
        "No Identity Document": 51.5,
        "Insufficient Income": 43.2,
        "Lack of Trust": 28.7,
        "Distance to Agent": 24.3,
        "Literacy Barriers": 19.8,
        "Relevance": 3.8,
        "Others": 7.3
    }

    years = [2013, 2016, 2019, 2021, 2024]

    improvement_rates = {
        "No Mobile Phone": 0.85,
        "No Identity Document": 0.90,
        "Insufficient Income": 0.92,
        "Lack of Trust": 0.88,
        "Distance to Agent": 0.82,
        "Literacy Barriers": 0.91,
        "Relevance": 0.97,
        "Others": 1.05
    }

    records = []
    for barrier, rate_2024 in barriers.items():
        improve  =  improvement_rates[barrier]
        for i, year in enumerate(years):
            years_back = 2024 - year
            historical_rate = rate_2024 /  (improve ** (years_back / 4))
            noise  =  np.random.uniform(-1.5, 1.5)
            records.append({
                "barrier": barrier,
                "year" : year,
                "percentage": round(min(95, max(2, historical_rate + noise)), 1)
            })

    df = pd.DataFrame(records)
    filepath = f"{RAW_DATA_PATH}/barriers.csv"
    df.to_csv(filepath,index=False)
    print(f"Barriers data generated! {len(df)} records saved.")
    return df        

def extract_products():

    """
    Generate financial product uptake data.
    Show growth of M-pesa, banks, SACCOs, MFIs etc.
    """
    print("Generating financial products data...")

    products_2024 = {
        "M-pesa": 52.6,
        "Bank Account": 38.4,
        "SACCO": 11.7,
        "Mobile Bank (M-Shwari etc)": 19.3,
        "Digital MFI": 14.2,
        "Fuliza Overdraft": 16.8,
        "Hustler Fund": 12.4,
        "Insurance": 8.9,
        "Chama/ROSCA": 5.2 
    }

    years = [2006, 2009, 2016, 2019, 2021, 2024]

    # Launch years - product didn't exist before these dates
    launch_years = {
        "M-pesa": 2007,
        "Bank Account": 2006,
        "SACCO": 2006,
        "Mobile Bank(M-shwari etc)": 2013,
        "Digital MFI": 2016,
        "Fuliza Overdraft": 2019,
        "Hustler Fund": 2022,
        "Insurance": 2006,
        "Chama/ROSCA": 2006
    }

    records = []
    for product, rate_2024 in products_2024.items():
        launch = launch_years[product]
        for year in years:
            if year < launch:
                uptake = 0.0
            else:
                years_since = year - launch
                total_years = 2024 - launch
                growth = (years_since / total_years) ** 0.7
                noise = np.random.uniform(-1, 1)
                uptake = round(max(0, min(99, rate_2024 * growth + noise)), 1)  

                records.append({
                    "product": product,
                    "year": year,
                    "uptake_rate": uptake,
                    "is_digital": 1 if product in [
                        "M-Pesa", "Mobile Bank(M-Shwari etc)",
                        "Digital MFI", "Fuliza Overdraft", "Hustler Fund"
                    ] else 0

                }) 

    df = pd.DataFrame(records)
    filepath = f"{RAW_DATA_PATH}/products.csv"
    df.to_csv(filepath, index=False)
    print(f"Products data generated {len(df)} records saved")  
    return df           

def extract_all():
    """Run all extraction steps"""
    print("Starting PesaInsight data extraction...")

    setup_folders()

    county = extract_county_inclusion()
    mpesa = extract_mpesa_trends()
    demographics = extract_demographics()
    barriers = extract_barriers()
    products = extract_products()

    print("All data Extracted Successfully")

    return {
        "county": county,
        "mpesa": mpesa,
        "demographics": demographics,
        "barriers": barriers,
        "products": products
    }
if __name__ == "__main__":
    extract_all()

