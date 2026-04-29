"""
This file uses machine learning to predict Kenya,s finnacial Inclusion rates upto 2030- county by county

It uses Linear Regression from scikit-learn, the simplest but most applicable ML model. 
The model fits this project because, Inclusion rate have been trending consitently upward, Linear regression captures that trend  cleanly, and lastly the results are explainabel.
Linear Regression - ML model that predicts future values
r2_score - Measures how well the model fits the data
mean_absolute_error - Measures average prediction error in % points
"""

import pandas as pd
import numpy as np
import sqlite3
import os
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_absolute_error

DATABASE_PATH = "database/finance.db"

def get_connection():
    conn = sqlite3.connect(DATABASE_PATH)
    return conn

def forecast_county_inclusion():
    print("Forecasting county Financial inclusion rates upto 2030 using linear regression on historical data")

    conn = get_connection()
    df = pd.read_sql("SELECT * FROM county_inclusion_data", conn)
    conn.close()

    forecast_years = [2025, 2026, 2027, 2028, 2029, 2030]
    all_counties = df["county"].unique()

    forecast_records = []

    for county in all_counties:
        county_data = df[df["county"] == county].sort_values("year")

        # Scikit learn expects X as a 2D array, reshape function converts a flat lits into a column
        x = county_data["year"].values.reshape(-1, 1)
        y = county_data["inclusion_rate"].values

        # Train the model, the model finds  a straight line between the points between the points x and Y
        model = LinearRegression()
        model.fit(x, y)

        # Model Quality metrics
        # r2 = measures how well the line fits the data
        y_pred = model.predict(x)
        r2 = round(r2_score(y, y_pred), 3)
        mae = round(mean_absolute_error(y, y_pred), 2)

        # Predict future years
        for year in forecast_years:
            predicted = model.predict([[year]])[0]

            # Cap between current rate and 99%
            # We never predict inclusion going backwards- that would be misleading
            current_rate = county_data["inclusion_rate"].iloc[-1]
            predicted = round(min(99, max(current_rate, predicted)), 1)

            # Confidence interval - wider for further years
            # Prediction gets less certain, the further into the future we look
            years_ahead = year -2024
            margin = round(mae * (1 + years_ahead * 0.1), 1)

            forecast_records.append({
                "county": county,
                "year": year,
                "predicted_inclusion_rate": predicted,
                "lower_bound": round(max(0, predicted - margin), 1),
                "upper_bound": round(min(99, predicted + margin), 1),
                "model_r2": r2,
                "model_mae": mae,
                "forecast_type": "County Linear Regression"
            })

        forecast_df = pd.DataFrame(forecast_records)
        print(f"County Forecasts generated {len(forecast_records)} predictions.")
        return forecast_df   
    

def forecast_national_trend():
    print("Forecasting national trend to 2030...")

    conn = get_connection()
    mpesa_df = pd.read_sql("SELECT * FROM mpesa_trends", conn)
    conn.close()

    forecast_years = [2025, 2026, 2027, 2028, 2029, 2030]
    forecast_records = []

    #Forecast Each Metric
    metrics = [
        "subscribers_millions",
        "active_users_millions",
        "transactions_billions_kes",
        "revenue_billions_kes"
    ]

    for metric in metrics:
        data =  mpesa_df.sort_values("year")
        x = data["year"].values.reshape(-1, 1)
        y = data[metric].values

        model =LinearRegression()
        model.fit(x, y)

        y_pred = model.predict(x)
        mae = round(mean_absolute_error(y, y_pred), 2)

        for year in forecast_years:
            predicted = model.predict([[year]])[0]
            years_ahead = year - 2024
            margin = mae * (1 + years_ahead * 0.1)

            forecast_records.append({
                "metric": metric,
                "year": year,
                "predicted_value": round(max(0, predicted), 2),
                "lower_bound": round(max(0, predicted - margin), 2),
                "upper_bound": round(predicted + margin, 2),
                "model_name": mae

            })
    forecast_df = pd.DataFrame(forecast_records)
    print(f"National forecasts generated {len(forecast_df)} predictions")
    return forecast_df

def model_intervention(intervention_type: str, intensity: float = 0.5):
    """
    Model the impact of a specific intervention on inclusion rates.

    interventions:
    - 'smartphone_subsidy' -> subsidise phones for excluded people
    - 'agent_expansion' -> expand M-pesa agent network
    - 'id_registration' -> mass national ID registration drive
    - 'financial_literacy' -> financial literacy training program

    intensity: 0.0 to 1.0 - how aggressive the intervention is

    This model is useful in cases where:
    "If we Invest KES 2 Billion in smartphone subsidies, how much inclusion improve and where"

    """
    print(f" Modelling intervention: {intervention_type} (intensity: {intensity})...")

    conn = get_connection()
    df = pd.read_sql("SELECT * FROM county_inclusion_data", conn)
    conn.close()

    # How much each intervention boosts inclusion
    intervention_impacts = {
        "smartphone_subsidy": {
            "boost_pct": 8.5 * intensity,
            "target": "low_inclusion",
            "description": "Subsidised smartphone distribution"
        },
        "agent_expansion": {
            "boost_pct": 5.2 * intensity,
            "target": "rural",
            "description": "M-pesa agent network expansion"
        },
        "id_registration":{
            "boost_pct": 6.8 * intensity,
            "target": "low_inclusion",
            "description": "National ID registration drive"
        },
        "financial_literacy": {
            "boost_pct": 3.5 * intensity,
            "target": "all",
            "description": "Financial literacy training"
        }
    }

    if intervention_type not in intervention_impacts:
        print(f"Unknown intervention: {intervention_type}")
        return None
    
    impact =  intervention_impacts[intervention_type]
    latest = df[df["year"] == 2024].copy()

    results = []
    for _, row in latest.iterrows():
        baseline = row["inclusion_rate"]
        boost = impact["boost_pct"]

        #Higher boost for counties that need it most
        if impact["target"] == "low_inclusion":
            if baseline < 70:
                boost *= 1.5
            elif baseline < 80:
                boost *= 1.2

        projected = round(min(99, baseline + boost), 1)

        results.append({
            "county": row["county"],
            "baseline_2024": baseline,
            "projected_2027": projected,
            "improvement": round(projected - baseline, 1),
            "intervention": intervention_type,
            "description": impact["description"],
            "intensity": intensity
        }) 

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values("improvement", ascending=False)

    print("Intervention model Complete")
    print(f"Average improvement: {results_df['improvement'].mean():.1f}%")
    print(f"Counties most impacted: {results_df.head(3)['county'].tolist()}")

    return results_df     


def save_forecasts(county_forecast, national_forecast):
    print("Saving forecasts to database...")


    conn = get_connection()

    county_forecast.to_sql("county_forecasts", conn, if_exists = "replace", index=False)

    national_forecast.to_sql("national_forecasts", conn, if_exists = "replace", index = False)

    conn.close()
    print("Forecasts saved to database!")

def run_all_forecasts():
    print("Starting PesaInsight Forecasting...")

    #County Forecasts
    county_forecast = forecast_county_inclusion()

    #National Forecasts
    national_forecast = forecast_national_trend()

    # Sample Intervention models
    smartphone = model_intervention("smartphone_subsisdy", intensity=0.7)
    agent = model_intervention("agent_exapnsion", intensity=0.5)

    #Save to database
    save_forecasts(county_forecast, national_forecast)
    print("All forecasts complete!")
    print(f"2030 Predictions Sample:")

    sample = county_forecast[
        county_forecast["year"] == 2030
    ].sort_values("predicted_inclusion_rate", ascending=False)

    print("\nTop 5 counties by predicted 2030 inclusion:")
    print(sample[["county", "predicted_inclusion_rate", "upper_bound"]]
          .head(5).to_string(index=False))

    print("\nBottom 5 counties — most at risk:")
    print(sample[["county", "predicted_inclusion_rate", "lower_bound"]]
          .tail(5).to_string(index=False))

    return {
        "county_forecast": county_forecast,
        "national_forecast": national_forecast,
        "smartphone_intervention": smartphone,
        "agent_intervention": agent
    }

if __name__ == "__main__":
    run_all_forecasts()




