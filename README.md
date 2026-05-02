## PesaInsight - Kenya Mobile Mney and Financial Inclusion

An AI-powered intelligence platform that tracks M-pesa adoption, financial inclusion gaps and exclusion barriers across Kenya, with forecasting to 2030 and an AI adviser for SME,s and policy makers
 
"Kenya leads the world in mobile money, Yet a minority of Kenyans remain completely financially excluded and in remote areas, the percentage is even higher. PesaInsights exists to make the contradiction visible, understable and actionable."



## Data Sources
1. 2025 FinAccess Household Survey

2. World Bank Global Findex Database

3. Safaricom Annual Reports

4. CBK Financial Sector Reports


## TechStack
1. Apache Airflow 3.0 => Automated weekly data updates

2. SQLite => Zero setup, portable and fast database.

3. Pandas => Cleaning, aggregation, feature engineering.

4. Numpy => Realistic county-level data generation.

5. Scikit-learn => Forecasting through linea regression for 2030 projections.

6. Streamlit => Interactive Dashboard in pure python.

7. Plotly Express => Interactive, professional visualisations charts

8. Ollama + TinyLlama => Free, private local AI to give predictions that worsk offline.

9. Python => Core language that ties everything together.

## Project Structure
```markdown
pesa-insight/
│
├── dags/
│   └── finance_pipeline.py      # Airflow 3.0 DAG
│
├── data/
│   └── raw/                     # Raw extracted CSV files
│
├── database/
│   └── finance.db               # SQLite database (auto-created)
│
├── src/
│   ├── extract.py               # Step 1 — Pull data from sources
│   ├── transform.py             # Step 2 — Clean & enrich
│   ├── load.py                  # Step 3 — Write to SQLite
│   ├── forecast.py              # Step 4 — Generate 2030 projections
│   └── ai_advisor.py            # AI insights, SME & personal advisor
│
├── dashboard/
│   └── app.py                   # Streamlit dashboard
│
├── requirements.txt
└── README.md
```
## Key Metrics
1. Inclusion_rate => Percentage of adults with access to formal financial services.

2. Exclusion_rate => Percentage with no access to financial services.

3. M-pesa Adoption => Percentage of people using mobile money.

4. Gender-Gap => Difference in male vs female inclusion rates.

5. Urban_Rural_Gap => Difference in urban vs rural inclusion.

6. Financial_health => Percentage of people considered healthy.

7. Credit_Uptake => Percentage of people who have access to credit.

8. Savings_rate => Percentage of people who save formally.

9. Barrier_no_phone => Percentage of people excluded citing no mobile phone.

10. barrier_no_id => Percentage of people exclude citing no identity document.

## Setup and Installation
1. Step 1 - Clone the Repository
```bash
git clone https://github.com/Chege-jr0/PESA_Insight
```
2. Create Virtual Environment
```bash
python -m venv venv
source venv/Scripts/Activate
```
3. Install Dependencies
```bash
pip install -r requirements.txt
```
4. Pull the AI model
```bash
ollama serve
ollama pull tinyllama
```
5. Run the Pipeline
```bash
python src/extract.py
python src/transform.py
python src/load.py
python src/forecast.py
```
6. Launch the Dashboard
```bash
streamlit run dashboard/app.py
```

## Related Articles
1. Exposing Kenya's Hidden Financial Exclusion Crises

link: https://medium.com/@paulgikonyo100/exposing-kenyas-hidden-financial-exclusion-crises-c6317d1a4424

2. Linkedin: https://www.linkedin.com/posts/paul-gikonyo-15389418b_kenya-financialinclusion-mpesa-ugcPost-7456018322669764608-Jfjy?utm_source=share&utm_medium=member_desktop&rcm=ACoAACzNhM8B6HD_yIkGpHSdjRGHqGPBsClH7fs

## Author
Built as part of a self-directed AI Engineerinn learning journey, combining my background in data analytics with modern AI to solve Kenya's most important development challenges.

Paul Gikonyo Data Analyst@Everything Data Africa

## License
MIT License - feel free to use, modify and build on it.