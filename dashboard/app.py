import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import sys
import os

# Add src to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
src_path = os.path.join(project_root, 'src')

if src_path not in sys.path:
    sys.path.insert(0, src_path)
if project_root not in sys.path:
    sys.path.insert(0, project_root) 


from ai_advisor import (
    generate_policy_insights,
    advise_sme,
    advise_individual,
    ask_financial_question,
    get_market_context
)     

DATABASE_PATH = os.path.join(project_root, "database", "finance.db")

st.set_page_config(
    page_title= "PesaInsigh Kenya",
    layout = "wide"
)

st.title("PesaInsight - Kenya Financial Inclusion Tracker")
st.markdown("AI-powered intelligence on M-pesa adoption and financial inclusion across Kenya")

@st.cache_data
def load_data():
    """
    Load all tables from SQLite database.
    """
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        county = pd.read_sql(
            "SELECT * FROM county_inclusion_data", conn
        )
        mpesa = pd.read_sql(
            "SELECT * FROM mpesa_trends", conn
        )
        demographics = pd.read_sql(
            "SELECT * FROM demographics", conn
        )
        barriers = pd.read_sql(
            "SELECT * FROM barriers", conn
        )
        products = pd.read_sql(
            "SELECT * FROM products", conn
        )

        # Load Forecasts if they exist
        try:
            county_forecast = pd.read_sql(
                "SELECT * FROM county_forecasts", conn
            )
        except:
            county_forecast = None

        conn.close()
        return county, mpesa, demographics, barriers, products, county_forecast
            
    except Exception as e:
        st.error(f"Database error: {e}. Run the pipeline first")
        st.stop()
county_df, mpesa_df, demo_df, barriers_df, products_df, forecast_df = load_data()

st.sidebar.title("Dashboard Filters")
st.sidebar.markdown("---")

selected_year = st.sidebar.selectbox(
    "Select Year",
    options = sorted(county_df["year"].unique(), reverse=True)
)

selected_county = st.sidebar.selectbox(
    "Select County",
    options = ["All Counties"] + sorted(
        county_df["county"].unique().tolist()
    )
)
st.sidebar.markdown("---")
st.sidebar.markdown("Data Sources")
st.sidebar.markdown("2024 FinAccess Household Survey")
st.sidebar.markdown("Safaricom Annual Reports")
st.sidebar.markdown("World Bank Global Findex")
st.sidebar.markdown("Central Bank of Kenya")
st.markdown("---")
st.sidebar.caption("Pipeline runs every Monday 8AM via Apache Airflow")

latest = county_df[county_df["year"] == selected_year]
latest_mpesa = mpesa_df[mpesa_df["year"] == selected_year]

avg_inclusion = round(float(latest["inclusion_rate"].mean()), 1)
avg_exclusion = round(float(latest["exclusion_rate"].mean()), 1)
avg_health = round(float(latest["financial_health"].mean()), 1)
subscribers = float(latest_mpesa["subscribers_millions"].iloc[0]) \
    if len(latest_mpesa) > 0 else 0.0

worst_county = str(latest.loc[
    latest["inclusion_rate"].idxmin(), "county"
])

best_county = str(latest.loc[
    latest["inclusion_rate"].idxmax(), "county"
])

st.subheader(f"National Overview - {selected_year}")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        label = "Avg Inclusion Rate",
        value = f"{avg_inclusion}%"
    )
with col2:
    st.metric(
        label = "Avg Exclusion Rate",
        value = f"{avg_exclusion}%"
    )
with col3:
    st.metric(
        label = "M-pesa subscribers",
        value = f"{subscribers}M"
    )   

with col4:
    st.metric(
        label = "Financial Health",
        value = f"{avg_health}%"
    )

with col5:
    st.metric(
        label = "Most Excluded County",
        value = worst_county
    )


st.subheader("Inclusion & M-Pesa")

col1, col2 = st.columns(2)

with col1:
    # Chart 1 - County Inclusion Chart
    county_sorted = latest.sort_values(
        "inclusion_rate", ascending = True
    )
    fig_county = px.bar(
        county_sorted,
        x = "inclusion_rate",
        y = "county",
        orientation="h",
        title=f"Financial Inclusion by County ({selected_year})",
        labels= {
            "inclusion_rate": "Inclusion Rate (%)",
            "county": "County"
        },
        color = "inclusion_rate",
        color_continuous_scale = "RdYlGn"

    )
    fig_county.update_layout(
        plot_bgcolor = "white",
        xaxis_ticksuffix = "%",
        height = 500
    )
    st.plotly_chart(fig_county, use_container_width=True)

# Chart 2 - Mpesa Growth Line Chart
with col2:
    fig_mpesa = px.line(
        mpesa_df.sort_values("year"),
        x = "year",
        y = "subscribers_millions",
        title = "Mpesa Subscriber Growth (2007-2024)",
        labels={
            "subscribers_millions": "Subscribers (Millions)",
            "year": "Year"
        }, 
        markers = True,
        color_discrete_sequence = ["#00A651"]
    )
    fig_mpesa.add_annotation(
        x = 2020, y = mpesa_df[mpesa_df["year"] == 2020][
            "subscribers_millions"
        ].iloc[0],
        text = "COVID-19 \n Acceleration",
        showarrow=True,
        arrowhead=2,
        font = dict(size=10)
    )
    fig_mpesa.update_layout(
        plot_bgcolor = "white",
        yaxis_title = "Subscribers (Millions)"

    )
    st.plotly_chart(fig_mpesa, use_container_width= True)
st.markdown("---")    
        

# Charts 3 and 4
st.subheader("Gender Gap and Barriers")

col1, col2 = st.columns(2)

#Gender Gap Over Time
with col1:
    gender_data = demo_df.groupby("year")[
        ["male_inclusion", "female_inclusion"]
    ].mean().reset_index()

    fig_gender = px.line(
        gender_data,
        x = "year",
        y = ["male_inclusion", "female_inclusion"],
        title= "Gender Gap in Financial Inclusion(2006- 2024)",
        labels = {
            "value": "Inclusion Rate(%)",
            "year": "Year",
            "variable": "Gender"
        },
        markers= True,
        color_discrete_map={
            "male_inclusion": "#2196F3",
            "female inclusion": "#E91E63"
        }
    )
    fig_gender.update_layout(
        plot_bgcolor = "white",
        yaxis_ticksuffix = "%"
    )
    st.plotly_chart(fig_gender, use_container_width=True)

#Chart 4 - Barriers Breakdown
with col2:
    barriers_latest = barriers_df[
        barriers_df["year"] == 2024
    ].sort_values("percentage", ascending=True)

    fig_barriers = px.bar(
        barriers_latest,
        x = "percentage",
        y = "barrier",
        orientation="h",
        title = "Financial Exclusion Barriers(2024)",
        labels= {
            "percentage": "% of Excluded Population",
            "barrier": "Barrier"
        },
        color = "percentage",
        color_continuous_scale = "Reds"
    )
    fig_barriers.update_layout(
        plot_bgcolor = "white",
        xaxis_ticksuffix = "%"
    )
    st.plotly_chart(fig_barriers, use_container_width=True)
st.markdown("---")    

st.subheader("Products, Urban-Rural & Financial Health")

col1, col2, col3 = st.columns(3)

# Chart 5 - Product Uptake
with col1:
    products_latest = products_df[
        products_df["year"] == selected_year
    ].sort_values("uptake_rate", ascending= True)

    fig_products = px.bar(
        products_latest,
        x = "uptake_rate",
        y = "product",
        orientation= "h",
        title=f"Financial Product Uptake ({selected_year})",
        labels= {
            "uptake_rate": "Uptake Rate (%)",
            "product": "Product"
        }, 
        color = "product_type",
        color_discrete_map= {
            "Digital": "#00A651",
            "Traditional": "#2196F3"

        }
    )
    fig_products.update_layout(
        plot_bgcolor = "white",
        xaxis_ticksuffix = "%"
    )
    st.plotly_chart(fig_products, use_container_width=True)

# Chart 2
with col2:
    urban_rural = demo_df[
        demo_df["year"] == selected_year
    ].groupby("age_group")[
        ["urban_inclusion", "rural_inclusion"]
    ].mean().reset_index()

    fig_urban = px.bar(
        urban_rural, 
        x = "age_group",
        y = ["urban_inclusion", "rural_inclusion"],
        title=f"Urban vs Rural by Age Group ({selected_year})",
        labels = {
            "value": "Inclusion Rate (%)",
            "age_group": "Age Group",
            "variable": "Area"
        },
        barmode = "group",
        color_discrete_map={
            "urban_inclusion": "#FF9800",
            "rural_inclusion": "#4CAF50"

        }
    )
    fig_urban.update_layout(
        plot_bgcolor = "white",
        yaxis_ticksuffix = "%"
    )
    st.plotly_chart(fig_urban, use_container_width=True)

# Chart 7 - Financial Health Index
with col3:
    health_data = latest.sort_values(
        "inclusion_index", ascending=False
    ).head(15)

    fig_health = px.bar(
        health_data, 
        x = "county",
        y = "inclusion_index",
        title = f"Financial Inclusion Index - Top 15 ({selected_year})",
        labels={
            "inclusion_index": "Inclusion Index",
            "county": "County"
        },
        color="inclusion_index",
        color_continuous_scale="Greens"

    )
    fig_health.update_layout(
        plot_bgcolor = "white",
        xaxis_tickangle = -45
    )
    st.plotly_chart(fig_health, use_container_width=True)

st.markdown("---")    

st.subheader("2030 Financial Inclusion Forecast")

if forecast_df is not None:
    col1, col2 = st.columns(2)
    with col1:
        #Forecast for selected County
        fc_county = selected_county \
            if selected_county != "All Counties" else "Nairobi"
        county_fc = forecast_df[
            forecast_df["county"] == fc_county
        ].sort_values("year")

        # Combine historical + forecast
        historical = county_df[
            county_df["county"] == fc_county
        ][["year", "inclusion_rate"]].sort_values("year")

        fig_forecast = go.Figure()

        # Historical line
        fig_forecast.add_trace(go.Scatter(
            x = historical["year"],
            y = historical["inclusion_rate"],
            name = "Historical",
            line = dict(color = "#2196F3", width = 2),
            mode = "lines+markers"
        ))

        # Forecast line
        fig_forecast.add_trace(go.Scatter(
            x = county_fc["year"],
            y = county_fc["predicted_inclusion_rate"],
            name = "Forecast",
            line = dict(color = "#E63946", width= 2, dash = "dash"),
            mode = "lines+markers"
        ))

        # Confidence band
        fig_forecast.add_trace(go.Scatter(
            x=list(county_fc["year"]) + list(county_fc["year"])[::-1],
            y=list(county_fc["upper_bound"]) +
                list(county_fc["lower_bound"])[::-1],
            fill="toself",
            fillcolor="rgba(230, 57, 70, 0.1)",
            line=dict(color="rgba(255,255,255,0)"),
            name="Confidence Interval"
        ))

        fig_forecast.update_layout(
            title=f"{fc_county} — Inclusion Forecast to 2030",
            plot_bgcolor="white",
            yaxis_ticksuffix="%",
            yaxis_title="Inclusion Rate (%)",
            xaxis_title="Year"
        )
        st.plotly_chart(fig_forecast, use_container_width=True)

    with col2:
         # 2030 predictions table
            predictions_2030 = forecast_df[
                forecast_df["year"] == 2030
            ].sort_values(
                "predicted_inclusion_rate", ascending=False
            )[["county", "predicted_inclusion_rate",
            "lower_bound", "upper_bound"]].head(20)

            predictions_2030.columns = [
                "County", "Predicted 2030 (%)",
                "Lower Bound", "Upper Bound"
            ]

            st.markdown(f"**📊 2030 Predictions — All Counties**")
            st.dataframe(predictions_2030, use_container_width=True)
            st.caption("Shaded band shows forecast confidence interval")

else: 
    st.info("Run 'python src/forecast.py' to geneate 2030 predictions!")        

st.markdown("---")


st.subheader("AI Financial Advisor")

tab1, tab2, tab3, tab4 = st.tabs([
    "Policy Analyst",
    "SME Advisor",
    "Personal Advisor",
    "Ask Anything"
])

with tab1:
    col1, col2 = st.columns([2, 1])
    with col1:
        if st.button("Generate Policy Insights"):
            with st.spinner("Analysing Kenya Financial Inclusion..."):
                try:
                    context = get_market_context()
                    insights = generate_policy_insights(context)
                    st.success("AI Policy Analysis:")
                    st.write(insights)
                except Exception as e:
                    st.error(f"{e}")

    with col2:
        st.info("""
      For: Development organisations, government policy makers, NGOs.
      Generates: 4 specific findings and policy recommendations based on current inclusion data          
                
""")                
with tab2:
    col1, col2 = st.columns(2)
    with col1:
        business_type = st.text_input(
        "What type of business do you run?",
        placeholder="e.g food vendor, matatu owner, salon, farm"
    )
    with col2:
        sme_county = st.selectbox(
            "Which county are you in?",
            options = sorted(county_df["county"].unique().tolist()),
            key = "sme_county"
        )   

    if st.button("Get Business Advice"):
        if business_type == "":
            st.warning("Please enter business type!")
        else:
            with st.spinner("Finding the best financial products for you..."):
                try:
                    context = get_market_context()
                    advice = advise_sme(business_type, sme_county, context)
                    st.success("Yout Financial Recommendations")
                    st.write(advice)
                except Exception as e:
                    st.error(f"{e}")           

    
with tab3:
    col1, col2, col3 = st.columns(3)
    with col1: 
        personal_county = st.selectbox(
           "Your county:",
           options = sorted(county_df["county"].unique().tolist()),
           key = "personal_county" 
        )   

    with col2:
        income_level = st.selectbox(
            "Income level:",
            options = ["low", "medium", "high"]
        )  

    with col3:
        has_phone = st.checkbox("I have a mobile phone", value = True)
        has_id = st.checkbox("I have a National ID", value = True)

    if st.button("Get Personal Advice"):
        with st.spinner("Finding the right services for you..."):
            try:
                context = get_market_context()
                advice = advise_individual(
                    income_level, personal_county,
                    has_phone, has_id, context
                ) 
                st.success("Your Financial Inclusion Guide: ")
                st.write(advice)
            except Exception as e:
                st.error(f"{e}")

with tab4:
    question =  st.text_input(
        "Ask anything about Kenya's financial inclusion",
        placeholder="e.g Which county has the worst M-pesa adoption? Why is Turkana County Excluded"
    )
    if st.button("Ask AI"):
        if question == "":
            st.warning("Please type a question")
        else:
            with st.spinner("Thinking..."):
                try:
                    context = get_market_context()
                    answer = ask_financial_question(question, context)
                    st.success("Answer")
                    st.write(answer)
                except Exception as e:
                    st.error(f"{e}")

st.markdown("---")                    
st.subheader("Raw Data Explorer")

table_choice = st.selectbox(
    "Select dataset:",
    options=[
        "County Inclusion",
        "M-Pesa Trends",
        "Demographics",
        "Barriers",
        "Financial Products",
        "2030 Forecasts"
    ]
)
if st.checkbox("Show Raw Data"):
    table_map = {
        "County Inclusion": county_df[
            county_df["year"] == selected_year
        ] if selected_county == "All Counties"
        else county_df[county_df["county"] == selected_county],
        "M-Pesa Trends": mpesa_df,
        "Demographics": demo_df[demo_df["year"] == selected_year],
        "Barriers": barriers_df[barriers_df["year"] == 2024],
        "Financial Products": products_df[
            products_df["year"] == selected_year
        ],
        "2030 Forecasts": forecast_df if forecast_df is not None
        else pd.DataFrame()
    }

    display_df = table_map[table_choice]
    st.dataframe(display_df, use_container_width=True)
    st.caption(f"Showing {len(display_df)} records")

st.markdown("---")  
st.caption("""
Pesa Insight - Kenya Mobile Money & Financial Inclusion Tracker
           Built with Streamlit, Plotly, SQLite, Apache Airflow, Ollama, Scikit Learn.
           Data: 2024 FinAccess Survey, Safaricom Annual Reports, World Bank, CBK
""")  


                    




