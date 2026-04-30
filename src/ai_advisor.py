import ollama
import sqlite3
import pandas as pd

DATABASE_PATH = "database/finance.db"

# Loading live data from the database
#Every time the AI is called, it gets the freshest numbers
def get_market_context() -> dict:
    """
    Pull latest stats from database to build AI context.
    """
    conn = sqlite3.connect(DATABASE_PATH)

    county_df = pd.read_sql(
        "SELECT * FROM county_inclusion_data WHERE year = 2024",
        conn
    )
    mpesa_df = pd.read_sql(
        "SELECT * FROM mpesa_trends WHERE year = 2024",
        conn
    )
    barriers_df = pd.read_sql(
        "SELECT * FROM barriers WHERE year = 2024",
        conn
    )
    conn.close()

    worst_county = county_df.loc[
        county_df["inclusion_rate"].idxmin(), "county"
    ]
    best_county = county_df.loc[
        county_df["inclusion_rate"].idxmax(), "county"
    ]

    top_barrier = barriers_df.loc[
        barriers_df["percentage"].idxmax(), "barrier"
    ]

    return {
        "national_inclusion": float(county_df["inclusion_rate"].mean().round(1)),
        "national_exclusion": float(county_df["exclusion_rate"].mean().round(1)),
        "worst_county": str(worst_county),
        "worst_rate": float(county_df["inclusion_rate"].min().round(1)),
        "best_county": str(best_county),
        "best_rate": float(county_df["inclusion_rate"].max().round(1)),
        "mpesa_subscribers": float(mpesa_df["subscribers_millions"].iloc[0]),
        "avg_financial_health": float(county_df["financial_health"].mean().round(1)),
        "top_barrier": str(top_barrier),
        "avg_gender_gap": float(
            (county_df["inclusion_rate"].mean() * 0.016).round(1)
        )
    }


def build_context_brief(context: dict) -> str:
    """Build a structured brief from context dictionary."""
    return f"""
    Kenya Financial Inclusion Intelligence Brief (2024):

    National Overview:
    - Average inclusion rate: {context['national_inclusion']}%
    - Average exclusion rate: {context['national_exclusion']}%
    - M-Pesa subscribers: {context['mpesa_subscribers']} million
    - Average financial health score: {context['avg_financial_health']}%

    County Analysis:
    - Best performing county: {context['best_county']} at {context['best_rate']}%
    - Worst performing county: {context['worst_county']} at {context['worst_rate']}%

    Key Barrier:
    - Biggest exclusion barrier: {context['top_barrier']}
    - Average gender gap: {context['avg_gender_gap']}%
    """
def generate_policy_insights(context: dict = None) -> str:
    """
    Mode 1 — Policy Analyst
    Generates insights for development organisations
    and government policy makers.
    """
    if context is None:
        context = get_market_context()

    brief = build_context_brief(context)

    prompt = f"""You are a senior financial inclusion policy analyst
    at a leading development organisation working in Kenya.

    Based on the data brief below, provide exactly 4 specific
    policy insights and recommendations for closing Kenya's
    financial inclusion gap.

    Focus on:
    - Which counties need most urgent intervention
    - What specific policy actions will have most impact
    - How to address the gender and urban-rural gaps
    - Which barriers are most addressable with current resources

    {brief}

    Provide exactly 4 insights in this format:
    1. [Finding]: specific observation
    [Action]: concrete policy recommendation

    2. [Finding]: specific observation
    [Action]: concrete policy recommendation

    3. [Finding]: specific observation
    [Action]: concrete policy recommendation

    4. [Finding]: specific observation
    [Action]: concrete policy recommendation
    """

    try:
        response = ollama.chat(
            model="tinyllama",
            messages=[{"role": "user", "content": prompt}]
        )
        return response["message"]["content"]
    except Exception as e:
        return f" AI unavailable. Make sure Ollama is running. {str(e)}"
    
def advise_sme(business_type: str, county: str, context: dict = None) -> str:
    """
    Mode 2 — SME Financial Advisor
    Recommends financial products for small businesses
    based on their type and location.
    """
    if context is None:
        context = get_market_context()

    brief = build_context_brief(context)

    prompt = f"""You are a friendly and knowledgeable financial advisor
    specialising in helping small businesses in Kenya access
    the right financial products and services.

    A small business owner has come to you for advice:
    - Business type: {business_type}
    - Location: {county}, Kenya

    Kenya Financial Context:
    {brief}

    Please provide:
    1. Top 3 recommended financial products for this business
    (from: M-Pesa, Lipa Na M-Pesa, M-Shwari, KCB M-Pesa,
    Fuliza, Hustler Fund, SACCO, Bank Account, Insurance)
    2. Why each product suits this specific business
    3. How to get started with each product
    4. One warning about financial risks to avoid

    Keep advice practical, specific and actionable.
    Mention actual product names, loan limits and steps.
    """

    try:
        response = ollama.chat(
            model="tinyllama",
            messages=[{"role": "user", "content": prompt}]
        )
        return response["message"]["content"]
    except Exception as e:
        return f" AI unavailable. Make sure Ollama is running.\nError: {str(e)}"


def advise_individual(income_level: str, county: str, has_phone: bool, has_id: bool, context: dict = None) -> str:

    """
    Mode 3 - Personal Financial Inclusion Advisor
    Helps individuals understand which financial 
    services they can access and how to get started.

    income_level: 'low' / 'medium' / 'high'
    """

    if context is None:
        context = get_market_context()

    #Build personalised context
    access_status = []
    if not has_phone:
        access_status.append("does not have a mobile phone")
    if not has_id:
        access_status.append("does not have a national ID")
    if not access_status:
        access_status.append("has both phone and ID")

    access_str = " and ".join(access_status)

    prompt = f"""You are a compassionate financial inclusion advisor
        helping ordinary Kenyans access financial services for the first time.

        The person you are helping:
        - Lives in: {county}, Kenya
        - Income level: {income_level}
        - Access status: {access_str}

        Kenya's national financial inclusion rate is {context['national_inclusion']}%
        but {county} may have different access levels.

        Please provide:
        1. Which financial services this person can access RIGHT NOW
        2. Step by step guide to getting started with the most important one
        3. If they lack phone or ID — exactly how to get those first
        4. One realistic savings goal they can achieve in 3 months

        Use simple, encouraging language. Avoid jargon.
        Be specific about amounts, steps and locatio

        """   
    try:
        response = ollama.chat(
            model = "tinyllama",
            messages=[{"role": "user", "content": prompt}]
        )
        return response["message"]["content"]
    except Exception as e:
        return f"AI unaivailable. Make sure Ollama is running {str(e)}"

def ask_financial_question(question: str, context: dict = None) -> str:
    """
    General Q&A about Kenya Financial inclusion.
    Used in dashboard Q&A section.
    """       
    if context is None:
        context = get_market_context()

    brief = build_context_brief(context)

    prompt = f"""You are a Kenya financial inclusion expert with deep
    knowledge of M-Pesa, mobile banking, SACCOs and financial
    access patterns across all 47 counties.

    Use the data brief below to answer the question accurately.
    Be specific — mention county names, percentages and products.
    If data is not in the brief say:
    "This requires additional data beyond the current brief."

    {brief}

    Question: {question}

    Answer:"""

    try:
        response = ollama.chat(
            model="tinyllama",
            messages=[{"role": "user", "content": prompt}]
        )
        return response["message"]["content"]

    except Exception as e:
        return f"Could not get an answer. Make sure Ollama is running: {str(e)}"
if __name__ == "__main__":
    print("🧪 Testing PesaInsight AI Advisor...")
    print("=" * 50)

    context = get_market_context()
    print("Context loaded!")
    print(f"   National inclusion: {context['national_inclusion']}%")
    print(f"   Worst county: {context['worst_county']}")
    print(f"   Top barrier: {context['top_barrier']}")

    print("\n Testing Policy Mode...")
    print("=" * 50)
    insights = generate_policy_insights(context)
    print(insights)

    print("\n Testing SME Mode...")
    print("=" * 50)
    sme_advice = advise_sme("food vendor", "Kisumu", context)
    print(sme_advice)

    print("\n Testing Personal Mode...")
    print("=" * 50)
    personal = advise_individual(
        income_level="low",
        county="Turkana",
        has_phone=False,
        has_id=True,
        context=context
    )
    print(personal)
    
    