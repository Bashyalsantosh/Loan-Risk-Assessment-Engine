import streamlit as st
import pandas as pd
import numpy as np
import builtins

py_round = builtins.round

# --------------------------------------------------
# Page Configuration
# --------------------------------------------------

st.set_page_config(
    page_title="NRB Loan Risk Engine",
    page_icon="🏦",
    layout="wide"
)

# --------------------------------------------------
# Header
# --------------------------------------------------

st.title("🏦 NRB Loan Risk Assessment Engine")

st.caption(
    "Real-time credit-risk decision support powered by "
    "data engineering and machine-learning concepts."
)

st.divider()

# --------------------------------------------------
# Sidebar
# --------------------------------------------------

st.sidebar.header("🧮 Loan Application Simulator")

applicant_income = st.sidebar.number_input(
    "Applicant Monthly Income (NPR)",
    min_value=10000,
    value=75000,
    step=5000
)

coapplicant_income = st.sidebar.number_input(
    "Co-Applicant Monthly Income (NPR)",
    min_value=0,
    value=25000,
    step=5000
)

monthly_debt = st.sidebar.number_input(
    "Existing Monthly Debt (NPR)",
    min_value=0,
    value=20000,
    step=5000
)

loan_amount = st.sidebar.number_input(
    "Loan Amount (NPR)",
    min_value=50000,
    value=1200000,
    step=50000
)

credit_score = st.sidebar.slider(
    "CIB Credit Score",
    min_value=300,
    max_value=850,
    value=680
)

collateral_value = st.sidebar.number_input(
    "Collateral Valuation (NPR)",
    min_value=100000,
    value=2500000,
    step=100000
)

loan_term = st.sidebar.slider(
    "Loan Term (Years)",
    min_value=1,
    max_value=30,
    value=10
)

# --------------------------------------------------
# Feature Engineering
# --------------------------------------------------

total_income = applicant_income + coapplicant_income

if total_income > 0:
    dti_ratio = monthly_debt / total_income
else:
    dti_ratio = 0.0

if collateral_value > 0:
    ltv_ratio = loan_amount / collateral_value
else:
    ltv_ratio = 0.0

# --------------------------------------------------
# Risk Engine
# --------------------------------------------------

risk_points = 0

if dti_ratio > 0.45:
    risk_points += 35

if ltv_ratio > 0.80:
    risk_points += 30

if credit_score < 600:
    risk_points += 35
elif credit_score < 700:
    risk_points += 15

if loan_term > 20:
    risk_points += 10

risk_probability = min(risk_points / 100, 0.99)

# --------------------------------------------------
# Risk Category
# --------------------------------------------------

if risk_probability >= 0.70:
    risk_category = "HIGH RISK"
    recommendation = "Manual Credit Review"
elif risk_probability >= 0.40:
    risk_category = "MEDIUM RISK"
    recommendation = "Additional Verification"
else:
    risk_category = "LOW RISK"
    recommendation = "Standard Review"

# --------------------------------------------------
# Main Metrics
# --------------------------------------------------

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "DTI Ratio",
    f"{dti_ratio:.2f}"
)

col2.metric(
    "LTV Ratio",
    f"{ltv_ratio:.2f}"
)

col3.metric(
    "Credit Score",
    f"{credit_score}"
)

col4.metric(
    "Risk Probability",
    f"{risk_probability:.1%}"
)

st.divider()

# --------------------------------------------------
# Risk Assessment
# --------------------------------------------------

st.subheader("🎯 Risk Assessment")

col1, col2 = st.columns([1, 2])

with col1:

    st.metric(
        "Risk Score",
        f"{risk_probability:.1%}"
    )

with col2:

    if risk_category == "HIGH RISK":
        st.error(
            f"🔴 {risk_category}"
        )

    elif risk_category == "MEDIUM RISK":
        st.warning(
            f"🟡 {risk_category}"
        )

    else:
        st.success(
            f"🟢 {risk_category}"
        )

    st.info(
        f"Recommended Action: **{recommendation}**"
    )

# --------------------------------------------------
# Feature Summary
# --------------------------------------------------

st.subheader("📊 Application Features")

feature_data = pd.DataFrame({
    "Feature": [
        "Total Monthly Income",
        "Monthly Debt",
        "Loan Amount",
        "Collateral Value",
        "DTI Ratio",
        "LTV Ratio",
        "Credit Score",
        "Loan Term"
    ],
    "Value": [
        f"NPR {total_income:,.0f}",
        f"NPR {monthly_debt:,.0f}",
        f"NPR {loan_amount:,.0f}",
        f"NPR {collateral_value:,.0f}",
        f"{dti_ratio:.2f}",
        f"{ltv_ratio:.2f}",
        credit_score,
        f"{loan_term} years"
    ]
})

st.dataframe(
    feature_data,
    use_container_width=True,
    hide_index=True
)

# --------------------------------------------------
# Disclaimer
# --------------------------------------------------

st.divider()

st.caption(
    "Portfolio project for educational and demonstration purposes. "
    "It is not an official Nepal Rastra Bank system and should not "
    "be used as a real lending decision engine."
)
