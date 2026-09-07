 # 🏦 Real-Time Loan Risk Analytics & Decision-Support Platform

A production-oriented **Loan Risk Analytics and Default Prediction Platform** designed to demonstrate how financial institutions can combine **real-time streaming, lakehouse architecture, feature engineering, machine learning, and executive analytics** to support credit-risk assessment.

The project processes loan application events through an event-driven data pipeline and transforms raw application data into ML-ready risk features and actionable dashboard insights.

> **Disclaimer:** This is an independent educational/portfolio project. It is not an official Nepal Rastra Bank system, regulatory model, or banking decision engine.

---

## 🚀 Project Overview

Financial institutions need reliable mechanisms to identify potentially high-risk loan applications and monitor credit-risk indicators.

This project demonstrates an end-to-end architecture for:

* Real-time loan application ingestion
* Distributed stream processing
* Lakehouse-style Bronze/Silver/Gold data layers
* Data cleansing and feature engineering
* Loan risk prediction using machine learning
* Risk probability and risk-category generation
* Executive-level visualization through Streamlit

The system is designed as a **decision-support platform**, rather than an automated loan approval/rejection system.

---

## 🏗️ System Architecture

```text
                    ┌─────────────────────┐
                    │ Loan Application    │
                    │ Event Simulator     │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Apache Kafka      │
                    │   nrb_loans Topic   │
                    └──────────┬──────────┘
                               │
                               ▼
              ┌────────────────────────────────┐
              │ PySpark Structured Streaming   │
              └───────────────┬────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  🥉 BRONZE      │
                    │ Raw Loan Events │
                    │ Delta / Parquet │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  🥈 SILVER      │
                    │ Cleaned Data    │
                    │ Feature Engine  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   🥇 GOLD       │
                    │ ML-Ready Data   │
                    │ Risk Analytics  │
                    └────────┬────────┘
                             │
                    ┌────────┴─────────┐
                    ▼                  ▼
             ┌─────────────┐    ┌──────────────┐
             │ ML Risk     │    │ PostgreSQL   │
             │ Prediction  │    │ Analytics DB │
             └──────┬──────┘    └──────┬───────┘
                    │                  │
                    └────────┬─────────┘
                             ▼
                    ┌─────────────────┐
                    │   Streamlit     │
                    │ Executive       │
                    │ Risk Dashboard  │
                    └─────────────────┘
```

---

## ⚡ Key Features

### 1. Real-Time Streaming Ingestion

Loan application events are simulated and published to an Apache Kafka topic.

```text
kafka_producer.py
        ↓
Kafka Broker
        ↓
nrb_loans
```

The pipeline is designed to process loan events continuously using **PySpark Structured Streaming**.

---

### 2. 🥉 Bronze Layer — Raw Data Ingestion

The Bronze layer stores the incoming loan events with minimal transformation.

Responsibilities include:

* Raw event ingestion
* Schema enforcement
* Ingestion timestamps
* Persistent storage
* Streaming checkpoints

Example fields:

```text
event_id
loan_id
bfi_code
applicant_income
coapplicant_income
loan_amount
credit_score
collateral_value
timestamp
ingestion_timestamp
```

---

### 3. 🥈 Silver Layer — Data Quality & Feature Engineering

The Silver layer transforms raw events into clean analytical data.

Processing includes:

* Null-value validation
* Data-type validation
* Duplicate detection
* Invalid-record filtering
* Income calculations
* Loan-to-Income analysis
* Loan-to-Value calculation
* Credit-score validation
* Feature generation

Example derived features:

```text
total_income
loan_to_income
ltv_ratio
credit_score
```

> **DTI Note:** Standard Debt-to-Income (DTI) requires debt obligations and income measured on the same time basis. Therefore, the project should only label a feature as `dti_ratio` when the underlying dataset contains appropriate debt-obligation information.

---

### 4. 🥇 Gold Layer — Machine Learning

The Gold layer produces ML-ready features for loan-risk prediction.

Potential models include:

* Random Forest
* PySpark Gradient Boosted Trees (GBT)
* XGBoost

The final repository should identify **one primary production model** and clearly document any other model as a baseline/comparison model.

The ML pipeline can generate:

```text
risk_probability
risk_category
prediction
model_version
prediction_timestamp
```

Example risk categories:

```text
LOW
MEDIUM
HIGH
```

---

## 📊 Model Evaluation

The initial prototype reports approximately:

| Metric    | Reported Score |
| --------- | -------------: |
| Accuracy  |            93% |
| Precision |            90% |
| Recall    |            77% |

These figures should be treated as **dataset/model-specific results**, not guaranteed production performance.

For a robust credit-risk evaluation, the project should report:

* Accuracy
* Precision
* Recall
* F1-score
* ROC-AUC
* PR-AUC
* Confusion Matrix
* Class distribution
* Threshold analysis

### Why accuracy alone is insufficient

Loan-default datasets can contain imbalanced classes. A model can achieve high accuracy while performing poorly on the minority default class.

Therefore, **recall, precision, F1-score, ROC-AUC and PR-AUC** are important for evaluating the model's ability to identify potential defaults.

---

## 🧠 Business Use Case

The platform can support financial-risk analysts by providing:

### Loan-level analysis

```text
Applicant Income
Co-Applicant Income
Loan Amount
Credit Score
Collateral Value
Loan-to-Value Ratio
Risk Probability
Risk Category
```

### Portfolio-level analytics

```text
Total Applications
High-Risk Applications
Average Risk Probability
Default Risk Distribution
Credit Score Distribution
Loan Amount Distribution
```

---

## 📊 Executive Dashboard

The Streamlit dashboard provides an interactive interface for exploring loan-risk information.

Planned dashboard components include:

* 📈 Portfolio risk overview
* 🚨 High-risk application count
* 💳 Loan amount analysis
* 📊 Credit-score distribution
* 🏦 Risk-category breakdown
* 🎯 Loan risk probability
* 🔍 Individual loan evaluation
* ⚙️ Pipeline/model status

The dashboard acts as the **presentation and decision-support layer**, while data processing and ML workloads remain in the backend pipeline.

---

## 🛠️ Technology Stack

| Category               | Technology                    |
| ---------------------- | ----------------------------- |
| Programming            | Python                        |
| Distributed Processing | Apache Spark / PySpark        |
| Streaming              | Apache Kafka                  |
| Storage                | Delta Lake / Parquet          |
| Machine Learning       | Scikit-Learn / PySpark ML     |
| ML Algorithms          | Random Forest / GBT / XGBoost |
| Data Processing        | Pandas / NumPy                |
| Visualization          | Plotly                        |
| Dashboard              | Streamlit                     |
| Database               | PostgreSQL                    |
| Containerization       | Docker                        |
| Testing                | Pytest                        |
| CI/CD                  | GitHub Actions                |
| Cloud                  | AWS / Streamlit Cloud         |

---

## 📂 Repository Structure

```text
nrb-loan-risk-engine/
│
├── app/
│   └── app.py
│
├── src/
│   ├── kafka_producer.py
│   ├── medallion_pipeline.py
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   └── ml/
│       ├── train.py
│       ├── predict.py
│       └── evaluate.py
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── models/
│
├── tests/
│   ├── test_features.py
│   ├── test_pipeline.py
│   └── test_model.py
│
├── notebooks/
│   └── model_exploration.ipynb
│
├── configs/
│   └── config.yaml
│
├── screenshots/
│
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── .gitignore
├── README.md
└── LICENSE
```

---

## 🔄 End-to-End Data Flow

```text
1. Generate loan application event
             ↓
2. Publish JSON event to Kafka
             ↓
3. PySpark consumes Kafka stream
             ↓
4. Validate and store Bronze data
             ↓
5. Clean and transform Silver data
             ↓
6. Generate ML-ready Gold features
             ↓
7. Apply trained ML model
             ↓
8. Generate risk probability
             ↓
9. Store analytical results
             ↓
10. Visualize through Streamlit
```

---

## ▶️ Running the Project

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Kafka

Ensure your Kafka broker is available at:

```text
localhost:9092
```

Create/use the topic:

```text
nrb_loans
```

---

### 3. Start the Kafka producer

```bash
python kafka_producer.py
```

The producer publishes simulated loan application events.

---

### 4. Start the PySpark pipeline

```bash
spark-submit medallion_pipeline.py
```

The pipeline processes the incoming data through the Medallion architecture.

---

### 5. Start the Streamlit dashboard

```bash
streamlit run app.py
```

The dashboard can then be accessed through the local Streamlit interface.

---

## 🧪 Testing

The production-oriented version of the project should include automated tests for:

```text
✓ Schema validation
✓ Feature calculations
✓ DTI/LTV calculations
✓ Invalid input handling
✓ Duplicate handling
✓ ML prediction
✓ Model loading
✓ Pipeline transformations
```

Run tests with:

```bash
pytest
```

---

## 🐳 Docker

The complete environment can be containerized using Docker.

Example services:

```text
Kafka
Spark
PostgreSQL
Loan Producer
Streaming Pipeline
Streamlit
```

This allows the complete data platform to be reproduced consistently across development environments.

---

## 🔐 Production Considerations

A production deployment should additionally implement:

* Environment-based configuration
* Secret management
* Kafka authentication
* Database credential protection
* Data-quality monitoring
* Structured logging
* Streaming checkpoints
* Dead-letter/error records
* Schema evolution
* Model versioning
* Data drift monitoring
* Model drift monitoring
* CI/CD automation
* Infrastructure as Code
* Cloud deployment

---

## 📈 Future Enhancements

Planned improvements include:

* [ ] Kafka Schema Registry / Avro
* [ ] Delta Lake Bronze/Silver/Gold implementation
* [ ] PostgreSQL analytics sink
* [ ] MLflow model tracking
* [ ] Model versioning
* [ ] Data-quality framework
* [ ] Automated testing
* [ ] GitHub Actions CI/CD
* [ ] Docker Compose environment
* [ ] AWS deployment
* [ ] Terraform Infrastructure as Code
* [ ] Data drift monitoring
* [ ] Model drift monitoring
* [ ] Real-time alerting
* [ ] Role-based dashboard access

---

## 🎯 Project Objectives

This project demonstrates practical experience with:

```text
Python
   ↓
Apache Kafka
   ↓
PySpark Structured Streaming
   ↓
Lakehouse / Medallion Architecture
   ↓
Feature Engineering
   ↓
Machine Learning
   ↓
PostgreSQL
   ↓
Streamlit
   ↓
Docker / CI/CD / Cloud
```

The primary objective is to demonstrate **end-to-end Data Engineering and Machine Learning system design**, rather than simply building a standalone classification model.

---

## 🤝 Contributing

Contributions and suggestions are welcome.

To contribute:

```bash
git clone <repository-url>
cd nrb-loan-risk-engine
```

Create a feature branch, implement your changes, add appropriate tests, and submit a pull request.

---

## 👨‍💻 Author

**Prem Bashyal**

Data Engineering | Machine Learning | Cloud | Real-Time Data Systems

---

## ⚠️ Disclaimer

This project is an independent portfolio/educational implementation inspired by financial-services data engineering and credit-risk use cases.

It is **not affiliated with Nepal Rastra Bank**, does not represent an official regulatory methodology, and should not be used for real-world lending decisions without appropriate financial, legal, regulatory, model-risk, fairness, security, and governance validation.

```
```
