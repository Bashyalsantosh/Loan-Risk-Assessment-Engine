import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class ProductionSettings(BaseSettings):
    app_name: str = "Real-Time Loan Risk Analytics Engine"
    environment: str = Field(default="production", validation_alias="ENVIRONMENT")
    
    # Kafka Configuration
    kafka_bootstrap_servers: str = Field(default="localhost:9092", validation_alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_loan_applications: str = "loan.applications.raw"
    
    # Database Configuration
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:secure_password@localhost:5432/loan_risk_db",
        validation_alias="DATABASE_URL"
    )
    
    # ML & Risk Engine Configuration
    weight_dti: float = Field(default=0.40)
    weight_ltv: float = Field(default=0.35)
    weight_p_instances: float = Field(default=0.25)
    
    threshold_critical: float = Field(default=0.75)
    threshold_watchlist: float = Field(default=0.45)
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = ProductionSettings()
