from fastapi import FastAPI, HTTPException, status, Depends
from pydantic import BaseModel, Field
import numpy as np
import joblib
import os
from src.config import settings

app = FastAPI(
    title=settings.app_name,
    version="2.0.0",
    description="Enterprise Real-Time Credit Risk Scoring REST API"
)

# Load Model Artifact
MODEL_PATH = "models/xgboost_risk_model.pkl"
model = joblib.load(MODEL_PATH) if os.path.exists(MODEL_PATH) else None

class RiskInferenceRequest(BaseModel):
    application_id: str = Field(..., example="APP-2026-8891")
    dti: float = Field(..., ge=0.0, le=1.0, example=0.38)
    ltv: float = Field(..., ge=0.0, le=2.0, example=0.65)
    p_instances: int = Field(..., ge=0, example=1)

class RiskInferenceResponse(BaseModel):
    application_id: str
    default_probability: float
    rc_score: float
    risk_category: str
    status: str = "SUCCESS"

@app.post("/api/v2/predict-risk", response_model=RiskInferenceResponse)
async def predict_risk(request: RiskInferenceRequest):
    try:
        # Algorithmic Metric Calculation
        rc_score = (settings.weight_dti * request.dti) + \
                   (settings.weight_ltv * request.ltv) + \
                   (settings.weight_p_instances * (min(request.p_instances, 5) / 5.0))
        
        # ML Inference (if artifact available)
        if model:
            features = np.array([[request.dti, request.ltv, request.p_instances]])
            default_prob = float(model.predict_proba(features)[0][1])
        else:
            default_prob = round(rc_score, 4)
            
        # Decision Boundaries
        if default_prob >= settings.threshold_critical:
            category = "CRITICAL DEFAULT RISK"
        elif default_prob >= settings.threshold_watchlist:
            category = "WATCHLIST ELEVATED"
        else:
            category = "PASSABLE LOW RISK"
            
        return RiskInferenceResponse(
            application_id=request.application_id,
            default_probability=round(default_prob, 4),
            rc_score=round(rc_score, 4),
            risk_category=category
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Inference execution failed: {str(e)}"
        )
