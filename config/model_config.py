from typing import List, Optional
from pydantic_settings import BaseSettings 

class ModelConfig(BaseSettings):
    # Model paths 
    model_path: str = "data/models/isolation_forest_model.pkl"
    scaler_path: str = "data/models/scaler.pkl"
    label_encoder_path: str = "data/models/label_encoder.pkl"