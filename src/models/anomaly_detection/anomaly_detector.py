import pickle 
import pandas as pd 
import numpy as np
from typing import List, Dict, Any, Tuple 
from loguru import logger 
from datetime import datetime 
from config.data_config import dataconfig 

class AnomalyDetector:
    def __init__(self, model_path: str):
        self.model = None 
        self.load_model(model_path)

    def load_model(self, model_path: str):
        """Load Isolation Forest model from disk."""
        try: 
            with open(model_path, 'rb') as f:
                self.model = pickle.load(f) 
            logger.info("Anomaly detection model loaded successfully.")
        except Exception as e:
            logger.error(f"Error loading model: {e}") 
            raise ValueError("Failed to load model.")
        
    def detect_anomalies(self, processed_data: pd.DataFrame, original_data: List[Dict]) -> pd.DataFrame:
        """Detect anomalies in the processed data and return original records with anomaly labels."""
        try: 
            original_data_df = pd.DataFrame(original_data)
            if self.model is None:
                logger.error("Anomaly detection model is not loaded.")
                return pd.DataFrame()
            if processed_data is None or processed_data.empty:
                logger.warning("Processed data is empty or None.")
                return pd.DataFrame()

            columns = dataconfig.COLUMNS_MODEL_USE
            missing_cols = [col for col in columns if col not in processed_data.columns]
            if missing_cols:
                logger.error(f"Missing columns for anomaly detection: {missing_cols}")
                return pd.DataFrame()
            processed_data = processed_data[columns]
            predictions = self.model.predict(processed_data)
            original_data_df['is_anomaly'] = predictions
            return original_data_df

            anomaly_scores = self.model.decision_function(processed_data) # < dataconfig.ANOMALY_THRESHOLD 

            # results = []
            # for i, (pred, score, original_doc) in enumerate(zip(predictions, anomaly_scores, original_data)):
            #     result = {
            #         "original_data": original_doc, 
            #         "is_anomaly": pred == -1, 
            #         "anomaly_score": float(score),
            #         "confidence": abs(float(score)),
            #         "detection_time": datetime.now().isoformat() + "Z",
            #         "model_version": "1.0.0"
            #     }
            #     results.append(result)

            anomaly_count = sum(1 for r in results if r["is_anomaly"])
            total_count = len(results)
            if total_count > 0:
                anomaly_percentage = (anomaly_count / total_count) * 100 
                logger.info(f"Processed {total_count} records, "
                            f"detected {anomaly_count} anomalies ({anomaly_percentage:.2f}%).")
            return results 
        
        except Exception as e:
            logger.error(f"Error during anomaly detection: {e}")
            return pd.DataFrame()
        
    
    def get_anomalies_only(self, results: List[Dict]) -> List[Dict]:
        """Filter and return only the detected anomalies from results."""
        return [res for res in results if res["is_anomaly"]]

