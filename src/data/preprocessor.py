import pandas as pd 
import pickle 
import numpy as np 
from typing import List, Dict, Any 
from loguru import logger 
from datetime import datetime 
from sklearn.preprocessing import LabelEncoder 
from config.data_config import dataconfig

class DataPreprocessor:
    def __init__(self, scaler_path: str, label_encoder_path: str):
        self.scaler = None 
        self.label_encoders = {}
        self.load_models(scaler_path, label_encoder_path) 

    def _drop_unnecessary_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop columns that are not needed for analysis."""
        df.drop(columns=dataconfig.COLUMNS_TO_DROP, errors='ignore', inplace=True)
        return df 
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in the DataFrame."""
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df 
    
    def _extract_timestamp_features(self, df: pd.DataFrame) -> pd.DataFrame: 
        """Extract timestamp features"""
        try:
            timestamp_cols = dataconfig.COLUMNS_TIMESTAMP_FEATURES 

            for col in timestamp_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    
                    if col == '@timestamp':
                        df['year'] = df[col].dt.year
                        df['month'] = df[col].dt.month 
                        df['day'] = df[col].dt.day 
                        df['hour'] = df[col].dt.hour 
                        df['minute'] = df[col].dt.minute 
                        df['weekday'] = df[col].dt.weekday 
                        df['second'] = df[col].dt.second 

                    else: 

                        prefix = col.replace('@', '').replace('.', ' ')
                        df[f'{prefix}_year'] = df[col].dt.year 
                        df[f'{prefix}_month'] = df[col].dt.month 
                        df[f'{prefix}_day'] = df[col].dt.day 
                        df[f'{prefix}_hour'] = df[col].dt.hour 
                        df[f'{prefix}_minute'] = df[col].dt.minute 
                        df[f'{prefix}_weekday'] = df[col].dt.weekday 
                        df[f'{prefix}_second'] = df[col].dt.second 
                    
            df.drop(columns=timestamp_cols)
            return df 
        
        except Exception as e:
            logger.error(f'Error extracting timestamp features: {e}')
            return pd.DataFrame()
        
    def _encode_categorical_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Encode categorical features using pre-trained label encoder."""
        try: 
            categorical_cols = dataconfig.COLUMNS_CATEGORICAL_FEATURES
            for col in categorical_cols:
                if col in df.columns: 
                    if col in self.label_encoders:
                        le = self.label_encoders[col] 
                        try:
                            df[col] = le.transform(df[col].astype(str))
                        except Exception as e:
                            logger.warning(f"Error encoding column {col}: {e}")
                            df[col] = df[col].astype(str) 
                    else:
                        le = LabelEncoder()
                        df[col] = le.fit_transform(df[col].astype(str))
                        self.label_encoders[col] = le 
            return df 
        except Exception as e:
            logger.error(f"Error encoding categorical features: {e}")
            return pd.DataFrame() 
        
    def _scale_numerical_features(self, df: pd.DataFrame) -> pd.DataFrame: 
        """Scale numerical features using pre-trained scaler."""
        try: 
            num_cols = dataconfig.COLUMNS_NUMERICAL_FEATURES 
            availabel_cols = [col for col in num_cols if col in df.columns] 
            if availabel_cols and self.scaler is not None:
                df_scaled = df.copy()
                try:
                    df_scaled[availabel_cols] = self.scaler.transform(df[availabel_cols])
                except Exception as e:
                    logger.warning(f"Error scaling numerical features: {e}")
                    return pd.DataFrame()
            return df_scaled 
        except Exception as e: 
            logger.error(f"Error scaling numerical features: {e}")
            return pd.DataFrame() 



    def load_models(self, scaler_path: str, label_encoder_path: str):
        """Load pre-trained scaler and label encoders from dist."""
        try:
            with open(scaler_path, 'rb') as f:
                self.scaler = pickle.load(f) 

            try:
                with open(label_encoder_path, 'rb') as f:
                    self.label_encoders = pickle.load(f) 
            except FileNotFoundError:
                logger.warning(f"Label encoder file not found at {label_encoder_path}.")
            logger.info("Models loaded successfully.")
        except Exception as e:
            logger.error(f"Error loading models: {e}")
            return 
    
    def preprocess(self, data: List[Dict]) -> pd.DataFrame:
        """Preprocess raw data into a DataFrame suitable for analysis."""
        try:
            df = pd.DataFrame(data)
            df = self._drop_unnecessary_columns(df)
            df = self._handle_missing_values(df) 
            df = self._extract_timestamp_features(df) 
            df = self._encode_categorical_features(df) 
            df = self._scale_numerical_features(df) 
            logger.info(f"Columns are preprocessing {df.columns.tolist()}") 
            cols = dataconfig.COLUMNS_TO_USE
            missing_cols = [col for col in cols if col not in df.columns]
            if missing_cols:
                logger.warning(f"Missing columns in input data: {missing_cols}")
                return pd.DataFrame()
            # Only selected columns that exist 
            df = df[cols]

            return df 
        
        except Exception as e:
            logger.error(f"Error in preprocessing: {e}")
            return pd.DataFrame()
        



