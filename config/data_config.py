from typing import List, Optional 
from pydantic_settings import BaseSettings

class DataConfig(BaseSettings):
    # Elasticsearch settings
    HOST: str = "localhost"
    PORT: int = 3432
    USER: str = "aliru"
    PASSWORD: str = "aliru"

    # Index settings
    INDEX_NAME: str = "log-app-jamarequest*"
    OUTPUT_INDEX_NAME: str = "log-app-jamarequest-processed"

    

