import asyncio 
import uvicorn 
from fastapi import FastAPI, HTTPException, BackgroundTasks 
from fastapi.responses import JSONResponse 
from contextlib import asynccontextmanager 
from loguru import logger 
import sys 
from datetime import datetime 
from typing import List, Dict, Optional 
import tempfile 
import pandas as pd 
from config.data_config import dataconfig 
from config.model_config import ModelConfig 
from src.data.collector import ElasticsearchService 
from src.data.preprocessor import DataPreprocessor 
from src.models.anomaly_detection.anomaly_detector import AnomalyDetector
from dotenv import load_dotenv 
import os

# Load environment variables from .env file 
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
print(USERNAME, PASSWORD)

if USERNAME is None or PASSWORD is None:
    raise ValueError("Elasticsearch USERNAME and PASSWORD environment variables must be set.")

es_service = None 
preprocessor = None 
anomaly_detector = None 
last_processed_timestamp = datetime(2025, 9, 21)
is_running = False 

model_config = ModelConfig()


@asynccontextmanager 
async def lifespan(app: FastAPI):
    """Manage service lifecycle"""
    global es_service, preprocessor, anomaly_detector 

    # Startup 
    logger.info("Starting up services...")

    # Initialize Elasticsearch service 
    es_service = ElasticsearchService(
        username = USERNAME,
        password = PASSWORD 
    )

    preprocessor = DataPreprocessor(
        scaler_path = model_config.scaler_path,
        label_encoder_path = model_config.label_encoder_path
    )

    anomaly_detector = AnomalyDetector(
        model_path = model_config.model_path 
    )

    logger.info("Service started Successfully.")
    yield 

    logger.info("Shutting down services...")
    if es_service:
        await es_service.close()
    logger.info("Services shut down successfully.")


# FastAPI app
app = FastAPI(
    title="Anomaly Detection Service",
    description="A service to detect anomalies",
    version="1.0.0",
    lifespan=lifespan
)
@app.get("/")
def home():
    return {"message": "Anomaly Detection Service is running."}

async def process_data():
    global last_processed_timestamp 

    try:
        # Ensure services are initialized 
        global es_service, preprocessor, anomaly_detector 
        logger.info("Processing new data batch...")
        if es_service is None or preprocessor is None or anomaly_detector is None:
            logger.error("Services are not initialized.")
            raise RuntimeError("Services are not initialized.")
        
        # Fetch new data from Elasticsearch 
        found_data = False 
        logger.info("Fetching new data from Elasticsearch...")
        async for new_data in es_service.fetch_data(
            index=dataconfig.INDEX_NAME,
            size=dataconfig.BATCH_SIZE,
            last_timestamp=last_processed_timestamp
        ):
            if new_data:
                found_data = True
            if not new_data:
                logger.info("No new data found.")
                continue 

            logger.info(f"Fetched {len(new_data)} new records from Elasticsearch.")


            # Write batch to CSV for processing 
            with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False) as temp:
                df = pd.DataFrame(new_data) 
                df.to_csv(temp.name, index=False)
                logger.info(f"Batch written to CSV: {temp.name}")

            # Preprocess data 
            processed_data = preprocessor.preprocess(new_data)

            if processed_data.empty:
                logger.warning("No valid data after preprocessing.")
                continue 

            # Detect anomalies 
            logger.info("Running anomaly detection...")
            results = anomaly_detector.detect_anomalies(processed_data, new_data)
            print(results.head())

            # Get only anomalies
            anomalies = anomaly_detector.get_anomalies_only(results.to_dict(orient="records")) 

    except Exception as e:
        logger.error(f"Error in processing data: {e}")


async def continuous_data_processing():
    """Continuously process data at defined intervals."""
    global is_running 

    while is_running:
        try:
            await process_data() 
            logger.info("Conmpleted a processing cycle.")
            await asyncio.sleep(dataconfig.PROCESSING_INTERVAL)
        except Exception as e:
            logger.error(f"Error in continuous processing: {e}")
            await asyncio.sleep(dataconfig.PROCESSING_INTERVAL)


@app.post("/start")
async def start_processing(background_tasks: BackgroundTasks):
    """Start the anomaly detection service."""
    global is_running 

    if is_running:
        return {"message": "Service is already running."}
    
    is_running = True 
    background_tasks.add_task(continuous_data_processing)
    logger.info("Anomaly detection service started.")

    return {"message": "Anomaly detection service started."}

@app.post('/stop')
async def stop_processing():
    """Stop the anomaly detection service."""
    global is_running 
    is_running = False 
    logger.info("Anomaly detection service stopped.")
    return {"message": "Anomaly detection service stopped."}

@app.post('/process_manual')
def manual_process():
    """Manually trigger data processing."""
    try: 
        asyncio.create_task(process_data())
        return {"message": "Manual data processing triggered."}
    except Exception as e:
        logger.error(f"Error triggering manual processing: {e}")
        raise HTTPException(status_code=500, detail="Error triggering manual processing.")
    



if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stdout, level="INFO", format="{time} - {level} - {message}")

    # Run the app with Uvicorn 
    uvicorn.run(
        "main:app", 
        host=dataconfig.HOST, 
        port=dataconfig.PORT,
        reload=False,
        log_level="info"
    )


            



