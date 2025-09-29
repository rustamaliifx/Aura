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
from src.data.preprocessor import DataPreprocessor 
from src.models.anomaly_detection.anomaly_detector import AnomalyDetector
from dotenv import load_dotenv 
import os
from src.data.hybrid_collector import HybridElasticsearchService

# Load environment variables from .env file 
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

# Debug print (remove in production)
logger.info(f"Loaded credentials - Username: {'SET' if USERNAME else 'NOT SET'}, Password: {'SET' if PASSWORD else 'NOT SET'}")

if not USERNAME or not PASSWORD:
    raise ValueError("Elasticsearch USERNAME and PASSWORD environment variables must be set in .env file")

es_service = None 
preprocessor = None 
anomaly_detector = None 
last_processed_timestamp = dataconfig.LAST_PROCESSED_TIMESTAMP
is_running = False 

# Global variable to store latest anomalies
latest_anomalies = []

model_config = ModelConfig()


@asynccontextmanager 
async def lifespan(app: FastAPI):
    """Manage service lifecycle"""
    global es_service, preprocessor, anomaly_detector 

    # Startup 
    logger.info("Starting up services...")

    # Initialize Elasticsearch service 
    es_service = HybridElasticsearchService(
        username=USERNAME,  # type: ignore (already validated above)
        password=PASSWORD   # type: ignore (already validated above)
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
    # HybridElasticsearchService doesn't have a close method
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
    return {"message": "Anomaly Detection Service is running with Hybrid Elasticsearch Collector."}

@app.post("/api/initialize")
async def initialize_data():
    """Initialize the system with historical data if database is empty."""
    try:
        global es_service
        
        if es_service is None:
            raise HTTPException(status_code=500, detail="Service not initialized")
        
        current_count = es_service.get_data_count()
        
        if current_count > 0:
            return JSONResponse(content={
                "message": f"Database already contains {current_count} records. Use /api/bulk-sync for additional data.",
                "current_records": current_count
            })
        
        # First time setup - fetch recent historical data
        logger.info("Initializing with historical data...")
        sync_result = await es_service.incremental_fetch()
        
        return JSONResponse(content={
            "message": "System initialized successfully",
            "sync_result": sync_result,
            "records_loaded": sync_result.get("new_records", 0)
        })
        
    except Exception as e:
        logger.error(f"Error initializing system: {e}")
        raise HTTPException(status_code=500, detail=f"Error initializing system: {str(e)}")

async def process_data():
    global last_processed_timestamp 

    try:
        # Ensure services are initialized 
        global es_service, preprocessor, anomaly_detector 
        logger.info("Processing new data batch...")
        if es_service is None or preprocessor is None or anomaly_detector is None:
            logger.error("Services are not initialized.")
            raise RuntimeError("Services are not initialized.")
        
        # Sync new data from Elasticsearch using hybrid collector
        logger.info("Syncing data from Elasticsearch...")
        sync_result = await es_service.incremental_fetch()
        logger.info(f"Sync completed: {sync_result}")
        
        # Load recent data for processing
        new_data_df = es_service.load_recent_data(hours=1)  # Load last 1 hour of data
        
        if new_data_df.empty:
            logger.info("No new data found.")
            return
            
        # Convert DataFrame to list of dictionaries for processing
        new_data = new_data_df.to_dict(orient='records')
        
        logger.info(f"Processing {len(new_data)} records from local database.")

        if not new_data:
            logger.info("No data to process.")
            return

        # Write batch to CSV for processing 
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False) as temp:
            df = pd.DataFrame(new_data) 
            df.to_csv(temp.name, index=False)
            logger.info(f"Batch written to CSV: {temp.name}")

        # Preprocess data 
        processed_data = preprocessor.preprocess(new_data)

        if processed_data.empty:
            logger.warning("No valid data after preprocessing.")
            return

        # Detect anomalies 
        logger.info("Running anomaly detection...")
        results = anomaly_detector.detect_anomalies(processed_data, new_data)
        print(results.head())

        # Store all data with anomaly flags globally for API endpoint
        global latest_anomalies
        latest_anomalies = results.to_dict(orient="records")
        
        # Count actual anomalies for logging
        anomaly_count = len([r for r in latest_anomalies if r.get('is_anomaly', False)])
        logger.info(f"Stored {len(latest_anomalies)} total records with {anomaly_count} anomalies for API access.") 

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
async def manual_process():
    """Manually trigger data processing."""
    try: 
        await process_data()
        return {"message": "Manual data processing completed."}
    except Exception as e:
        logger.error(f"Error triggering manual processing: {e}")
        raise HTTPException(status_code=500, detail="Error triggering manual processing.")
    

def clean_data_for_json(data_list):
    """Clean data to handle NaN values for JSON serialization."""
    cleaned_data = []
    for item in data_list:
        if isinstance(item, dict):
            # Replace NaN values with None (which becomes null in JSON)
            cleaned_item = {}
            for key, value in item.items():
                if pd.isna(value) or (isinstance(value, float) and not pd.isna(value) and (value == float('inf') or value == float('-inf'))):
                    cleaned_item[key] = None
                else:
                    cleaned_item[key] = value
            cleaned_data.append(cleaned_item)
        else:
            cleaned_data.append(item)
    return cleaned_data

@app.get("/api/data")
async def get_all_data():
    """Return all processed data with anomaly flags."""
    try: 
        global latest_anomalies
        
        cleaned_data = clean_data_for_json(latest_anomalies)
        anomaly_count = len([r for r in cleaned_data if r.get('is_anomaly', False)])
        
        return JSONResponse(content={
            "data": cleaned_data, 
            "timestamp": datetime.now().isoformat(),
            "total_count": len(cleaned_data),
            "anomaly_count": anomaly_count,
            "normal_count": len(cleaned_data) - anomaly_count
        })
    except Exception as e:
        logger.error(f"Error fetching all data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching all data")

@app.get("/api/anomalies")
async def get_latest_anomalies():
    """Return only the anomalous data points."""
    try: 
        global latest_anomalies
        
        # Filter only anomalies
        anomalies_only = [r for r in latest_anomalies if r.get('is_anomaly', False)]
        cleaned_anomalies = clean_data_for_json(anomalies_only)
        
        return JSONResponse(content={
            "anomalies": cleaned_anomalies, 
            "timestamp": datetime.now().isoformat(),
            "count": len(cleaned_anomalies)
        })
    except Exception as e:
        logger.error(f"Error fetching anomalies: {e}")
        raise HTTPException(status_code=500, detail="Error fetching anomalies")

@app.get("/api/status")
async def get_service_status():
    """Get service status and database information."""
    try:
        global es_service, is_running
        
        if es_service is None:
            return JSONResponse(content={
                "service_running": False,
                "error": "Service not initialized"
            })
        
        total_records = es_service.get_data_count()
        last_fetch = es_service.get_last_fetch_timestamp()
        
        return JSONResponse(content={
            "service_running": is_running,
            "total_records": total_records,
            "last_fetch_timestamp": last_fetch.isoformat() if last_fetch else None,
            "database_path": str(es_service.db_path),
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error getting service status: {e}")
        raise HTTPException(status_code=500, detail="Error getting service status")

@app.post("/api/bulk-sync")
async def trigger_bulk_sync(start_date: str, end_date: Optional[str] = None, max_workers: int = 4):
    """Trigger bulk parallel data synchronization for historical data."""
    try:
        global es_service
        
        if es_service is None:
            raise HTTPException(status_code=500, detail="Service not initialized")
        
        # Parse dates
        start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        end_dt = datetime.now() if end_date is None else datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        
        logger.info(f"Starting bulk sync from {start_dt} to {end_dt} with {max_workers} workers")
        
        # Run bulk parallel fetch
        sync_result = await es_service.bulk_parallel_fetch(start_dt, end_dt, max_workers)
        
        return JSONResponse(content={
            "success": True,
            "sync_result": sync_result,
            "message": f"Bulk sync completed. Processed {sync_result['new_records']} new records"
        })
        
    except Exception as e:
        logger.error(f"Error in bulk sync: {e}")
        raise HTTPException(status_code=500, detail=f"Error in bulk sync: {str(e)}")

@app.get("/api/recent-data")
async def get_recent_raw_data(hours: int = 1):
    """Get recent raw data from database without anomaly detection."""
    try:
        global es_service
        
        if es_service is None:
            raise HTTPException(status_code=500, detail="Service not initialized")
        
        # Load recent data
        recent_df = es_service.load_recent_data(hours=hours)
        
        if recent_df.empty:
            return JSONResponse(content={
                "data": [],
                "count": 0,
                "hours": hours,
                "message": "No recent data found"
            })
        
        # Convert to records and clean for JSON
        recent_data = recent_df.to_dict(orient='records')
        cleaned_data = clean_data_for_json(recent_data)
        
        return JSONResponse(content={
            "data": cleaned_data,
            "count": len(cleaned_data),
            "hours": hours,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error fetching recent data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching recent data")
    



if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stdout, level="INFO", format="{time} - {level} - {message}")

    logger.info("Starting Anomaly Detection Service with Hybrid Elasticsearch Collector...")
    logger.info(f"Server will run on http://{dataconfig.HOST}:{dataconfig.PORT}")
    logger.info("Available endpoints:")
    logger.info("  GET  /                     - Service status")
    logger.info("  POST /api/initialize       - Initialize with historical data")
    logger.info("  POST /start                - Start continuous processing")
    logger.info("  POST /stop                 - Stop processing")
    logger.info("  GET  /api/status           - Get service and database status")
    logger.info("  GET  /api/data             - Get all processed data with anomalies")
    logger.info("  GET  /api/anomalies        - Get only anomalous data points")
    logger.info("  GET  /api/recent-data      - Get recent raw data")
    logger.info("  POST /api/bulk-sync        - Trigger bulk historical sync")
    
    # Run the app with Uvicorn 
    uvicorn.run(
        "main:app", 
        host=dataconfig.HOST, 
        port=dataconfig.PORT,
        reload=False,
        log_level="info"
    )


            



