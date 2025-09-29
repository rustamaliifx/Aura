import asyncio
import sqlite3
import json
from typing import List, Dict, Any, Optional
from elasticsearch import AsyncElasticsearch
from loguru import logger
from datetime import datetime, timedelta
import pandas as pd
import time
from typing import AsyncGenerator
import sys
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import threading

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from config.data_config import dataconfig
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')

class HybridElasticsearchService:
    """Hybrid Elasticsearch service combining parallel processing with persistent storage."""
    
    def __init__(self, username: str, password: str, db_path: Optional[str] = None):
        """Initialize the service with Elasticsearch client and local database."""
        self.api = dataconfig.API
        self.username = username
        self.password = password
        
        # Setup local database for persistence
        if db_path is None:
            self.db_path = project_root / "data" / "processed" / "elasticsearch_cache.db"
        else:
            self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize local database
        self._init_database()
        logger.info("Hybrid Elasticsearch service initialized.")

    def _init_database(self):
        """Initialize SQLite database for data persistence."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        # Enable WAL mode for better concurrent access
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA synchronous=NORMAL;')
        conn.execute('PRAGMA cache_size=10000;')
        conn.execute('PRAGMA temp_store=memory;')
        cursor = conn.cursor()
        
        # Create table for storing fetched data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS elasticsearch_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                raw_data TEXT NOT NULL,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create index of timestamp column 
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_timestamp ON elasticsearch_data(timestamp)
        ''')
        
        # Create table for tracking fetch metadata
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fetch_metadata (
                id INTEGER PRIMARY KEY,
                last_fetch_timestamp TEXT,
                total_records INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )           
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")

    def get_last_fetch_timestamp(self) -> Optional[datetime]:
        """Get the timestamp of the last successful fetch."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT last_fetch_timestamp FROM fetch_metadata WHERE id = 1')
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0]:
            return datetime.fromisoformat(result[0])
        return None

    def update_last_fetch_timestamp(self, timestamp: datetime):
        """Update the last fetch timestamp."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO fetch_metadata (id, last_fetch_timestamp, updated_at)
            VALUES (1, ?, datetime('now'))
        ''', (timestamp.isoformat(),))
        
        conn.commit()
        conn.close()

    def get_data_count(self) -> int:
        """Get total number of records in local database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM elasticsearch_data')
        count = cursor.fetchone()[0]
        conn.close()
        return count

    def _sequential_database_writer(self, data_queue: queue.Queue, stop_event: threading.Event) -> int:
        """Sequential database writer that processes data from queue."""
        total_inserted = 0
        conn = sqlite3.connect(self.db_path, timeout=60.0)
        conn.execute('PRAGMA journal_mode=WAL;')
        cursor = conn.cursor()
        
        try:
            while not stop_event.is_set() or not data_queue.empty():
                try:
                    # Get batch from queue with timeout
                    batch_data = data_queue.get(timeout=1.0)
                    
                    if batch_data is None:  # Sentinel value to stop
                        break
                    
                    # Insert batch sequentially
                    cursor.executemany(
                        'INSERT INTO elasticsearch_data (timestamp, raw_data) VALUES (?, ?)',
                        batch_data
                    )
                    conn.commit()
                    total_inserted += len(batch_data)
                    
                    logger.info(f'Sequential writer: Inserted {len(batch_data)} records. Total: {total_inserted}')
                    data_queue.task_done()
                    
                except queue.Empty:
                    continue  # Check stop_event and continue
                except Exception as e:
                    logger.error(f"Error in sequential writer: {e}")
                    
        finally:
            conn.close()
            logger.info(f"Sequential writer finished. Total inserted: {total_inserted}")
            
        return total_inserted

    async def _fetch_date_range(self, start_date: datetime, end_date: datetime, data_queue: queue.Queue) -> int:
        """Fetch data for a specific date range and store in database."""
        day_start_time = time.time()
        day_str = start_date.strftime("%Y-%m-%d")
        
        # Initialize Elasticsearch client for this task
        auth = (self.username, self.password)
        client = AsyncElasticsearch(
            [self.api],
            basic_auth=auth,
            verify_certs=True,
            request_timeout=dataconfig.REQUEST_TIMEOUT
        )
        
        try:
            start_time = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_time = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            query = {
                "query": {
                    "range": {
                        "@timestamp": {
                            "gte": start_time,
                            "lt": end_time
                        }
                    }
                },
                "sort": [{"@timestamp": {"order": "asc"}}] 
            }
            
            logger.info(f"Starting fetch for {day_str} at {time.ctime()}")
        
            new_records = 0
        
            response = await client.search(
                index=dataconfig.INDEX_NAME,
                query=query["query"],
                sort=query["sort"],
                size=dataconfig.BATCH_SIZE,
                scroll=dataconfig.SCROLL_TIME
            )

            logger.info(f'Found {response['hits']['total']['value']} new records')
        
            sid = response['_scroll_id']
            hits = response['hits']['hits']
        
            while hits:
                # Process batch
                batch_data = []
                for hit in hits:
                    source_data = hit['_source']
                    timestamp = source_data.get('@timestamp')
                    
                    batch_data.append((
                        timestamp,
                        json.dumps(source_data)
                    ))
            
                # Add batch to queue for sequential processing
                if batch_data:
                    data_queue.put(batch_data)
                    new_records += len(batch_data)
                    logger.info(f'Queued {len(batch_data)} records for {day_str}. Total fetched: {new_records}')
            
                # Get next batch
                response = await client.scroll(scroll_id=sid, scroll=dataconfig.SCROLL_TIME)
                sid = response['_scroll_id']
                hits = response['hits']['hits']
        
            day_end_time = time.time()
            day_execution_time = day_end_time - day_start_time
        
            logger.info(f"Fetched {new_records} records for {day_str} in {day_execution_time:.2f} seconds")
            return new_records
            
        except Exception as e:
            logger.error(f"Error fetching {day_str}: {e}")
            return 0
        finally:
            await client.close()


    async def bulk_parallel_fetch(self, start_date: datetime, end_date: datetime = datetime.now(), max_workers: int = 4) -> Dict[str, Any]:
        """Fetch historical data in parallel using multiple workers with sequential database insertion."""
        initial_start_time = time.time()
        initial_count = self.get_data_count()
        
        logger.info(f"Starting parallel bulk fetch from {start_date} to {end_date}")
        logger.info(f"Using {max_workers} parallel workers with sequential database insertion")
        
        # Create time windows (day by day)
        time_windows = []
        current = start_date
        delta = timedelta(days=1)
        
        while current < end_date:
            next_time = min(current + delta, end_date)
            time_windows.append((current, next_time))
            current = next_time
        
        logger.info(f"Created {len(time_windows)} time windows for parallel processing")
        
        # Create queue for sequential database insertion
        data_queue = queue.Queue()
        stop_event = threading.Event()
        
        # Start sequential database writer thread
        writer_thread = threading.Thread(
            target=self._sequential_database_writer,
            args=(data_queue, stop_event)
        )
        writer_thread.start()
        
        try:
            # Process in parallel with semaphore to limit concurrent connections
            total_fetched_records = 0
            semaphore = asyncio.Semaphore(max_workers)
            
            async def fetch_with_semaphore(start, end):
                async with semaphore:
                    return await self._fetch_date_range(start, end, data_queue)
            
            # Execute all tasks in parallel
            tasks = [fetch_with_semaphore(start, end) for start, end in time_windows]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for i, result in enumerate(results):
                if isinstance(result, int):
                    total_fetched_records += result
                else:
                    logger.error(f"Error in time window {i}: {result}")
            
            # Signal writer to stop and wait for completion
            logger.info("All fetching completed, waiting for database writer to finish...")
            data_queue.put(None)  # Sentinel value
            stop_event.set()
            writer_thread.join(timeout=60)  # Wait up to 60 seconds
            
            if writer_thread.is_alive():
                logger.warning("Database writer thread did not finish in time")
            
        except Exception as e:
            logger.error(f"Error during parallel fetch: {e}")
            stop_event.set()
            writer_thread.join(timeout=10)
            raise
        
        # Update last fetch timestamp
        self.update_last_fetch_timestamp(end_date)
        
        final_count = self.get_data_count()
        end_time = time.time()
        total_execution_time = end_time - initial_start_time
        
        return {
            "initial_count": initial_count,
            "new_records": total_fetched_records,
            "final_count": final_count,
            "time_windows_processed": len(time_windows),
            "total_execution_time": total_execution_time,
            "average_time_per_window": total_execution_time / len(time_windows) if time_windows else 0,
            "records_per_second": total_fetched_records / total_execution_time if total_execution_time > 0 else 0,
            "last_sync": end_date.isoformat()
        }

    async def incremental_fetch(self) -> Dict[str, Any]:
        """Fetch only new data since last sync (single-threaded for recent data)."""
        last_timestamp = self.get_last_fetch_timestamp()
        current_time = datetime.now()
        
        if last_timestamp is None:
            # First time - use parallel bulk fetch for historical data
            historical_start = current_time - timedelta(days=dataconfig.START_FETCH_DAY)
            logger.info(f"First time fetch - using parallel bulk fetch for last {dataconfig.START_FETCH_DAY} days")
            return await self.bulk_parallel_fetch(historical_start, current_time)
        else:
            # Incremental fetch for recent data
            logger.info(f"Incremental fetch since {last_timestamp}")
            
            # Use single connection for recent data (more efficient for small datasets)
            auth = (self.username, self.password)
            client = AsyncElasticsearch(
                [self.api],
                basic_auth=auth,
                verify_certs=True,
                request_timeout=dataconfig.REQUEST_TIMEOUT
            )
            
            try:
                start_time = last_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
                end_time = current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                
                query = {
                    "query": {
                        "range": {
                            "@timestamp": {
                                "gt": start_time,
                                "lte": end_time
                            }
                        }
                    },
                    "sort": [{"@timestamp": {"order": "asc"}}]
                }
                
                initial_count = self.get_data_count()
                new_records = 0
                
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                response = await client.search(
                    index=dataconfig.INDEX_NAME,
                    query=query["query"],
                    sort=query["sort"],
                    size=dataconfig.BATCH_SIZE,
                    scroll=dataconfig.SCROLL_TIME
                )
                
                sid = response['_scroll_id']
                hits = response['hits']['hits']
                
                while hits:
                    batch_data = []
                    for hit in hits:
                        source_data = hit['_source']
                        timestamp = source_data.get('@timestamp')
                        
                        batch_data.append((
                            timestamp,
                            json.dumps(source_data)
                        ))
                    
                    if batch_data:
                        cursor.executemany(
                            'INSERT INTO elasticsearch_data (timestamp, raw_data) VALUES (?, ?)',
                            batch_data
                        )
                        new_records += len(batch_data)
                    
                    response = await client.scroll(scroll_id=sid, scroll=dataconfig.SCROLL_TIME)
                    sid = response['_scroll_id']
                    hits = response['hits']['hits']
                
                conn.commit()
                conn.close()
                
                # Update last fetch timestamp
                self.update_last_fetch_timestamp(current_time)
                
                final_count = self.get_data_count()
                
                return {
                    "initial_count": initial_count,
                    "new_records": new_records,
                    "final_count": final_count,
                    "last_sync": current_time.isoformat(),
                    "fetch_type": "incremental"
                }
                
            finally:
                await client.close()

    def load_all_data(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Load all data from local database as DataFrame."""
        conn = sqlite3.connect(self.db_path)
        
        query = 'SELECT timestamp, raw_data FROM elasticsearch_data ORDER BY timestamp'
        if limit:
            query += f' LIMIT {limit}'
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if not df.empty:
            parsed_data = df['raw_data'].apply(json.loads).tolist()
            expanded_df = pd.json_normalize(parsed_data)
            expanded_df['@timestamp'] = df['timestamp'].values
            logger.info(f"Loaded {len(expanded_df)} records from local database")
            return expanded_df
        
        return pd.DataFrame()

    def load_recent_data(self, hours: int = 24) -> pd.DataFrame:
        """Load recent data from local database."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        cutoff_str = cutoff_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(
            'SELECT timestamp, raw_data FROM elasticsearch_data WHERE timestamp > ? ORDER BY timestamp',
            conn,
            params=(cutoff_str,)
        )
        conn.close()
        
        if not df.empty:
            parsed_data = df['raw_data'].apply(json.loads).tolist()
            expanded_df = pd.json_normalize(parsed_data)
            expanded_df['@timestamp'] = df['timestamp'].values
            logger.info(f"Loaded {len(expanded_df)} recent records")
            return expanded_df
        
        return pd.DataFrame()


async def main():
    """Main function demonstrating hybrid approach."""
    if not USERNAME or not PASSWORD:
        raise ValueError("USERNAME and PASSWORD environment variables must be set")
    
    service = HybridElasticsearchService(USERNAME, PASSWORD)
    
    try:
        print(f"Starting hybrid data synchronization at {time.ctime()}")
        start_time = time.time()
        
        # Option 1: Bulk parallel fetch for historical data
        # historical_start = datetime(2025, 8, 24)
        # historical_end = datetime.now()
        # sync_result = await service.bulk_parallel_fetch(historical_start, historical_end, max_workers=4)
        
        # Option 2: Smart incremental fetch (parallel for first time, incremental for subsequent)
        sync_result = await service.incremental_fetch()
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print("\n" + "="*60)
        print("HYBRID FETCH RESULTS")
        print("="*60)
        print(f"Initial records: {sync_result['initial_count']:,}")
        print(f"Final total records: {sync_result['final_count']:,}")
        print(f"Total execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        
        if 'time_windows_processed' in sync_result:
            print(f"Time windows processed: {sync_result['time_windows_processed']}")
            print(f"Average time per window: {sync_result['average_time_per_window']:.2f} seconds")
        
        if 'records_per_second' in sync_result:
            print(f"Processing rate: {sync_result['records_per_second']:.0f} records/second")
        
        print(f"Last sync: {sync_result['last_sync']}")
        print("="*60)
        
        # Load and display some recent data
        recent_data = service.load_recent_data(hours=1)
        if not recent_data.empty:
            print(f"\nSample of recent data ({len(recent_data)} records):")
            print(recent_data.head())
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())