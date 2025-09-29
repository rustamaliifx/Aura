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

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from config.data_config import dataconfig
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')

class PersistentElasticsearchService:
    """Enhanced Elasticsearch service with data persistence and incremental fetching."""
    
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
        
        # Initialize Elasticsearch client
        auth = (username, password)
        self.client = AsyncElasticsearch(
            [self.api],
            basic_auth=auth,
            verify_certs=True,
            request_timeout=dataconfig.REQUEST_TIMEOUT
        )
        
        # Initialize local database
        self._init_database()
        logger.info("Persistent Elasticsearch service initialized.")

    def _init_database(self):
        """Initialize SQLite database for data persistence."""
        conn = sqlite3.connect(self.db_path)
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
        
        # Create index for timestamp column (separate statement)
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

    async def fetch_incremental_data(self, index: str, size: int = 1000) -> int:
        """Fetch only new data since last fetch."""
        last_timestamp = self.get_last_fetch_timestamp()
        current_time = datetime.now()
        
        if last_timestamp is None:
            # First time fetch - get data from 60 days ago
            last_timestamp = current_time - timedelta(days=60)
            logger.info("First time fetch - getting last 60 days of data")
        else:
            logger.info(f"Incremental fetch since {last_timestamp}")

        new_records = 0
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
            "sort": [{"@timestamp": {"order": "asc"}}]  # Sort to ensure chronological order
        }

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            response = await self.client.search(
                index=index,
                query=query["query"],
                sort=query["sort"],
                size=size,
                scroll=dataconfig.SCROLL_TIME 
            )
            
            logger.info(f"Found {response['hits']['total']['value']} new records")
            
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
                
                # Insert batch into database
                if batch_data:
                    cursor.executemany(
                        'INSERT INTO elasticsearch_data (timestamp, raw_data) VALUES (?, ?)',
                        batch_data
                    )
                    new_records += len(batch_data)
                    logger.info(f"Inserted {len(batch_data)} records. Total new: {new_records}")
                
                # Get next batch
                response = await self.client.scroll(scroll_id=sid, scroll='2m')
                sid = response['_scroll_id']
                hits = response['hits']['hits']
            
            conn.commit()
            conn.close()
            
            # Update last fetch timestamp
            self.update_last_fetch_timestamp(current_time)
            
            logger.info(f"Incremental fetch completed. Added {new_records} new records.")
            return new_records
            
        except Exception as e:
            logger.error(f"Error during incremental fetch: {e}")
            return 0

    def load_all_data(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Load all data from local database as DataFrame."""
        conn = sqlite3.connect(self.db_path)
        
        query = 'SELECT timestamp, raw_data FROM elasticsearch_data ORDER BY timestamp'
        if limit:
            query += f' LIMIT {limit}'
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if not df.empty:
            # Parse JSON data
            parsed_data = df['raw_data'].apply(json.loads).tolist()
            # Expand JSON into columns
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

    async def sync_data(self) -> Dict[str, Any]:
        """Sync data with Elasticsearch and return summary."""
        initial_count = self.get_data_count()
        new_records = await self.fetch_incremental_data(dataconfig.INDEX_NAME, dataconfig.BATCH_SIZE)
        final_count = self.get_data_count()
        
        return {
            "initial_count": initial_count,
            "new_records": new_records,
            "final_count": final_count,
            "last_sync": datetime.now().isoformat()
        }

    async def close(self):
        """Close the Elasticsearch client."""
        await self.client.close()


async def main():
    """Main function to demonstrate usage."""
    if not USERNAME or not PASSWORD:
        raise ValueError("USERNAME and PASSWORD environment variables must be set")
    
    service = PersistentElasticsearchService(USERNAME, PASSWORD)
    
    try:
        logger.info("Starting data synchronization...")
        sync_result = await service.sync_data()
        
        print("Sync Results:")
        print(f"  Initial records: {sync_result['initial_count']}")
        print(f"  New records added: {sync_result['new_records']}")
        print(f"  Total records: {sync_result['final_count']}")
        print(f"  Last sync: {sync_result['last_sync']}")
        
        # Load and display some recent data
        recent_data = service.load_recent_data(hours=1)
        if not recent_data.empty:
            print(f"\nSample of recent data ({len(recent_data)} records):")
            print(recent_data.head())
        
    finally:
        await service.close()


if __name__ == "__main__":
    asyncio.run(main())