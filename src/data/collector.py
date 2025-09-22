import asyncio 
from typing import List, Dict, Any, Optional 
from elasticsearch import AsyncElasticsearch 
from loguru import logger 
from datetime import datetime, timedelta 
import pandas as pd 
import csv 
from typing import AsyncGenerator
from config.data_config import dataconfig 
from dotenv import load_dotenv 
import os 

# Load environment variables from .env file 
load_dotenv()

username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")

class ElasticsearchService:
    """Service to interact with Elasticsearch asynchronously."""
    def __init__(self, username: str, password: str): 
        """Initialize the Elasticsearch client."""
        self.api = f"https://{dataconfig.HOST}:{dataconfig.PORT}"
        self.username = username 
        self.password = password

        auth = (username, password) 
        self.client = AsyncElasticsearch(
            [self.api],
            basic_auth=auth,
            verify_certs=True,
        )

    async def fetch_data(self, index: str, size: int = 100) -> AsyncGenerator[List[Any], None]:
        """Fetch data from Elasticsearch index and return batches as lists."""
        query = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": "now-1m",
                        "lt": "now/d"
                    }
                }
            }
        }
        try:
            response = await self.client.search(
                index=index,
                query=query,
                size=size,
                scroll='2m'
            )

            sid = response['_scroll_id']
            hits = response['hits']['hits']
            while hits: 
                batch = [hit['_source'] for hit in hits]
                yield batch 
                response = await self.client.scroll(scroll_id=sid, scroll='2m')
                sid = response['_scroll_id']
                hits = response['hits']['hits']
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            return 
        

    async def close(self):
        """Close the Elasticsearch client."""
        await self.client.close()
                