import asyncio 
from typing import List, Dict, Any, Optional 
from elasticsearch import AsyncElasticsearch 
from loguru import logger 
from datetime import datetime, timedelta 
import pandas as pd 
import csv 
from typing import AsyncGenerator
# from config.data_config import dataconfig 
from dotenv import load_dotenv 
import os 

# Load environment variables from .env file 
load_dotenv()


class ElasticsearchService:
    """Service to interact with Elasticsearch asynchronously."""
    def __init__(self, username: str, password: str): 
        """Initialize the Elasticsearch client."""
        # self.api = f"https://{dataconfig.HOST}:{dataconfig.PORT}"
        self.api = f"https://esdev01api.vih.infineon.com:9200"
        print(self.api)
        self.username = username 
        self.password = password

        auth = (username, password) 
        self.client = AsyncElasticsearch(
            [self.api],
            basic_auth=auth,
            verify_certs=True,
            request_timeout=60
        )
        logger.info("Elasticsearch client initialized.")

    async def fetch_data(self, index: str, size: int = 100, last_timestamp: datetime = None) -> AsyncGenerator[List[Any], None]:
        """Fetch data from Elasticsearch index and return batches as lists."""
        start_time = last_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        query = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start_time,
                        "lt": end_time
                    }
                }
            }
        }
        all_docs = []
        try:
            response = await self.client.search(
                index=index,
                body=query,
                size=size,
                scroll='2m'
            )
            logger.info(f"Initial search response received with {len(response['hits']['hits'])} hits.")

            sid = response['_scroll_id']
            hits = response['hits']['hits']
            while hits: 
                batch = [hit['_source'] for hit in hits]
                all_docs.extend(batch)
                if batch:  # Only yield non-empty batches
                    yield batch
                response = await self.client.scroll(scroll_id=sid, scroll='2m')
                sid = response['_scroll_id']
                hits = response['hits']['hits']

            if all_docs: 
                with open("fetched_data.csv", mode='w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=all_docs[0].keys())
                    writer.writeheader()
                    for doc in all_docs:
                        writer.writerow(doc)


            
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
        

    async def close(self):
        """Close the Elasticsearch client."""
        await self.client.close()

                
        
if __name__ == "__main__":
    es_service = ElasticsearchService(username='aliru', password='Billionaire#9183')
    async def test_fetch():
        async for batch in es_service.fetch_data(index='log-app-jamarequest--*', size=5000, last_timestamp=datetime(2025, 9, 21)):
            print(batch)
    asyncio.run(test_fetch())
    asyncio.run(es_service.close())