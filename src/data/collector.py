import asyncio 
from typing import List, Dict, Any, Optional 
from elasticsearch import AsyncElasticsearch 
from loguru import logger 
from datetime import datetime, timedelta 
import pandas as pd 
import time
import csv 
from typing import AsyncGenerator
import sys
import os
from pathlib import Path


project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from config.data_config import dataconfig 
from dotenv import load_dotenv 

# Load environment variables from .env file 
load_dotenv()
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')

class ElasticsearchService:
    """Service to interact with Elasticsearch asynchronously."""
    def __init__(self, username: str, password: str): 
        """Initialize the Elasticsearch client."""
        # self.api = f"https://{dataconfig.HOST}:{dataconfig.PORT}"
        self.api = dataconfig.API
        print(self.api)
        self.username = username 
        self.password = password

        auth = (username, password) 
        self.client = AsyncElasticsearch(
            [self.api],
            basic_auth=auth,
            verify_certs=True,
            request_timeout=dataconfig.REQUEST_TIMEOUT
        )
        logger.info("Elasticsearch client initialized.")

    async def fetch_data(self, index: str, size: int = 100, last_timestamp: Optional[datetime] = None) -> AsyncGenerator[List[Any], None]:
        """Fetch data from Elasticsearch index and return batches as lists."""
        # Record initial fetch start time
        fetch_start_time = time.time()
        logger.info(f"Starting data fetch at {time.ctime()}")
        
        if last_timestamp is None:
            last_timestamp = datetime.now() - timedelta(days=30)  # Default to last 30 days
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
                query=query["query"],
                size=size,
                scroll=dataconfig.SCROLL_TIME
            )
            logger.info(f"Initial search response received with {len(response['hits']['hits'])} hits.")

            sid = response['_scroll_id']
            hits = response['hits']['hits']
            while hits: 
                batch = [hit['_source'] for hit in hits]
                all_docs.extend(batch)
                if batch:  # Only yield non-empty batches
                    yield batch
                response = await self.client.scroll(scroll_id=sid, scroll=dataconfig.SCROLL_TIME)
                sid = response['_scroll_id']
                hits = response['hits']['hits']

            # if all_docs: 
            #     with open("fetched_data.csv", mode='w', newline='', encoding='utf-8') as csvfile:
            #         writer = csv.DictWriter(csvfile, fieldnames=all_docs[0].keys())
            #         writer.writeheader()
            #         for doc in all_docs:
            #             writer.writerow(doc)


            
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
        

    async def close(self):
        """Close the Elasticsearch client."""
        await self.client.close()

                
        
if __name__ == "__main__":
    # Validate environment variables
    if not USERNAME or not PASSWORD:
        raise ValueError("USERNAME and PASSWORD environment variables must be set")
    
    # es_service = ElasticsearchService(USERNAME, PASSWORD)
    # print(f'Started Fetching at {time.ctime()}')
    # async def test_fetch():
    #     async for batch in es_service.fetch_data(index=dataconfig.INDEX_NAME, size=dataconfig.BATCH_SIZE, last_timestamp=datetime(2025, 8, 24)):
    #         pass
    # asyncio.run(test_fetch())
    # print(f'Finished Fetching at {time.ctime()}')
    # asyncio.run(es_service.close())

    async def main():
        """Main async function"""
        es_service = ElasticsearchService(USERNAME, PASSWORD) 

        try: 
            print(f'Started Fetching the data from elk index at {time.ctime()}')
            start_time = time.time() 

            async for batch in es_service.fetch_data(
                index=dataconfig.INDEX_NAME, 
                size=dataconfig.BATCH_SIZE, 
                last_timestamp=datetime(2025, 8, 24) 
            ):
                pass
            end_time = time.time() 
            execution_time = end_time - start_time 

            print(f'Finished Fetching at {time.ctime()}') 
            print(f'Total time it took {execution_time:.2f} seconds')
        finally:
            await es_service.close()
    
    asyncio.run(main())

