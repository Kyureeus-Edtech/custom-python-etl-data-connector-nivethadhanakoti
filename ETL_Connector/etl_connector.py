import os
import requests
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

class ETLExtractor:    
    def __init__(self, base_url, api_key):
        self.base_url = base_url
        self.api_key = api_key
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
    def fetch_top_headlines(self, country='us'):
        url = f"{self.base_url}/top-headlines"
        params = {
            'country': country,
            'apiKey': self.api_key
        }
        try:
            logger.info(f"Fetching data from: {url} with params {params}")
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            if data.get('status') == 'ok':
                return data.get('articles')
            else:
                logger.error(f"API returned error status: {data.get('status')}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data: {e}")
            return None

class ETLTransformer:    
    @staticmethod
    def transform_articles(articles):
        if not articles:
            return None
        
        transformed = []
        ingestion_time = datetime.utcnow()
        
        for article in articles:
            transformed.append({
                'source': article.get('source'),
                'author': article.get('author'),
                'title': article.get('title'),
                'description': article.get('description'),
                'url': article.get('url'),
                'publishedAt': article.get('publishedAt'),
                'content': article.get('content'),
                'ingestion_timestamp': ingestion_time
            })
        return transformed

class ETLLoader:
    """Handles loading data into MongoDB"""
    
    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.client = None
        
    def __enter__(self):
        try:
            self.client = MongoClient(self.mongo_uri)
            logger.info("Connected to MongoDB")
            return self
        except Exception as e:
            logger.error(f"MongoDB connection error: {e}")
            raise
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
            
    def load_data(self, collection_name, data):
        if not data:
            logger.warning("No data to load")
            return False
        
        try:
            db = self.client[self.db_name]
            collection = db[collection_name]
            result = collection.insert_many(data)
            logger.info(f"Inserted {len(result.inserted_ids)} documents into {collection_name}")
            return True
        except Exception as e:
            logger.error(f"Error loading data into MongoDB: {e}")
            return False

def run_etl():
    NEWSAPI_KEY = os.getenv('NEWSAPI_KEY')
    MONGO_URI = os.getenv('MONGODB_URI')
    DB_NAME = 'news_db'
    COLLECTION_NAME = 'newsapi_raw'
    BASE_URL = 'https://newsapi.org/v2'
    
    if not NEWSAPI_KEY:
        logger.error("NEWSAPI_KEY is not set in .env")
        return
    
    if not MONGO_URI:
        logger.error("MONGODB_URI is not set in .env")
        return
    
    extractor = ETLExtractor(BASE_URL, NEWSAPI_KEY)
    transformer = ETLTransformer()
    
    logger.info("Starting ETL pipeline - Extraction")
    articles = extractor.fetch_top_headlines()
    
    if not articles:
        logger.error("Extraction failed or returned no articles")
        return
    
    logger.info("Transforming data")
    transformed_data = transformer.transform_articles(articles)
    
    if not transformed_data:
        logger.error("Transformation failed or no data after transformation")
        return
    
    logger.info("Loading data into MongoDB")
    with ETLLoader(MONGO_URI, DB_NAME) as loader:
        success = loader.load_data(COLLECTION_NAME, transformed_data)
    
    if success:
        logger.info("ETL pipeline completed successfully")
    else:
        logger.error("ETL pipeline failed during loading")

if __name__ == '__main__':
    run_etl()
