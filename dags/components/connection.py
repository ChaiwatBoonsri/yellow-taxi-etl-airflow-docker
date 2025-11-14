import sqlalchemy
import os
from dotenv import load_dotenv
load_dotenv() # load environment variables from .env file

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def connection():
    """Connect to the database and return the connection object."""
    # Establish database connection
    try:
        connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}" 
        engine = sqlalchemy.create_engine(connection_string) # create SQLAlchemy engine
        connection = engine.connect() # establish connection
        print("Database connection established")
        return connection_string
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return