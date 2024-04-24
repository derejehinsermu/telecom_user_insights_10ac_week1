from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

def get_engine():
    # Retrieve connection parameters from environment variables
    database_name = os.getenv('DB_NAME', 'DB_NAME') 
    connection_params = {
        "host": os.getenv('DB_HOST', 'DB_HOST'),
        "user": os.getenv('DB_USER', 'DB_USER'),
        "password": os.getenv('DB_PASS', 'DB_PASS'),
        "port": os.getenv('DB_PORT', 'DB_PORT'),
        "database": database_name
    }
    
    # Create the database engine using the connection parameters
    engine_string = f"postgresql+psycopg2://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
    engine = create_engine(engine_string)
    
    return engine
