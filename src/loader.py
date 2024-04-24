
import sys
sys.path.append('../')

import pandas as pd
from db_connection.db_connection import get_engine

def load_data(query):
    engine = get_engine()
    return pd.read_sql(query, con=engine)
