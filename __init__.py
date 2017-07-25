import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

CONNECTION_STRING = 'postgresql://{}:{}@localhost:5432/{}'.format(os.environ['PG_USERNAME'], 
		os.environ['PG_PASSWORD'], os.environ['PG_DATABASE'])

PG_ENGINE = create_engine(CONNECTION_STRING)
PG_CONNECTION = PG_ENGINE.connect()