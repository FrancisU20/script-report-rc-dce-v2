from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()

def database_connection():
    # Cargar variables de entorno en una variable 
    url = os.getenv('API_URL')
    # Configura la conexión a la base de datos
    engine = create_engine(url)
    return engine