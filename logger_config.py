import logging
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd

# Wczytanie konfiguracji z pliku .env
load_dotenv()
SERVER = os.getenv('DB_SERVER')
DATABASE = os.getenv('DB_DATABASE')
USERNAME = os.getenv('DB_USERNAME')
PASSWORD = os.getenv('DB_PASSWORD')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Konfiguracja bazy danych
DB_CONNECTION_STRING = f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
ENGINE = create_engine(DB_CONNECTION_STRING)

class DatabaseHandler(logging.Handler):
    """
    Custom logging handler to save logs into a database.
    """
    def __init__(self, engine, script_id):
        super().__init__()
        self.engine = engine
        self.script_id = script_id

    def emit(self, record):
            # Tworzenie log_message na podstawie rekordów logów
            log_to_add = {
                'script_id': self.script_id,
                'log_datetime': datetime.now(),  # Użycie aktualnej daty i czasu
                'log_type': record.levelname,
                'log_topic': record.name,
                'log_message': record.getMessage()  # Formatowanie wiadomości logowania
            }
            # Wstawienie loga do tabeli Logs
            try:
                df_log_to_add = pd.DataFrame([log_to_add])
                df_log_to_add.to_sql('Logs', con=ENGINE, if_exists='append', index=False)
            except Exception as e:
                print(f"Błąd podczas zapisywania logu do bazy danych: {e}")


def setup_logger(name, log_file=None, level=logging.INFO, script_id=None):
    """
    Konfiguracja loggera z handlerami do pliku, konsoli i bazy danych.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(name)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Tworzenie handlera do pliku
    if log_file is not None:       
        file_handler = logging.FileHandler(os.path.join(BASE_DIR, log_file))
        file_handler.setFormatter(formatter)

    # Tworzenie handlera do konsoli
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Tworzenie handlera do bazy danych (jeśli script_id jest podane)
    if script_id is not None:
        db_handler = DatabaseHandler(ENGINE, script_id)
        db_handler.setFormatter(formatter)

    # Dodajemy handlery tylko, jeśli jeszcze ich nie ma, aby uniknąć duplikacji
    if not logger.handlers:
        logger.addHandler(console_handler)
        if log_file is not None:
            logger.addHandler(file_handler)       
        if script_id is not None:
            logger.addHandler(db_handler)
            print(f"DatabaseHandler dodany do loggera: {name}")

    return logger