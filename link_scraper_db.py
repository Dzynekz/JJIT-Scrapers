from playwright.sync_api import sync_playwright
import time
import json
from datetime import datetime, timedelta
import random
import requests
import os
import pandas as pd
import pyodbc
from logger_config import setup_logger
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy.types import NVARCHAR, Integer, Float

'''
if getattr(sys, 'frozen', False):  # Jeśli uruchomione z pliku .exe
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = os.path.join(sys._MEIPASS, "ms-playwright")
else:
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = os.path.join(os.path.dirname(__file__), "ms-playwright")
    '''

BASE_DIR = os.path.dirname(os.path.abspath(sys.executable)) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))

BASE_URL = "https://justjoin.it"
# Definiowanie danych połączenia
load_dotenv()
SERVER = os.getenv('DB_SERVER')
DATABASE = os.getenv('DB_DATABASE')
USERNAME = os.getenv('DB_USERNAME')
PASSWORD = os.getenv('DB_PASSWORD')
# Tworzenie łańcucha połączenia z bazą danych
CONNECTION_STRING = f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&charset=utf8"
# Tworzenie silnika połączenia
ENGINE = create_engine(CONNECTION_STRING)

# Import i konfiguracja loggerów
log_links = setup_logger("LINKS", script_id=1)
log_db = setup_logger("DATABASE", script_id=1)

def dynamic_dtype_mapping(dataframe):
        """
        Mapowanie dynamiczne typów danych na podstawie DataFrame.
        """
        dtype_mapping = {}
        for column, dtype in dataframe.dtypes.items():
            if dtype == 'object' or dtype.name == 'string':
                dtype_mapping[column] = NVARCHAR(255)  # Dla tekstowych wymuszamy NVARCHAR
            elif dtype.name.startswith('int'):
                dtype_mapping[column] = Integer
            elif dtype.name.startswith('float'):
                dtype_mapping[column] = Float
        return dtype_mapping

def get_existing_links():
    """
    Wczytuje wszystkie istniejące linki z bazy danych i zwraca je jako zbiór.
    """
    try:
        with ENGINE.connect() as connection:
            # Pobranie wszystkich linków z tabeli Links
            query = "SELECT link_name FROM Links"
            existing_links = pd.read_sql(text(query), connection)
            existing_links = set(existing_links['link_name'])
            log_db.info(f"Liczba wczytanych linków z bazy danych: {len(existing_links)}")
            return existing_links
    except Exception as e:
        log_db.error(f"Błąd podczas wczytywania linków z bazy danych: {e}")
        return set()

def insert_links_to_db(new_links):
    """
    Zapisuje nowe linki z datą, zachowując istniejące linki i ich daty.
    """ 
    try: 
        with ENGINE.connect() as connection:     
            links_to_add = []

            for link in new_links:
                links_to_add.append({
                    "link_name": link,
                    "link_date": datetime.now().date(),  # Dzisiejsza data
                    "is_read": False  # Domyślnie ustaw na False
                })
            df_links_to_add = pd.DataFrame(links_to_add)

            if not df_links_to_add.empty:
                dtype_mapping = dynamic_dtype_mapping(df_links_to_add)
                try:
                    df_links_to_add.to_sql('Links', con=ENGINE, if_exists='append', index=False, dtype=dtype_mapping)
                except pyodbc.IntegrityError as e:
                    if "2627" in str(e):
                        log_db.error(f"Link nie został zapisany z powodu naruszenia integralności: {e}")
                    else:
                        raise    
            else:
                log_db.warning(f"Nie zapisano nowych linków do bazy danych")

            query = "SELECT COUNT(link_name) AS count FROM Links"
            links_count = pd.read_sql(text(query), connection)
            total_links_count = links_count.iloc[0, 0]

            log_db.info(f"Zapisano nowych linków: {len(new_links)}")    
            log_db.info(f"Łącznie w bazie danych: {total_links_count}.")   
    except Exception as e:
        log_db.error(f"Błąd podczas zapisywania linków do bazy danych: {e}")       

def load_all_offers():
    try:
        # Wczytaj istniejące linki do zbioru all_links
        all_links = get_existing_links()
        todays_links = set()  # Zbiór na linki zebrane w danym dniu

        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=True)  # Ustaw headless=True dla pracy w tle
                page = browser.new_page()
                page.goto("https://justjoin.it/job-offers?orderBy=DESC&sortBy=newest")
                time.sleep(random.randint(4, 5))
                # Przewijanie strony w dół w małych krokach
                previous_count = len(all_links)
                last_logged_count = 0
                retries = 0          
                max_retries = 15  # Maksymalna liczba prób bez nowych linków
                scroll_step = 500  # Liczba pikseli do przewinięcia na raz

                while retries < max_retries:
                    # Pobierz obecne linki ofert pracy
                    current_links = page.query_selector_all("a[href^='/job-offer/']")

                    new_links = [f"{BASE_URL}{link.get_attribute('href')}" for link in current_links if link.get_attribute("href")]

                    # Filtruj nowe linki, które nie są już w all_links
                    unique_links = [link for link in new_links if link not in all_links]

                    # Dodaj unikalne linki do zbioru all_links i todays_links
                    all_links.update(unique_links)
                    todays_links.update(unique_links)

                    # Jeśli brak nowych linków, zwiększ licznik prób
                    if len(all_links) == previous_count:
                        retries += 1
                    else:
                        retries = 0  # Zresetuj licznik, gdy znajdziesz nowe linki
                        previous_count = len(all_links)

                    # Logowanie tylko nowych linków
                    if len(todays_links) != last_logged_count:
                        log_links.info(f"Zebrano nowych unikalnych linków: {len(todays_links)}")
                        last_logged_count = len(todays_links)

                    # Przewiń w dół o mały krok
                    page.evaluate(f"window.scrollBy(0, {scroll_step})")
                    time.sleep(random.randint(3, 5))  # Poczekaj, aż nowe dane zostaną załadowane
                
                browser.close()

            except Exception as e:
                log_links.error(f"Błąd Playwright w bloku głównym: {e}")

        # Zapisz linki do bazy danych
        insert_links_to_db(todays_links)

    except Exception as e:
        log_links.error(f"Błąd podczas zbierania ofert pracy: {e}")

# Uruchomienie funkcji
load_all_offers()

input('Naciśnij dowolny klawisz, aby wyłączyć konsolę')