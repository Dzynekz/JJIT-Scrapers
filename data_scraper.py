import time
import random
from playwright.sync_api import sync_playwright
import os
import json
import requests
from datetime import datetime
from logger_config import setup_logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.types import NVARCHAR, Integer, Float
import pandas as pd
from sqlalchemy.types import DECIMAL
import re
import emoji
import sys


BASE_DIR = os.path.dirname(os.path.abspath(sys.executable)) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))


BASE_URL = "https://justjoin.it"
# Definiowanie danych połączenia
load_dotenv(os.path.join(BASE_DIR, ".env"))
SERVER = os.getenv('DB_SERVER')
DATABASE = os.getenv('DB_DATABASE')
USERNAME = os.getenv('DB_USERNAME')
PASSWORD = os.getenv('DB_PASSWORD')

# Tworzenie łańcucha połączenia z bazą danych
CONNECTION_STRING = f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&charset=utf8"
# Tworzenie silnika połączenia
engine = create_engine(CONNECTION_STRING, use_setinputsizes=False)

# Konfiguracja loggerów
log_general = setup_logger("GENERAL", script_id=2)
log_joboffer = setup_logger("JOB_OFFER", script_id=2)
log_db = setup_logger("DATABASE", script_id=2)


def check_language(skills, language_keywords):
    if not isinstance(skills, list):
        return False
    for skill in skills:
        if 'name' in skill and any(keyword.lower() in skill['name'].lower() for keyword in language_keywords):
            return True
    return False

def contains_unusual_characters(text):
    if not text:
        return False
    
    if isinstance(text, list):
        name_text = ''
        for name_level in text:
            for name in name_level.keys():
                name_text += name
        text = name_text

    allowed_pattern = re.compile(
        r"[^"  # Otwieramy negację
        r"a-zA-Z0-9"  # Tylko litery łacińskie i cyfry     
        r"ąćęłńóśźżĄĆĘŁŃÓŚŹŻ" #Polskie litery
        r"äöüÄÖÜßẞ" # Niemieckie litery
        r"\U0001F600-\U0001F64F"  # Emotikony twarzy
        r"\U0001F300-\U0001F5FF"  # Symbole i pikogramy
        r"\U0001F680-\U0001F6FF"  # Transport i symbole miejsc
        r"\U0001F1E0-\U0001F1FF"  # Flagi
        r"\U00002702-\U000027B0"  # Dodatkowe symbole (strzałki, gwiazdy)
        r"\U000024C2-\U0001F251"  # Enclosed characters
        r"\U0001F900-\U0001F9FF"  # Emotikony dodatkowe (np. 🧑‍💻)
        r"\U0001FA70-\U0001FAFF"  # Dodatkowe symbole i emotikony
        r"\U00002600-\U000026FF"  # Miscellaneous Symbols (☀️, ☔️, ♨️)
        r"\U0001F700-\U0001F77F"  # Alchemiczne symbole
        r"\u2000-\u200F"  # Znaki sterujące
        r"\u2028-\u202F"  # Znaki przestrzeni specjalnych
        r"\u2060-\u206F"  # Znaki formatowania
        r"\s~@&#$^*()_„“+!=[\]{}<>'\"/;½`%|\\,.?: \-"  # Dopuszczalne znaki specjalne
        r"\u2013"  # Pauza (–)
        r"\u00AE" # ®
        r"]"
    )
    non_emoji_text = ''.join([char for char in text if not emoji.is_emoji(char)])
    # Jeśli znajdziemy znak spoza dopuszczonych zakresów, zwracamy True
    return bool(re.search(allowed_pattern, non_emoji_text))
def dynamic_dtype_mapping(dataframe):
        """
        Mapowanie dynamiczne typów danych na podstawie DataFrame.
        """
        dtype_mapping = {}
        for column, dtype in dataframe.dtypes.items():
            if column == 'job_description':
                dtype_mapping[column] = NVARCHAR(None)
            if column == 'latitude' or  column == 'longitude':
                dtype_mapping[column] = DECIMAL(10, 7)
            elif dtype == 'object' or dtype.name == 'string':
                dtype_mapping[column] = NVARCHAR(255)  # Dla tekstowych wymuszamy NVARCHAR
            elif dtype.name.startswith('int'):
                dtype_mapping[column] = Integer
            elif dtype.name.startswith('float'):
                dtype_mapping[column] = Float
        return dtype_mapping
def insert_data_to_table(dataframe, table_name):
    """
    Wstawia dane do tabeli, unikając duplikatów.

    :param dataframe: DataFrame z danymi do wstawienia.
    :param table_name: Nazwa tabeli w bazie danych.
    :param lookup_columns: Lista kolumn, które identyfikują unikalny wiersz (jeśli None, porównuje wszystkie kolumny).
    """
    with engine.connect() as connection:
        # Pobierz istniejące dane z tabeli
        lookup_columns = dataframe.columns.tolist()
        existing_data_query = f"SELECT {', '.join(lookup_columns)} FROM {table_name}"
        existing_data = pd.read_sql(text(existing_data_query), connection)

        # Znajdź nowe wiersze, które nie istnieją w tabeli
        new_data = dataframe.merge(existing_data, how='left', on=lookup_columns, indicator=True)
        to_insert = new_data[new_data['_merge'] == 'left_only'].drop(columns=['_merge'])

        if not to_insert.empty:          
            # Wstaw nowe wiersze 
            dtype_mapping = dynamic_dtype_mapping(to_insert)
            to_insert.to_sql(table_name, con=engine, if_exists='append', index=False, dtype=dtype_mapping)
            log_db.info(f"Dodano {len(to_insert)} nowych wierszy do tabeli: {table_name}.")
        else:
            log_db.info(f"Brak nowych wierszy dla tabeli: {table_name}.")
def bulk_get_or_insert(dataframe, table_name, lookup_columns, id_column):
    '''
    Uniwersalna funkcja wsadowego wstawiania danych i uzyskiwania ID z obsługą wielu kolumn do porównania.

    :param dataframe: DataFrame z danymi wejściowymi.
    :param table_name: Nazwa tabeli w bazie danych.
    :param lookup_columns: Lista kolumn używanych do porównania (list).
    :param insert_columns: Kolumny do wstawienia do tabeli.
    :param id_column: Nazwa kolumny identyfikatora w tabeli.
    :return: Kolumna ID dla danych wejściowych.
    '''
    if isinstance(lookup_columns, str):
        lookup_columns = [lookup_columns]

    with engine.connect() as connection:
        # Pobranie istniejących danych z tabeli
        existing_df = pd.read_sql(text(f"SELECT * FROM {table_name}"), connection)

        for col in lookup_columns:
            if col.endswith('_norm'):
                col_strip = col.rstrip('_norm') 
                if dataframe[col_strip].dtype == 'string':     
                    existing_df[col] = existing_df[col_strip].astype(str).str.replace(r"[\s-]", "", regex=True).str.lower()
        
        # Łączenie danych, aby znaleźć brakujące wpisy
        merged_df = dataframe.merge(
            existing_df,
            how='left',
            on=lookup_columns,
            suffixes=('', '_db')  # Dodanie dynamicznych sufiksów
        )
        # Przygotowanie danych do wstawienia (filtrowanie brakujących ID)]
        selected_columns = [col.rstrip('_norm') if dataframe[col].dtype == 'string' else col for col in lookup_columns]
        # Wybierz wiersze, gdzie id_column jest NaN, i zachowaj tylko unikalne wiersze
        to_insert_df = merged_df[merged_df[id_column].isna()][selected_columns].drop_duplicates()
        # Wstawianie brakujących danych
        if not to_insert_df.empty:
            dtype_mapping = dynamic_dtype_mapping(to_insert_df)
            to_insert_df.to_sql(table_name, con=engine, if_exists='append', index=False, dtype=dtype_mapping)
            log_db.info(f"Dodano {len(to_insert_df)} nowych wierszy do tabeli: {table_name}.")
        else:
            log_db.info(f"Brak nowych wierszy dla tabeli: {table_name}.")
    
        # Aktualizacja merged_df z nowymi ID
        updated_existing_df = pd.read_sql(text(f"SELECT * FROM {table_name}"), connection)

        for col in lookup_columns:
            if col.endswith('_norm'):  # Sprawdzaj tylko kolumny kończące się na '_norm'
                col_strip = col.rstrip('_norm')  # Usuń '_norm' z nazwy
                if dataframe[col_strip].dtype == 'string':     
                    updated_existing_df[col] = updated_existing_df[col_strip].astype(str).str.replace(r"[\s-]", "", regex=True).str.lower()

        result_df = dataframe.merge(
            updated_existing_df,
            how='left',
            on=lookup_columns,
            suffixes=('', '_db')
        )
        
        result_df = result_df[[col for col in result_df.columns if not col.endswith('_db')]]
        return result_df
        #return dataframe.merge(result_df, how='left', on=list(result_df.columns.drop([id_column])))
def bulk_get_id(dataframe, table_name, lookup_columns, id_column):
    """
    Pobiera identyfikatory z wybranej tabeli na podstawie danych wejściowych.

    :param dataframe: DataFrame z danymi wejściowymi.
    :param table_name: Nazwa tabeli w bazie danych.
    :param lookup_columns: Lista kolumn używanych do porównania (list).
    :param id_column: Nazwa kolumny identyfikatora w tabeli.
    :return: DataFrame z dodaną kolumną ID.
    """
    if isinstance(lookup_columns, str):
        lookup_columns = [lookup_columns]

    with engine.connect() as connection:
        # Pobranie istniejących danych z tabeli
        query = f"SELECT {', '.join(lookup_columns + [id_column])} FROM {table_name}"
        existing_df = pd.read_sql(text(query), connection)

        # Łączenie danych, aby znaleźć istniejące ID
        result_df = dataframe.merge(
            existing_df,
            how='left',
            on=lookup_columns
        )
        return result_df


# Wczytaj linki z Bazy danych
def get_new_links():
    """
    Wczytuje wszystkie linki z atrybutem is_read = 0.
    """
    try:
        with engine.connect() as connection:
            # Pobranie wszystkich nieprzeczytanych linków
            query = "SELECT link_name, link_date FROM Links WHERE is_read = 0"
            existing_links_df = pd.read_sql(text(query), connection)
            # Konwersja do słownika {link_name: link_date}
            existing_links = existing_links_df.set_index('link_name')['link_date'].to_dict()
            log_db.info(f"Liczba wczytanych linków z bazy danych: {len(existing_links)}")
            return existing_links
    except Exception as e:
        log_db.error(f"Błąd podczas wczytywania linków z bazy danych: {e}")
        return set()

def mark_links_as_read(offers):
    if not offers:  
        return

    link_names = [offer["job_offer_id"] for offer in offers if "job_offer_id" in offer and offer["job_offer_id"]]
    if not link_names:
        return 
    
    with engine.connect() as connection:
        query = text("UPDATE Links SET is_read = 1 WHERE link_name = :link_name")
        connection.execute(query, [{"link_name": ln} for ln in link_names])
        connection.commit()
    

links_data = get_new_links()
job_offers = []

processed_links = 0
error_count = 0

# Rozpoczęcie scrapowania
with sync_playwright() as playwright:
    browser = playwright.chromium.launch(headless=True)

    for link_name, link_date in links_data.items():
       
        try:
            # Przejdź do strony oferty pracy
            page = browser.new_page()
            page.goto(link_name, timeout=60000)
            job_offer_data = {
                "job_title": "",
                "date": "",
                "city": "",
                "company_name": "",
                "field": "",
                "type_of_work": "",
                "experience_level": "",
                "operating_mode": "",
                "employment_types": [],
                "skills": [],
                "job_description": "",
                "job_offer_id": ""
            }
            page.wait_for_selector("div.MuiBox-root.css-s52zl1", timeout=5000)
            # Data
            job_offer_data['date'] = link_date

            # Job offer id
            job_offer_id = page.url.split("/")[-1]
            job_offer_data["job_offer_id"] = job_offer_id

            log_general.info(f"Rozpoczęcie przetwarzania oferty pracy: {job_offer_id}")

            # Job title
            div_element = page.query_selector("div.MuiBox-root.css-s52zl1")
            if div_element:
                job_title_element = div_element.query_selector("h1")
                if job_title_element:
                    job_offer_data["job_title"] = job_title_element.inner_text()
                else:
                    log_joboffer.error("Nie znaleziono elementu h1 dla tytułu stanowiska")
                    error_count += 1
            else:
                log_joboffer.error("Nie znaleziono elementu div dla tytułu stanowiska")
                error_count += 1

            # Field
            div_element = page.locator("div.css-1aq4u2o")
            if div_element:
                job_offer_data["field"] = div_element.inner_text()
            else:
                log_joboffer.error("Nie znaleziono elementu div dla specjalizacji")
                error_count += 1
            
            # Company name
            div_element = page.query_selector("div.MuiBox-root.css-70qvj9")
            if div_element:
                company_element = div_element.query_selector("h2")
                if company_element:
                    job_offer_data["company_name"] = company_element.inner_text()
                else:
                    log_joboffer.error("Nie znaleziono elementu h2 dla nazwy firmy")
                    error_count += 1
            else:
                log_joboffer.error("Nie znaleziono elementu div dla nazwy firmy")
                error_count += 1
            
            # City
            div_element = page.query_selector("div.MuiBox-root.css-1un5sk1")
            div_elements2 = page.query_selector_all("div.MuiBox-root.css-mswf74")
            if div_element:
                city_element = div_element.query_selector("span")
                if city_element:
                    job_offer_data["city"] = city_element.inner_text()
                else:
                    log_joboffer.error("Nie znaleziono elementu span dla miasta")
                    error_count += 1
            elif div_elements2:
                if len(div_elements2) >= 2:
                    div_element2 = div_elements2[1] 
                    job_offer_data["city"] = div_element2.inner_text() 
            else:
                log_joboffer.error("Nie znaleziono elementu div dla miasta")
                error_count += 1
                
            # Employment types
            div_element = page.query_selector("div.MuiBox-root.css-ntm2tb")
            div_elements2 = page.query_selector_all("div.MuiBox-root.css-17h1y7k div.MuiBox-root.css-pretdm")
            
            if div_element:
                employment_types_list = []
                for employment_type in div_element.query_selector_all("div.MuiBox-root.css-9sbnxm"):
                    if employment_type:
                        employment_type_data = {
                        "employment_type": "",
                        "salary": {
                            "from": None,
                            "to": None,
                            "currency": None
                            }
                        }
                        employment_type_element = employment_type.query_selector("div.MuiBox-root.css-1km0bek")
                        if employment_type_element:
                            employment_type_details = employment_type_element.query_selector_all("span")
                            if len(employment_type_details) >= 4:  # Sprawdzamy, czy jest przynajmniej 4 elementy w liście
                                employment_type_data["employment_type"] = employment_type_details[3].inner_text()
                                employment_type_data["salary"]["currency"] = employment_type_details[0].inner_text().split()[-1]  # CURRENCY
                                employment_type_data["salary"]["from"] = employment_type_details[1].inner_text()  # FROM
                                employment_type_data["salary"]["to"] = employment_type_details[2].inner_text()  # TO

                        employment_types_list.append(employment_type_data)
                job_offer_data["employment_types"] = employment_types_list   

            elif div_elements2:
                if len(div_elements2) >= 3:
                    div_element2 = div_elements2[2] 
                    div_inner = div_element2.query_selector("div.MuiBox-root.css-if24yw")
                    in_text = div_inner.query_selector("div.MuiBox-root.css-ktfb40").inner_text()
                    parts = [part.strip() for part in in_text.split(",")]
                    employment_types_list = []
                    for part in parts:
                        employment_type_data = {
                                "employment_type": "",
                                "salary": {
                                    "from": None,
                                    "to": None,
                                    "currency": None
                                    }
                                }
                        employment_type_data['employment_type'] = part
                        employment_types_list.append(employment_type_data)         
                        job_offer_data["employment_types"] = employment_types_list
            else:
                log_joboffer.error("Nie znaleziono elementu div dla typów zatrudnienia")
                job_offer_data["employment_types"] = []

            # Type of work | Experience level | Operating mode
            div_elements = page.query_selector_all("div.MuiBox-root.css-17h1y7k div.MuiBox-root.css-pretdm")
            if len(div_elements) >= 4:
                job_offer_data["type_of_work"] = div_elements[0].query_selector("div.MuiBox-root.css-if24yw div.MuiBox-root.css-ktfb40").inner_text()
                job_offer_data["experience_level"] = div_elements[1].query_selector("div.MuiBox-root.css-if24yw div.MuiBox-root.css-ktfb40").inner_text()
                job_offer_data["operating_mode"] = div_elements[3].query_selector("div.MuiBox-root.css-if24yw div.MuiBox-root.css-ktfb40").inner_text() 
            else:
                log_joboffer.error("Nie znaleziono elementu div dla typu pracy")
                error_count += 1

            # Tech stack
            div_elements = page.query_selector_all("div.MuiBox-root.css-qsaw8:has(h4)")
            if div_elements:
                tech_stack_list = []        
                for tech_stack in div_elements:
                    employment_type_data = {
                    "skill": "",
                    "level": ""
                    }
                    employment_type_data["skill"] = tech_stack.query_selector("h4").inner_text()
                    employment_type_data["level"] = tech_stack.query_selector("span").inner_text()
                    tech_stack_list.append(employment_type_data)
                job_offer_data["skills"] = tech_stack_list
            else:
                log_joboffer.error("Nie znaleziono elementu div dla tech stack")
                error_count += 1

            # Job description
            div_element = page.query_selector("div.MuiBox-root.css-1p81rs")
            if div_element:
                job_offer_data["job_description"] = div_element.inner_text()
            else:
                log_joboffer.error("Nie znaleziono elementu div dla opisu stanowiska")
                error_count += 1
           

            job_offers.append(job_offer_data)

            if processed_links < 5 and error_count > 3:
                log_joboffer.error("Prawdopodobna zmiana kodu strony - przerwanie działania scrapera")
                sys.exit(1)

            processed_links += 1

            log_general.info(f"Zakończenie scrapowania oferty pracy: {job_offer_id}")

            # Przerwa między kolejnymi linkami
            time.sleep(random.uniform(2, 3))

            # Przerwa po 70-100 linkach
            if processed_links % random.randint(70, 100) == 0:
                browser.close()
                browser = playwright.chromium.launch(headless=True)

        except Exception as e:
            log_joboffer.error(f"Błąd podczas przetwarzania linku {link_name}: {e}")
            error_count += 1

        finally:
            page.close() 

    browser.close()

log_general.info(f"Proces scrapowania zakończony. Przetworzona liczba linków: {processed_links}")

if job_offers:
    job_df = pd.DataFrame(job_offers)
    # Mapowanie level_number na level_name
    level_mapping = {
        "Nice To Have": 1,
        "Junior": 2, 
        "Regular": 3,
        "Advanced": 4,
        "Master": 5,
        "": 0
    }

    # Usunięcie informacji o strefie czasowej (psuje się w połączeniu z dataframe)
    job_df['date'] = pd.to_datetime(job_df['date'], errors='coerce').dt.tz_localize(None)

    # Zmiana typów
    job_df = job_df.astype({
    'job_title': 'string',
    'date': 'datetime64[ns]',
    'city': 'string',
    'company_name': 'string',
    'field': 'string',
    'type_of_work': 'string',
    'experience_level': 'string',
    'operating_mode': 'string',
    'job_description': 'string',
    'job_offer_id': 'string', 
    })

    # Definicja regex do usuwania emotikonów
    emoji_and_invisible_pattern = re.compile(
        "[" 
        "\U0001F600-\U0001F64F"  # Emotikony twarzy
        "\U0001F300-\U0001F5FF"  # Symbole i pikogramy
        "\U0001F680-\U0001F6FF"  # Transport i symbole miejsc
        "\U0001F1E0-\U0001F1FF"  # Flagi
        "\U00002702-\U000027B0"  # Dodatkowe symbole (strzałki, gwiazdy itp.)
        "\U000024C2-\U0001F251"  # Enclosed characters
        "\U0001F900-\U0001F9FF"  # Emotikony dodatkowe (np. 🧑‍💻)
        "\U0001FA70-\U0001FAFF"  # Dodatkowe symbole i emotikony
        "\U00002600-\U000026FF"  # Miscellaneous Symbols (☀️, ☔️, ♨️)
        "\U0001F700-\U0001F77F"  # Alchemiczne symbole
        "\u2000-\u200F"  # Znaki sterujące
        "\u2028-\u202F"  # Znaki przestrzeni specjalnych
        "\u2060-\u206F"  # Znaki formatowania
        "]+", 
        flags=re.UNICODE
    )

    # Usunięcie emotikonów z każdej kolumny tekstowej
    for col in job_df.select_dtypes(include=['string']).columns:
        if col != 'skills' and col != 'employment_types':
            job_df[col] = job_df[col].str.replace(emoji_and_invisible_pattern, '', regex=True)

    # Dodanie brakujacych informacji
    job_df['country_code'] = pd.NA
    job_df['latitude'] = pd.Series(float('nan'))
    job_df['longitude'] = pd.Series(float('nan'))
    job_df['company_url'] = pd.NA
    job_df['company_size'] = pd.NA

    job_df['is_english_required'] = (
        job_df['skills'].apply(lambda skills: check_language(skills, ['English', 'Angielski'])) |
        job_df[['job_description']].apply(lambda row: any(row.astype(str).str.contains('English|Angielski', regex=True, na=False)), axis=1)
    )
    job_df['is_german_required'] = (
        job_df['skills'].apply(lambda skills: check_language(skills, ['German', 'Niemiecki'])) |
        job_df[['job_description']].apply(lambda row: any(row.astype(str).str.contains('German|Niemiecki', regex=True, na=False)), axis=1)
    )

    # Zmiana typów
    job_df = job_df.astype({
    'country_code': 'string',
    'company_url': 'string',
    'company_size': 'string',
    'is_english_required': 'boolean',
    'is_german_required': 'boolean'
    })

    job_df = job_df.rename(columns={'city': 'city_name', 'experience_level': 'experience_level_name', 'job_offer_id': 'job_offer_name', 'field':'field_name',
                            'operating_mode':'operating_mode_name', 'type_of_work': 'work_type_name'})

    # Normalizacja i przygotowanie danych do wstawienia do tabel

    # Cities 
    job_df['city_name'] = job_df['city_name'].str.replace(r'\d', '', regex=True).str.title().str.strip()
    job_df['country_code'] = job_df['country_code'].str.upper().str.strip()
    job_df[f"city_name_norm"] = job_df['city_name'].astype(str).str.replace(r"[\s-]", "", regex=True).str.lower()
    job_df[f"country_code_norm"] = job_df['country_code'].astype(str).str.replace(r"[\s-]", "", regex=True).str.lower()

    with engine.connect() as connection:
        existing_cities = pd.read_sql(text("SELECT city_name, district_id, is_polish FROM Cities"), connection)

    cities_df = job_df[['city_name', 'city_name_norm']]# .drop_duplicates()
    cities_df = cities_df.merge(existing_cities, on='city_name', how='left')
    cities_df.loc[cities_df['district_id'].notna(), ['country_code', 'country_code_norm']] = ['PL', 'pl']
    cities_df[['city_name', 'country_code', 'city_name_norm', 'country_code_norm']] = (
                                    cities_df[['city_name', 'country_code', 'city_name_norm', 'country_code_norm']].astype('string'))
    cities_df.loc[(cities_df['country_code'] == 'PL') & (cities_df['is_polish'].isna()), 'is_polish'] = True
    cities_df['is_polish'] = cities_df['is_polish'].astype('boolean')
    cities_df['is_polish'] = cities_df['is_polish'].fillna(False)
    cities_df['district_id'] = cities_df['district_id'].astype('Int64') # Int64 pozwala na wartości NA (zwykły int nie pozwala)
    cities_df = cities_df.drop_duplicates()

    # Fields
    job_df['field_name'] = job_df['field_name'].str.capitalize().str.strip()
    fields_df = job_df[['field_name']].drop_duplicates().astype('string')

    # Companies
    job_df['company_name'] = job_df['company_name'].str.strip()
    companies_df = job_df[['company_name']].drop_duplicates().astype('string')

    # Experience_levels
    job_df['experience_level_name'] = job_df['experience_level_name'].str.capitalize().str.strip()
    experience_levels_df = job_df[['experience_level_name']].drop_duplicates().astype('string')

    # Dates
    job_df['date_full'] = pd.to_datetime(job_df['date']).dt.date  
    job_df['year'] = pd.to_datetime(job_df['date']).dt.year
    job_df['month'] = pd.to_datetime(job_df['date']).dt.month
    job_df['day'] = pd.to_datetime(job_df['date']).dt.day
    job_df['month_name'] = pd.to_datetime(job_df['date']).dt.strftime('%B').str.capitalize().astype('string')
    job_df['day_name'] = pd.to_datetime(job_df['date']).dt.strftime('%A').str.capitalize().astype('string')
    dates_df = job_df[['date_full','year','month','day','month_name','day_name']].drop_duplicates()


    # Operating_modes
    job_df['operating_mode_name'] = job_df['operating_mode_name'].str.title().str.strip()
    operating_modes_df = job_df[['operating_mode_name']].drop_duplicates().astype('string')

    # Work_types
    job_df['work_type_name'] = job_df['work_type_name'].str.title().str.strip()
    work_types_df = job_df[['work_type_name']].drop_duplicates().astype('string')  



    # Pobieranie i  ściąganie danych z tabel słownikowych
    cities_df = bulk_get_or_insert(cities_df, 'Cities', ['city_name_norm', 'district_id', 'is_polish'], 'city_id')
    cities_df = cities_df[['city_name_norm','country_code','city_id']].drop_duplicates()
    fields_df = bulk_get_or_insert(fields_df, 'Fields', ['field_name'], 'field_id')
    companies_df = bulk_get_or_insert(companies_df, 'Companies', ['company_name'], 'company_id') # , 'company_url', 'company_size'
    companies_df = companies_df.drop_duplicates()
    companies_df = companies_df.sort_values('company_id', ascending=False).drop_duplicates(subset='company_name', keep='first')
    experience_levels_df = bulk_get_or_insert(experience_levels_df, 'Experience_Levels', ['experience_level_name'], 'experience_level_id')
    dates_df = bulk_get_or_insert(dates_df, 'Dates', ['date_full', 'year', 'month', 'day', 'month_name', 'day_name'], 'date_id')
    operating_modes_df = bulk_get_or_insert(operating_modes_df, 'Operating_Modes', ['operating_mode_name'], 'operating_mode_id')
    work_types_df = bulk_get_or_insert(work_types_df, 'Work_Types', ['work_type_name'], 'work_type_id')


    # Praca na dataframe Job_Offers
    job_df['job_offer_name'] = job_df['job_offer_name'].str.lower()
    job_df['job_title'] = job_df['job_title'].str.title()

    job_offers_df = job_df[['field_name', 'city_name', 'city_name_norm', 'company_name', 'company_size', 'company_url', 'date_full', 'year', 'month', 'day',
                            'month_name', 'day_name', 'work_type_name', 'operating_mode_name', 'experience_level_name', 'job_offer_name',
                            'job_title', 'country_code', 'is_english_required', 'is_german_required' , 'latitude', 'longitude', 'job_description']] #

    # --------------------------------------------------------------------------------------------------------------------------------------------------
    job_offers_with_countries = job_offers_df.merge(cities_df[['city_name_norm', 'country_code','city_id']], how='left', on='city_name_norm',suffixes=('', '_from_cities')) 
    job_offers_with_countries['country_code'] = job_offers_with_countries['country_code'].fillna(job_offers_with_countries['country_code_from_cities'])
    job_offers_with_countries = job_offers_with_countries.drop(columns=['country_code_from_cities'])
    job_offers_df = job_offers_with_countries
    job_offers_df.rename(columns={'city_id_from_cities': 'city_id'}, inplace=True) 
    '''
    # Check for rows where city_id is null 
    null_city_rows = job_offers_df[job_offers_df['city_id'].isna()]
    if not null_city_rows.empty:
        print("\nRows with missing city_id:")
        print(job_offers_df[job_offers_df['city_id'].isna()])
    input(int())
    '''
    job_offers_df = job_offers_df.merge(fields_df, how='inner', on=['field_name'])
    job_offers_df = job_offers_df.merge(companies_df, how='inner', on=['company_name']) # , 'company_url', 'company_size'
    job_offers_df = job_offers_df.merge(experience_levels_df, how='inner', on=['experience_level_name'])
    job_offers_df = job_offers_df.merge(dates_df, how='inner', on=['date_full', 'year', 'month', 'day', 'month_name', 'day_name'])
    job_offers_df = job_offers_df.merge(operating_modes_df, how='inner', on=['operating_mode_name'])
    job_offers_df = job_offers_df.merge(work_types_df, how='inner', on=['work_type_name'])

    job_offers_to_insert = job_offers_df[['field_id', 'city_id', 'company_id', 'date_id', 'work_type_id',
                                            'operating_mode_id', 'experience_level_id', 'job_offer_name',
                                            'job_title', 'country_code', 
                                            'is_english_required', 'is_german_required', 'latitude', 'longitude', 'job_description']] # 

    insert_data_to_table(job_offers_to_insert, 'Job_Offers')

    # Praca na dataframe Job_Offers_Skills
    job_offers_skills_df = job_df[['job_offer_name', 'skills']].explode('skills')
    skills_normalized_df = pd.json_normalize(job_offers_skills_df['skills'])
    job_offers_skills_df = pd.concat([job_offers_skills_df[['job_offer_name']].reset_index(drop=True), skills_normalized_df], axis=1).rename(columns={'skill': 'skill_name', 'level': 'level_name'})
    job_offers_skills_df = job_offers_skills_df.astype({'job_offer_name': 'string', 'skill_name': 'string'})

    for col in job_offers_skills_df.select_dtypes(include=['string']).columns:
        job_offers_skills_df[col] = job_offers_skills_df[col].str.replace(emoji_and_invisible_pattern, '', regex=True)

        # Filtrowanie wierszy z niedozwolonymi znakami
    job_offers_skills_df = job_offers_skills_df[
        ~job_offers_skills_df['job_offer_name'].apply(contains_unusual_characters) &
        ~job_offers_skills_df['skill_name'].apply(contains_unusual_characters)
    ]


    job_offers_skills_df['level_number'] = job_offers_skills_df['level_name'].map(level_mapping)

    skills_df = job_offers_skills_df[['skill_name']].drop_duplicates().astype('string')


    levels_df = job_offers_skills_df[['level_name', 'level_number']].drop_duplicates().astype({'level_name': 'string', 'level_number': 'int'})

    skills_df = bulk_get_or_insert(skills_df, 'Skills', ['skill_name'], 'skill_id')
    levels_df = bulk_get_or_insert(levels_df, 'Levels', ['level_name', 'level_number'], 'level_id')

    job_offers_id_df = job_offers_skills_df['job_offer_name'].drop_duplicates()
    job_offers_id_df = job_offers_id_df.to_frame().astype('string') 

    job_offers_id_df = bulk_get_id(job_offers_id_df, 'Job_Offers', ['job_offer_name'], 'job_offer_id')

    job_offers_skills_df = job_offers_skills_df.merge(skills_df, how='inner', on=['skill_name'])
    job_offers_skills_df = job_offers_skills_df.merge(levels_df, how='inner', on=['level_name', 'level_number'])
    job_offers_skills_df = job_offers_skills_df.merge(job_offers_id_df, how='inner', on=['job_offer_name'])

    job_offers_skills_to_insert = job_offers_skills_df[['job_offer_id', 'skill_id', 'level_id']]


    insert_data_to_table(job_offers_skills_to_insert, 'Job_Offers_Skills')

    # Praca na dataframe Job_Offers_Salaries
    job_offers_salaries_df = job_df[['job_offer_name', 'employment_types']].explode('employment_types')
    employment_types_normalized_df = pd.json_normalize(job_offers_salaries_df['employment_types'])
    job_offers_salaries_df = pd.concat([job_offers_salaries_df[['job_offer_name']].reset_index(drop=True), employment_types_normalized_df], axis=1)  

    job_offers_salaries_df = job_offers_salaries_df.rename(columns={'salary.from':'salary_from','salary.to':'salary_to',
                                                                    'salary.currency':'salary_currency', 'employment_type': 'employment_type_name'})

    job_offers_salaries_df['salary_from'] = job_offers_salaries_df['salary_from'].str.replace(' ', '', regex=False)
    job_offers_salaries_df['salary_to'] = job_offers_salaries_df['salary_to'].str.replace(' ', '', regex=False)

    job_offers_salaries_df = job_offers_salaries_df.astype({'salary_from': 'float', 'salary_to': 'float','job_offer_name': 'string', 'salary_currency': 'string', 'employment_type_name': 'string'})
    job_offers_salaries_df['salary_currency'] = job_offers_salaries_df['salary_currency'].str.upper()

    for col in job_offers_salaries_df.select_dtypes(include=['string']).columns:
        job_offers_salaries_df[col] = job_offers_salaries_df[col].str.replace(emoji_and_invisible_pattern, '', regex=True)   

    employment_types_df = job_offers_salaries_df['employment_type_name'].drop_duplicates().astype('string')
    employment_types_df= employment_types_df.to_frame() 

    salaries_df = job_offers_salaries_df[['salary_from','salary_to','salary_currency']].drop_duplicates()
    salaries_df = salaries_df.astype({'salary_from': 'float', 'salary_to': 'float', 'salary_currency': 'string'})


    salaries_df['salary_from'] = salaries_df['salary_from'].where(pd.notnull(salaries_df['salary_from']), None)
    salaries_df['salary_to'] = salaries_df['salary_to'].where(pd.notnull(salaries_df['salary_to']), None)
    salaries_df['salary_currency'] = salaries_df['salary_currency'].where(pd.notnull(salaries_df['salary_currency']), None)

    employment_types_df = bulk_get_or_insert(employment_types_df, 'Employment_Types', ['employment_type_name'], 'employment_type_id')
    salaries_df = bulk_get_or_insert(salaries_df, 'Salaries', ['salary_from', 'salary_to', 'salary_currency'], 'salary_id')

    job_offers_salaries_df = job_offers_salaries_df.merge(employment_types_df, how='inner', on=['employment_type_name'])
    job_offers_salaries_df = job_offers_salaries_df.merge(salaries_df, how='inner', on=['salary_from', 'salary_to', 'salary_currency'])
    job_offers_salaries_df = job_offers_salaries_df.merge(job_offers_id_df, how='inner', on=['job_offer_name'])

    job_offers_salaries_to_insert = job_offers_salaries_df[['job_offer_id', 'salary_id', 'employment_type_id']]

    insert_data_to_table(job_offers_salaries_to_insert, 'Job_Offers_Salaries')

    for offer in job_offers:
        offer["job_offer_id"] = "https://justjoin.it/job-offer/" + offer["job_offer_id"]

    mark_links_as_read(job_offers)
    job_offers.clear() 
    log_general.info(f"Import do bazy danych zakończony. Liczba dodanych ofert pracy: {len(job_offers)}")
