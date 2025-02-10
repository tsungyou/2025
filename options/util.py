from config import API_EODHD
import pandas as pd
import psycopg2
import numpy as np
import requests

class SymbolParams:
    def __init__(self, underlying_symbol, start_date, end_date, type_):
        self.underlying_symbol = underlying_symbol;
        self.start_date = start_date
        self.end_date = end_date
        self.type_ = type_
    def __str__(self):
        return f"SymbolParams(underlying_symbol={self.underlying_symbol}, start_date={self.start_date}, end_date={self.end_date}, type={self.type_})"


class DbCreation:
    def __init__(self):
        self.name = "factor"
    
    def url_to_dataframe(self, url):
        res = requests.get(url)

        res_json = res.json()
        res_json.keys()
        data = res_json['data']
        attributes = [i['attributes'] for i in data]
        a = pd.DataFrame(attributes)
        return a
    
    def get_exp_date_list(self, symbol: SymbolParams):
        exp_date_array = np.array([])
        index = 0;
        while True:
            
            print(symbol)
            url2 = f'https://eodhd.com/api/mp/unicornbay/options/contracts?filter[underlying_symbol]={symbol.underlying_symbol}&filter[type]={symbol.type_}&filter[exp_date_from]={symbol.start_date}&filter[exp_date_to]={symbol.end_date}&sort=exp_date&api_token={API_EODHD}'
            a2 = self.url_to_dataframe(url2)
            exp_date_list = a2['exp_date'].unique()
            if index == 0: exp_date_array = np.append(exp_date_array, exp_date_list); index +=1
            else: exp_date_array = np.append(exp_date_array, exp_date_list[1:]);
            last = exp_date_list[-1]
            
            if (pd.to_datetime(symbol.end_date) - pd.to_datetime(last)).days < 7: break
            symbol.start_date = last
        return exp_date_array

    def create_table(self):
        # CREATE TABLE expiration_dates (
        #     id SERIAL PRIMARY KEY,
        #     exp_date DATE UNIQUE  -- Ensures unique expiration dates
        # );
        pass
    
if __name__ == "__main__":
    symbol = SymbolParams("AAPL", start_date = "2024-01-01", end_date = "2025-02-26", type_='call')
    dbc = DbCreation()
    exp_date_array = dbc.get_exp_date_list(symbol)
    # functions to create database
    DB_HOST = 'localhost'
    DB_NAME = 'us'
    DB_USER = 'postgres'
    DB_PASS = 'buddyrich134'
    def get_db_connection():
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        return conn


    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT INTO expiration_dates (exp_date) VALUES (%s) ON CONFLICT (exp_date) DO NOTHING",
        [(d,) for d in exp_date_array]
    )
    conn.commit()