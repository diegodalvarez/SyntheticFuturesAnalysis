# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 22:28:30 2023

@author: Diego
"""

import os
import pandas as pd

from DateGenerator import DateGenerator
from PriceGenerator import PriceGenerator

def generate_data():
    
    DateGenerator().save_data()
    PriceGenerator().save_data()
    
def save_sample(df: pd.DataFrame, data_path: str, file_name: str):
    
    parquet_path = os.path.join(data_path, "{}.parquet".format(file_name))
    csv_path = os.path.join(data_path, "{}.csv".format(file_name))
    
    print("Writing Sample Parquet")
    df.to_parquet(path = parquet_path, engine = "pyarrow")
    print("writing Sample CSV")
    df.to_csv(path_or_buf = csv_path)
    
def make_sample():

    parent_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    data_path = os.path.join(parent_path, "data")
    prices_path = os.path.join(data_path, "prices.parquet")
    
    df_price = pd.read_parquet(path = prices_path, engine = "pyarrow")

    df_sample = (df_price.assign(
        date = lambda x: pd.to_datetime(x.local_time.dt.date),
        year = lambda x: x.date.dt.year).
        query("year == year.max()").
        drop(columns = ["date", "year"]))
    
    save_sample(df = df_sample, data_path = data_path, file_name = "prices_sample")
    

if __name__ == "__main__":
    
    generate_data()
    make_sample()
    
    