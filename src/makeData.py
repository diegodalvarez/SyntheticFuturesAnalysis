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
    
def make_sample():
    
    parent_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    data_path = os.path.join(parent_path, "data")
    prices_path = os.path.join(data_path, "prices.parquet")
    prices_sample_parquet_path = os.path.join(data_path, "prices_samples.parquet")
    prices_sample_csv_path = os.path.join(data_path, "prices_samples.csv")
    
    df_prices = (pd.read_parquet(
        path = prices_path, engine = "pyarrow").
        assign(year = lambda x: x.local_time.dt.year))
    
    years = df_prices.year.drop_duplicates().sort_values().tail(3).to_list()
    print("Using these {} as sample years".format(years))
    
    df_tmp = (df_prices.query(
        "year == @years").
        drop(columns = ["year"]))
    
    df_tmp.to_parquet(path = prices_sample_parquet_path, engine = "pyarrow")
    df_tmp.to_csv(path_or_buf = prices_sample_csv_path)
    
    print("Files Saved to\n{}\n{}".format(prices_sample_parquet_path, prices_sample_csv_path))

if __name__ == "__main__":
    
    generate_data()
    make_sample()
    
    