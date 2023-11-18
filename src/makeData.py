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
    
    df.to_parquet(path = parquet_path, engine = "pyarrow")
    df.to_csv(path_or_buf = csv_path)
    
def make_sample():

    parent_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    data_path = os.path.join(parent_path, "data")
    prices_path = os.path.join(data_path, "prices.parquet")
    
    df_year = (pd.read_parquet(
        path = prices_path, engine = "pyarrow").
        assign(year = lambda x: x.local_time.dt.year))
    
    # for 3y 
    years = df_year.year.drop_duplicates().sort_values().tail(3).to_list()
    print("Using these {} as 3y sample".format(years))
    
    df_3y = (df_year.query(
        "year == @years").
        drop(columns = ["year"]))
    
    save_sample(df = df_3y, data_path = data_path, file_name = "prices3y")
    
    # for 1y
    year = years[0]
    print("Using {} as 1y sample".format(year))
    
    df_1y = (df_year.query(
        "year == @year").
        drop(columns = ["year"]))

    save_sample(df = df_1y, data_path = data_path, file_name = "prices1y")
    
    df_month = (df_1y.assign(
        month = lambda x: x.local_time.dt.month))
    
    months = df_month.month.drop_duplicates().sort_values().to_list()[1:4]
    print("Using months: {} of the year {} for 3 month sample".format(months, year))
    
    # for 3 month
    df_3month = (df_month.query(
        "month == @months").
        drop(columns = ["month"]))
    
    save_sample(df = df_3month, data_path = data_path, file_name = "prices3m")
    
    month = months[0]
    print("Using month: {} of the year {} for 1 month sample".format(month, year))
    
    # for 1 month
    df_1month = (df_month.query(
        "month == @month").
        drop(columns = ["month"]))
    
    save_sample(df = df_1month, data_path = data_path, file_name = "prices1m")
    
    print("Files Saved to directory\n{}".format(data_path))

if __name__ == "__main__":
    
    generate_data()
    make_sample()
    
    