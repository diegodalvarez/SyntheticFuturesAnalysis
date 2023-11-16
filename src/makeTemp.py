# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 09:24:39 2023

@author: Diego
"""

import os
import pandas as pd

def main():

    parent_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    data_path = os.path.join(parent_path, "data")
    date_path = os.path.join(data_path, "date.parquet")
    cut_parquet = os.path.join(data_path, "cut.parquet")
    cut_csv = os.path.join(data_path, "cut.csv")
    
    df_tmp = pd.read_parquet(path = date_path, engine = "pyarrow")
    
    specific_contracts = (df_tmp[
        ["contract_name", "zone"]].
        groupby(["zone"]).
        head(1)
        ["contract_name"].
        to_list())
    
    df_cut = (df_tmp.assign(
        year = lambda x: x.date.dt.year).
        query("contract_name == @specific_contracts & year > 2021"))
    
    print("Adding Temp files to", data_path)
    df_cut.to_parquet(path = cut_parquet, engine = "pyarrow")
    df_cut.to_csv(path_or_buf = cut_csv)


if __name__ == "__main__":
    main()