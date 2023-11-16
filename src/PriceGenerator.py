# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 01:10:19 2023

@author: Diego
"""

import os
import numpy as np
import pandas as pd

class PriceGenerator:
    
    def __init__(
            self,
            scale: float = 0.002,
            loc: float = 0.001):
        
        # path management
        self.parent_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
        self.data_path = os.path.join(self.parent_path, "data")
        self.file_path = os.path.join(self.data_path, "date.parquet")
        
        self.df_date = pd.read_parquet(path = self.file_path, engine = "pyarrow")
        
        # generate random data will append it and then when market is close set it to 0
        self.rand_rtns = np.random.normal(loc = loc, scale = scale, size = len(self.df_date))
        
        self.df_rtn = (self.df_date.assign(
            rtn = self.rand_rtns,
            rtn_mod = lambda x: np.where(x.market_hour == "closed", 0, x.rtn)).
        drop(columns = ["rtn"]).
        rename(columns = {"rtn_mod": "rtn"}))
        
        self.contract_names = self.df_rtn["contract_name"].drop_duplicates().to_list()
        self.contract_count = len(self.contract_names)
        self.rand_start_prices = np.random.normal(loc = 1_000, scale = 30, size = self.contract_count)
        self.start_price = pd.DataFrame({
            "contract_name": self.contract_names,
            "start_price": self.rand_start_prices})
        
        self.df_start = (self.df_rtn.merge(
            right = self.start_price, how = "inner", on = ["contract_name"]).
            assign(quarter = lambda x: pd.PeriodIndex(x.local_time, freq = "Q")))
        
price_generator = PriceGenerator()
df_start = price_generator.df_start
