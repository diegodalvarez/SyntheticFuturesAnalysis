# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 01:10:19 2023

@author: Diego
"""

import os
import numpy as np
import pandas as pd

class PriceGenerator:
    
    # function to find quarterly role
    def _find_quarterly_roll(self, df: pd.DataFrame) -> pd.DataFrame:
        
        df_tmp = df.query("month == month.min() & day == 15")
        df_tmp_open = df_tmp.query("market_day == 'open'")
        df_tmp_closed = df_tmp.query("market_day == 'closed'")
        
        if len(df_tmp_open) != 0:
            return(df_tmp_open)
            
        else:
            
            closed_date = df_tmp_closed.head(1).date.iloc[0]

            return(df.sort_values(
                "date").
                query("date > @closed_date & market_day == 'open'").
                head(1))
    
    # function to add in roll price (via rtn) of first minute of the day
    def _find_first_trade_bar(self, df: pd.DataFrame) -> pd.DataFrame:
        return(df.query("local_time == local_time.min()"))
        
    def __init__(
            self,
            scale: float = 0.002,
            loc: float = 0.001):
        
        np.random.seed(123)
        
        # path management
        self.parent_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
        self.data_path = os.path.join(self.parent_path, "data")
        self.file_path = os.path.join(self.data_path, "cut.parquet")
        
        self.df_date = pd.read_parquet(path = self.file_path, engine = "pyarrow")
        
        # generate random data will append it and then when market is close set it to 0
        self.rand_rtns = np.random.normal(loc = loc, scale = scale, size = len(self.df_date))
        
        # add returns and 0 out when market is closed ideally would merge but very expensive
        self.df_rtn = (self.df_date.assign(
            rtn = self.rand_rtns,
            rtn_mod = lambda x: np.where(x.market_hour == "closed", 0, x.rtn)).
        drop(columns = ["rtn"]).
        rename(columns = {"rtn_mod": "rtn"}))
        
        # get contract data and generate random start prices
        self.contract_names = self.df_rtn["contract_name"].drop_duplicates().to_list()
        self.contract_count = len(self.contract_names)
        self.rand_start_prices = np.random.normal(loc = 1_000, scale = 30, size = self.contract_count)
        self.start_price = pd.DataFrame({
            "contract_name": self.contract_names,
            "start_price": self.rand_start_prices})

        self.df_start = (self.df_rtn.merge(
            right = self.start_price, how = "inner", on = ["contract_name"]).
            assign(quarter = lambda x: pd.PeriodIndex(x.local_time, freq = "Q")))
        
        # this finds the days and specific minutes for the roles
        self.df_roll = (self.df_start[
            ["zone", "date", "quarter", "weekday", "market_day"]].
            assign(
                month = lambda x: x.date.dt.month,
                day = lambda x: x.date.dt.day).
            drop_duplicates().
            groupby(["zone", "quarter"]).
            apply(self._find_quarterly_roll).
            drop(columns = ["month", "day"]).
            reset_index(drop = True).
            merge(right = self.df_start, how = "inner", on = ["zone", "date", "quarter", "weekday", "market_day"]).
            groupby(["zone", "quarter", "date"]).
            apply(self._find_first_trade_bar).
            reset_index(drop = True))
        
        # to simulate roll changes add an additional 2% change to rtn 
        # assuming that the curve trades in contango and backwardation in even proportions
        # initialize a np array of 0s and 1s via binomial replace 0s with -1s multiply by 2
        # add back to returns
        self.curve_roll = np.random.binomial(n = 1, p = 0.5, size = len(self.df_roll))
        self.curve_roll[self.curve_roll == 0] = -1
        self.curve_roll = self.curve_roll * 2 / 100
        
        self.df_roll_add = (self.df_roll.assign(
            roll = self.curve_roll,
            rtn = lambda x: x.rtn + x.roll).
            drop(columns = ["roll"]).
            assign(zone_local_locater = lambda x: x.zone + " " + x.local_time.dt.strftime("%Y-%m-%d %H:%M")))
        
        self.roll_specific_datetimes = self.df_roll_add.zone_local_locater.to_list()
        
        self.df_non_roll = (self.df_start.assign(
            zone_local_locater = lambda x: x.zone + " " + x.local_time.dt.strftime("%Y-%m-%d %H:%M")).
            query("zone_local_locater != @self.roll_specific_datetimes"))
        
        self.df_combined = (pd.concat(
            [self.df_roll_add, self.df_non_roll]))
        
price_generator = PriceGenerator()
df_tmp = price_generator.df_combined
