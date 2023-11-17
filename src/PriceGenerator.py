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
        df_tmp_closed = df_tmp.query("market_day == ['closed', 'holiday']")

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
    
    # checks data to make ohlc is preserved
    def _check_ohlc(self):
        
        open_low_check = len(self.df_ohlc.query("open_price < low_price"))
        open_high_check = len(self.df_ohlc.query("open_price > high_price"))
        
        close_low_check = len(self.df_ohlc.query("close_price < low_price"))
        close_high_check = len(self.df_ohlc.query("close_price > high_price"))
        
        low_high_check = len(self.df_ohlc.query("low_price > high_price"))
        
        if open_low_check != 0: print("There are open prices lower than low price")
        if open_high_check  != 0: print("There are open prices higher than high price")
        if close_low_check != 0: print("There are close prices lower than low price")
        if close_high_check != 0: print("There are close prices higher than higher price")
        if low_high_check != 0: print("There are low prices higher than high prices")
        
        if (open_low_check == 0 and 
            open_high_check == 0 and 
            close_low_check == 0 and 
            close_high_check == 0 and 
            low_high_check == 0):
            
            print("OHLC data is checked")
        
    # add contracts names for they are rolled
    def _add_contract_name(self, df: pd.DataFrame) -> pd.DataFrame:
        
        contract = df.contract_name.drop_duplicates().to_list()[0]
        contract_spec = ["{}_{}".format(contract, i + 2) for i in range(len(df))]
        df_out = df.assign(contract = contract_spec)
        
        return df_out
    
    # calculate cumulative return
    def _cum_rtn(self, df: pd.DataFrame) -> pd.DataFrame:
        
        return(df.sort_values(
            "nyc_time").
            assign(
                open_rtn = lambda x: np.cumprod(1 + x.rtn)))
        
    def __init__(
            self,
            scale: float = 0.0002,
            loc: float = 0.000003,
            verbose = True):
        
        np.random.seed(123)
        self.verbose = True
        
        # path management
        self.parent_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
        self.data_path = os.path.join(self.parent_path, "data")
        self.file_path = os.path.join(self.data_path, "date.parquet")
        
        self.df_date = pd.read_parquet(path = self.file_path, engine = "pyarrow")
        
        # generate random data will append it and then when market is close set it to 0
        self.rand_rtns = np.random.normal(loc = loc, scale = scale, size = len(self.df_date))
        
        if self.verbose == True: print("Adding in Random Returns")
        
        # add returns and 0 out when market is closed ideally would merge but very expensive
        self.df_rtn = (self.df_date.assign(
            rtn = self.rand_rtns,
            rtn_mod = lambda x: np.where(x.market_hour == "closed", 0, x.rtn)).
        drop(columns = ["rtn"]).
        rename(columns = {"rtn_mod": "rtn"}).
        groupby(["contract_name", "nyc_time"]).
        head(1))
        
        if self.verbose == True: print("Initializing Price Data")
        
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
        
        if self.verbose == True: print("Finding roll dates")
        
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
        
        if self.verbose == True: print("Rolling Contracts")
        
        # add roll
        self.df_roll_add = (self.df_roll.assign(
            roll = self.curve_roll,
            rtn = lambda x: x.rtn + x.roll).
            drop(columns = ["roll"]))
        
        # since we roll onto a new contract we need to add the name
        self.df_roll_name = (self.df_roll_add.groupby([
            "contract_name"]).
            apply(self._add_contract_name))
        
        # combine the roll changes and add in the names of the contract
        self.df_combined = (self.df_roll_name.merge(
            right = self.df_start,
            how = "outer",
            on = self.df_start.columns.to_list()).
            sort_values(["contract_name", "nyc_time"]).
            fillna(method = "ffill").
            assign(contract = lambda x: x.contract.fillna(x.contract_name + "_1")).
            groupby(["contract_name", "nyc_time"]).
            head(1))
        
        if self.verbose == True: print("Calculating Cumulative Return to back out time series")
        
        # now calculate the cumulative returns as a multiplier for price per each contract
        self.df_cumprod = (self.df_combined.groupby([
            "contract_name"]).
            apply(self._cum_rtn).
            assign(
                open_price = lambda x: x.start_price * x.open_rtn).
            drop(columns = ["rtn", "start_price", "open_rtn"]))
        
        self.high_add = abs(np.random.normal(loc = 0.0, scale = 1, size = len(self.df_cumprod)))
        self.low_add = -1 * abs(np.random.normal(loc = 0.0, scale = 1, size = len(self.df_cumprod)))
        
        self.close_add = self.high_add + self.low_add + np.random.normal(
            loc = 0.0, scale = 0.0000001, size = len(self.df_cumprod))
        
        if self.verbose == True: print("Adding OHLC Data to time series")
        
        # create OHLC data, the random normal data gets put in furst and then zereod out for when markets
        # are closed
        self.df_ohlc = (self.df_cumprod.assign(
            high_add = self.high_add,
            low_add = self.low_add,
            close_add = self.close_add).assign(
                high_add = lambda x: np.where(x.market_hour == "closed", np.nan, x.high_add),
                low_add = lambda x: np.where(x.market_hour == "closed", np.nan, x.low_add),
                close_add = lambda x: np.where(x.market_hour == "closed", np.nan, x.close_add)).
            fillna(method = "ffill").
            fillna(method = "bfill").
            assign(
                high_price = lambda x: x.open_price + x.high_add,
                low_price = lambda x: x.open_price + x.low_add,
                close_price = lambda x: x.open_price + x.close_add).
            drop(columns = ["high_add", "low_add", "close_add"]).
            groupby(["contract", "local_time"]).
            head(1))
                
        vol_count = len(self.df_ohlc)
        buy_vol = np.round(np.random.normal(loc = 1_000_000, scale = 300_000, num = vol_count))
        sell_vol = np.round(np.random.normal())
                
        if self.verbose == True: print("Checking OHLC relationship is preserved")
        self._check_ohlc()
        
    def save_data(self):
        
        if os.path.exists(self.data_path) == False: os.makedirs(self.data_path)
        self.file_out = os.path.join(self.data_path, "prices.parquet")
        self.df_ohlc.to_parquet(path = self.file_out, engine = "pyarrow")
        
        if self.verbose == True: print("File Written to", self.file_out)
    
'''
if __name__ == "__main__":        

    generator = PriceGenerator()
    generator.save_data()
'''