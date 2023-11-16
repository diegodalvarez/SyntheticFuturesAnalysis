# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 13:24:31 2023

@author: Diego
"""

import string
import numpy as np
import pandas as pd
import datetime as dt


#cols = [date, country, future, open, high, low, close, buy vol, sell vol]

class DataGenerator:
    
    def _gen_rand_holidays(self, df: pd.DataFrame) -> pd.DataFrame:
        
        df_shuffled = df.sample(frac = 1)
        df_market_open = df_shuffled.iloc[:250].assign(market_day = "weekday")
        df_market_holiday = df_shuffled.iloc[250:].assign(market_day = "holiday")
        
        df_out = pd.concat([df_market_open, df_market_holiday])
        return df_out
    
    # generates market trading days / hours / timezones
    def __init__(
            self, 
            country_contract: dict = None,
            end_date: dt.datetime = dt.datetime(year = dt.date.today().year, month = 1, day = 1), 
            # default first of current year
            year_lookback: int = 10):
        
        self.seed = 1234
        np.random.seed(self.seed)
        
        self.country_contract = {
            "NYC": 5,
            "Chicago": 4,
            "London": 3,
            "Tokyo": 3}
        
        # when contract gets passed through just ensure it meets correct formatting
        if country_contract != None:
            
            for country, num in country_contract.items(): 
                
                if country not in list(self.country_contract.keys()): 
                    raise ValueError("Only accepting NYC, Chicago, London, Tokyo contracts")
                    
                if type(num) != int:
                    raise TypeError("Only accepting int for contract count")
                    
            self.country_contract = country_contract
                    
            
        self.contract_count = sum(list(self.country_contract.values()))
        
        # ensure year passed through is type int
        if type(year_lookback) != int: raise TypeError("year_lookback must be type int")
        
        self.end_date = end_date
        self.start_date = dt.datetime(
            year = self.end_date.year - year_lookback, 
            month = self.end_date.month,
            day = self.end_date.day)
        
        self.dates = [
            self.start_date + dt.timedelta(minutes=x) for x in range(
                0, int((self.end_date - self.start_date).total_seconds() / 60) + 1, 5)]
        
        # define contract names
        self.contract_names = []
        for country in self.country_contract.keys():
            
            for i in range(self.country_contract[country]):
                
                contract_name = "{}{}".format(country, i+1)
                self.contract_names.append(contract_name)
                
        translation_table = str.maketrans('', '', string.digits)
                
        # initilizing longer data with datetimes
        self.df_time = (pd.DataFrame(
            columns = self.contract_names,
            index = self.dates).
            reset_index().
            melt(id_vars = "index", var_name = "contract_name").
            drop(columns = ["value"]).
            rename(columns = {"index": "utc_time"}).
            assign(
                zone = lambda x: x.contract_name.str.translate(translation_table),
                utc_time = lambda x: x.utc_time.dt.tz_localize("UTC"),
                nyc_time = lambda x: x.utc_time.dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d %H:%M")))
        
        # making dataframe of timezones with daylight savings to merge
        # unfortunately when using tz_convert it makes a type specific to zone so the melt changes it to class object
        # instead coerce datetimes to string and then coerce back to datetimes when joined
        self.df_timezones_add = (pd.DataFrame({
            "utc_time": self.df_time.utc_time.drop_duplicates().sort_values().to_list()}).
            assign(
                NYC = lambda x: x.utc_time.dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d %H:%M"),
                Chicago = lambda x: x.utc_time.dt.tz_convert("America/Chicago").dt.strftime("%Y-%m-%d %H:%M"),
                London = lambda x: x.utc_time.dt.tz_convert("Europe/London").dt.strftime("%Y-%m-%d %H:%M"),
                Tokyo = lambda x: x.utc_time.dt.tz_convert("Asia/Tokyo").dt.strftime("%Y-%m-%d %H:%M")).
            melt(id_vars = "utc_time", var_name = "zone", value_name = "local_time"))
        
        # merge time zones to get local times check for weekends and add in holidays
        self.df_date_merge = (self.df_time.merge(
            right = self.df_timezones_add, how = "inner", on = ["utc_time", "zone"]).
            drop(columns = ["utc_time"]).
            assign(
                nyc_time = lambda x: pd.to_datetime(x.nyc_time),
                local_time = lambda x: pd.to_datetime(x.local_time),
                weekday = lambda x: x.local_time.dt.weekday,
                date = lambda x: pd.to_datetime(x.local_time.dt.strftime("%Y-%m-%d"))))
        
        self.df_holiday = (self.df_date_merge[
            ["zone", "date"]].
            drop_duplicates().
            assign(
                year = lambda x: x.date.dt.year,
                weekday = lambda x: x.date.dt.weekday).
            query("weekday != [5,6]").
            groupby("year").
            apply(self._gen_rand_holidays).
            reset_index(drop = True).
            sort_values("date"))
        
data_generator = DataGenerator(
    country_contract = {"NYC": 1, "Chicago": 1, "London": 1, "Tokyo": 1},
    year_lookback = 2)
df_holiday = data_generator.df_holiday
df_date_merge = data_generator.df_date_merge