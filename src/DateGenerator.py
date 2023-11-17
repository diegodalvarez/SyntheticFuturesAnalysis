# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 13:24:31 2023

@author: Diego
"""

import os
import string
import numpy as np
import pandas as pd
import datetime as dt

class DateGenerator:
    
    def _gen_rand_holidays(self, df: pd.DataFrame) -> pd.DataFrame:
        
        df_shuffled = df.sample(frac = 1)
        df_market_open = df_shuffled.head(250).assign(market_day = "open")
        good_dates = df_market_open.date.drop_duplicates().to_list()
        df_market_holiday = df_shuffled.query("date != @good_dates").assign(market_day = "holiday")
        
        df_out = pd.concat([df_market_open, df_market_holiday])
        return df_out
    
    # function to check that we have 250 trading days
    def _check_days_count(self):
        
        bad_data = (self.df_market.query(
            "market_day == 'open'")
            [["contract_name", "local_time"]].
            assign(
                date = lambda x: x.local_time.dt.strftime("%Y-%m-%d"), 
                year = lambda x: x.local_time.dt.year).
            drop(columns = ["local_time"]).
            drop_duplicates().
            groupby(["contract_name", "year"]).
            agg("count")
            ["date"].
            reset_index().
            query("date != 250"))
        
        if len(bad_data) == 0:
            if self.verbose == True: print("Data has correct days per year")
        else: 
            if self.verbose == True: print("Data does not have correct days per year")

    def _check_hours_count(self):
    
        # due to using year end and international times there are some days 
        # with less than a full trading day which needs to be accounted for
        first_date = self.df_market.local_time.min().date().strftime("%Y-%m-%d")
    
        bad_data = (self.df_market.query(
            "market_hour == 'open'")
            [["contract_name", "local_time"]].
            assign(
                date_hour = lambda x: x.local_time.dt.strftime("%Y-%m-%d %H"), 
                date = lambda x: x.local_time.dt.strftime("%Y-%m-%d")).
            drop(columns = ["local_time"]).
            drop_duplicates().
            groupby(["contract_name", "date"]).
            agg("count")
            ["date_hour"].
            reset_index().
            query("date_hour != 20 & date > @first_date"))
        
        if len(bad_data) == 0: 
            if self.verbose == True: print("Data has correct hours per day")
            
        else: 
            if self.verbose == True: print("Data does not have correct hours per day")    
        
    # generates market trading days / hours / timezones
    def __init__(
            self, 
            country_contract: dict = None,
            end_date: dt.datetime = dt.datetime(year = dt.date.today().year, month = 1, day = 1), 
            # default first of current year
            year_lookback: int = 10,
            verbose: bool = True):
        
        self.seed = 1234
        np.random.seed(self.seed)
        
        self.verbose = verbose
        
        self.country_contract = {
            "NYC": 1,
            "Chicago": 1,
            "London": 1,
            "Tokyo": 1,
            "Frankfurt": 1}
        
        # when contract gets passed through just ensure it meets correct formatting
        if country_contract != None:
            
            for country, num in country_contract.items(): 
                
                if country not in list(self.country_contract.keys()): 
                    raise ValueError("Only accepting NYC, Chicago, London, Tokyo, or Frankfurt contracts")
                    
                if type(num) != int:
                    raise TypeError("Only accepting int for contract count")
                    
            self.country_contract = country_contract
                    
        self.contract_count = sum(list(self.country_contract.values()))
        
        # ensure year passed through is type int
        if type(year_lookback) != int: raise TypeError("year_lookback must be type int")
        if year_lookback < 2: raise ValueError("year_lookback must be greater than a year")
        
        self.end_date = end_date
        self.start_date = dt.datetime(
            year = self.end_date.year - year_lookback, 
            month = self.end_date.month,
            day = self.end_date.day)
        
        if self.verbose == True:
            print("Generating {} contracts\n{} year lookback\nstart date: {}\nend date: {}".format(
                self.country_contract,
                year_lookback,
                self.start_date,
                self.end_date))
        
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
                
        if self.verbose == True: print("Standardizing Time to NYC")
        
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
                nyc_time = lambda x: x.utc_time.dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d %H:%M")).
            groupby(["contract_name", "nyc_time"]).
            head(1))
        
        if self.verbose == True: print("Adding Time Zone changes")
        
        # making dataframe of timezones with daylight savings to merge
        # unfortunately when using tz_convert it makes a type specific to zone so the melt changes it to class object
        # instead coerce datetimes to string and then coerce back to datetimes when joined
        self.df_timezones_add = (pd.DataFrame({
            "utc_time": self.df_time.utc_time.drop_duplicates().sort_values().to_list()}).
            assign(
                NYC = lambda x: x.utc_time.dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d %H:%M"),
                Chicago = lambda x: x.utc_time.dt.tz_convert("America/Chicago").dt.strftime("%Y-%m-%d %H:%M"),
                London = lambda x: x.utc_time.dt.tz_convert("Europe/London").dt.strftime("%Y-%m-%d %H:%M"),
                Tokyo = lambda x: x.utc_time.dt.tz_convert("Asia/Tokyo").dt.strftime("%Y-%m-%d %H:%M"),
                Frankfurt = lambda x: x.utc_time.dt.tz_convert("Europe/Berlin").dt.strftime("%Y-%m-%d %H:%M")).
            melt(id_vars = "utc_time", var_name = "zone", value_name = "local_time"))
        
        # tz_convert will occassionaly bring in some date times from previous years when switching UTC to a
        # specific datetime
        min_year = self.start_date.year
        max_year = self.end_date.year
        
        # merge time zones to get local times check for weekends and add in holidays
        self.df_date_combined = (self.df_time.merge(
            right = self.df_timezones_add, how = "inner", on = ["utc_time", "zone"]).
            drop(columns = ["utc_time"]).
            assign(
                nyc_time = lambda x: pd.to_datetime(x.nyc_time),
                local_time = lambda x: pd.to_datetime(x.local_time),
                weekday = lambda x: x.local_time.dt.weekday,
                date = lambda x: pd.to_datetime(x.local_time.dt.strftime("%Y-%m-%d")),
                year = lambda x: x.date.dt.year).
            query("year >= @min_year & year < @max_year").
            drop(columns = ["year"])) 
        
        if self.verbose == True: print("Adding Holidays")
        
        # create mask-like dataframe to join back with holiday and open market days
        self.df_holiday = (self.df_date_combined[
            ["zone", "date"]].
            drop_duplicates().
            assign(
                year = lambda x: x.date.dt.year,
                weekday = lambda x: x.date.dt.weekday).
            query("weekday != [5,6]").
            groupby(["zone", "year"]).
            apply(self._gen_rand_holidays).
            reset_index(drop = True).
            sort_values("date").
            drop(columns = ["year"]))
        
        # join back holidays on date and fillna with closed since its a weekend
        self.df_open = (self.df_date_combined.merge(
            right = self.df_holiday,
            how = "outer", 
            on = ["date", "weekday", "zone"]).
            assign(
                market_day = lambda x: x.market_day.fillna("closed"),
                hour = lambda x: x.local_time.dt.hour))
        
        if self.verbose == True: print("Adding Market Hours")
        
        # put market hours in 
        # preferably use assign but since _ror is not accepted with np.where
        # slice dataframe by hour and concat
        closed_hours = [17, 18, 19, 20]
        
        # we can get our specific market hours
        self.df_hour_open = (self.df_open.query(
            "market_day == 'open' & hour != @closed_hours").
            assign(market_hour = "open"))
        
        # then join then back to the original data and fillna as closed
        self.df_market = (self.df_open.merge(
            right = self.df_hour_open,
            how = "outer",
            on = self.df_open.columns.to_list()).
            assign(market_hour = lambda x: x.market_hour.fillna("closed")))
        
        self._check_days_count()
        self._check_hours_count()
        
    def save_data(self):
        
        self.parent_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
        self.data_path = os.path.join(self.parent_path, "data")
        
        if os.path.exists(self.data_path) == False: os.makedirs(self.data_path)
        self.file_out = os.path.join(self.data_path, "date.parquet")
        self.df_market.to_parquet(path = self.file_out, engine = "pyarrow")
        
        if self.verbose == True: print("File Written to", self.file_out)

if __name__ == "__main__":

    date_generator = DateGenerator()
    date_generator.save_data()