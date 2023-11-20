import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class MarketStats:

    def __init__(self, file_path: str, verbose: str = False): 

        self.df_price = pd.read_parquet(path = file_path, engine = "pyarrow")
        self.contracts = self.df_price.contract_name.drop_duplicates().to_list()
        self.min_date = self.df_price.local_time.min().date()
        self.max_date = self.df_price.local_time.max().date()
        self.verbose = verbose

    def _zero_out_roll(self, df: pd.DataFrame) -> pd.DataFrame:
        
        df_first = (df.query(
            "local_time == local_time.min()").
            assign(roll_adj = 0))
        
        df_other = (df.query(
            "local_time != local_time.min()").
            assign(roll_adj = lambda x: x.roll_unadj))
        
        df_combined = pd.concat([df_first, df_other])
        return df_combined
    
    def _get_first(self, df: pd.DataFrame) -> pd.DataFrame: 
        return(df.query("local_time == local_time.min()"))

    def _get_roll_adjusted_close(self, df: pd.DataFrame) -> pd.DataFrame:

        df_rtn_hour = (df[
            ["contract_name", "local_time", "close_price"]].
            pivot(index = "local_time", columns = "contract_name", values = "close_price").
            pct_change().
            reset_index().
            melt(id_vars = "local_time", value_name = "roll_unadj").
            fillna(0).
            merge(
                right = df[["contract_name", "local_time", "contract", "market_hour"]],
                how = "inner",
                on = ["contract_name", "local_time"]))

        df_rtn_open = df_rtn_hour.query("market_hour == 'open'")
        df_rtn_close = df_rtn_hour.query("market_hour == 'close'")

        df_open_roll_adj = (df_rtn_open.groupby(
            "contract").
            apply(self._zero_out_roll).
            reset_index(drop = True))

        df_out = pd.concat([df_rtn_close, df_open_roll_adj])

        df_wider = (df_open_roll_adj[
            ["local_time", "contract_name", "roll_unadj", "roll_adj"]].
            melt(id_vars = ["local_time", "contract_name"]).
            assign(contract = lambda x: x.contract_name + "_" + x.variable).
            drop(columns = ["contract_name", "variable"]).
            pivot(index = "local_time", columns = "contract", values = "value"))

        df_cum_longer = (np.cumprod(
            1 + df_wider).
            reset_index().
            melt(id_vars = "local_time").
            assign(
                contract_name = lambda x: x.contract.str.split("_").str[0],
                roll = lambda x: x.contract.str.split("_").str[-1]).
            drop(columns = ["contract"]))

        df_start_prices = (self.df_price[
            ["contract_name", "local_time", "close_price"]].
            groupby("contract_name").
            apply(self._get_first).
            reset_index(drop = True).
            drop(columns = ["local_time"]))

        df_roll_adj = (df_cum_longer.merge(
            right = df_start_prices, how = "inner", on = ["contract_name"]).
            assign(price = lambda x: x.close_price * x.value).
            drop(columns = ["value", "close_price"]).
            pivot(index = ["local_time", "contract_name"], columns = "roll", values = "price").
            reset_index())
        
        return df_roll_adj

    def get_roll_adjusted_close(self):
        
        self.df_roll_adj = (self.df_price.groupby([
            "contract_name"]).
            apply(self._get_roll_adjusted_close).
            reset_index(drop = True))
        
        if self.verbose == True: print("Roll Adjusted closed saved as attribute df_roll_adj")

    def plot_roll_adjusted_close(self, figssize: tuple = (30,6)):

        fig, axes = plt.subplots(ncols = len(self.contracts), figsize = figssize)

        for i, contract in enumerate(self.contracts):

            (self.df_roll_adj.query(
                "contract_name == @contract").
                set_index("local_time").
                rename(columns = {
                    "adj": "Adjusted",
                    "unadj": "Unadjusted"}).
                plot(
                    ax = axes[i],
                    title = contract,
                    ylabel = "Price ($)",
                    xlabel = "NYC Time"))

        fig.suptitle("Plotting Roll Adjusted vs. Roll Unadjusted Close Price from {} to {}".format(
            self.min_date,
            self.max_date))

        plt.tight_layout(pad = 3)
        plt.show()

    def plot_specific_roll(self, year: int):

        df_roll_year_spef = (self.df_roll_adj.assign(
            date = lambda x: pd.to_datetime(x.local_time.dt.date),
            year = lambda x: x.date.dt.year).
            query("year == @year").
            drop(columns = ["date", "year"]))

        df_roll_contract_name = df_roll_year_spef.merge(
            right = self.df_price[["local_time", "contract_name", "contract"]],
            how = "inner",
            on = ["local_time", "contract_name"])

        contracts = df_roll_year_spef.contract_name.drop_duplicates().to_list()
        fig, axes = plt.subplots(ncols = len(contracts), nrows = 2, figsize = (30,12))

        for i, contract in enumerate(contracts):

            df_contract = (df_roll_contract_name.query(
                "contract_name == @contract").
                drop(columns = ["contract_name"]))  

            spef_contracts = df_contract.contract.drop_duplicates().to_list()
            for spef_contract in spef_contracts:

                df_spef_contract = (df_contract.query(
                    "contract == @spef_contract").
                    drop(columns = ["contract"]).
                    set_index("local_time"))

                (df_spef_contract[
                    ["adj"]].
                    rename(columns = {"adj": spef_contract}).
                    plot(
                        ax = axes[0,i],
                        title = "{}\nAdjusted Roll".format(contract),
                        xlabel = "NYC Time",
                        ylabel = "Price ($)"))

                (df_spef_contract[
                    ["unadj"]].
                    rename(columns = {"unadj": spef_contract}).
                    plot(
                        ax = axes[1,i],
                        title = "Unadjusted Roll",
                        xlabel = "NYC Time",
                        ylabel = "Price ($)"))


        fig.suptitle("Plotting Adjusted vs. Unadjusted Roll coloring by specific contract from {} to {}".format(
            df_roll_contract_name.local_time.min().date(),
            df_roll_contract_name.local_time.max().date()))

        plt.tight_layout(pad = 3.5)
        plt.show()
    
    def get_volume_stats(self):

        self.daily_vol = (self.df_price[
            ["contract_name", "local_time", "buy_vol", "sell_vol", "market_hour"]].
            query("market_hour == 'open'").
            assign(date = lambda x: x.local_time.dt.date).
            drop(columns = ["local_time", "market_hour"]).
            groupby(["contract_name", "date"]).
            agg("sum").
            reset_index())
        
        self.daily_avg_vol = (self.daily_vol.drop(
            columns = ["date"]).
            groupby("contract_name").
            agg("mean"))
        
        if self.verbose == True: print("Daily volume saved as attribute daily_vol\naverage daily volume saved as daily_avg_vol")
        
    def plot_daily_volume_hist(self):

        fig, axes = plt.subplots(ncols = len(self.contracts), nrows = 2, figsize = (30,8))

        for i, contract in enumerate(self.contracts):

            df_tmp = (self.daily_vol.query(
                "contract_name == @contract & date != date.min()").
                drop(columns = ["contract_name"]).
                set_index("date"))

            buy_vol_mean = df_tmp["buy_vol"].mean()
            df_tmp["buy_vol"].plot(
                ax = axes[0,i], 
                kind = "hist", 
                title = "{}\nBuy (Mean: {})".format(contract, round(buy_vol_mean)), 
                bins = 30,
                xlabel = "Volume")
            
            axes[0,i].axvline(buy_vol_mean, color = "red")

            sell_vol_mean = df_tmp["sell_vol"].mean()
            df_tmp["sell_vol"].plot(
                ax = axes[1,i], 
                kind = "hist", 
                bins = 30,
                xlabel = "Volume",
                title = "Sell (Mean: {})".format(round(sell_vol_mean)))
            
            axes[1,i].axvline(buy_vol_mean, color = "red")

        fig.suptitle("Distribution of Daily Buy & Sell Volume summed from {} to {}".format(
            self.min_date,
            self.max_date))

        plt.tight_layout(pad = 3)
        plt.show() 

    def plot_avg_volume_bar(self):

        fig, axes = plt.subplots(figsize = (30,6))

        (self.daily_avg_vol.
            rename(columns = {
                "buy_vol": "Buy Volume", 
                "sell_vol": "Sell Volume"}).
            plot(
                kind = "bar", 
                subplots = True, 
                legend = False, 
                layout = (1,2), 
                ax = axes,
                ylabel = "Volume", 
                xlabel = "contract"))

        fig.suptitle("Average Daily Volume per Contract from {} to {}".format(
            self.min_date,
            self.max_date))

        plt.tight_layout(pad = 3)
        plt.show()

    def _get_min_max_date(self, df: pd.DataFrame) -> pd.DataFrame: 
        return(df.query(
            "nyc_time == nyc_time.max() | nyc_time == nyc_time.min()").drop_duplicates())

    def _get_rtn(self, df: pd.DataFrame) -> pd.DataFrame:
        
        df_out = (df.drop(
            columns = ["date"]).
            pivot(index = "nyc_time", columns = "contract_name", values = "adj").
            pct_change().
            dropna().
            reset_index().
            melt(id_vars = "nyc_time"))
    
        return(df_out)

    def get_avg_intraday_nyc_rtn(
        self,
        nyc_hour1: int = 9,
        nyc_hour2: int = 12):

        self.nyc_intraday_hour1, self.nyc_intraday_hour2 = nyc_hour1, nyc_hour2

        if nyc_hour1 > nyc_hour2: raise ValueError("nyc_hour1 must be less than nyc_hour2")

        df_min_max = (self.df_roll_adj.merge(
            right = self.df_price.query("zone == 'NYC'")[["nyc_time", "market_hour", "local_time"]],
            how = "inner",
            on = "local_time").
            drop(columns = ["local_time"]).
            query("market_hour == 'open'").
            assign(
                date = lambda x: x.nyc_time.dt.date,
                hour = lambda x: x.nyc_time.dt.hour).
            query("hour >= @nyc_hour1 & hour < @nyc_hour2")
            [["date", "contract_name", "adj", "nyc_time"]].
            groupby(["date", "contract_name"]).
            apply(self._get_min_max_date).
            reset_index(drop = True))

        self.intraday_rtn = (df_min_max.groupby(
            ["date", "contract_name"]).
            apply(self._get_rtn).
            drop(columns = ["contract_name", "nyc_time"]).
            reset_index().
            drop(columns = ["level_2"]))
        
        self.avg_intraday_rtn = (self.intraday_rtn.drop(
            columns = ["date"]).
            groupby("contract_name").
            mean() * 100)
        
        if self.verbose == True: print("Intraday returns saved as attribute intraday_rtn\naverage intraday return save as attribute avg_intraday_rtn")

    def plot_avg_intraday_nyc_rtn(self):

        (self.avg_intraday_rtn.sort_values(
            "value").
            plot(
                kind = "bar",
                legend = False,
                xlabel = "Contract",
                ylabel = "Return (%)",
                figsize = (12,6),
                title = "Average 5 minute Intraday Return between NYC {}:00 and {}:00 from {} to {}".format(
                    self.nyc_intraday_hour1,
                    self.nyc_intraday_hour2,
                    self.min_date,
                    self.max_date)))

        plt.tight_layout()
        plt.show()

    def plot_intraday_nyc_rtn_hist(self):    

        fig, axes = plt.subplots(ncols = len(self.contracts), figsize = (30,6))

        for i, contract in enumerate(self.contracts):

            df_tmp = (self.intraday_rtn.query(
                "contract_name == @contract").
                drop(columns = ["contract_name"]).
                set_index("date"))

            df_tmp = df_tmp * 100

            (df_tmp.plot(
                ax = axes[i],
                legend = False,
                kind = "hist",
                bins = 30))

            mean_rtn = df_tmp.mean().iloc[0]

            axes[i].axvline(mean_rtn, color = "red")
            axes[i].set_xlabel("Return (%)")
            axes[i].set_title("{} Mean: {}".format(contract, round(mean_rtn, 2)))

        fig.suptitle("Daily 5 minute Intraday returns from {}:00 to {}:00 NYC hours from {} to {}".format(
            self.nyc_intraday_hour1, 
            self.nyc_intraday_hour1,
            self.min_date,
            self.max_date))
        
        plt.tight_layout(pad = 3)
        plt.show()

    def get_roll_adjusted_prices(self):

        df_spread = (self.df_roll_adj.assign(
            spread = lambda x: x.adj- x.unadj).
            drop(columns = ["adj", "unadj"]))

        self.df_prices_adj = (self.df_price[
            ["contract_name", "local_time", "open_price", "high_price", "low_price", "close_price"]].
            melt(id_vars = ["contract_name", "local_time"], var_name = "field").
            merge(right = df_spread, how = "inner", on = ["local_time", "contract_name"]).
            assign(adj = lambda x: x.spread + x.value).
            drop(columns = ["spread"]).
            rename(columns = {"value": "unadj"}))
        
        if self.verbose == True: print("roll adjusted prices saved as attribute df_prices_adj")
    
    def get_intraday_price_range(self):
        
        self.df_intraday_range = (self.df_prices_adj.query(
            "field != 'open_price'").
            melt(id_vars = ["contract_name", "local_time", "field"], var_name = "roll").
            pivot(index = ["contract_name", "local_time", "roll"], columns = ["field"], values = "value").
            reset_index().
            assign(price_range = lambda x: (x.high_price - x.low_price) / x.close_price).
            drop(columns = ["close_price", "high_price", "low_price"]).
            merge(
                right = self.df_price[["contract_name", "local_time", "market_hour"]], 
                how = "inner", on = ["contract_name", "local_time"]).
            query("market_hour == 'open'").
            drop(columns = ["market_hour"]))
        
        self.df_intraday_range_avg = (self.df_intraday_range.drop(
            columns = ["local_time"]).
            groupby(["contract_name", "roll"]).
            agg("mean").
            reset_index().
            pivot(index = ["contract_name"], columns = "roll", values = "price_range").
            rename(columns = {
                "adj": "Adjusted", 
                "unadj": "Unadjutsted"}))
        
        if self.verbose == True: print("Intraday range saved as attribute df.intraday_rate\nintraday average range saved as attribute df_intraday_range_avg")
        
    def plot_intraday_price_range_avg(self):

        (self.df_intraday_range_avg.plot(
            kind = "bar",
            ylabel = "True Range",
            xlabel = "Contract Name",
            figsize = (20,6),
            title = "Average True Range 5 minute bars of Roll Adjusted and Roll Unadjusted from {} to {}".format(
                self.min_date,
                self.max_date)).
        legend(loc = "upper left"))

        plt.tight_layout()
        plt.show()

    def plot_intraday_range_hist(self): 

        fig, axes = plt.subplots(ncols = len(self.contracts), nrows = 2, figsize = (30,12))
        for i, contract in enumerate(self.contracts):

            df_tmp = (self.df_intraday_range.query(
                "contract_name == @contract").
                drop(columns = ["contract_name"]))

            df_tmp_wider = (df_tmp.pivot(
                index = "local_time", columns = "roll", values = "price_range").
                rename(columns = {
                    "adj": "Adjusted",
                    "unadj": "Unadjusted"}))

            df_adj_mean, df_unadj_mean = df_tmp_wider["Adjusted"].mean(), df_tmp_wider["Unadjusted"].mean()

            (df_tmp_wider[
                ["Unadjusted"]].
                plot(
                    kind = "hist",
                    ax = axes[0,i],
                    xlabel = "True Range",
                    legend = False,
                    bins = 30,
                    title = "{}\n Unadjusted (Mean: {})".format(
                        contract,
                        round(df_unadj_mean,4))))

            axes[0,i].axvline(df_unadj_mean, color = "r")

            (df_tmp_wider[
                ["Adjusted"]].
                plot(
                    kind = "hist",
                    ax = axes[1,i],
                    bins = 30,
                    xlabel = "True Range",
                    legend = False,
                    title = "Adjusted (Mean: {})".format(
                        round(df_adj_mean, 4))))

            axes[1,i].axvline(df_adj_mean, color = "r")

        fig.suptitle("Intraday 5 minute True range from Roll Adjusted and Roll Unadjsted from {} to {}".format(
            self.min_date,
            self.max_date))

        plt.tight_layout(pad = 3)
        plt.show()

    def _get_first_date(self, df: pd.DataFrame) -> pd.DataFrame: 
        return(df.query("local_time == local_time.min()"))
    
    def _get_last_date(self, df: pd.DataFrame) -> pd.DataFrame: 
        return(df.query("local_time == local_time.max()"))

    def resample_bars_daily(self):

        df_price_adj_longer = (self.df_prices_adj.melt(
            id_vars = ["contract_name", "local_time", "field"], var_name = "roll").
            assign(date = lambda x: x.local_time.dt.date))

        if self.verbose == True: print("Working on Open")
        df_open = (df_price_adj_longer.query(
            "field == 'open_price'").
            drop(columns = ["field"]).
            groupby(["contract_name", "roll", "date"]).
            apply(self._get_first_date).
            reset_index(drop = True).
            rename(columns = {"value": "open_price"}).
            drop(columns = ["local_time"]))

        if self.verbose == True: print("Working on Close")
        df_close = (df_price_adj_longer.query(
            "field == 'close_price'").
            drop(columns = ["field"]).
            groupby(["contract_name", "roll", "date"]).
            apply(self._get_last_date).
            reset_index(drop = True).
            rename(columns = {"value": "close_price"}).
            drop(columns = ["local_time"]))

        if self.verbose == True: print("Working on High")
        df_high = (df_price_adj_longer.query(
            "field == 'high_price'").
            drop(columns = ["field", "local_time"]).
            groupby(["contract_name", "roll", "date"]).
            agg("max")
            ["value"].
            reset_index().
            rename(columns = {"value": "high_price"}))

        if self.verbose == True: print("Working on Low")
        df_low = (df_price_adj_longer.query(
            "field == 'low_price'").
            drop(columns = ["field", "local_time"]).
            groupby(["contract_name", "roll", "date"]).
            agg("min")
            ["value"].
            reset_index().
            rename(columns = {"value": "low_price"}))

        self.daily_price = (df_open.merge(
            right = df_close, how = "inner", on = ["contract_name", "roll", "date"]).
            merge(right = df_high, how = "inner", on = ["contract_name", "roll", "date"]).
            merge(right = df_low, how = "inner", on = ["contract_name", "roll", "date"]).
            rename(columns = {"date": "local_date"}))
        
        if self.verbose == True: print("daily bars saved as attribute daily_price")
        
    def get_daily_true_range(self):
        
        self.df_daily_true_range = (self.daily_price.assign(
            price_range = lambda x: (x.high_price - x.low_price) / x.close_price).
            drop(columns = ["close_price", "high_price", "low_price", "open_price"]))
        
        self.df_daily_true_range_avg = (self.df_daily_true_range.drop(
            columns = ["local_date"]).
            groupby(["contract_name", "roll"]).
            agg("mean")
            ["price_range"].
            reset_index().
            pivot(index = "contract_name", columns = "roll", values = "price_range"))
        
        if self.verbose == True: print("Daily true range saved as attribute df_daily_true_range\nDaily true average range saved as attribute df_daily_true_range_avg")
        
    def plot_daily_avg_true_range(self):

        (self.df_daily_true_range_avg.rename(
            columns = {
                "adj": "Adjusted",
                "unadj": "Unadjusted"}).
            plot(
                kind = "bar",
                figsize = (12,6),
                ylabel = "True Range",
                xlabel = "Contract Name",
                title = "Daily (localalized to local time) true range from {} to {}".format(
                    self.min_date,
                    self.max_date)).
            legend(loc = "upper left"))

        plt.tight_layout()
        plt.show()

    def plot_daily_avg_true_range_hist(self):

        fig, axes = plt.subplots(ncols = len(self.contracts), nrows = 2, figsize = (30,12))

        for i, contract in enumerate(self.contracts):

            df_tmp = (self.df_daily_true_range.query(
                "contract_name == @contract").
                drop(columns = ["contract_name"]).
                set_index("local_date"))

            df_adj, df_unadj = df_tmp.query("roll == 'adj'").drop(columns = ["roll"]), df_tmp.query("roll == 'unadj'").drop(columns = ["roll"])
            df_adj_mean, df_unadj_mean = df_adj.mean().iloc[0], df_unadj.mean().iloc[0]

            (df_adj.plot(
                ax = axes[0,i],
                kind = "hist",
                bins = 30,
                xlabel = "True Range",
                legend = False,
                title = "{}\nRoll Adjusted (Mean: {})".format(
                    contract,
                    round(df_adj_mean, 4))))

            axes[0,i].axvline(df_adj_mean, color = "red")

            (df_unadj.plot(
                ax = axes[1,i],
                kind = "hist",
                bins = 30,
                legend = False,
                xlabel = "True Range",
                title = "Roll Unadjusted (Mean: {})".format(
                    round(df_unadj_mean, 4))))

            axes[1,i].axvline(df_unadj_mean, color = "red")

        fig.suptitle("Average True Range Daily (localized to local time) from {} to {}".format(
            self.min_date,
            self.max_date))


        plt.tight_layout(pad = 3)
        plt.show()

    def _calc_rtn(self, df: pd.DataFrame) -> pd.DataFrame:
        return(df.query(
            "nyc_time == nyc_time.min() | nyc_time == nyc_time.max()").
            sort_values("nyc_time").
            assign(rtn = lambda x: x.price.pct_change() * 100).
            dropna().
            drop(columns = ["nyc_time"]))

    def get_intraday_total_nyc_return(
        self,
        nyc_hour1: int = 9,
        nyc_hour2: int = 12):

        self.nyc_intraday_total_hour1, self.nyc_intraday_total_hour2 = nyc_hour1, nyc_hour2

        df_min_max = (self.df_roll_adj.merge(
            right = self.df_price.query("zone == 'NYC'")[["nyc_time", "market_hour", "local_time"]],
            how = "inner",
            on = "local_time").
            drop(columns = ["local_time"]).
            query("market_hour == 'open'").
            assign(
                date = lambda x: x.nyc_time.dt.date,
                hour = lambda x: x.nyc_time.dt.hour))

        self.intraday_total_return = (df_min_max.query(
            "hour == @nyc_hour1 | hour == @nyc_hour2").
            drop(columns = ["market_hour", "hour"]).
            melt(id_vars = ["nyc_time", "contract_name", "date"], var_name = "roll", value_name = "price").
            groupby(["date", "contract_name", "roll"]).
            apply(self._calc_rtn).
            reset_index(drop = True).
            drop(columns = ["price"]))
        
        self.intraday_avg_total_return = (self.intraday_total_return.drop(
            columns = ["date"]).
            groupby(["contract_name", "roll"]).
            agg("mean").
            reset_index())
        
        if self.verbose == True: print("Intraday total return as attribute intraday_total_return\nIntraday average total return saved as attribute intraday_avg_total_return")
        
    def plot_avg_total_intraday_nyc_rtn(self):
        
        (self.intraday_avg_total_return.pivot(
            index = ["contract_name"], columns = "roll", values = "rtn").
            rename(columns = {
                "adj": "Adjusted",
                "unadj": "Unadjusted"}).
            plot(
                kind = "bar",
                figsize = (12,6),
                xlabel = "Contract",
                ylabel = "Return (%)",
                title = "Average Return of contracts from {}:00 to {}:00 NYC Hours concurrently from {} to {}".format(
                    self.nyc_intraday_total_hour1,
                    self.nyc_intraday_total_hour2,
                    self.min_date,
                    self.max_date)))

        plt.tight_layout()
        plt.show()

    def plot_avg_total_intraday_nyc_rtn_hist(self):

        fig, axes = plt.subplots(ncols = len(self.contracts), nrows = 2, figsize = (30,12))

        for i, contract in enumerate(self.contracts):

            df_tmp = (self.intraday_total_return.query(
                "contract_name == @contract").
                drop(columns = ["contract_name"]))

            df_adj, df_unadj = df_tmp.query("roll == 'adj'"), df_tmp.query("roll == 'unadj'")
            df_adj_mean, df_unadj_mean = df_adj.rtn.mean(), df_unadj.rtn.mean()

            (df_adj.plot(
                ax = axes[0,i],
                kind = "hist",
                legend = False,
                bins = 30,
                title = "{}\nAdjusted (Mean: {}%)".format(
                    contract,
                    round(df_adj_mean,2))))

            axes[0,i].axvline(df_adj_mean, color = "red")

            (df_unadj.plot(
                ax = axes[1,i],
                kind = "hist",
                legend = False,
                bins = 30,
                title = "Unadjusted (Mean {}%)".format(
                    round(df_unadj_mean, 2))))

            axes[1,i].axvline(df_unadj_mean, color = "red")

            axes[0,i].set_xlabel("Return (%)")
            axes[1,i].set_xlabel("Return (%)")

        fig.suptitle("Average Total Period Return between {}:00 {}:00 NYC time from {} to {}".format(
            self.nyc_intraday_total_hour1,
            self.nyc_intraday_total_hour2,
            self.min_date,
            self.max_date))

        plt.tight_layout(pad = 3)
        plt.show()