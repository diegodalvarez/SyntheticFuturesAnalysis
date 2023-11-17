# Futures
Futures Project

# Repo layout
```bash
    Futures
      └───notebook
          │   analysis.ipynb
      └───src
          │   DateGenerator.py
          │   PriceGenerator.py
          │   makeData.py
      └───data
          │   dates.parquet
          │   prices.parquet
          │   prices_samples.parquet
          │   prices_samples.csv
```

src files:
* ```DateGenerator.py```: Creates data frame mask for specific contracts. When object is instantiated it defaults to required futures contract but can take an arbitrary number of contracts. Upon initialization the object makes a dataframe with correct open market days & hours. There are also functions within code to ensure that there are right number of days per year and hours per day (```_check_days_count()``` and ```_check_hours_count()``` respectively). File outputs ```dates.parquet```.
* ```PriceGenerator.py```: Creates synthetic price time series data built on top of output from ```DateGenerator.py``` using ```dates.parquet```. Synthetic time series includes price roll which is assumed to be the 15th of the first month of the quarter (if weekend or holiday then following trading day). Upon instantiation of object the code creates the time series. There is also a helper function to ensure that OHLC relationship is preserved (```_check_ohlc```). File output ```prices.parquet```
* ```makeData.py```: Creates each object and uses method ```save_data()``` within ```DateGenerator.py``` and ```PriceGenerator.py```. Then runs ```make_sample()``` function which gets the last 3 years of the ```prices.parquet``` dataset and saves to file as ```prices_sample.parquet``` and ```prices_sample.csv```. 

data files
* ```dates.parquet```: DataFrame mask for price series containing all contracts, all 5 min bars, with correct market open days and hours. Output from ```__init__()``` function of ```DateGenerator.py```.
* ```prices.parquet```: Synthetic price time series OHLC containing all contracts, accounting for change in contract (roll). Output from ```__init__()``` function of ```PriceGenerator.py```

notebook files
* ```analysis.ipynb```: Jupyter Notebook to run the calculations required. 

# Data Generation 

### Project Requirements
1. 5 min OHLC of 10y worth of data
2. 15 Futures Contracts trading in their respective time zones (5 NYC, 5 Chicago, 3 London, 3 Tokyo)
3. Contracts get rolled on the 15th or the following monday if weekend (roll cost will be an extra 2%)
4. Work on a 250 day calendar and account for time zone changes
5. Market hours are 9pm to 5pm the following days
6. Once data is simulated calculate the following

## Date Generation (```DateGenerator.py```)
### Functionality
Upon initialization of the DataGenerator object most of the parameters can be modified although they are defaulted upon initialiation. Arguments are defaulted as 
1. country_contract: type dictionary for modified the number of contracts per each country
2. end_date: type datetime preset for the first day of the current year
3. year_lookback: type int preset for 10y which creates the lookback window

### Notation
Once the object has been fully initialzied it is only prepped with dates and has the following format
contracts get generic names based on the country that is passed through following the form "Country Code" + num

### Holidays
Rather than accounting for specific holidays across market hours and the chance that market holidays may occur on weekends. Since the specific questions were 250 trading days, the following method will be used: Respective for the market's local time, there are (260 to 261) weekdays that are eligible candidates as trading days. The weekdays will be randomized and the first 250 will be considered trading days the remaining days (not including weekends) will be considered holidays. Unfortunately since there is no gaurantee that the holiday will land on a weekday in the following years every week the holidays change every year. This is to fit in accordance with the 250 day rule.

## Time Series generations (```PriceGenerator.py```)
### Price creation
Prices are created by sampling normal distributed to act as return. Rather than recursively summing values and trying to account for role, return prices are simulated and then cumulative multiplied to back a time series out. Starting price values are sampled normally with mean $1,000 +- $30. Using cumulative returns and multiplying by a starting price makes the time series look more akin to financial time series. Although random seed is set returns get zeroed out if market is closed therefore when calculations are done, returns can't be "backed-out" by using the same random seed. 

### Roll
Roll is done quarterly by the first 15th of the first month of each quarter. If the 15th is closed (weekend or holiday) it moves to the following open day. To account for roll cost an extra +-2% is added to the curve. The 2% gets added to the synthetic return data and is assumed to be rolled on the first bar or the trade open. Backwardation and contango are assumed to appear in equal proportions implying that on roll day there is a 50-50 chance that cost may be +-2%. This is done by sampling binomial distribution replacing 0s with -1s multiplying by 2 and scaling for percentage. 
