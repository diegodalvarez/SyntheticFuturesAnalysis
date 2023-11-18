# Futures Synthetic Price Generation & Analysis
Futures Project

## Dependencies & Codebase
For ease of use the notebook only uses ```pandas``` ```numpy``` and ```matplotlib``` with no other packages. A majority of the code if not all is written in fully vectorized pandas thus using minimal amount of for loops and relying on ```pd.groupby``` ```pd.agg``` ```pd.pivot``` ```pd.query``` and ```pd.melt```. The two main data generation files ```DateGenerator.py``` and ```PriceGenerator.py``` are fully OOP and upon instantiation of object they generate the data. Date is saved within ```.parquet``` to conserve space and data is preserved in longer format, although sample data is saved as ```.csv```. 

# Repo layout
```bash
    Futures
      └───notebook
          │   analysis.ipynb
          │   MakeReadmePlots.ipynb
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
```data``` directory is not present if repo is created from clone via git to preserve repo space. If repo is cloned ```data``` directory is created and then filled. If repo is sent via ```.zip``` then ```data``` directory is present and filled with files.  

src files:
* ```DateGenerator.py```: Creates data frame mask for specific contracts. When object is instantiated it defaults to required futures contract but can take an arbitrary number of contracts. Upon initialization the object makes a dataframe with correct open market days & hours. There are also functions within code to ensure that there are right number of days per year and hours per day (```_check_days_count()``` and ```_check_hours_count()``` respectively). File outputs ```dates.parquet```.
* ```PriceGenerator.py```: Creates synthetic price time series data built on top of output from ```DateGenerator.py``` using ```dates.parquet```. Synthetic time series includes price roll which is assumed to be the 15th of the first month of the quarter (if weekend or holiday then following trading day). Upon instantiation of object the code creates the time series. There is also a helper function to ensure that OHLC relationship is preserved (```_check_ohlc```). File output ```prices.parquet```
* ```makeData.py```: Creates each object and uses method ```save_data()``` within ```DateGenerator.py``` and ```PriceGenerator.py```. Then runs ```make_sample()``` function which gets the last 3 years of the ```prices.parquet``` dataset and saves to file as ```prices_sample.parquet``` and ```prices_sample.csv```.

data files
* ```dates.parquet```: DataFrame mask for price series containing all contracts, all 5 min bars, with correct market open days and hours. Output from ```__init__()``` function of ```DateGenerator.py```.
* ```prices.parquet```: Synthetic price time series OHLC containing all contracts, accounting for change in contract (roll). Output from ```__init__()``` function of ```PriceGenerator.py```

notebook files
* ```analysis.ipynb```: Jupyter Notebook to run the calculations required.
* ```MakeReadmePlots.ipynb```: Makes plots for ReadMe file

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
Once the object has been fully initialzied it is only prepped with dates and has the following format contracts get generic names based on the country that is passed through following the form "Country Code" + num. Later in ```PriceGenerator.py``` and ```prices.parquet``` contracts get rolled and thus new names. For example NYC1 and Chicago1 are akin to NYMEX Crude and CME Crude. By analog NYC1_1 and Chicago1_1 are akin to first NYMEX Crude contract and first CME Crude contract. For example:
| Start Date (NYC Localized)   | contract    |
|:-----------------------------|:------------|
| 2020-01-01 00:00:00          | NYC1_29     |
| 2020-01-01 01:00:00          | Chicago1_29 |
| 2020-01-15 00:00:00          | NYC1_30     |
| 2020-01-15 01:00:00          | Chicago1_30 |
| 2020-04-15 00:00:00          | NYC1_31     |
| 2020-04-15 01:00:00          | Chicago1_31 |

### Holidays
Rather than accounting for specific holidays across market hours and the chance that market holidays may occur on weekends. Since the specific questions were 250 trading days, the following method will be used: Respective for the market's local time, there are (260 to 261) weekdays that are eligible candidates as trading days. The weekdays will be randomized and the first 250 will be considered trading days the remaining days (not including weekends) will be considered holidays. Unfortunately since there is no gaurantee that the holiday will land on a weekday in the following years every week the holidays change every year. This is to fit in accordance with the 250 day rule.

Example of market trading days. Green: open, Blue: closed (weekend), red: closed (holiday) localized to local time. 
![image](https://github.com/diegodalvarez/Futures/assets/48641554/ecbb3cd0-1788-421e-a36f-7097642154d5)

## Time Series generation (```PriceGenerator.py```)
### Price creation
Prices are created by sampling normal distribution to act as return. Rather than recursively summing values and trying to account for roll, return prices are simulated and then cumulative multiplied to back out a time series. Starting price values are sampled normally with mean $1,000 +- $30. Using cumulative returns and multiplying by a starting price makes the time series look more akin to financial time series and allows roll cost to be directly added in before cumulative return. Although random seed is set, returns get zeroed out if market is closed therefore when calculations are done, returns can't be "backed-out" by using the same random seed. 
![image](https://github.com/diegodalvarez/Futures/assets/48641554/91bf4699-322f-41aa-aed4-e845b646069e)

### Roll
Roll is done quarterly by the first 15th of the first month of each quarter. If the 15th is closed (weekend or holiday) it moves to the following open day. To account for roll cost an extra +-2% is added to the curve. The 2% gets added to the synthetic return data and is assumed to be rolled on the first bar or the trade open. Backwardation and contango are assumed to appear in equal proportions implying that on roll day there is a 50-50 chance that cost may be +-2%. This is done by sampling binomial distribution replacing 0s with -1s multiplying by 2 and scaling for percentage. 
![image](https://github.com/diegodalvarez/Futures/assets/48641554/2546c8f1-2229-49fa-a0e8-39c66ee49cea)

### Buy and Sell Volume Generation
This is done by sampling from normal distribution with average ```1,000,000``` with +- ```300,000```
### OHLC creation and preservation
Once time series have been backed out through cumulative returns multiplied to starting price its considered to be ```Open Price```. To simulate low, high, and close price sample from normal distribution to get synthetic dollar price move. To ensure OHLC relationship is preserved take the absolute value and random normals and multiply by 1 and -1 respectively to get high and low. Close price is sampled from normal distribution with extremely small standard deviation and then added to close. There is a chance (extremely likely) that the dollar price change for close price is higher or lower than the high price and low price its yet to occur. Also the method ```_check_ohlc()``` ensures that the relationship is preserved.

Sample OHLC bars for a random day
![image](https://github.com/diegodalvarez/Futures/assets/48641554/0304fd13-ec40-4741-b14a-3a3a1cd8eafb)
