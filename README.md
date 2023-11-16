# Futures
Futures Project

# Data Generation

### Project Requirements
1. 5 min OHLC of 10y worth of data
2. 15 Futures Contracts trading in their respective time zones (5 NYC, 5 Chicago, 3 London, 3 Tokyo)
3. Contracts get rolled on the 15th or the following monday if weekend (roll cost will be an extra 2%)
4. Work on a 250 day calendar and account for time zone changes
5. Once data is simulated calculate the following

### Functionality
Upon initialization of the object 

### Holidays
Rather than accounting for specific holidays across market hours and the chance that market holidays may occur on weekends. Since the specific questions were 250 trading days, the following method will be used: Respective for the market's local time, there are (260 to 261) weekdays that are eligible candidates as trading days. The weekdays will be randomized and the first 250 will be considered trading days the remaining days (not including weekends) will be considered holidays. Unfortunately since there is no gaurantee that the holiday will land on a weekday in the following years every week the holidays change every year. This is to fit in accordance with the 250 day rule.
