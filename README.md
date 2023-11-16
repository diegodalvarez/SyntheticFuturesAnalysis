# Futures
Futures Project

# Data Generation

### Holidays
Rather than accounting for specific holidays across market hours and then chance that market holidays may occur on weekends, since the specific questions were 250 trading days, the following method will be used. Respective for the market's local time, there are 260 to 261 weekdays that are eligble candidates as trading days. The weekdays will be randomized and the first 250 will be considered trading days the remaining days (not including weekends) will be considered holidays. Unfortunately since there is no gaurantee that the holiday will land on a weekday every week the holidays change every year. This is to fit in accordance with the 250 day rule.
