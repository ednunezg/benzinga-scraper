# Benzinga scrape

Scrape news from Benzinga web portal. Script uses selenium webdriver crawler to spin an instance of Google Chrome pointing to https://benzinga.com/stock/{stock_name} and scrape news content into a CSV.

Inevitably, Benzinga might change the DOM structure of their site which can render this script un-usable. However the core logic should stay the same and the location or names DIV elements selected need to be changed. Feel free to help me maintain this project by putting up Pull Requests for updating the scraping

## Setup

Make sure you have Python >=3.7

1. Install requirements

```
pip install -r requirements.txt
```


2. Install selenium chromedriver using brew

```
brew install --cask chromedriver
```

# Fetch news

Specify symbol/s and date
```
python scrape.py [SYMBOL OR COMMA SEPERATED LIST] -d [YYYY-MM-DD]
```

or specify symbol and start/end date

```
python scrape.py [SYMBOL OR COMMA SEPERATED LIST] -start [YYYY-MM-DD] -end [YYYY-MM-DD]
```

Example:
```
python scrape.py AAPL,GOOG,FB -start 2021-06-01 -end 2021-06-05
```