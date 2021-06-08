import csv
import os

import pandas_market_calendars as mcal
import traceback
import pytz
from datetime import datetime

NYSE_CALENDAR = mcal.get_calendar('NYSE').schedule(start_date='2016-01-01', end_date=datetime.now().strftime('%Y-%m-%d'))
NEW_YORK_TIMEZONE = pytz.timezone('America/New_York')

def get_stock_list_from_file(filename):
  if not os.path.isfile(filename):
    print("File {} does not exist".format(filename))
    exit(1)
  
  csv_file = open(filename, 'r', encoding='utf-8-sig')
  csv_reader = csv.reader(csv_file, delimiter=',')
  all_stocks_csv = list(csv_reader)
  
  results = []
  for row in all_stocks_csv:
    if len(row) == 0:
      continue

    left_most_col = row[0]

    if len(left_most_col.split()) > 1:
      continue

    if not left_most_col.isupper():
      continue

    results.append(left_most_col)
  return results

def get_traceback(exception):
  return "".join(traceback.TracebackException.from_exception(exception).format())


def is_valid_trading_day(date):
  cal = NYSE_CALENDAR
  date_market_open = date.replace(hour=9, minute=30, second=0, microsecond=0)
  date_market_open_utc = date_market_open.astimezone(pytz.utc)
  return len(cal.loc[(cal['market_open'] == date_market_open_utc)]) > 0


def get_previous_trading_day_close(date):
  cal = NYSE_CALENDAR
  date_market_open = date.replace(hour=9, minute=30, second=0, microsecond=0)
  date_market_open_utc = date_market_open.astimezone(pytz.utc)
  prev_trading_day_info = cal.loc[(cal['market_open'] == date_market_open_utc).shift(-1).fillna(False)].iloc[0]

  res = prev_trading_day_info['market_close'].astimezone(NEW_YORK_TIMEZONE)
  return res