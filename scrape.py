import time
from lxml import html
import pandas as pd
import argparse
import shutil
import requests
import pytz
import os


from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# Webdriver
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

import util

NEW_YORK_TIMEZONE = pytz.timezone('America/New_York')

# Save all outputs to ./output
OUTPUT_DIR = './output'

# Benzinga will stop displaying many news the further days you lookback. We only allow scraping for 3 years max
MAX_LOOKBACK_WINDOW_ALLOWED = int(3 * 365)

# Timeouts + Retries
INITAL_PAGE_LOAD_TIMEOUT = 60
INITAL_PAGE_LOAD_RETRIES = 5
LOAD_MORE_NEWS_BUTTON_TIMEOUT = lambda retry_number: 0.5 * (1.7 ** retry_number)
GET_MORE_NEWS_MAX_RETRIES = 8

# Webdriver elements
LOAD_MORE_BUTTON_EL = '/html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[7]/div/div/div[1]/a/span[1]'
ARTICLE_LIST_ID = "stories-headlines"

# Directory where all results for this run are saved. Make empty initially
ALL_OUTPUT_FOLDER = '{}/LAST_RUN_ALL'.format(OUTPUT_DIR)
ERR_OUTPUT_FOLDER = '{}/ERROR'.format(OUTPUT_DIR)
if os.path.exists(ALL_OUTPUT_FOLDER):
	shutil.rmtree(ALL_OUTPUT_FOLDER)

# File where log is saved
LOG_DATA = []
LOG_OUTPUT_DIR = '{}/LOGS'.format(OUTPUT_DIR)
LOG_OUTPUT_FILEPATH =  '{}/{}.csv'.format(LOG_OUTPUT_DIR, datetime.now().strftime("%Y_%m_%d_%H_%M"))

# Make dirs
os.makedirs(LOG_OUTPUT_DIR, exist_ok=True)
os.makedirs(ALL_OUTPUT_FOLDER, exist_ok=True)
os.makedirs(ERR_OUTPUT_FOLDER, exist_ok=True)
DEBUG_ON = True

wd = None

def get_date_ny():
  return datetime.now(NEW_YORK_TIMEZONE)

def init_webdriver():
	global wd
	if wd is not None:
		return
	# Initialize webdriver
	# Chrome webdriver
	try:
		print("Initializing webdriver...", end=' ')
		webdriver_opts = webdriver.ChromeOptions()
		# Choose to open browser (headless=False) or not (headless=True)
		webdriver_opts.headless = False
		# Eager page loading strategy so we don't wait on resources we don't need
		caps = DesiredCapabilities().CHROME
		# caps["pageLoadStrategy"] = "normal"  #  complete
		caps["pageLoadStrategy"] = "none"   #  undefined
		# caps["pageLoadStrategy"] = "eager"  #  interactive
		# Init
		wd = webdriver.Chrome(desired_capabilities=caps, options=webdriver_opts)
		time.sleep(3)
		print("[DONE]")
	except Exception as e:
		print("Failed to initialize webdriver: {}".format(e))
		exit()

def dataset_range_already_exists(symbol, date_start, date_end):
	dataset_already_exists = True
	i = date_start
	while i <= date_end:
		fpath = get_dataset_filepath(symbol, i)
		if not os.path.exists(fpath):
			dataset_already_exists = False
			break
		i = i + timedelta(days=1)
	return dataset_already_exists

def log_scrape_for_stock(symbol, status, err, num_news, runtime):
	cur_datestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	LOG_DATA.append([cur_datestr, symbol, status, err, num_news, runtime])
	pd.DataFrame(
		LOG_DATA,
		columns = ['Date', 'Symbol', 'Status', 'Error', 'Number of News', 'Runtime Seconds']
	).to_csv(LOG_OUTPUT_FILEPATH, index=False)

def get_dataset_filepath(symbol, date):
	output_folder = '{}/{}'.format(OUTPUT_DIR, date.strftime("%Y%m%d"))
	return '{}/{}.csv'.format(output_folder, symbol)

def debug_print(str):
	if DEBUG_ON:
		print(str)

def clear_date_time_component(date):
	return date.replace(hour=0, minute=0, second=0, microsecond=0)

def get_headline_date_from_url(url):
	r = requests.get(url=url)

	if r.status_code < 200 or r.status_code >= 300:
		raise Exception("Got status code {} for url {}".format(r.status_code, url))

	page = r.text

	soup = BeautifulSoup(page, 'html.parser')
	date = (soup.findAll('span', {'class': 'date'})[0].text)
	tree = html.fromstring(page)
	title = tree.xpath('//*[@id="title"]')[0].text

	date = date.replace("\\n", "")
	return title, date

def bazinga_datestring_to_datetime(cur_date, bdate):
	if ('-0400' in bdate) or ('-0500' in bdate):
		return pd.Timestamp(bdate, tz='America/New_York')

	if bdate == 'a day ago':
		d = (cur_date - timedelta(days=1))
		return clear_date_time_component(d)

	if 'ago' in bdate:
		timeperiod = bdate[::-1]
		timeperiod = timeperiod[timeperiod.find(' ') + 1:][::-1]
		d = (cur_date - pd.Timedelta(timeperiod))
		return clear_date_time_component(d)

	d = pd.Timestamp(bdate, tz='America/New_York')
	return clear_date_time_component(d)

def get_benzinga_data(stock, minimum_date, maximum_date=None):
	cur_date = get_date_ny()
	if maximum_date == None: maximum_date = cur_date
	days_to_look_back = (cur_date - minimum_date).days
	benzinga_url = 'https://benzinga.com/stock/{}'.format(stock.lower())

	if days_to_look_back > MAX_LOOKBACK_WINDOW_ALLOWED:
		print("Bazing scraper only supports looking back upto {} days, date provided is {} days ago".format(
			MAX_LOOKBACK_WINDOW_ALLOWED,
			days_to_look_back
		))
		exit()

	# Load page
	try:
		timeout = INITAL_PAGE_LOAD_TIMEOUT
		debug_print("\n⭐️ Loading Benzinga URL {} [w/ {}s timeout]".format(benzinga_url, round(timeout,2)))
	
		wd.get(benzinga_url)
		time.sleep(1)
		article_list = WebDriverWait(wd, timeout).until(
			EC.presence_of_element_located((By.ID, ARTICLE_LIST_ID))
		)
		article_list_elements = article_list \
															.find_element_by_tag_name('ul') \
															.find_elements_by_tag_name("li")
		num_articles = article_list_elements is not None and len(article_list_elements) > 0
		if num_articles == 0:
			return None, "Page for stock {} - {} does not contain any articles".format(stock, benzinga_url)
	except TimeoutException as e:
		return None, "Request to benzinga page {} timed out".format(benzinga_url)
	except Exception as e:
		return None, "Request to benzinga page {} received unknown error: {}".format(benzinga_url, str(e))

	print("   |---- Done loading")

	# Start scraping
	analyst_ratings = []
	current_index = 0
	last_article_count = 0 # Number of article in last iteration
	scrape_done = False
	article_list_elements = []

	time.sleep(5)

	while not scrape_done:
		# Try clicking into 'Load More button'
		num_retry = 0
		while num_retry < GET_MORE_NEWS_MAX_RETRIES:
			num_retry = num_retry+1
			show_more_timeout = LOAD_MORE_NEWS_BUTTON_TIMEOUT(num_retry)
			try:
				debug_print("\n👈 Clicking 'Show more' button [w/ {}s timeout]".format(round(show_more_timeout, 1)))
				wait = WebDriverWait(wd, 10)
				elem = wait.until(EC.element_to_be_clickable((By.XPATH, LOAD_MORE_BUTTON_EL)))
				wd.execute_script("arguments[0].scrollIntoView();", elem)
				time.sleep(0.3)
				elem.click()
			except: pass
			try:
				wd.find_element_by_xpath('//*[@id="onesignal-popover-cancel-button"]').click()
			except: pass
			try:
				wd.find_element_by_xpath('/html/body/div[22]/div/div/button').click()
			except: pass
			try:
				wd.find_element_by_xpath('//*[@id="shreveport-ButtonElement--zs4zLUkKVVfSEq8qDkow"]').click()
			except: pass
			
			try:
				# Check if the laod more button loaded more articles
				time.sleep(show_more_timeout)
				article_list_elements = wd.find_element_by_id(ARTICLE_LIST_ID) \
																	.find_element_by_tag_name('ul') \
																	.find_elements_by_tag_name("li")

				# If we managed to get more articles, break the loop. Else we try again
				if last_article_count != len(article_list_elements):
					last_article_count = len(article_list_elements)
					break
			except Exception as e:
				debug_print("Get number of articles exception: '{}'".format(e))
				pass
		
		if num_retry==GET_MORE_NEWS_MAX_RETRIES:
			return None, "Maximum retries for the Load More News button exceded"

		# Get articles
		while current_index < last_article_count:
			try:
				article = article_list_elements[current_index]
				headline = article.find_element_by_tag_name("a").text
				url = article.find_element_by_tag_name("a").get_attribute('href')
				try:
					publisher = article.find_element_by_class_name("author").text
				except:
					publisher = article.find_elements_by_tag_name("span")[0].text
				try:
					datestr = article.find_element_by_class_name("date").text
				except:
					datestr = article.find_elements_by_tag_name("span")[1].text

				# Parse datestring into actual date
				date = bazinga_datestring_to_datetime(cur_date, datestr)

				print("  |--- {} -> '{}'".format(date, headline), end='')

				if date > maximum_date + timedelta(days=1):
					print(" [🔽SKIPPING]")
					current_index += 1
					continue
				else:
					print(" [✅]")

				if date < minimum_date:
					debug_print("\n🛑 Date {} is older than min_date {}. Stopping scrape\n".format(date, minimum_date))
					scrape_done = True
					break
				
				analyst_ratings.append([headline, url, publisher, date])						
				current_index += 1
			except Exception as e:
				return None, "Get articles exception {}".format(e)
				# We stop the infinite loop when we ran out of articles we can pull
				break
	
	analyst_ratings = pd.DataFrame(analyst_ratings, columns = ['title', 'url', 'publisher', 'date'])

	# Correct title and date from article source itself
	corrected_data = []
	debug_print("Correcting article dates by pulling from source...")
	for i in analyst_ratings.index:
		row = analyst_ratings.iloc[i]
		debug_print("  |--- Correcting '{}'".format(row['title']))
		try:
			corrected_title, corrected_date = get_headline_date_from_url(row['url'], row['title'], row['date'])
			corrected_data.append([corrected_title, corrected_date])
		except Exception as e:
			continue
	debug_print("")

	# Correct DF with data
	corrected_data = pd.DataFrame(corrected_data, columns = ['title', 'date'])
	analyst_ratings[corrected_data.columns] = corrected_data

	# Make date localized to NY time
	analyst_ratings['date'] = pd.to_datetime(analyst_ratings['date']).dt.tz_localize("America/New_York")
	return analyst_ratings, None

def save_data(data, scrape_err, symbol, start_date, end_date):
	start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
	end_date   = end_date.replace(hour=23, minute=59, second=59, microsecond=0)
	err_output_filepath = '{}/{}.txt'.format(ERR_OUTPUT_FOLDER, symbol)

	# If we have no data, save into ERROR folder
	if scrape_err is not None:
		with open(err_output_filepath, 'w') as f:
			f.write(scrape_err)
		return

	# Save all data first
	data.to_csv('{}/{}.csv'.format(ALL_OUTPUT_FOLDER, symbol))

	# Save all data in dates subsets
	i = start_date
	while i <= end_date:
		# Get the filepath
		filepath = get_dataset_filepath(symbol, i, backtesting=False)
		# If we already have news for this stock for this day saved on our harddrive, skip
		if os.path.exists(filepath):
			debug_print("|---- Dataset '{}' already exists. Won't save for this date.".format(filepath))
			i = i + timedelta(days=1)
			continue
		# Get subset of news found that fit this date range and save to file
		this_range_start = i.replace(hour=0, minute=0, second=0)
		this_range_end = i.replace(hour=23, minute=59, second=59)
		data_sub = data.loc[(data['date'] >= this_range_start) & (data['date'] < this_range_end)]
		# Save data to folder
		os.makedirs(os.path.dirname(filepath), exist_ok=True)
		data_sub.to_csv(filepath)
		# Next iteration
		i = i + timedelta(days=1)
	# Remove any error folder for this stock if one exists
	if os.path.exists(err_output_filepath):
		os.remove(err_output_filepath)

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("stock_list",        help="Enter the directory for the file containing stock list OR enter stocks seperated by comma", type=str)
	parser.add_argument("-d",                help="Enter time for single day search (in format YYYY-MM-DD)")
	parser.add_argument("-start",            help="Enter time start (in format YYYY-MM-DD)")
	parser.add_argument("-end",              help="Enter time end (in format YYYY-MM-DD)")
	args = parser.parse_args()

	# Date start is required (unless single day search -d is provided)
	if not args.d and not args.start:
		print("-start argument is required (in format YYYY-MM-DD)")
		exit()

	if not args.stock_list:
		print("stock_list argument is required")
		exit()

	if args.d:
		args.start = args.d
		args.end = args.d

	date_start = NEW_YORK_TIMEZONE.localize(datetime.strptime(args.start, "%Y-%m-%d")) \
																					.replace(hour=0, minute=0, second=0, microsecond=0)

	if util.is_valid_trading_day(date_start):
		# Get the previous trading day from date_start
		date_start = util.get_previous_trading_day_close(date_start) \
											.replace(hour=0, minute=0, second=0, microsecond=0)
	# Date end is optional
	if args.end:
		date_end = datetime.strptime(args.end, "%Y-%m-%d").replace(hour=12) \
									.astimezone(NEW_YORK_TIMEZONE) \
									.replace(hour=23, minute=59, second=59, microsecond=0)
	else:
		date_end = get_date_ny()

	# Get stock list
	if "csv" in args.stock_list or "txt" in args.stock_list:
		stock_list = util.get_stock_list_from_file(args.stock_list)
	else:
		stock_list = args.stock_list.split(',')

	symbol_no = 0
	for symbol in stock_list:
		symbol_no += 1 
		# Make the stock always upper case without any slashes
		symbol = symbol.replace('/', '.').upper()
		scrape_start_time = time.time()

		print()
		print("-------------------------------------------------------------")
		print("Scraping news for {} ({} of {})".format(symbol, symbol_no, len(stock_list)))
		print("   |--- Starting at {}".format(date_start))
		print("   |--- Ending   at {}".format(date_end))
		print()

		# If we already have a dataset for the whole date range, skip this stock
		exists = dataset_range_already_exists(symbol, date_start, date_end)
		if exists:
			log_scrape_for_stock(symbol, "SKIPPED", "", 0, 0)
			print("⏩ We already have a {} dataset with this date range. Skipping...".format(symbol))
			continue

		# Init webdriver if not done so already
		init_webdriver()
		
		# Scrape data
		result, scrape_err = get_benzinga_data(symbol, date_start, date_end)
		if (scrape_err is None) and (result is None):
			scrape_err = "Unknown error occured. Try running script again?"

		if scrape_err is not None:
			debug_print("\nGot err: {}\n".format(scrape_err))
			print("🛑 Error fetching news for {}".format(symbol))
		else:
			print("✅ Done fetching news for {}".format(symbol))

		# Save data into CSVs
		save_data(result, scrape_err, symbol, date_start, date_end)
		if scrape_err is not None:
			print("💽 Saved log message in ./outputs/ERR/{}.txt".format(symbol))
		else:
			print("💽 Done saving dataset files for {}".format(symbol))

		# Add this entry to log
		runtime = round(time.time() - scrape_start_time)
		status = "SUCCESS" if scrape_err is None else "FAIL"
		num_news = 0 if (result is None) else len(result)
		log_scrape_for_stock(symbol, status, scrape_err, num_news, runtime)


	print()
	print("-------------------------------------------------------------")
	print()
	print("👍 Finished fetching all")
	print("🔽 Saved report to {}".format(LOG_OUTPUT_FILEPATH))
	if wd is not None:
		wd.quit()

if __name__ == '__main__':
  try:
    main()
  except Exception as e:
    print("Unknown error occured:\n{}".format(e))
    print(util.get_traceback(e))