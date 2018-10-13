
import time
import json
from datetime import datetime
from bs4 import BeautifulSoup
import urllib
import os
import re
import pandas as pd
from collections import namedtuple
import glob
from pandas.io.json import json_normalize


URL = "https://www.gsmarena.com/" # domain in question
ATTR_DIRECTORY = 'model_attributes'

def lite_request(page_url, parser='html.parser'):

	response = urllib.request.urlopen(page_url)
	if response.code == 200: # good to go
		html_str = response.read()
		soup = BeautifulSoup(html_str, parser)
		return soup
	else:
		return # nothing

def get_brands():

	"""Returns dataframe of all brands"""

	all_brands_soup = lite_request(page_url = "https://www.gsmarena.com/makers.php3")

	# case is simple here, one table in page for all brands,
	# we want both the href (the url for brand page) and the brand name as it might appear commonly
	brand_table = all_brands_soup.find('table')

	# tr html tag represents a table row, we want all rows and use the find_all method on the soup object
	# this returns a list
	brand_rows = brand_table.find_all('tr')

	temp_store = []
	for row in brand_rows:
		for column in row.find_all('td'):
			brand_url = column.find('a')['href'] # this is only the path - we need it complete for it to be of any use
			brand_url = urllib.parse.urljoin(URL, brand_url) # urljoin ensures we avoid sequential forward slashes (invalid url)
			# acer-phones-59.php -> https://www.gsmarena.com/acer-phones-59.php
			brand_name = column.text
			devices_available = re.search(r'\d+',brand_name).group(0)
			brand_name = re.split(devices_available, brand_name)[0]
			temp_store.append((brand_url, brand_name, int(devices_available)))

	records = {}
	records['url'], records['brand'], records['n_models'] = zip(*temp_store)
	brands_df = pd.DataFrame(records)
	return brands_df


def get_model_attributes(model_soup, brand, **kwargs):

	time.sleep(3)
	# stagger requests, feel free to remove if user-agent and proxy enabled # TO-DO

	spotlight = model_soup.find('ul', {'class' : 'specs-spotlight-features'})
	popularity = spotlight.find('li', {'class' : re.compile(r'.+popularity')})
	hits = popularity.find('span').text # string numbers
	trend_rate = popularity.contents[1].text.strip() # string percentage

	all_tables = model_soup.find_all('table')

	# eventual storage
	model_attributes = {'model_name' : kwargs.get('model_name'),
						'model_page' : kwargs.get('model_page'),
						'brand' : brand,
						'hits' : hits,
						'trend' : trend_rate}

	for table in all_tables: # each table represents one major category

		main_cat = None

		for spec in table.find_all('tr'):

			if spec.find('th'): # table's first element contains category name
				main_cat = spec.find('th').text

			# also sub categories e.g. hardware -> storage
			sub_cat = spec.find('td', {'class' : 'ttl'})

			try:
				# adding hidden attribute descriptor
				split_meta = sub_cat.find('a')['href'].split('term=')
				if len(split_meta) == 2:
					sub_cat_meta = split_meta[1]
				else:
					sub_cat_meta = None
			except:
				sub_cat_meta = None

			sub_cat = sub_cat.text if sub_cat else None
			attribute = spec.find('td', {'class' : 'nfo'})
			# website contains other embedded info on attribute, keeping these too
			attr_value = attribute.text if attribute else None
			attr_meta = attribute['data-spec'] if attribute and 'data-spec' in attribute.attrs else None

			if main_cat not in model_attributes:
				model_attributes[main_cat] = {}

			model_attributes[main_cat][sub_cat] = {
						'attr_value' : attr_value,
						'attr_meta' : attr_meta,
						'sub_cat_meta' : sub_cat_meta}

	return model_attributes

def brand_go_ahead(brand_df, brand_name = 'Amazon'):

	"""CRITERIA TO PROCEED / HALT """

	# superfluous
	match = brand_df.query('brand == @brand_name')

	if match.empty:

		raise ValueError("Brand doesn't exist")

	else:

		os.makedirs(ATTR_DIRECTORY, exist_ok=True)
		# get all existing files
		brands_w_models = glob.glob(f'{ATTR_DIRECTORY}/*_models.json')

		# path
		fp = brand_name + '_models.json'
		fp = os.path.join(*[ATTR_DIRECTORY, fp])

		listing_page, _, expected_models = match.values[0]

		filter_data = pd.DataFrame()

		if fp in brands_w_models:

			with open(fp, 'r') as json_lines:
				local_models = [json.loads(line) for line in json_lines if line]


			models = json_normalize(local_models)
			# compare local file to number received by refreshed brand page
			if len(models) == expected_models:
				raise ValueError('No new models')

			else:
				# later used to differentiate new models
				filter_data = models

		# first page soup
		brand_soup = lite_request(listing_page)
		page_bar = brand_soup.find('div', {'class': 'nav-pages'})
		all_pages = []
		if page_bar:
			all_pages = [p['href'] for p in page_bar.find_all('a', href=True)]
			all_pages = [urllib.parse.urljoin(*[URL, p]) for p in all_pages]
		listing = brand_soup.find('div', {'id' : 'review-body'}).find_all('li')
		return listing, fp, filter_data, all_pages

def get_models(brand_name):
	try:
		data, fp, filter_data, all_pages = brand_go_ahead(brand_name)
	except Exception as e:
		return

	counter = 0
	for ix, page in enumerate([data] + all_pages):

		if ix != 0:
			soup_object = lite_request(page).find('div', {'id' : 'review-body'}).find_all('li')
		else:
			soup_object = page # loaded htnl

		all_models = []

		existing_models = set(filter_data.model_name.tolist()) \
							if filter_data.empty is False else {}

		with open(fp, 'a') as outfile:

			for model in soup_object:

				meta_info = {}
				meta_info['model_url'] = model.find('a')['href']
				meta_info['model_page'] = urllib.parse.urljoin(URL, meta_info.get('model_url'))
				meta_info['model_name'] = model.find('strong').text
				meta_info['date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

				if meta_info.get('model_name') not in existing_models: # appending to file, ensure no dupes

					counter += 1

					try:
						model_soup = lite_request(meta_info.get('model_page'))

						model_attrs = get_model_attributes(
											model_soup = model_soup,
											brand = brand_name,
												**meta_info)
						# print (model_attrs)
						model_attrs['fail'] = False

					except Exception as e:
						# for now avoid resolving
						print ('{} failed to get attribtues'.format(meta_info.get('model_name')))
						print (str(e))
						error_record = {'fail' : True, 'err' : str(e)}
						model_attrs = {**meta_info, **error_record}
					finally:
						# always write
						outfile.write(json.dumps(model_attrs) + '\n')

				else:
					print ('Skipping {}'.format(meta_info.get('model_name')))
	print ('{} NEW MODELS ADDED FOR {}'.format(counter, brand_name))


if __name__ == "__main__":
	all_brands = get_brands()
	for brand in all_brands.brand:
		# assign as global var
		get_models(brand, brands_df = all_brands)
