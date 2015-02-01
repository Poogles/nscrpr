#!/usr/bin/python

import json
import requests
import xmltodict
import redis
import hashlib
import pyteaser
import logging
from goose import Goose
from elasticsearch import Elasticsearch


def scrape(url):
	raw_page = requests.get(url, headers={'User-agent': 'nscrpr'}).text.encode("utf-8")
	logging.warning('Fetching: %s, Response: %s' % (url, requests.codes.ok))
	return raw_page


def parse(raw):
	article_list = []
	news_dict = xmltodict.parse(raw)
	logging.warning('Parsing fetch through xmltodict.')

	for x in news_dict['urlset']['url']:
		try:
			location = x['loc']
		except:
			location = ""
		try:
			publication = x['n:news']['n:publication']['n:name']
		except KeyError: 
			publication = x['news:news']['news:publication']['news:name']
		except:
			publication = ""
		try:
			title = x['n:news']['n:title']
		except KeyError:
			title = x['news:news']['news:title']
		except:
			title = ""
		try:
			publication_date = x['n:news']['n:publication_date']
		except KeyError:
			publication_date = x['news:news']['news:publication_date']
		except:
			publication_date = ""
		try:
			keywords = x['n:news']['n:keywords']
		except KeyError:
			keywords = x['news:news']['news:keywords']
		except:
			keywords = ""
		logging.info('Parsing over: ' + location)
		output = json.dumps({'location': location,
							'publication': publication,
							'title': title,
							'publication_date': publication_date,
							'keywords': keywords})
		article_list.append(output)
	logging.warning('Completed site fetch.')
	return article_list


def cleaner(json_list_raw):
	logging.warning('Deduping through Redis.')
	# Redis connection string.
	rds = redis.StrictRedis(host='localhost', port=6379, db=0)

	# Define a clean list.
	json_list_deduped = []

	# Filter dat shit.
	for article in json_list_raw:
		jarticle = json.loads(article)
		rkey = hashlib.md5(jarticle['location']).hexdigest()
		if rds.get(rkey) == None:
			json_list_deduped.append(article)
			logging.warning('Found unindexed article through Redis: ' + jarticle['location'])
		else:
			pass

	logging.warning('Finished deduping through Redis.')
	return json_list_deduped



def grab(location, keywords, publication, publication_date, title):
	goose = Goose()
	try:
		raw_article = goose.extract(url=location)
		description =  raw_article.meta_description.encode("utf8")
		article =  raw_article.cleaned_text.encode("utf8")

		summary = pyteaser.SummarizeUrl(location)		
		output = json.dumps({
			"title": title, 
			"keywords": keywords, 
			"publication": publication,
			"publication_date": publication_date,
			"description": description, 
			"source" : location, 
			"article": article, 
			"summary": summary})
		logging.warning('Succesfully grabbed through Goose.')
		logging.warning('Location: %s, Publication: %s' % (location, publication))
		return output
	except:
		logging.critical('Unable to get article through Goose.')
		logging.critical('Location: %s, Publication: %s' % (location, publication))
		return None


def index(blob):
	es = Elasticsearch()
	result = es.index(index="nscrpr", doc_type="nscrpr.article", body=blob)
	logging.warning('Indexed into Elasticsearch')
	logging.warning('Response: %s' % (result))

	return result


def main(site):
	# Grab raw pages.
	raw_pages = scrape(site)
	logging.warning('Grabbing raw pages.')

	# Parse that XML to tasty json.
	parsed_article_list = parse(raw_pages)
	logging.warning('Parising XML to JSON.')

	# Clean it up.
	logging.warning('Deduping.')
	deduped_list = cleaner(parsed_article_list)

	# Go out and fetch stuff.
	for article in deduped_list:
		jarticle =json.loads(article)

		location = jarticle['location']
		publication_date = jarticle['publication_date']
		keywords = jarticle['keywords']
		title = jarticle['title']
		publication = jarticle['publication']

		# Go out, grab the article and summerise.
		blob = grab(location, keywords, publication, publication_date, title)

		rkey = hashlib.md5(location).hexdigest()

		if blob != None:

			# Index our blob.
			index(blob)

			# Set key in Redis.
			rds = redis.StrictRedis(host='localhost', port=6379, db=0)
			rds.set(rkey, 1)
			rds.expire(rkey, 172800)
			logging.warning('Article indexed.')

		else:
			logging.critical('Something went wrong, dumping all knowledge.')
			logging.critical('rkey: %s, blob: %s', (rkey, blob))



if __name__ == '__main__':
	logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
						filename='scraper.log',
						level=logging.WARNING)
	harvest_list = ["http://www.dailymail.co.uk/newssitemap1.xml", 
	"www.independent.co.uk/googlenewssitemap.jsp", 
	"http://www.standard.co.uk/googlenewssitemap.jsp",
	"www.telegraph.co.uk/sitemaps/news/append/news_app1.xml",
	"http://www.theguardian.com/newssitemap.xml"

	for site in harvest_list:
		main(site)
