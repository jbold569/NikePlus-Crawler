from base64 import b64encode
from bs4 import BeautifulSoup as bs
import datetime as dt
import urllib2, json, gzip, argparse, os, re

auth_url = "https://api.twitter.com/oauth2/token"
search_url = "https://api.twitter.com/1.1/search/tweets.json%s"
base_query = "?q=nikeplus&count=100&include_entities=true&result_type=recent&since_id=%s"

def serviceCall(url, data=None, headers={}, error_message="Request Failed"):
	request = urllib2.Request(url, data, headers)
	response = None
	try:
		response = urllib2.urlopen(request)
		return response.read()
	except urllib2.HTTPError as e:
		print error_message
		print "Error %i: %s\nHTTP Method: %s" % (e.code, e.reason, request.get_method())
		return None
	except ValueError:
		print url

def authenticate(consumer_key, consumer_secret):
	# HTTP POST parameters
	headers = {
		'Authorization': "Basic " + b64encode(consumer_key + ':' + consumer_secret),
		'Content-Type': "application/x-www-form-urlencoded;charset=UTF-8"
	}
	data = "grant_type=client_credentials"
	dContent = json.loads(serviceCall(auth_url, data, headers, "Authentication failed"))
	return dContent['access_token']

def parseVars(html):
	varPattern = re.compile('[\w._]+\s*=\s*{\s*["\'].*?\s*};', re.DOTALL)
	dVariables = {}
	
	objects = re.findall(varPattern, html)
	for obj in objects:
		name, data = obj.split('=', 1)
		# Since the variable are in dot notation grab the last var name
		name = name.split('.')[-1].strip()
		# remove the trailing ';' and convert the json string to dict or directly eval() it
		try:
			data = json.loads(data[:-1].encode("utf-8"))
		except ValueError:
			data = eval(data[:-1])
		except:
			data = None
		
		if data:
			dVariables[name] = data 
	return dVariables

def main(args):
	
	max_id = "0"
	try:
		with  open("./.crawler_data/nikeplus/max_id", 'r') as id_file:
			max_id = id_file.read()
	except IOError:
		os.makedirs(".crawler_data/nikeplus/")
		if args.verbose: print "Running crawler for the first time..."
		else: pass
		
	if args.verbose: print "Max ID: " + max_id
	
	headers = {
		'Authorization': "Bearer " + authenticate(args.consumer_key, args.consumer_secret) 
	}
	dContent = json.loads(serviceCall(url=search_url % (base_query % (max_id)), headers=headers, error_message="Search Failed"))


	ct = dt.datetime.utcnow()
	path = os.path.join(args.output, "%i/%i/%i/" % (ct.year, ct.month, ct.day))
	filename = "%i-%02i-%02i_%02i-%02i.txt.gz" % (ct.year, ct.month, ct.day, ct.hour, ct.minute)

	if not os.path.exists(path):
		os.makedirs(os.path.join(path))

	if args.verbose: print os.path.join(path, filename)

	max_id_set = False

	with gzip.open(os.path.join(path, filename), 'wb') as file:
		while True:
			if not max_id_set:
				with open("./.crawler_data/nikeplus/max_id", 'w') as id_file:
					id_file.write(dContent['search_metadata']['max_id_str'])
				max_id_set = True
				
			if args.verbose: print len(dContent['statuses']), " ", dContent['search_metadata']['max_id_str']
			
			
			for tweet in dContent['statuses']:
				nike_url = ""
				urls = tweet['entities']['urls']
				if len(urls) == 1:
					nike_url = urls[0]['expanded_url']
					nike_page = serviceCall(nike_url, error_message="Nike+ Page not Found")
					
					if nike_page:
						nikeVars = parseVars(nike_page)
						if not nikeVars: continue
						tweet['nikeplus'] = nikeVars
					try:
						file.write(json.dumps(tweet, separators=(',', ':')) + '\n')
					except UnicodeDecodeError:
						if args.verbose: print "Unicode characters in tweet, skipping..."
						continue
			try:
				dContent = json.loads(serviceCall(url=search_url % (dContent['search_metadata']['next_results']) + "&since_id=%s" % (max_id), headers=headers, error_message="Search Failed"))	
			except KeyError:
				break

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("consumer_key", help="Twitter application consumer key")
	parser.add_argument("consumer_secret", help="Twitter application consumer secret")
	parser.add_argument("-o", "--output", help="Crawler output directory", default="./")
	parser.add_argument("-v", "--verbose", help="Enable debug information", action="store_true")

	main(args=parser.parse_args())
