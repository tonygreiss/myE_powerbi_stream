import requests
import json
import pandas as pd
from pandas.io.json import json_normalize

def myE_auth(key):
	response = requests.post('https://api.myelefant.com/v1/token', headers={'Authorization': 'Basic {}'.format(key)})
	response_text = json.loads(response.text)
	access_token = response_text['access_token']
	success = response_text['success']
	return access_token, success

def myE_campaign_list(access_token, start, end):
	#export data
	r = requests.get("https://api.myelefant.com/v1/campaigns", params={'start':start, 'stop':end}, headers={'Authorization': 'Bearer '+access_token})
	#Convert campaigns response to dataframe
	data = json.loads(r.text)['campaigns']
	return data

def myE_events(access_token, campaign_uuid, start, stop):
	#export data
	r = requests.get("https://api.myelefant.com/v1/campaign/log", params={'campaign_uuid':campaign_uuid,'start':start, 'stop':stop}, headers={'Authorization': 'Bearer '+access_token})
	success = json.loads(r.text)['success']

	if success == True:
		#Convert campaigns response to dataframe
		df = pd.json_normalize(json.loads(r.text)['log'])
		return df
	else:
		invalid = r.text
		return invalid

	



	