from myelefant_api import myE_auth, myE_campaign_list, myE_events
from powerbi_api import events_stream
import csv
import requests
import datetime
import pandas as pd
import time

#defining export start & end datetimes
today = datetime.datetime(datetime.datetime.now().year,datetime.datetime.now().month,datetime.datetime.now().day)
#campaign list dates
campaign_start =  (today - datetime.timedelta(weeks=4)).strftime("%Y-%m-%d %H:%M:%S")
campaign_end = today.strftime("%Y-%m-%d %H:%M:%S")
#events dates
event_start = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
event_end = today.strftime("%Y-%m-%d %H:%M:%S")

#create a text file for logs and report
now = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
myreport = open(f'report_{now}.txt', 'w')

#from config file run the exports and datastream imports
with open('config.csv') as csv_file:
	csv_reader = csv.reader(csv_file, delimiter=',')
	next(csv_reader)  # Skip header row
	
	for account, api_key, push_url in csv_reader:
		#authenticate
		#CHECK THAT AUTHENTICATION WAS SUCCESSFUL 
		print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ f" Export Starting for: {account}")
		try:
			token, success = myE_auth(f"{api_key}")
		except:
			print("Could not connect to myElefant API with API KEY")
			myreport.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ f" Account: {account} - Could not connect to myElefant API with current API KEY\n")
			continue

		#export list of campaigns
		campaigns = myE_campaign_list(token, campaign_start, campaign_end)

		##CHECK IF THERE WERE CAMPAIGNS SENT IN THE PERIOD OR ELSE MOVE TO NEXT ACCOUNT
		print(len(campaigns))
		if len(campaigns) >0:
			#export events of campaigns 
			for campaign in campaigns:
				#check that camaign was actually sent
				if campaign['status'] == 'sent':
					print(token, campaign['id'], event_start, event_end)
					df_events = myE_events(token, campaign['id'], event_start, event_end)
					#check if myE_events function returned a dataframe
					if isinstance(df_events, pd.DataFrame) == True:
						if len(df_events) > 0:
							events_stream(f"{push_url}", campaign, account, df_events)
							myreport.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ f" Account: {account} - Total Campaigns: {len(campaigns)}, Total Events: {len(df_events)}\n")
							time.sleep(1)
						else:
							print(campaign['name']+" no events")
					else:
						print(df_events+" for "+campaign['name'])
						myreport.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ f" Account: {account} - {df_events}\n")
						break
				else:
					print('No campaign for the account in the past 4 weeks')
					myreport.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ f" Account: {account} - No campaign for the account in the past 4 weeks\n")
					pass
		else:
			print('No campaign sent for the account in the past 4 weeks')
			myreport.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ f" Account: {account} - No campaign for the account in the past 4 weeks\n")
			continue

		print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ f" Export Ending for: {account}")

#close file
myreport.close()
			

