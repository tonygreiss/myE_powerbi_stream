import requests
import pandas as pd

def campaign_list_stream(rest_api_url,campaign):
	data = [
	{
   	"status" :campaign["status"],
   	"code" :campaign["code"],
   	"send_date" :campaign["send_date"].replace(" ","T")+".000Z",
   	"name" :campaign["name"],
   	"message" :campaign["message"],
   	"contact_count" :campaign["contact_count"],
   	"id" :campaign["id"],
   	"sender" :campaign["sender"],
   	"is_test" :campaign["is_test"],
   	"tag" :campaign["tag"]
   	}
   	]

	requests.post(rest_api_url, json=data).text
	print("Campaign Pushed: "+campaign["name"])

def events_stream(rest_api_url, campaign, account, df_events):

	#stream SUBMIT data
	if "SUBMIT" in list(df_events.type.unique()):
		data = [
		{
		"account":account,
		"campaign_id":campaign["id"],
		"campaign_name":campaign["name"],
		"campaign_message":campaign["message"],
		"campaign_sender":campaign["sender"],
		"campaign_date" :campaign["send_date"].replace(" ","T")+".000Z",
		"event":"SUBMIT",
		"os":"",
		"device":"",
		"browser":"",
		"tracker_name":"",
		"tracker_value":"",
		"total_count":int(df_events[(df_events.type == "SUBMIT") & (df_events["data.success"] == True)]["data.nb_parts"].sum()),
		"distinct_count":df_events[(df_events.type == "SUBMIT") & (df_events["data.success"] == True)]["contact.msisdn"].nunique()
		}
		]
		requests.post(rest_api_url, json=data).text
	else:
		pass

	#stream DELIVER data
	if "DELIVER" in list(df_events.type.unique()):
		data = [
		{
		"account":account,
		"campaign_id":campaign["id"],
		"campaign_name":campaign["name"],
		"campaign_message":campaign["message"],
		"campaign_sender":campaign["sender"],
		"campaign_date" :campaign["send_date"].replace(" ","T")+".000Z",
		"event":"DELIVER",
		"os":"",
		"device":"",
		"browser":"",
		"tracker_name":"",
		"tracker_value":"",
		"total_count":int(df_events[(df_events.type == "DELIVER")]["data.nb_parts"].sum()),
		"distinct_count":df_events[(df_events.type == "DELIVER")]["contact.msisdn"].nunique()
		}
		]
		requests.post(rest_api_url, json=data).text
	else:
		pass

	#stream HARD BOUNCE data
	if "data.reason" in list(df_events.columns):
		data = [
		{
		"account":account,
		"campaign_id":campaign["id"],
		"campaign_name":campaign["name"],
		"campaign_message":campaign["message"],
		"campaign_sender":campaign["sender"],
		"campaign_date" :campaign["send_date"].replace(" ","T")+".000Z",
		"event":"HARD BOUNCE",
		"os":"",
		"device":"",
		"browser":"",
		"tracker_name":"",
		"tracker_value":"",
		"total_count":int(df_events[df_events["data.reason"] == "Permanent failure"]["data.nb_parts"].sum()),
		"distinct_count":df_events[df_events["data.reason"] == "Permanent failure"]["contact.msisdn"].nunique()
		}
		]
		requests.post(rest_api_url, json=data).text
	else:
		pass
	'''
	#stream SOFT BOUNCE DATA
	data = [
	{
	"account":account,
	"campaign_id":campaign["id"],
	"campaign_name":campaign["name"],
	"campaign_message":campaign["message"],
	"campaign_sender":campaign["sender"],
	"campaign_date" :campaign["send_date"].replace(" ","T")+".000Z",
	"event":"SOFT BOUNCE",
	"os":"",
	"device":"",
	"browser":"",
	"tracker_name":"",
	"tracker_value":"",
	"total_count":df_events[df_events.type == "SUBMIT"]["contact.msisdn"].nunique() - df_events[df_events.type == "DELIVER"]["contact.msisdn"].nunique() - df_events[df_events["data.reason"] == "Permanent failure"]["contact.msisdn"].nunique(),
	"distinct_count":df_events[df_events.type == "SUBMIT"]["contact.msisdn"].nunique() - df_events[df_events.type == "DELIVER"]["contact.msisdn"].nunique() - df_events[df_events["data.reason"] == "Permanent failure"]["contact.msisdn"].nunique()
	}
	]
	requests.post(rest_api_url, json=data).text
	'''
	#stream UNSUB data
	if "data.stop" in list(df_events.columns): 
		data = [
		{
		"account":account,
		"campaign_id":campaign["id"],
		"campaign_name":campaign["name"],
		"campaign_message":campaign["message"],
		"campaign_sender":campaign["sender"],
		"campaign_date" :campaign["send_date"].replace(" ","T")+".000Z",
		"event":"UNSUB",
		"os":"",
		"device":"",
		"browser":"",
		"tracker_name":"",
		"tracker_value":"",
		"total_count":int(df_events[df_events["data.stop"] == True]["contact.msisdn"].nunique()),
		"distinct_count":df_events[df_events["data.stop"] == True]["contact.msisdn"].nunique()
		}
		]
		requests.post(rest_api_url, json=data).text
	else:
		pass

	#stream RESPONSE data
	if "RESPONSE" in list(df_events.type.unique()):
		data = [
		{
		"account":account,
		"campaign_id":campaign["id"],
		"campaign_name":campaign["name"],
		"campaign_message":campaign["message"],
		"campaign_sender":campaign["sender"],
		"campaign_date" :campaign["send_date"].replace(" ","T")+".000Z",
		"event":"RESPONSE",
		"os":"",
		"device":"",
		"browser":"",
		"tracker_name":"",
		"tracker_value":"",
		"total_count":int(df_events[df_events["type"] == "RESPONSE"]["contact.msisdn"].count()),
		"distinct_count":df_events[df_events["type"] == "RESPONSE"]["contact.msisdn"].nunique()
		}
		]
		requests.post(rest_api_url, json=data).text
	else:
		pass


	#stream PREVIEW data
	if "OG_PREVIEW" in list(df_events.type.unique()):
		data = [
		{
		"account":account,
		"campaign_id":campaign["id"],
		"campaign_name":campaign["name"],
		"campaign_message":campaign["message"],
		"campaign_sender":campaign["sender"],
		"campaign_date" :campaign["send_date"].replace(" ","T")+".000Z",
		"event":"PREVIEW",
		"os":"",
		"device":"",
		"browser":"",
		"tracker_name":"",
		"tracker_value":"",
		"total_count":int(df_events[df_events["type"] == "OG_PREVIEW"]["contact.msisdn"].count()),
		"distinct_count":df_events[df_events["type"] == "OG_PREVIEW"]["contact.msisdn"].nunique()
		}
		]
		requests.post(rest_api_url, json=data).text
	else:
		pass

	#stream CLICK data
	if "VISIT" in list(df_events.type.unique()):
		for os in list(df_events[(df_events["type"] == "VISIT")]["data.os.family"].unique()):
			for device in list(df_events[(df_events["type"] == "VISIT")]["data.device.family"].unique()):
				for browser in list(df_events[(df_events["type"] == "VISIT")]["data.browser.family"].unique()):
					total_count = df_events[(df_events["type"] == "VISIT") & (df_events["data.os.family"] == os) & (df_events["data.device.family"] == device) & (df_events["data.browser.family"] == browser)]["contact.msisdn"].count()
					if total_count > 0:
						data = [
						{
						"account":account,
						"campaign_id":campaign["id"],
						"campaign_name":campaign["name"],
						"campaign_message":campaign["message"],
						"campaign_sender":campaign["sender"],
						"campaign_date" :campaign["send_date"].replace(" ","T")+".000Z",
						"event":"CLICK",
						"os":os,
						"device":device,
						"browser":browser,
						"tracker_name":"",
						"tracker_value":"",
						"total_count":int(total_count),
						"distinct_count":df_events[(df_events["type"] == "VISIT") & (df_events["data.os.family"] == os) & (df_events["data.device.family"] == device) & (df_events["data.browser.family"] == browser)]["contact.msisdn"].nunique()
						}
						]
						requests.post(rest_api_url, json=data).text
					else:
						pass
	else:
		pass

	#stream TRACK data
	if "TRACKER" in list(df_events.type.unique()):
		for name in list(df_events[(df_events["type"] == "TRACKER")]["data.name"].unique()):
			for value in list(df_events[(df_events["type"] == "TRACKER")]["data.value"].unique()):
				total_count = df_events[(df_events["type"] == "TRACKER") & (df_events["data.name"] == name) & (df_events["data.value"] == value)]["contact.msisdn"].count()
				if total_count > 0:
					data = [
					{
					"account":account,
					"campaign_id":campaign["id"],
					"campaign_name":campaign["name"],
					"campaign_message":campaign["message"],
					"campaign_sender":campaign["sender"],
					"campaign_date" :campaign["send_date"].replace(" ","T")+".000Z",
					"event":"TRACKER",
					"os":"",
					"device":"",
					"browser":"",
					"tracker_name":name,
					"tracker_value":value,
					"total_count":int(total_count),
					"distinct_count":df_events[(df_events["type"] == "TRACKER") & (df_events["data.name"] == name) & (df_events["data.value"] == value)]["contact.msisdn"].nunique()
					}
					]
					requests.post(rest_api_url, json=data).text

				else:
					pass
	else:
		pass
