import os,time 
from datetime import datetime, timedelta 
import sys 
import main
import requests
import json

def DeltaSeconds(): 
    SECONDS_PER_DAY = 24 * 60 * 60 
    curTime = datetime.now() 
    
    desTime = curTime.replace(hour=11, minute=0, second=0, microsecond=0)
    delta = desTime - curTime 
    skipSeconds01 = delta.total_seconds() % SECONDS_PER_DAY 
    
    
    desTime = curTime.replace(hour=14, minute=0, second=0, microsecond=0)
    delta = desTime - curTime 
    skipSeconds02 = delta.total_seconds() % SECONDS_PER_DAY  
    
    desTime = curTime.replace(hour=17, minute=0, second=0, microsecond=0) 
    delta = desTime - curTime 
    skipSeconds03 = delta.total_seconds() % SECONDS_PER_DAY  
     
    skipSeconds= min( skipSeconds01, skipSeconds02, skipSeconds03 )

    print ("Must sleep %d seconds" % skipSeconds )
    return skipSeconds 



"""

while True:
    try:
        s = DeltaSeconds()
        time.sleep(s)
        temp = main.Run()
    except Exception as e:
        print(e)
        url = 'https://oapi.dingtalk.com/robot/send?access_token=aba051af1e51986d166fc17d39ea48cb0fdd63a171974fc92c58bb47e2c20fa8'
        d = {  "msgtype": "text", "text": { "content": str(e) },
             "at": { "atMobiles": [ "17681882711"], "isAtAll": False }}
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        r = requests.post(url = url, headers=headers, data= json.dumps(d))

"""



try:
	temp = main.Run()
except Exception as e:
	print(e)
	url = 'https://oapi.dingtalk.com/robot/send?access_token=aba051af1e51986d166fc17d39ea48cb0fdd63a171974fc92c58bb47e2c20fa8'
	d = {  "msgtype": "text", "text": { "content": str(e) },
		 "at": { "atMobiles": [ "17681882711"], "isAtAll": False }}
	headers = {'Content-Type': 'application/json; charset=utf-8'}
	r = requests.post(url = url, headers=headers, data= json.dumps(d))













