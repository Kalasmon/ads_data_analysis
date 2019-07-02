#!coding:utf-8

import os,sys
import json
import logging
import time

def set_logs(debug=0):
  global logging
  # debug 1 为测试环境 0 为正式环境

  reload(logging)
  logging.shutdown()
  if debug==0:
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s ## %(message)s')
  else:
    logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(levelname)s ## %(message)s')

try:
  start_time=sys.argv[1]
except:
  start_time=os.popen("gdate -d '-1day' +%Y-%m-%d").read().replace('\n','')

try:
  end_time=sys.argv[2]
except:
  end_time=os.popen("gdate -d '0day' +%Y-%m-%d").read().replace('\n','')

try:
  debug=int(sys.argv[3])
except:
  debug=0

# 全局变量
token=''
account_id_list=['xxxxxxxxxx','xxxxxxxxxx']
timezone='-07'
adaccounts_campaigns_url='curl -H "Authorization: Bearer {token}" "https://mediago.baidu.com/api/ads/?sf=v1/adaccounts/%s/campaigns"'
campaign_stats_url='curl -H "Authorization: Bearer {token}" "https://mediago.baidu.com/api/ads/?sf=v1/campaigns/%s/stats&granularity=DAY&start_time=%sT00:00:00.000%s:00&end_time=%sT00:00:00.000%s:00&fields=spend,impressions,swipes,total_installs"'

set_logs(debug)

def refresh_token():
  global token
  url='curl -X POST -d "grant_type=refresh_token" -d "code=xxxxxxxx" https://mediago.baidu.com/api/refreshtoken'
  r=os.popen(url).read()
  rr=json.loads(r)
  token=rr['access_token']

def get_result(base_url):
  global token
  #print token
  wrong_nums=0
  while 1:
    try:
      url=base_url.format(token=token)
      r=os.popen(url).read()
      if r=='':
        refresh_token()
        #print token
        url=base_url.format(token=token)
        r=os.popen(url).read()
        #print r
      rr=json.loads(r)
      break
    except:
      wrong_nums+=1
      if wrong_nums>=3:
        os._exit()
      time.sleep(60)
  return rr

rrr=''
f=open('result.txt','w')
for i in account_id_list:
  logging.info('account_id: ' + i)
  url=adaccounts_campaigns_url%(i)
  r1=get_result(url)
  logging.debug('account_id response: ' + str(r1))
  rr=r1["campaigns"]
  for j in rr:
    #print j
    ad_account_id=j['campaign']['ad_account_id']
    status=j['campaign']['status']
    campaign_start_time=j['campaign']['start_time']
    name=j['campaign']['name']
    created_at=j['campaign']['created_at']
    updated_at=j['campaign']['updated_at']
    android_app_url=j['campaign']['measurement_spec']['android_app_url']
    ios_app_id=j['campaign']['measurement_spec']['ios_app_id']
    objective=j['campaign']['objective']
    campaign_id=j['campaign']['id']
    logging.info('campaign_id: ' + campaign_id)
    url=campaign_stats_url%(campaign_id,start_time,timezone,end_time,timezone)
    r2=get_result(url)
    logging.debug('campaign_id response: ' + str(r2))
    if r2['request_status']=='ERROR' and r2['error_code']=='E1008':
      if 'Asia/Shanghai' in r2['debug_message']:
        timezone='%2b08'
      elif 'America/Los_Angeles' in r2['debug_message']:
        timezone='-07'
      url=campaign_stats_url%(campaign_id,start_time,timezone,end_time,timezone)
      r2=get_result(url)
    #print r2
    r2_result=r2['timeseries_stats'][0]['timeseries_stat']['timeseries'][0]['stats']
    paid_impressions=r2_result['impressions']
    total_installs=r2_result['total_installs']
    orig_spend=r2_result['spend']
    swipes_ups=r2_result['swipes']
    spend=float(orig_spend)/1000/1000
    if paid_impressions!=0:
      Paid_eCPM=float(orig_spend)/paid_impressions/1000
    else:
      Paid_eCPM=0
    if swipes_ups!=0:
      eCPSU=float(orig_spend)/1000/1000/swipes_ups
    else:
      eCPSU=0
    if total_installs!=0:
      eCPI=float(orig_spend)/1000/1000/total_installs
    else:
      eCPI=0
    try:
      country=name.split('_')[2]
      your_code=name.split('_')[4]
      account_id=name.split('_')[3]
      OS=name.split('_')[1].upper()
    except:
      country=u"UNKNOWN"
      your_code=u"UNKNOWN"
      account_id=u"UNKNOWN"
      OS=u"UNKNOWN"
    #rrr+= ad_account_id + '\t' + status + '\t' + campaign_start_time + '\t' + name + '\t' + created_at + '\t' + updated_at + '\t' + android_app_url + '\t' + ios_app_id + '\t' + objective + '\t' + str(campaign_id) + '\t' + str(paid_impressions) + '\t' + str(total_installs) + '\t' + str(orig_spend) + '\t' + str(swipes_ups) + '\t' + str(spend) + '\t' + str(Paid_eCPM) + '\t' + str(eCPSU) + '\t' + str(eCPI) + '\t' + country + '\t' + your_code + '\t' + account_id + '\t' + OS + '\n'
    result=ad_account_id + '\t' + status + '\t' + campaign_start_time + '\t' + name + '\t' + created_at + '\t' + updated_at + '\t' + android_app_url + '\t' + ios_app_id + '\t' + objective + '\t' + str(campaign_id) + '\t' + str(paid_impressions) + '\t' + str(total_installs) + '\t' + str(orig_spend) + '\t' + str(swipes_ups) + '\t' + str(spend) + '\t' + str(Paid_eCPM) + '\t' + str(eCPSU) + '\t' + str(eCPI) + '\t' + country + '\t' + your_code + '\t' + account_id + '\t' + OS + '\n'
    logging.debug('Result: ' + result)
    f.write(result.encode('utf-8'))
    #f.write(result)

#logging.info("Write to txt")
#f=open('result.txt','w')
#f.write(rrr.decode('utf8'))
f.close()
logging.info("Write complete!")

sql_list=["drop table if exists tmp.xxxx_snapchat;","""
create table tmp.xxxx_snapchat(
ad_account_id string,
status string,
campaign_start_time string,
name string,
created_at string,
updated_at string,
android_app_url string,
ios_app_id string,
objective string,
campaign_id string,
paid_impressions bigint,
total_installs bigint,
orig_spend bigint,
swipes_ups bigint,
spend double,
Paid_eCPM bigint,
eCPSU bigint,
eCPI bigint,
country string,
your_code string,
account_id string,
OS string)
row format delimited fields terminated by '\\t'
LINES TERMINATED BY '\\n'
stored as textfile;""",
"load data local INPATH 'result.txt' into table tmp.xxxx_snapchat;",
"""
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table algo.xxxx_snapchat_by_day partition(day)
select *,'%s' as day from tmp.xxxx_snapchat;
"""%start_time]

for sql in sql_list:
  logging.info("Running sql: " + sql[:50])
  os.popen('spark-sql -e "%s"'%sql).read()
