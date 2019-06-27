#!/bin/env python
#*- encoding:utf-8 -*

import os, sys, requests
import traceback
import logging
import datetime
import time
import pandas as pd
import dateutil.parser

from bigolive_hiido_api import *
from bigolive_report_mysqldb import *
from done_flag_runner import *
try:
    import http.client as http_client
except ImportError:
    # Python 2
    import httplib as http_client
http_client.HTTPConnection.debuglevel = 1
# You must initialize logging, otherwise you'll not see debug output.
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

sys.path.insert(0, ".")
reload(sys)
sys.setdefaultencoding('utf8')
reqtimeout=500

app_id_platform = {'android': 'sg.bigo.live', 'ios': 'id1077137248'}
##app_api_token = '3208b08a-bd17-4895-8dfe-d8972fca7cfa'
app_api_token = '82bc62b0-0c58-4907-8db0-a8bfce6c1422'
'''
https://hq.appsflyer.com/export/master_report/v4?api_token=3208b08a-bd17-4895-8dfe-d8972fca7cfa&app_id=sg.bigo.live&from=2018-05-20&to=2018-05-20&groupings=install_time,pid,af_prt,c,geo&kpis=installs,unique_users_af_complete_registration,event_counter_af_complete_registration,retention_day_1,retention_day_7,retention_day_14,retention_day_30
https://hq.appsflyer.com/export/master_report/v4?api_token=3208b08a-bd17-4895-8dfe-d8972fca7cfa&app_id=sg.bigo.live&from=2018-05-20&to=2018-05-20&groupings=install_time,pid,af_prt,c,geo&kpis=installs,unique_users_af_complete_registration,event_counter_af_complete_registration,retention_day_1,retention_day_7,retention_day_14,retention_day_30

 curl "https://hq.appsflyer.com/export/video.like/master_report/v4?api_token=3208b08a-bd17-4895-8dfe-d8972fca7cfa&from=2017-11-05&to=2017-11-05&groupings=pid,geo,install_day&kpis=installs,clicks,impressions,retention_day_1,retention_day_2,retention_day_3,retention_day_4,retention_day_5,retention_day_6,retention_day_7,retention_day_8,retention_day_9,retention_day_10,retention_day_11,retention_day_12,retention_day_13,retention_day_14,retention_day_15,retention_day_30" -o appslyer.log
'''
@sync_remote_file(os.path.dirname(os.path.abspath(__file__)))
def get_appsflyer_retetion(platform, from_day, to_day, target_filename = None, force_get = False):
    if target_filename is None:
        raise
    if not force_get and os.path.exists(target_filename):
        print target_filename, " already exists use exist "
        return target_filename
    url = 'https://hq.appsflyer.com/export/master_report/v4'
    headers = {
        'Content-Type':'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
    }
    params = {
        'api_token': app_api_token,
        'app_id': app_id_platform[platform],
        'from': from_day,
        'to': to_day,
        'groupings': 'install_time,pid,af_prt,c,geo',
        'kpis': 'installs,unique_users_af_complete_registration,event_counter_af_complete_registration,retention_day_1,retention_day_7,retention_day_14,retention_day_30,geo'
    }
    global requests

    r = requests.get(url=url, params=params, headers=headers,timeout=reqtimeout)
    if r.status_code == 200:
        f = open(target_filename, "wb")
        for chunk in r.iter_content(chunk_size=512):
            if chunk:
                f.write(chunk)
        f.close()
        #file_exist_with_sync(target_filename, localprefix=os.getcwd(), local_as_src=True)
        return target_filename
    else:
        print r.status_code
        print r.raw
        raise Exception("get data error!!!")

'''
20&groupings=install_time,pid,c,af_c_id,af_adset,af_adset_id,af_ad,af_ad_id,af_channel&kpis=installs,retention_day_1,unique_users_af_custom_event_linkd_connect,event_counter_af_custom_event_linkd_connect,unique_users_af_complete_registration,event_counter_af_complete_registration,unique_users_af_purchase,event_counter_af_purchase,revenue 
https://hq.appsflyer.com/export/master_report/v4?api_token=3208b08a-bd17-4895-8dfe-d8972fca7cfa&app_id=sg.bigo.live&from=2018-05-20&to=2018-05-20&groupings=install_time,pid,c,af_c_id,af_adset,af_adset_id,af_ad,af_ad_id,af_channel&kpis=installs,retention_day_1,unique_users_af_custom_event_linkd_connect,event_counter_af_custom_event_linkd_connect,unique_users_af_complete_registration,event_counter_af_complete_registration,unique_users_af_purchase,event_counter_af_purchase,revenue 
'''
@sync_remote_file(os.path.dirname(os.path.abspath(__file__)))
def get_appsflyer_ads(platform, from_day, to_day, target_filename = None, force_get = False):
    if target_filename is None:
        raise
    if not force_get and os.path.exists(target_filename):
        print target_filename, " already exists use exist "
        return target_filename
    url = 'https://hq.appsflyer.com/export/master_report/v4'
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
    }
    params = {
        'api_token':app_api_token,
        'app_id': app_id_platform[platform],
        'from': from_day,
        'to': to_day,
        'groupings': 'install_time,pid,c,af_c_id,af_adset,af_adset_id,af_ad,af_ad_id,af_channel',
        'kpis': 'installs,retention_day_1,unique_users_af_custom_event_linkd_connect,event_counter_af_custom_event_linkd_connect,unique_users_af_complete_registration,event_counter_af_complete_registration,unique_users_af_purchase,event_counter_af_purchase,revenue'
    }
    global requests
    r = requests.get(url=url, params=params, headers=headers, timeout=reqtimeout)
    if r.status_code == 200:
        f = open(target_filename, "wb")
        for chunk in r.iter_content(chunk_size=512):
            if chunk:
                f.write(chunk)
        f.close()
        return target_filename
    else:
        print r.status_code
        print r.raw
        raise Exception("get data error!!!")

def get_install_date(row):
    str = dateutil.parser.parse(str(row["Install Time"])).strftime("%Y-%m-%d")
    return str

def to_storage_retetion(csvfile, platform, from_day = None, to_day = None):
    df = pd.read_csv(csvfile,  engine='python')
    df['platform'] = platform
    df['Install Time'] = df.apply(lambda x: dateutil.parser.parse(str(x["Install Time"])).strftime("%Y-%m-%d"), axis=1)
    df.rename(columns={'Install Time': 'stats_day', 'GEO': 'country', 'Media Source': 'media_source',
                       'Partner': 'partner', 'Campaign': 'campaign_name', 'Installs': 'installs',
                       'Unique Users - af_complete_registration': 'complete_reg_uniq_user', 'Event Counter - af_complete_registration': 'complete_reg_event_counter',
                       'Retention Day 1':'retention_day_1', 'Retention Day 7':'retention_day_7',
                       'Retention Day 14':'retention_day_14', 'Retention Day 30':'retention_day_30'}, inplace=True)
    if from_day is not None and to_day is not None:
        try:
            sqlconn = connect()
            cursor = sqlconn.cursor()
            cursor.execute("delete from bigo_stats_appsflyer_retetion where stats_day >= DATE_FORMAT(date_sub(%s,INTERVAL 1 DAY), '%%Y-%%m-%%d') and stats_day <  %s and platform = %s ",(from_day,to_day, platform))
            sqlconn.commit()
            sqlconn.close()
        except Exception, e:
            print traceback.format_exc()
            raise e
        finally:
            print cursor._last_executed
    con = get_conn_engine()
    df.to_sql('bigo_stats_appsflyer_retetion', con=con, if_exists='append', index=False, chunksize=1000)

def to_storage_retetion_summerize(from_day, to_day):
    try:
        sqlconn = connect()
        cursor = sqlconn.cursor()
        sqlconn.begin()
        cursor.execute(
            """
            delete from bigo_stats_appsflyer_retetion where (country = 'all' or platform = 'all' or media_source = 'all')  and stats_day >= DATE_FORMAT(date_sub(%s,INTERVAL 1 DAY), '%%Y-%%m-%%d') and stats_day <  %s
            """,
            (from_day, to_day ))
        cursor.execute(
            """
             insert into bigo_stats_appsflyer_retetion (stats_day, country, platform, media_source, installs)
select stats_day, 'all' as country, platform, media_source, sum(installs) as installs from bigo_stats_appsflyer_retetion where country != 'all' and platform != 'all' and media_source != 'all' and stats_day >= DATE_FORMAT(date_sub(%s,INTERVAL 1 DAY), '%%Y-%%m-%%d') and stats_day <  %s  group by stats_day, platform, media_source
union all 
select stats_day, country, 'all' as platform, media_source, sum(installs) as installs from bigo_stats_appsflyer_retetion where country != 'all' and platform != 'all' and media_source != 'all' and stats_day >= DATE_FORMAT(date_sub(%s,INTERVAL 1 DAY), '%%Y-%%m-%%d') and stats_day <  %s   group by stats_day, country, media_source
union all 
select stats_day, country,  platform, 'all' as media_source, sum(installs) as installs from bigo_stats_appsflyer_retetion where country != 'all' and platform != 'all' and media_source != 'all' and stats_day >= DATE_FORMAT(date_sub(%s,INTERVAL 1 DAY), '%%Y-%%m-%%d') and stats_day <  %s   group by stats_day, country, platform
union all 
select stats_day, 'all' as country, 'all' as platform, media_source, sum(installs) as installs from bigo_stats_appsflyer_retetion where country != 'all' and platform != 'all' and media_source != 'all' and stats_day >= DATE_FORMAT(date_sub(%s,INTERVAL 1 DAY), '%%Y-%%m-%%d') and stats_day <  %s   group by stats_day , media_source
union all 
select stats_day, country, 'all' as platform, 'all' as media_source, sum(installs) as installs from bigo_stats_appsflyer_retetion where country != 'all' and platform != 'all' and media_source != 'all' and stats_day >= DATE_FORMAT(date_sub(%s,INTERVAL 1 DAY), '%%Y-%%m-%%d') and stats_day <  %s   group by stats_day , country
union all 
select stats_day, 'all' as country, platform, 'all' as media_source, sum(installs) as installs from bigo_stats_appsflyer_retetion where country != 'all' and platform != 'all' and media_source != 'all' and stats_day >= DATE_FORMAT(date_sub(%s,INTERVAL 1 DAY), '%%Y-%%m-%%d') and stats_day <  %s   group by stats_day , platform
union all 
select stats_day, 'all' as country, 'all' as platform, 'all' as media_source, sum(installs) as installs from bigo_stats_appsflyer_retetion where country != 'all' and platform != 'all' and media_source != 'all' and stats_day >= DATE_FORMAT(date_sub(%s,INTERVAL 1 DAY), '%%Y-%%m-%%d') and stats_day <  %s   group by stats_day            
            """,
            (from_day, to_day, from_day, to_day, from_day, to_day, from_day, to_day, from_day, to_day, from_day, to_day, from_day, to_day))
        sqlconn.commit()
        sqlconn.close()
    except Exception, e:
        print traceback.format_exc()
        raise e
    finally:
        print cursor._last_executed

def to_storage_ads(csvfile, platform, from_day = None, to_day = None):
    df = pd.read_csv(csvfile, engine='python')
    df['platform'] = platform
    if len(df.index) == 0:
        print "no data in ", csvfile
        return
    import hashlib
    #df['name_sum'] = df.apply(lambda row: hashlib.md5(str(row['Campaign'])).hexdigest() + "_" + hashlib.md5(str(row['Adset'])).hexdigest() + "_" + hashlib.md5(str(row['Ad'])).hexdigest(), axis=1)
    df['Install Time'] = df.apply(lambda x: dateutil.parser.parse(str(x["Install Time"])).strftime("%Y-%m-%d"), axis=1)
    df.rename(columns={'Install Time': 'stats_day', 'Media Source': 'media_source',
                       'Campaign': 'campaign_name', 'Campaign ID': 'campaign_id',  'Adset': 'adset_name', 'Adset ID': 'adset_id',
                        'Ad': 'ad_name', 'Ad ID': 'ad_id', 'Channel': 'channel',
                       'Retention Day 1':'retention_day_1',
                       'Installs': 'installs',
                       'Unique Users - af_custom_event_linkd_connect': 'linkd_conn_uniq_user',
                       'Event Counter - af_custom_event_linkd_connect': 'linkd_conn_event_counter',
                       'Unique Users - af_complete_registration' : 'complete_reg_uniq_user',
                       'Event Counter - af_complete_registration': 'complete_reg_event_counter',
                       'Unique Users - af_purchase': 'purchase_uniq_user',
                       'Event Counter - af_purchase': 'purchase_event_counter',
                       'Revenue': 'revenue'}, inplace=True)

    if from_day is not None and to_day is not None:
        try:
            sqlconn = connect()
            cursor = sqlconn.cursor()
            cursor.execute("delete from bigo_stats_appsflyer_ads where stats_day >= %s and stats_day <=  %s and platform = %s ",(from_day,to_day, platform))
            sqlconn.commit()
            sqlconn.close()
        except Exception, e:
            traceback.print_exc()
            print e
            raise e
        finally:
            print cursor._last_executed
    con = get_conn_engine()
    df.to_sql('bigo_stats_appsflyer_ads', con=con, if_exists='append', index=False)


def main(platform, from_day, to_day, redo = False):
    target_filename = os.path.join("bigolive_stats_appsflyer_dir",
                                   "bigolive_appsflyer_retetion_{}-{}-{}.csv".format(platform, from_day,
                                                                                to_day))
    csvfile = get_appsflyer_retetion(platform, from_day, to_day, target_filename = target_filename)
    to_storage_retetion(csvfile, get_platform_value(platform), from_day, to_day)

def main_ads(platform, from_day, to_day, redo = False):
    target_filename = os.path.join("bigolive_stats_appsflyer_dir",
                                   "bigolive_appsflyer_ads_{}-{}-{}.csv".format(platform, from_day,
                                                                                to_day))
    csvfile = get_appsflyer_ads(platform, to_day, to_day, target_filename = target_filename, force_get= redo)
    to_storage_ads(csvfile, get_platform_value(platform), to_day, to_day)


def UnicodeDictReader(utf8_data, **kwargs):
    import csv
    csv_reader = csv.DictReader(utf8_data, **kwargs)
    for row in csv_reader:
        yield {unicode(key, 'utf-8'):unicode(value, 'utf-8') for key, value in row.iteritems()}

'''
add group bigo_stats_appsflyer_ads_day_adid
'''
def main_ads_group(from_day, to_day):
    try:
        sqlconn = connect()
        cursor = sqlconn.cursor()
        cursor.execute(
            """
                 insert into   bigo_stats_appsflyer_ads_day_adid
                 select 
                    stats_day,
                    ad_id,
                    sum(installs) as af_install,
                    sum(retention_day_1) as af_day1
                from 
                    bigo_stats_appsflyer_ads
                    where stats_day >= %s and stats_day <= %s  
                group by 
                    stats_day,
                    ad_id
                on duplicate key update 
                af_install = values(af_install),
                af_day1 = values(af_day1)
            """,
            (from_day, to_day))
        sqlconn.commit()

        cursor.execute(
            """
                 insert into   bigo_stats_appsflyer_ads_day_campid
                 select 
                    stats_day,
                    campaign_id,
                    sum(installs) as af_install,
                    sum(retention_day_1) as af_day1
                from 
                    bigo_stats_appsflyer_ads
                    where stats_day >= %s and stats_day <= %s  
                group by 
                    stats_day,
                    campaign_id
                on duplicate key update 
                af_install = values(af_install),
                af_day1 = values(af_day1)
            """,
            (from_day, to_day))
        sqlconn.commit()
        sqlconn.close()
    except Exception, e:
        traceback.print_exc()
        raise e
    finally:
        print cursor._last_executed

def main_ads_aggregate_tb(from_day, to_day):
    try:
        sqlconn = connect()
        cursor = sqlconn.cursor()
        # delete
        cursor.execute(
            """
            delete from bigo_stats_ads_google_facebook where stats_day >= %s and stats_day <= %s;
            """,
            (from_day, to_day))
        sqlconn.commit()
        sqlconn.close()

        sqlconn = connect()
        cursor = sqlconn.cursor()
# google

        sql = """
                insert into bigo_stats_ads_google_facebook(`stats_day`, `account_name`,  `account_id`,  `campaign_name`,  `campaign_id`,  `adset_name`,  `adset_id`,  `ad_name`,  `ad_id`,  `platform`,  `country`,  `impressions`,  `views`,  `clicks`,  `nique_lin_click`,  `conversions`,  `cost`,  `is_retargeting`,  `af_install`,  `af_day1`,  `code`,  `media_source`) 
               select 
                    t3.stats_day,
                    t3.account_name,
                    t3.account_id,
                    t3.campaign_name,
                    t3.campaign_id,
                    t3.adset_name,
                    t3.adset_id,
                    t3.ad_name,
                    t3.ad_id,
                    t3.platform,
                    t3.country,
                    t3.impressions,
                    t3.views,
                    t3.clicks,
                    t3.nique_lin_click,
                    t3.conversions,
                    t3.cost,
                    t3.is_retargeting,
                    t3.af_install,
                    t3.af_day1,
                    t4.code,
                    t4.media_source
                from
                    (
                   select distinct t1.stats_day,
                        t1.account_name,
                        t1.account_id,
                        t1.campaign_name,
                        t1.campaign_id,
                        t1.adset_name,
                        t1.adset_id,
                        t1.ad_name,
                        t1.ad_id,
                        t1.platform,
                        t1.country,
                        t1.impressions,
                        t1.views,
                        t1.clicks,
                        t1.nique_lin_click,
                        t1.conversions,
                        t1.cost,
                        t1.is_retargeting,
                        t2.af_install,
                        t2.af_day1
                        from 
                        (select t3.stats_day,
                        t3.account_name,
                        t3.account_id,
                        t3.campaign_name,
                        t3.campaign_id,
                        '--' as adset_name,
                        0 as adset_id,
                        '--' as ad_name,
                        0 as ad_id,
                        (case when t3.platform=0 then 'Android' else 'iOS' end) as platform,
                        (case when t3.country='UK' then 'GB' else t3.country end) as country,
                        t3.impressions,
                        0 as views,
                        t3.clicks,
                        0 as nique_lin_click,
                        t3.conversions,
                        t3.cost as cost,
                        'FALSE' as is_retargeting
                        from  bigo_stats_google_ads as t3 where t3.stats_day >= '%s' and t3.stats_day <= '%s' 
                        )t1
                        left join
                        (select c.stats_day, c.af_install, c.af_day1,c.campaign_id from bigo_stats_appsflyer_ads_day_campid as c
                           where c.stats_day >= '%s' and c.stats_day <= '%s' 
                            group by  c.stats_day, c.af_install, c.af_day1,c.campaign_id 
                            ) as t2
                    on t1.stats_day=t2.stats_day and t1.campaign_id= t2.campaign_id           
                    ) as t3    
                    left join
                    (select  code,media_source, account_id from bigo_stats_account_id_to_code group by code,media_source, account_id) as t4
                on
                    t3.account_id=t4.account_id;
            """%(from_day, to_day, from_day, to_day)
        print(sql)
        logging.error("sql is" + sql)
        cursor.execute(sql)
        sqlconn.commit()

        sqlconn = connect()
        cursor = sqlconn.cursor()
       # facebook
        sql = """
                insert into bigo_stats_ads_google_facebook(`stats_day`, `account_name`,  `account_id`,  `campaign_name`,  `campaign_id`,  `adset_name`,  `adset_id`,  `ad_name`,  `ad_id`,  `platform`,  `country`,  `impressions`,  `views`,  `clicks`,  `nique_lin_click`,  `conversions`,  `cost`,  `is_retargeting`,  `af_install`,  `af_day1`,  `code`,  `media_source`)  
              select 
                    t3.stats_day,
                    t3.account_name,
                    t3.account_id,
                    t3.campaign_name,
                    t3.campaign_id,
                    t3.adset_name,
                    t3.adset_id,
                    t3.ad_name,
                    t3.ad_id,
                    t3.platform,
                    t3.country,
                    t3.impressions,
                    t3.views,
                    t3.clicks,
                    t3.nique_lin_click,
                    t3.conversions,
                    t3.cost,
                    t3.is_retargeting,
                    t3.af_install,
                    t3.af_day1,
                    t4.code,
                    t4.media_source
                from
                    (
                   select  distinct t1.stats_day,
                        t1.account_name,
                        t1.account_id,
                        t1.campaign_name,
                        t1.campaign_id,
                        t1.adset_name,
                        t1.adset_id,
                        t1.ad_name,
                        t1.ad_id,
                        t1.platform,
                        t1.country,
                        t1.impressions,
                        t1.views,
                        t1.clicks,
                        t1.nique_lin_click,
                        t1.conversions,
                        t1.cost,
                        t1.is_retargeting,
                        t2.af_install,
                        t2.af_day1 
                        from 
                        (select t3.stats_day,
                        t3.account_name,
                        t3.account_id,
                        t3.campaign_name,
                        t3.campaign_id,
                        t3.adset_name,
                        t3.adset_id,
                        t3.ad_name,
                        t3.ad_id,
                        (case when t3.platform=0 then 'Android' else 'iOS' end) as platform,
                        (case when t3.country='UK' then 'GB' else t3.country end) as country,
                        t3.impressions,
                        t3.action_video_view as views,
                        t3.action_link_click as clicks,
                        t3.uniq_action_link_click as nique_lin_click,
                        t3.action_mobile_app_install as conversions,
                        t3.spend as cost,       
                        (case when t3.campaign_name like '%%retargeting%%' then 'TRUE' else 'FALSE' end) as is_retargeting
                        from bigo_stats_facebook_ads as t3
                        where t3.stats_day >= '%s' and t3.stats_day <= '%s' 
                        )t1
                        left join 
                        (select distinct c.stats_day,c.af_install, c.af_day1,c.ad_id  from bigo_stats_appsflyer_ads_day_adid as c where c.stats_day >= '%s' and c.stats_day <= '%s'  )t2
                        on t1.stats_day=t2.stats_day and t1.ad_id=t2.ad_id 
                    ) as t3
                    left join
                    (select  code,media_source, account_id from bigo_stats_account_id_to_code group by code,media_source, account_id) as t4
                on
                    t3.account_id=t4.account_id;
            """%(from_day, to_day, from_day, to_day)
        print(sql)
        logging.error("sql is" + sql)
        cursor.execute(sql)
        sqlconn.commit()
        sqlconn.close()
    except Exception, e:
        traceback.print_exc()
        raise e
    finally:
        print cursor._last_executed


if __name__ == "__main__":
    mode = 'all' if len(sys.argv) < 2 else sys.argv[1]
    end_datetime = (datetime.datetime.now() - datetime.timedelta(1)) if len(sys.argv) < 3 else datetime.datetime.strptime(sys.argv[2].strip(), "%Y-%m-%d")
    begin_datetime = (end_datetime - datetime.timedelta(30)) if len(sys.argv) < 4 else datetime.datetime.strptime(sys.argv[3].strip(), "%Y-%m-%d")
    end_datetime_str = end_datetime.strftime("%Y-%m-%d")
    begin_datetime_str = begin_datetime.strftime("%Y-%m-%d")

    delta = end_datetime - begin_datetime

    if mode == 'retetion' or mode == "all":
        step = 10
        next_begin_datetime = begin_datetime
        for x in range(step, delta.days + 1, step):
            next_end_datetime = begin_datetime + datetime.timedelta(x)
            next_end_datetime = end_datetime if  next_end_datetime > end_datetime else next_end_datetime
            main('android', next_begin_datetime.strftime("%Y-%m-%d"), next_end_datetime.strftime("%Y-%m-%d"))
            time.sleep(3)
            main('ios', next_begin_datetime.strftime("%Y-%m-%d"), next_end_datetime.strftime("%Y-%m-%d"))
            time.sleep(3)
            next_begin_datetime = next_end_datetime

    if mode == "fix_retetion_sum" or mode == "all":
        to_storage_retetion_summerize(begin_datetime_str, end_datetime_str)
    if mode == 'ads' :
        main_ads('android', end_datetime_str, end_datetime_str)
        main_ads('ios', end_datetime_str, end_datetime_str)
        main_ads_group(end_datetime_str, end_datetime_str)
        main_ads_aggregate_tb(end_datetime_str, end_datetime_str)
    if mode == 'ads_only_agg' :
        main_ads_group(end_datetime_str, end_datetime_str)
        main_ads_aggregate_tb(end_datetime_str, end_datetime_str)