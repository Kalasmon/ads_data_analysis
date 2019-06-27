#!/bin/env python
#*- encoding:utf-8 -*

import os,sys


print os.path
print sys.path

import traceback
import random
import datetime
import time

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError
from facebook_business.adobjects.adreportrun import AdReportRun

# from facebook_business.adobjects.adsinsights import AdsInsights
# from facebook_business.adobjects.adreportrun import AdReportRun
# from facebook_business import FacebookAdsApi
# from facebook_business.exceptions import FacebookRequestError
# from facebook_business.adobjects.adaccount import AdAccount

from xxxxxxxx_report_mysqldb import *
from xxxxxxxx_hiido_api import *

# must add !!!!
sys.path.insert(0, ".")
reload(sys)
sys.setdefaultencoding('utf8')

access_token = 'xxxxxxxx'
app_secret = 'xxxxxxxx'
access_token = 'xxxxxxxx'
app_secret = 'xxxxxxxx'
FacebookAdsApi.init(access_token=access_token)


ad_account_ids = [
70882100596xxxx,
97104183641xxxx
]

def get_reqparas_by(date_ranges = None, delivery_info_status = None):
    params = {
        'fields': [
            AdsInsights.Field.account_id,
            AdsInsights.Field.account_name,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.adset_id,
            AdsInsights.Field.ad_id,
            AdsInsights.Field.adset_name,
            AdsInsights.Field.actions,
            AdsInsights.Field.spend,
            AdsInsights.Field.unique_actions,
            AdsInsights.Field.reach,
            AdsInsights.Field.impressions
        ],
        'breakdowns': [
            AdsInsights.Breakdowns.country,
        ],
        'level': 'ad',
        'date_preset': "yesterday",
        'time_increment': 1,
        'limit': 10000000000000,
    }
    if delivery_info_status is not None:
        params['filtering'] = [{"field": "campaign.delivery_info", "operator": "IN", "value": delivery_info_status}]
    # list<{'since':YYYY-MM-DD,'until':YYYY-MM-DD}>
    yesterday = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    if date_ranges is not None and len(date_ranges) > 0 and (yesterday != date_ranges[0] or yesterday != date_ranges[-1]):
        date_ranges_post = [{'since': date_str, 'until': date_str} for date_str in date_ranges]
        print "set date_ranges", date_ranges_post
        params['time_ranges'] = date_ranges_post

    return params

def get_by_account_id(ad_account_id, date_ranges = None, delivery_info_status = None, async= False, retry_max_time = 30):
    params = get_reqparas_by(date_ranges, delivery_info_status)
    print params
    if async:
        stats = get_acountinfo_async(ad_account_id, params)
    else:
        adAccount = AdAccount("act_" + str(ad_account_id))
        stats  = adAccount.get_insights(params=params)
    print len(stats)

    if stats is None:
        raise Exception("ad Account {} get error !!  ".format(ad_account_id))
    retry_time = 0
    while True: # retry_time < retry_max_time:
        try:
            retry_time += 1
            list_stat = list()
            for stat in stats:
                print "------- stas is ----- ", stat, "------------------"
                mobile_install = 0
                video_view = 0
                link_click = 0
                uniq_link_click = 0

                if "actions" in stat:
                    for x in stat['actions']:
                        mobile_install = x['value']  if x['action_type'] == "mobile_app_install" else mobile_install
                        video_view = x['value'] if x['action_type'] == "video_view" else video_view
                        link_click = x['value'] if x['action_type'] == "link_click" else link_click
                if "unique_actions" in stat:
                    for x in stat['unique_actions']:
                        uniq_link_click = x['value']  if x['action_type'] == "link_click" else uniq_link_click
                obj = {
                        'account_id' : stat['account_id'],
                        'account_name': stat['account_name'],
                        'ad_id': stat['ad_id'],
                        'ad_name': stat['ad_name'],
                        'adset_id': stat['adset_id'],
                        'adset_name': stat['adset_name'],
                        'stats_day' : stat['date_start'],
                        'campaign_id':  stat['campaign_id'],
                        'campaign_name': stat['campaign_name'],
                        "country": stat['country'],
                        'spend': str(stat['spend']),
                        "impressions": str(stat['impressions']),
                        "action_mobile_app_install": mobile_install,
                        "action_video_view": video_view,
                        "action_link_click": link_click,
                        "uniq_action_link_click":  uniq_link_click
                    }
                if not "platform" in stat:
                    obj['platform'] = get_platform_value('ot', 'ot')
                    if 'campaign_name' not in stat or len(stat['campaign_name'].split("_")) < 2:
                        continue
                    campaign_split = stat['campaign_name'].split("_")
                    platform = campaign_split[1].upper()
                    obj['platform'] = get_platform_value('android') if platform == 'ANDR' else get_platform_value('ios') if platform == 'IOS' else get_platform_value('ot', 'ot') # 3 is for unkown
                list_stat.append(obj)
        except Exception, e:
            if isinstance(e, KeyError):
                raise e
            print traceback.format_exc()
            continue
        if retry_time > retry_max_time or not stats.load_next_page():
            break

    return list_stat


def to_storage(data):
    conn = connect()
    sql_insert = """insert into xxxx_stats_facebook_ads(
                      account_id, account_name, stats_day, ad_id, ad_name, adset_id, adset_name, campaign_id,campaign_name,country,platform,
                      spend,impressions,action_video_view,action_mobile_app_install,action_link_click,uniq_action_link_click
                  ) values (%(account_id)s, %(account_name)s, %(stats_day)s,%(ad_id)s, %(ad_name)s, %(adset_id)s, %(adset_name)s, %(campaign_id)s, %(campaign_name)s, %(country)s, %(platform)s,
                      %(spend)s,%(impressions)s,%(action_video_view)s,%(action_mobile_app_install)s,%(action_link_click)s,%(uniq_action_link_click)s
                  ) on duplicate key update
                    account_name= values(account_name),
                    ad_name= values(ad_name),
                    adset_name= values(adset_name),
                    campaign_name= values(campaign_name),
                      spend= values(spend),
                      impressions = values(impressions),
                      action_video_view = values(action_video_view),
                      action_mobile_app_install = values(action_mobile_app_install),
                      action_link_click = values(action_link_click),
                      uniq_action_link_click = values(uniq_action_link_click)
                   """
    cursor = conn.cursor()
    try:
        cursor.execute("SET NAMES utf8")
        cursor.executemany(sql_insert, data)
        conn.commit()
    except Exception, e:
        traceback.print_exc()
        print cursor._last_executed
        raise e
    finally:
        conn.close()

def get_account_info_to_storage(account_id, date_ranges):
    retry_times = 1
    max_retry = 30
    data = None
    while retry_times < max_retry:
        try:
            retry_times += 1
            data = get_by_account_id(account_id, date_ranges, async=False)
            to_storage(data)
        except Exception, e:
            print traceback.format_exc(e)
            print e
            if isinstance(e, FacebookRequestError):
                try:
                    data = get_by_account_id(account_id, date_ranges, async=True)
                    to_storage(data)
                except Exception, e:
                    print traceback.format_exc()
                    print e
                if data is None:
                    continue
        break
    return data

def get_acountinfo_async(ad_account_id, reqparas, retry_max_time = 6):
    adAccount = AdAccount("act_" + str(ad_account_id))
    retry_time = 0
    stats = None
    while retry_time < retry_max_time:
        retry_time +=1
        try:
            async_job = adAccount.get_insights(params=reqparas, async=True)
            async_job.remote_read()
            while async_job[AdReportRun.Field.async_percent_completion] < 100:
                time.sleep(1)
                async_job.remote_read()
            time.sleep(1)
            from copy import deepcopy
            reqparas_copy = deepcopy(reqparas)
            if 'breakdowns' in reqparas_copy:
                del reqparas_copy['breakdowns']
            if 'filtering' in reqparas_copy:
                del reqparas_copy['filtering']
            if 'fields' in reqparas_copy:
                del reqparas_copy['fields']
            stats = async_job.get_result(params=reqparas_copy)
            if stats is None:
                raise Exception("stats null")
        except Exception, e:
            print "get_stats_async error ", e
            print "do some sleep"
            time.sleep(int(20 * random.random() + 10))
            continue
        break
    return stats

def get_all_ad_accounts_data(start_datetime = None, end_datetime = None):
    date_ranges = None

    if start_datetime is not None and end_datetime is not None:
        delta = end_datetime - start_datetime
        if delta.days < 0:
            raise Exception("begin date should before end date")
        date_ranges = [(end_datetime - datetime.timedelta(days=x)).strftime("%Y-%m-%d") for x in range(0, delta.days + 1)]
    yesterday = datetime.datetime.now() - datetime.timedelta(1)
    if start_datetime is None:
        start_datetime = yesterday
    if end_datetime is None:
        end_datetime = yesterday
    delta = end_datetime - start_datetime
    if delta.days < 0:
        raise Exception("begin date should before end date")
    single_date_str = yesterday.strftime("%Y%m%d") if date_ranges is None else "_".join(date_ranges)
    ad_account_ids = get_ad_account_ids_by_date(start_datetime, end_datetime)

    from done_flag_runner import *
    for account_id in ad_account_ids:
        print "get account for {} ".format(account_id)
        #if account_done(None, "xxxxxxxx_stats_facebook_ads_dir", single_date_str,  str(account_id)):
        #    continue
        get_account_info_to_storage(account_id, date_ranges)
        #account_done(True, "xxxxxxxx_stats_facebook_ads_dir", single_date_str, str(account_id))
        time.sleep(int(10* random.random()))

def get_ad_account_ids_by_date(start_date, end_date):
    conn = connect()
    import MySQLdb as mdb
    cursor = conn.cursor(mdb.cursors.DictCursor)
    cursor.execute("select account_id from xxxx_stats_facebook_ads_account_id where start_date <= %s and end_date >= %s ", (end_date, start_date))
    account_ids = cursor.fetchall()
    return [item['account_id'] for item in account_ids]


if __name__ == "__main__":
    end_datetime = datetime.datetime.strptime(sys.argv[1].strip(), "%Y-%m-%d") if len(sys.argv) > 1 else None
    start_datetime = datetime.datetime.strptime(sys.argv[2].strip(), "%Y-%m-%d") if len(sys.argv) > 2 else end_datetime
    get_all_ad_accounts_data(start_datetime, end_datetime)