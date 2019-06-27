#!/bin/env python
#-*- encoding:utf-8 -*-
from googleads import adwords
from googleads import oauth2
import datetime
import os
import sys
import pandas as pd
import time
from xxxxxxxx_report_mysqldb import *
from xxxxxxxx_hiido_api import *
from done_flag_runner import *
from suds.cache import NoCache
from googleads.common import ZeepServiceProxy
PAGE_SIZE = 500


def DisplayAccountTree(account, accounts, links, depth=0):
    """Displays an account tree.
    Args:
      account: dict The account to display.
      accounts: dict Map from customerId to account.
      links: dict Map from customerId to child links.
      depth: int Depth of the current account in the tree.
    """
    prefix = '-' * depth * 2
    print '%s%s, %s' % (prefix, account['customerId'], account['name'])
    if account['customerId'] in links:
        for child_link in links[account['customerId']]:
            child_account = accounts[child_link['clientCustomerId']]
            DisplayAccountTree(child_account, accounts, links, depth + 1)


def MyInitAdwordsClient(client_id, client_secret, refresh_token, developer_token, user_agent, client_customer_id):
    oauth2_client = oauth2.GoogleRefreshTokenClient(client_id, client_secret, refresh_token)
    adwords_client = adwords.AdWordsClient(developer_token, oauth2_client, user_agent,
                                           client_customer_id=client_customer_id, cache=ZeepServiceProxy.NO_CACHE)
    return adwords_client

def get_country_campain(row):
    if not 'Campaign' in row:
        return None
    campaign_ar = row['Campaign'].split("-")
    country = "UNKOWN" if len(campaign_ar) < 2 else campaign_ar[2]
    return country

def googleads_to_storage(csvfile, mcc, account_id):
    con = get_conn_engine()
    df = pd.read_csv(csvfile, skiprows=1, skipfooter=1, engine='python')
    df['country'] = df.apply(get_country_campain, axis=1)# df.apply(lambda row: row['Campaign'].split("_"), axis=1)
    df['mcc'] = mcc
    df['account_id'] = account_id
    df.rename(columns={'Day': 'stats_day', 'Campaign': 'campaign_name', 'Campaign ID': 'campaign_id', 'Campaign state': 'campaign_state',
                       'Account':'account_name', 'Budget':'budget',  'Impressions':'impressions',
                       'Clicks':'clicks', 'Cost':'cost'}, inplace=True)
    
    df['cost'] = df['cost'] / 1000000
    df.to_sql('xxxx_stats_google_ads', con=con, if_exists='append', index=False)

'''
用于修正conversion的临时表
'''
def googleads_to_storage_conversion_fix(csvfile, mcc, account_id):
    con = get_conn_engine()
    df = pd.read_csv(csvfile, skiprows=1, skipfooter=1, engine='python')
    # df['country'] = df.apply(get_country_campain, axis=1)# df.apply(lambda row: row['Campaign'].split("_"), axis=1)
    df['mcc'] = mcc
    df.rename(columns={'Day': 'stats_day','Campaign':'campaign_name', 'Campaign ID': 'campaign_id', 'Campaign state': 'campaign_state',
                       'Account':'account_name', 'Conversions':'conversions',  'Conversion name':'conversion_name'}, inplace=True)
    df.to_sql('xxxx_stats_google_ads_real_conversion', con=con, if_exists='append', index=False)

def import_csv_files_to_db(targetdir, yesterday, del_old = False):
    flist = os.listdir(targetdir)
    if del_old:
        del_google_ads_data_by_date(yesterday)
    for each_mcc in flist:
        if "fix" in each_mcc:
            mcc = "".join(each_mcc.split("_")[2:])
        else:
            mcc = "".join(each_mcc.split("_")[1:])
        csvdir = os.path.join(targetdir, each_mcc)
        csvlist = os.listdir(csvdir)
        if "fix" in each_mcc:
            for each_csv in csvlist:
                account_id = each_csv.replace(".csv", "")
                p = os.path.join(csvdir, each_csv)
                googleads_to_storage_conversion_fix(p, mcc, account_id)
        else:
            for each_csv in csvlist:
                account_id = each_csv.replace(".csv", "")
                p = os.path.join(csvdir, each_csv)
                googleads_to_storage(p, mcc, account_id)

def del_google_ads_data_by_date(to_day):
    try:
        sqlconn = connect()
        cursor = sqlconn.cursor()
        cursor.execute(
            "delete from xxxx_stats_google_ads where stats_day = %s  ",
            ( to_day))
        cursor.execute(
            "delete from xxxx_stats_google_ads_real_conversion where stats_day = %s  ",
            (to_day))
        sqlconn.commit()
        sqlconn.close()
    except Exception, e:
        traceback.print_exc()
        print e
        raise e

def update_ads_platform(stats_day):
    sql = """
    update xxxx_stats_google_ads ads   join xxxx_stats_google_accounts_platform p on (ads.account_id = p.account_id and ads.stats_day = %s) 
set ads.platform  = if(p.platform = 'ios', '{}', '{}')
    """.format(get_platform_value('ios'), get_platform_value('android'))
    try:
        sqlconn = connect()
        cursor = sqlconn.cursor()
        cursor.execute(sql,( stats_day))
        sqlconn.commit()
        sqlconn.close()
    except Exception, e:
        traceback.print_exc()
        print e
        raise e

def update_real_conversion(stats_day):
    sql = """ update xxxx_stats_google_ads ads   join (
select campaign_id, stats_day, sum(conversions) as conversions from xxxx_stats_google_ads_real_conversion c  join xxxx_stats_google_conversion_type t on (c.conversion_name = t.conversion_name and t.is_cal = 1
and c.stats_day = %s ) group by campaign_id, stats_day
) M on (M.campaign_id = ads.campaign_id and M.stats_day = ads.stats_day) set ads.conversions = M.conversions """
    try:
        sqlconn = connect()
        cursor = sqlconn.cursor()
        cursor.execute(sql,( stats_day))
        sqlconn.commit()
        sqlconn.close()
    except Exception, e:
        traceback.print_exc()
        print e
        raise e

def get_main_accounts(client):
    managed_customer_service = client.GetService('ManagedCustomerService', version= 'v201809' ) #''v201609')
    offset = 0
    selector = {
        'fields': ['CustomerId', 'Name'],
        'paging': {
            'startIndex': str(offset),
            'numberResults': str(PAGE_SIZE)
        }
    }
    more_pages = True
    accounts = {}
    child_links = {}
    parent_links = {}
    root_account = None

    while more_pages:
        # Get serviced account graph.
        page = managed_customer_service.get(selector)
        if 'entries' in page and page['entries']:
            # Create map from customerId to parent and child links.
            if 'links' in page:
                for link in page['links']:
                    if link['managerCustomerId'] not in child_links:
                        child_links[link['managerCustomerId']] = []
                    child_links[link['managerCustomerId']].append(link)
                    if link['clientCustomerId'] not in parent_links:
                        parent_links[link['clientCustomerId']] = []
                    parent_links[link['clientCustomerId']].append(link)
            # Map from customerID to account.
            for account in page['entries']:
                accounts[account['customerId']] = account
        offset += PAGE_SIZE
        selector['paging']['startIndex'] = str(offset)
        more_pages = offset < int(page['totalNumEntries'])

    # Find the root account.
    for customer_id in accounts:
        if customer_id not in parent_links:
            root_account = accounts[customer_id]

    return accounts


def csv_main(client, account_id, targetdir, startday, endday):
    report_downloader = client.GetReportDownloader(version='v201809')
    report = {
        'reportName': 'CAMPAIGN_PERFORMANCE_REPORT FROM 170721',
        'dateRangeType': 'CUSTOM_DATE',
        'reportType': 'CAMPAIGN_PERFORMANCE_REPORT',  ## change to campaign performance
        'downloadFormat': 'CSV',
        'selector': {
            'fields': ['Date', 'CampaignId', 'CampaignName', 'CampaignStatus', 'AccountDescriptiveName', 'Amount',  'Impressions', 'Clicks', 'Cost'],
            'dateRange': {'min': startday, 'max': endday} # {'min': '20171127', 'max': '20171207'}  ## beautiful try!!
        } ##  'Conversions',
    }

    isExists = os.path.exists(targetdir)
    if not isExists:
        print targetdir + ' 创建成功'
        os.makedirs(targetdir)
    else:
        print targetdir + ' 目录已存在'
    os.chdir(targetdir)
    csvfile = os.path.join(targetdir, account_id + '.csv')
    with open(csvfile, 'w') as tf:
        report_downloader.DownloadReport(
            report, tf, skip_report_header=False, skip_column_header=False,
            skip_report_summary=False, include_zero_impressions=False)

def csv_main_fix(client, account_id, targetdir, startday, endday):
    report_downloader = client.GetReportDownloader(version='v201809')
    report = {
        'reportName': 'CAMPAIGN_PERFORMANCE_REPORT FROM 170721',
        'dateRangeType': 'CUSTOM_DATE',
        'reportType': 'CAMPAIGN_PERFORMANCE_REPORT',  ## change to campaign performance
        'downloadFormat': 'CSV',
        'selector': {
            'fields': ['Date','CampaignId', 'CampaignName', 'CampaignStatus', 'AccountDescriptiveName', 'Conversions',   'ConversionTypeName'],
            'dateRange': {'min': startday, 'max': endday}
        }
    }
    isExists = os.path.exists(targetdir)
    if not isExists:
        print targetdir + ' 创建成功'
        mkdir_p(targetdir, False)
    else:
        print targetdir + ' 目录已存在'
    os.chdir(targetdir)
    csvfile = os.path.join(targetdir, account_id + '.csv')
    with open(csvfile, 'w') as tf:
        report_downloader.DownloadReport(
            report, tf, skip_report_header=False, skip_column_header=False,
            skip_report_summary=False, include_zero_impressions=False)

@sync_remote_file(os.path.dirname(os.path.abspath(__file__)))
def report_main(yesterday, overwriteExists = False, target_filename = None):
    mcclist = ['915-100-xxxx', '202-456-xxxx']
    mcclist2 = mcclist + [x.replace('-','') for x in mcclist]
    if not overwriteExists and os.path.exists(target_filename):
        return target_filename

    base_dir = target_filename
    for each_mcc in mcclist:
        CLIENT_ID = 'xxxxxx.apps.googleusercontent.com'
        CLIENT_SECRET = 'xxxxxxxx'
        REFRESH_TOKEN = 'xxxxxxxx'
        DEVELOPER_TOKEN = 'xxxxxxxx'
        USER_AGENT = 'BG'
        CLIENT_CUSTOMER_ID = each_mcc
        adwords_client = MyInitAdwordsClient(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, DEVELOPER_TOKEN, USER_AGENT,
                                             CLIENT_CUSTOMER_ID)
        d = get_main_accounts(adwords_client)

        targetdir = os.path.join(base_dir, 'mcc_' + each_mcc)
        fix_targetdir = os.path.join(base_dir, 'mcc_fix_' + each_mcc)
        for each_account in d:
            if str(d[each_account]['customerId']) in mcclist2:
                continue

            SUB_CLIENT_ID = 'xxxxxxxxx.apps.googleusercontent.com'
            SUB_CLIENT_SECRET = 'xxxxxxxxx'
            SUB_REFRESH_TOKEN = 'xxxxxxxxx'
            SUB_DEVELOPER_TOKEN = 'xxxxxxxxx'
            SUB_USER_AGENT = 'BG'
            SUB_CLIENT_CUSTOMER_ID = str(d[each_account]['customerId'])
            sub_adwords_client = MyInitAdwordsClient(SUB_CLIENT_ID, SUB_CLIENT_SECRET, SUB_REFRESH_TOKEN,
                                                     SUB_DEVELOPER_TOKEN, SUB_USER_AGENT, SUB_CLIENT_CUSTOMER_ID)
            csv_main(client=sub_adwords_client, account_id=SUB_CLIENT_CUSTOMER_ID, targetdir=targetdir, startday=yesterday, endday=yesterday)
            csv_main_fix(client=sub_adwords_client, account_id=SUB_CLIENT_CUSTOMER_ID, targetdir=fix_targetdir,
                     startday=yesterday, endday=yesterday)
    return base_dir

'''
10点后 前一天的数据
'''
if __name__ == '__main__':
    yesterday_obj = (datetime.datetime.now() - datetime.timedelta(1)) if len(sys.argv) < 2 else datetime.datetime.strptime(
        sys.argv[1].strip(), "%Y-%m-%d")
    base_path = os.path.dirname(os.path.abspath(__file__))
    targetdir = report_main(yesterday_obj.strftime("%Y%m%d"), target_filename=os.path.join("xxxxxxxx_stats_google_ads", yesterday_obj.strftime("%Y%m%d")))
    import_csv_files_to_db(targetdir,  yesterday_obj.strftime("%Y-%m-%d"), del_old = True)

    update_ads_platform(yesterday_obj.strftime("%Y-%m-%d"))
    update_real_conversion(yesterday_obj.strftime("%Y-%m-%d"))