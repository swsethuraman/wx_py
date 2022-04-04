import pandas as pd
from datetime import datetime
from xlib import xdb
import numpy as np
import xlsxwriter
from wx.providers.common import Common_Functions as CF

def load_new_data(yr, mon, day):
    df = pd.read_csv('/data/reports/counterparty-health-wx/'+yr+'/'+mon+'/counterparty-health-wx-'+yr+mon+day+'.csv')
    df.rename(columns={'Market Cap':'Market_Cap','CDS 1Y':'CDS_1Y','CDS 5Y':'CDS_5Y','Z-Spread':'Z_Spread','S&P Rating':'SP_Rating'},inplace=True)
    df.rename(columns={'Fitch Rating':'Fitch_Rating','Moody\'s Rating':'Moodys_Rating','Tier 1 Ratio':'Tier_1_Ratio','1D % chg':'oneday_chg','5D % chg':'fiveday_chg'},inplace=True)
    df.rename(columns={'1M % chg':'onemonth_chg','6M % chg':'sixmonth_chg','NAME':'company_name'},inplace=True)
    df['report_date'] = yr+mon+day
    db = 'WX2-GC'
    table='CREDIT_HEALTH_REPORT'
    CF.insert_update(db,table,df,'N')

def gen_new_report():
    db = 'WX2-GC'
    conn = xdb.make_conn(db, stay_open=True)
    df=conn.query('select * from WX2.CREDIT_HEALTH_REPORT_V')
    conn.close()
    df = df.drop(columns=['report_date','dod_cds_1y_chg','dod_cds_5y_chg','dod_cds_spread_chg','wow_cds_1y_chg','wow_cds_5y_chg','wow_cds_spread_chg','mom_cds_1y_chg','mom_cds_5y_chg','mom_cds_spread_chg'])
    df.rename(columns={'market_cap':'Market Cap','cds_1y':'CDS 1Y','cds_5y':'CDS 5Y','z_spread':'Z-Spread','sp_rating':'S&P Rating'},inplace=True)
    df.rename(columns={'fitch_rating':'Fitch Rating','moodys_rating':'Moody\'s Rating','tier_1_ratio':'Tier 1 Ratio','oneday_chg':'1D % chg','fiveday_chg':'5D % chg'},inplace=True)
    df.rename(columns={'onemonth_chg':'1M % chg','sixmonth_chg':'6M % chg','company_name':'Name','ticker':'Ticker'},inplace=True)
    return df

def format_new_report(df, yr, mon, day):
    writer = pd.ExcelWriter('/home/rday/data/Credit_Health_Report/wx_credit_health_report_'+yr+mon+day+'.xlsx', engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1',index=False)
    workbook  = writer.book
    worksheet = writer.sheets['Sheet1']
    bold = workbook.add_format({'bold': True})
    italic = workbook.add_format({'italic': True})
    color1 = workbook.add_format({'bg_color': 'green'})
    color2 = workbook.add_format({'bg_color': 'red'})
    color3 = workbook.add_format({'bg_color': 'yellow'})
    #Set counterparties currently on our book to italic
    exposed = np.where(df['exposure_flag']==1)[0]
    for i in exposed:
        worksheet.write(i+1, 0, df['Ticker'][i], italic)
    #Set counterparties with credit exposure to bold
    exposed = np.where(df['exposure_flag']==-1)[0]
    for i in exposed:
        worksheet.write(i+1, 0, df['Ticker'][i], bold)
    #Set counterparties with spread change to green
    alert1 = np.where(df['spread_change_flag']==1)[0]
    for j in alert1:
        worksheet.write(j+1, 1, df['Name'][j], color3)
    #Set counterparties with positive rating change flag to green
    alert1 = np.where(df['rating_change_flag']==1)[0]
    for j in alert1:
        worksheet.write(j+1, 1, df['Name'][j], color1)
    #Set counterparties with positive cds 1yr change flag to green
    alert2 = np.where(df['cds1y_change_flag']==1)[0]
    for k in alert2:
        worksheet.write(k+1, 1, df['Name'][k], color1)
    #Set counterparties with positive cds 5yr change flag to green
    alert2 = np.where(df['cds5y_change_flag']==1)[0]
    for k in alert2:
        worksheet.write(k+1, 1, df['Name'][k], color1)
    #Set counterparties with positive market cap change flag to green
    alert2 = np.where(df['market_cap_flag']==1)[0]
    for k in alert2:
        worksheet.write(k+1, 1, df['Name'][k], color1)
    #Set counterparties with negative rating change flag to red
    alert1 = np.where(df['rating_change_flag']==-1)[0]
    for j in alert1:
        worksheet.write(j+1, 1, df['Name'][j], color2)
    #Set counterparties with negative cds 1yr change flag to red
    alert2 = np.where(df['cds1y_change_flag']==-1)[0]
    for k in alert2:
        worksheet.write(k+1, 1, df['Name'][k], color2)
    #Set counterparties with negative cds 5yr change flag to red
    alert2 = np.where(df['cds5y_change_flag']==-1)[0]
    for k in alert2:
        worksheet.write(k+1, 1, df['Name'][k], color2)
    #Set counterparties with negative market cap change flag to red
    alert2 = np.where(df['market_cap_flag']==-1)[0]
    for k in alert2:
        worksheet.write(k+1, 1, df['Name'][k], color2)
    for idx, col in enumerate(df):  # loop through all columns
        series = df[col]
        max_len = max((
            series.astype(str).map(len).max(),  # len of largest item
            len(str(series.name))  # len of column name/header
            )) + 1  # adding a little extra space
        worksheet.set_column(idx, idx, max_len)
    writer.save()
