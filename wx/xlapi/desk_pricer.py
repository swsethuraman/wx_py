from wx.models.wx_helpers import VanillaSubIndex
import mysql.connector
import pandas as pd
import numpy as np
import datetime
from pyxll import xl_func, xl_macro, xl_app, xlcAlert
import wx.utils.common as common

# wp = Wx1Provider()

def desk_pricer_data_call(vs):
    # GET RELEVANT MONTHS FOR RISK PERIOD
    start_year = datetime.datetime.strptime(vs['risk_start'], '%Y-%m-%d').year
    end_year = datetime.datetime.strptime(vs['risk_end'], '%Y-%m-%d').year
    start_month = datetime.datetime.strptime(vs['risk_start'], '%Y-%m-%d').month
    end_month = datetime.datetime.strptime(vs['risk_end'], '%Y-%m-%d').month
    if end_year > start_year:
        date_where = '(extract(month from recorddate)>=' + str(
            start_month) + ' or extract(month from recorddate)<=' + str(end_month) + ')'
    else:
        date_where = '(extract(month from recorddate)>=' + str(
            start_month) + ' and extract(month from recorddate)<=' + str(end_month) + ')'

    # SET DB CONNECTION
    mydb = mysql.connector.connect(
        host='mdb-wx1.use4.gcp.laurion.corp',
        user='ssethuraman',
        password='BTdaWJep,C!BUx-gHJZ#:b}Mp2vUz#?P',
        database='WX1'
    )
    mycursor = mydb.cursor()

    # CONVERT WBAN TO GHCND ID
    sql = 'select stationID from WX1.NOAA_Ref where WBAN=\'' + str(vs['location'])[-5:] + '\''
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    station_id = myresult[0][0]

    # LOAD HISTORICAL DATA FOR RELEVANT MONTHS IN DF
    sql = """
    select recorddate,
        tmin,
        tmax
    from WX1.NOAA_Daily_Tempr 
    where recorddate>=date'1980-1-1'
    and stationid='""" + station_id + '\' and ' + date_where
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    df = pd.DataFrame(myresult)

    # CONVERT TO DAILY HDDs, CDDs
    df['TAvg'] = (df[1] + df[2]) / 2
    df['HDD'] = 65 - df['TAvg']
    df['HDD'] = np.where(df['HDD'] < 0, 0, df['HDD'])
    df['CDD'] = df['TAvg'] - 65
    df['CDD'] = np.where(df['CDD'] < 0, 0, df['CDD'])
    return df

@xl_macro("object vanilla, str model: object")
def desk_pricer(sub_index_spec):
    sub_index_spec = common.sanitize_none(sub_index_spec)
    vs = VanillaSubIndex(sub_index_spec)
    df = desk_pricer_data_call(vs)