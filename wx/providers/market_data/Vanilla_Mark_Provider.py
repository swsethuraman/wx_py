import pandas as pd
import numpy as np
from xlib import xdb
import openpyxl as op

###VANILLA MARK FUNCTIONS

def get_realized(db,loc_str):
    sql_realized = """
        select extract(month from OPR_DATE) as MONTH,
            ICAO,
            sum(HDDS) as HDDS,
            sum(CDDS) as CDDS,
            max(OPR_DATE) as OPR_DATE
        from (
            select OPR_DATE,
                STATION as ICAO,
                round(greatest(65-((TMIN+TMAX)/2),0),2) as HDDS,
                round(greatest(((TMIN+TMAX)/2)-65,0),2) as CDDS
            from WX1.MS_CWG_OBS_NA_F
            where extract(year from OPR_DATE)=extract(year from date_sub(curdate(),interval 1 day))
                and extract(month from OPR_DATE)=extract(month from date_sub(curdate(),interval 1 day))
                and OPR_DATE<curdate()
                and STATION in (""" + loc_str + """)
        ) a
        group by extract(month from OPR_DATE), ICAO
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_real = conn.query(sql_realized)
    conn.close()
    return df_real

def choose_fcst_date(last_realized):
    forecast_month = last_realized.month
    forecast_year = last_realized.year
    forecast_day = last_realized.day
    fcst_date = str(forecast_year)+'-'+str(forecast_month)+'-'+str(forecast_day)
    return fcst_date

def get_cwg_fcst(db,loc_str,fcst_date,prompt='N'):
    if prompt=='Y':
        operator='>'
        adder = '1'
    else:
        operator='='
        adder = '0'
    sql_fcst1 = """
        select extract(month from date_add(OPR_DATE,interval """+adder+""" month)) as MONTH,
            ICAO,
            sum(HDDS) as HDDS,
            sum(CDDS) as CDDS,
            min(OPR_DATE) as START_DATE,
            max(OPR_DATE) as END_DATE
        from (
            select date_add(OPR_DATE,interval cast(replace(FORECAST_DAY,'D','') as signed) day) as OPR_DATE,
                STATION as ICAO,
                greatest(65-((FCST_MN + FCST_MX)/2),0) as HDDS,
                greatest(((FCST_MN + FCST_MX)/2)-65,0) as CDDS
            from WX1.MS_CWG_FCST_NA
            where OPR_DATE = (
                select max(FCST_DATE) as FCST_DATE from (
                    select ifnull(b.FCST_DATE,max(a.OPR_DATE)) as FCST_DATE 
                    from WX1.MS_CWG_FCST_NA a
                    left join (
                        select min(OPR_DATE) as FCST_DATE from WX1.MS_CWG_FCST_NA
                        where opr_date>date'"""+fcst_date+"""'
                    ) b
                    on a.OPR_DATE>date_sub(curdate(), interval 1 day)
                    group by b.FCST_DATE
                ) c1
            )
            and STATION in (""" + loc_str + """)
        ) c
        where OPR_DATE>date'"""+fcst_date+"""'
        and extract(month from OPR_DATE) """+operator+""" extract(month from date'"""+fcst_date+"""')
        group by extract(month from date_add(OPR_DATE,interval """+adder+""" month)), ICAO
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_fcst1 = conn.query(sql_fcst1)
    conn.close()
    return df_fcst1

def format_fcst(df_fcst1):
    df_fcst_format = df_fcst1[['START_DATE','END_DATE']][0:1].reset_index(drop=True)
    df_cwg = df_fcst1[['ICAO','HDDS']].transpose()
    df_cwg = df_cwg.rename(columns=df_cwg.iloc[0])[1:].reset_index()
    df_cwg = df_cwg.rename(columns={'index':'SOURCE'})
    df_cwg['SOURCE'] = 'CWG'
    df_fcst_format_hdd = df_fcst_format.join(df_cwg,how='inner')
    df_cwg = df_fcst1[['ICAO','CDDS']].transpose()
    df_cwg = df_cwg.rename(columns=df_cwg.iloc[0])[1:].reset_index()
    df_cwg = df_cwg.rename(columns={'index':'SOURCE'})
    df_cwg['SOURCE'] = 'CWG'
    df_fcst_format_cdd = df_fcst_format.join(df_cwg,how='inner')
    return df_fcst_format_hdd, df_fcst_format_cdd

def get_balmo(db,last_realized,loc_str,prompt='N'):
    if prompt=='Y':
        operator='>='
    else:
        operator='>'
    sql_balmo1 = """
        select extract(YEAR from OPR_DATE) as YEAR,
            ICAO,
            sum(HDDS) as HDDS,
            sum(CDDS) as CDDS,
            min(OPR_DATE) as START_DATE,
            max(OPR_DATE) as END_DATE
        from (
            select OPR_DATE,
                ICAO,
                round(greatest(65-((tmin +tmax)/2),0),2) as HDDS,
                round(greatest(((tmin +tmax)/2)-65,0),2) as CDDS
            from WX1.WX_WEATHER_DAILY_CLEANED
            where opr_date>=date'1980-1-1'
            and extract(year from opr_date) < """ + str(last_realized.year) + """
            and extract(month from opr_date) = """ + str(last_realized.month) + """
            and extract(day from opr_date) """+operator + str(last_realized.day) + """
            and ICAO in (""" + loc_str + """)
        ) a
        group by extract(YEAR from OPR_DATE), ICAO
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_balmo1 = conn.query(sql_balmo1)
    conn.close()
    return df_balmo1

def format_balmo(df):
    df_balmo1_format = pd.DataFrame(df[['START_DATE','END_DATE','YEAR']].drop_duplicates().reset_index(drop=True))
    #BALMO HDDS
    try:
        del df_balmo_hdds
    except:
        pass
    for y in df['YEAR'].drop_duplicates():
        df_temp = df[['ICAO','HDDS']].iloc[np.where(df['YEAR']==y)]
        df_temp = df_temp.transpose().reset_index(drop=True)
        df_temp = df_temp.rename(columns=df_temp.iloc[0])[1:].reset_index(drop=True)
        df_temp['YEAR'] = y
        try:
            df_balmo_hdds = df_balmo_hdds.append(df_temp)
        except:
            df_balmo_hdds = df_temp.copy()
    df_balmo1_format_hdds = pd.merge(df_balmo1_format,df_balmo_hdds,how='inner',on='YEAR')
    #BALMO CDDS
    try:
        del df_balmo_cdds
    except:
        pass
    for y in df['YEAR'].drop_duplicates():
        df_temp = df[['ICAO','CDDS']].iloc[np.where(df['YEAR']==y)]
        df_temp = df_temp.transpose().reset_index(drop=True)
        df_temp = df_temp.rename(columns=df_temp.iloc[0])[1:].reset_index(drop=True)
        df_temp['YEAR'] = y
        try:
            df_balmo_cdds = df_balmo_cdds.append(df_temp)
        except:
            df_balmo_cdds = df_temp.copy()
    df_balmo1_format_cdds = pd.merge(df_balmo1_format,df_balmo_cdds,how='inner',on='YEAR')
    return df_balmo1_format_hdds, df_balmo1_format_cdds

def get_balmo_after_fcst(db,last_forecast,loc_str,prompt='N'):
    if prompt=='Y':
        operator='>'
    else:
        operator='>'
    sql_balmo2 = """
        select extract(YEAR from OPR_DATE) as YEAR,
            ICAO,
            sum(HDDS) as HDDS,
            sum(CDDS) as CDDS,
            min(OPR_DATE) as START_DATE,
            max(OPR_DATE) as END_DATE
        from (
            select OPR_DATE,
                ICAO,
                round(greatest(65-((tmin +tmax)/2),0),2) as HDDS,
                round(greatest(((tmin +tmax)/2)-65,0),2) as CDDS
            from WX1.WX_WEATHER_DAILY_CLEANED
            where opr_date>=date'1980-1-1'
            and extract(year from opr_date) < """ + str(last_forecast.year) + """
            and extract(month from opr_date) = """ + str(last_forecast.month) + """
            and extract(day from opr_date) """+operator + str(last_forecast.day) + """
            and ICAO in (""" + loc_str + """)
        ) a
        group by extract(YEAR from OPR_DATE), ICAO
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_balmo2 = conn.query(sql_balmo2)
    conn.close()
    return df_balmo2

def format_balmo_after_fcst(df):
    df_balmo2_format = pd.DataFrame(df[['START_DATE','END_DATE','YEAR']].drop_duplicates().reset_index(drop=True))
    #BALMO HDDS
    try:
        del df_balmo_hdds
    except:
        pass
    for y in df['YEAR'].drop_duplicates():
        df_temp = df[['ICAO','HDDS']].iloc[np.where(df['YEAR']==y)]
        df_temp = df_temp.transpose().reset_index(drop=True)
        df_temp = df_temp.rename(columns=df_temp.iloc[0])[1:].reset_index(drop=True)
        df_temp['YEAR'] = y
        try:
            df_balmo_hdds = df_balmo_hdds.append(df_temp)
        except:
            df_balmo_hdds = df_temp.copy()
    df_balmo2_format_hdds = pd.merge(df_balmo2_format,df_balmo_hdds,how='inner',on='YEAR')
    #BALMO CDDS
    try:
        del df_balmo_cdds
    except:
        pass
    for y in df['YEAR'].drop_duplicates():
        df_temp = df[['ICAO','CDDS']].iloc[np.where(df['YEAR']==y)]
        df_temp = df_temp.transpose().reset_index(drop=True)
        df_temp = df_temp.rename(columns=df_temp.iloc[0])[1:].reset_index(drop=True)
        df_temp['YEAR'] = y
        try:
            df_balmo_cdds = df_balmo_cdds.append(df_temp)
        except:
            df_balmo_cdds = df_temp.copy()
    df_balmo2_format_cdds = pd.merge(df_balmo2_format,df_balmo_cdds,how='inner',on='YEAR')
    return df_balmo2_format_hdds, df_balmo2_format_cdds

def style_xlsx(current_file,mon,day,yr):
    #LOAD WORKSHEET
    wb = op.load_workbook(filename=current_file)
    ws = wb['REALIZED']
    #WIDTHS AND HEIGHTS
    ws.column_dimensions['E'].width = 20
    for sheet in ['FORECAST (HDD)','FORECAST (CDD)','BALMO (HDD)','BALMO (CDD)','BALMO AFTER FORECAST (HDD)','BALMO AFTER FORECAST (CDD)']:
        ws = wb[sheet]
        ws.column_dimensions['A'].width = 15
        ws.column_dimensions['B'].width = 15
    wb.save(filename=current_file)