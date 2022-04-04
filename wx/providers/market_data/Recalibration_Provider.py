import pandas as pd
import numpy as np
import sqlite3
from xlib import xdb
import openpyxl as op
import os
from wx.providers.common import Common_Functions as CF

def run_recal(db, data_source, id_type, main_station, pair_stations, distance_threshold):
    #SET DEFAULT PARATMETERS BASED ON SELECTIONS ABOVE
    if data_source=='WATERFALL':
        table_name='WX_WEATHER_DAILY_CLEANED'
        id_type='icao'
        date_col = 'OPR_DATE'
        tmin_col = 'TMIN'
        tmax_col ='TMAX'
        min_date = '1980-1-1'
        min_year = '1980'
        ref_table = 'NOAA_WMO_WBAN'
    elif data_source=='NIWA':
        table_name='NIWA_CLIFLOW_DATA_CLEANED'
        id_type='station'
        date_col = 'OPR_DATE'
        tmin_col = 'TMIN'
        tmax_col ='TMAX'
        min_date = '2000-1-1'
        min_year = '2000'
        ref_table = 'PLEASE INSERT REF TABLE HERE'
    elif data_source=='CWG_C':
        table_name='CWG_CITY_OBS_C_V'
        date_col = 'EFFECTIVE_DATE'
        tmin_col = 'MIN_TEMP'
        tmax_col ='MAX_TEMP'
        min_date = '1980-1-1'
        min_year = '1980'
        ref_table = 'CWG_REF'
    elif data_source=='CWG_F':
        table_name='CWG_CITY_OBS_F_V'
        date_col = 'EFFECTIVE_DATE'
        tmin_col = 'MIN_TEMP'
        tmax_col ='MAX_TEMP'
        min_date = '1980-1-1'
        min_year = '1980'
        ref_table = 'CWG_REF'
    else:
        table_name='NOAA_Daily_Tempr'
        id_type='stationid'
        date_col = 'RecordDate'
        tmin_col = 'TMin'
        tmax_col ='TMax'
        min_date = '1980-1-1'
        min_year = '1980'
        ref_table = 'NOAA_Ref'
    df_stations, df_pair_options = eval_neighbors(db,main_station,pair_stations,ref_table,id_type,distance_threshold,min_date)
    df_corrs, df_raw = select_pairs(db, df_stations, df_pair_options, date_col, tmax_col, tmin_col, table_name, id_type, min_date)
    df_main, pairs, tier1 = gen_deltas(db, df_stations, df_corrs, df_pair_options, date_col, id_type, tmax_col, tmin_col, table_name, main_station, min_date)
    df_dh_main, tier2 = get_temps(db, df_pair_options, pairs, tier1)
    df_avg_hist = agg_temps(df_dh_main, min_year)
    df_delta_hist, num_cols = gen_hist_deltas(df_avg_hist, main_station, pairs, tier2)
    df_final = format_final(df_main, df_delta_hist, pairs, num_cols)
    #RENAME COLUMNS TO MATCH DB TABLES
    df_corrs = df_corrs.rename(columns={'MainStation':'MAIN_STATION','PairStation':'PAIR_STATION','CorrMax':'CORR_MAX','CorrMin':'CORR_MIN'})
    df_raw = df_raw.rename(columns={'RecordDate':'OPR_DATE','MainStation':'MAIN_STATION','TMax_m':'TMAX','TMin_m':'TMIN'})
    df_final = df_final.reset_index()
    df_final = df_final.rename(columns={'StationID_Main':'MAIN_STATION'})
    df_final.columns = [c.upper() for c in df_final.columns]
    #PUT DATA IN DB
    CF.insert_update(db,'WX_RECAL_CORRS',df_corrs,'N')
    CF.insert_update(db,'WX_RECAL_RAW',df_raw,'N')
    CF.insert_update(db,'WX_RECAL_FINAL',df_final,'N')
    #SAVE DATA TO EXCEL
    start_col=0
    start_row=0
    current_file = '/shared/wx/Data/recalibration/'+main_station+'.xlsx'
    try:
        os.remove(current_file)
    except:
        pass
    CF.df_to_xlsx(df_final,current_file,'RECAL',start_col,start_row)
    CF.df_to_xlsx(df_raw,current_file,'RAW',start_col,start_row)
    CF.df_to_xlsx(df_corrs,current_file,'CORRS',start_col,start_row)
    style_xlsx(current_file)

def eval_neighbors(db,main_station,pair_stations,ref_table,id_type,distance_threshold,min_date):
    #GENERATE A DATAFRAME FOR MAIN STATION AND POTENTIAL PAIRS
    df_stations = pd.DataFrame(columns = ['stationid'])
    new_row = {'stationid':main_station}
    df_stations = df_stations.append(new_row, ignore_index=True)
    #TRY PART SHOULD WORK IF pair_stations WERE ENTERED. EXCEPT SHOULD BE TRIGGERED IF pair_stations=[]
    try:
        pair_stations[0]
        df_pair_options = pd.DataFrame(pair_stations)
        df_pair_options.rename(columns={0:'stationid'},inplace=True)
        #IF PAIRS WERE SPECIFIED, ADD MAIN STATION TO PAIR LIST. NOT INTUITIVE, BUT REQUIRED FOR LATER STEPS
        df_pair_options = df_pair_options.append(new_row, ignore_index=True)
    except:
        lat_query =  "select lat from WX1." + ref_table + " where " + id_type + "='" + main_station + "'"
        lon_query =  "select lon from WX1." + ref_table + " where " + id_type + "='" + main_station + "'"
        conn = xdb.make_conn(db, stay_open=True)
        df_lat= conn.query(lat_query)
        df_lon= conn.query(lon_query)
        conn.close()
        lat = df_lat.values[0][0]
        lon = df_lon.values[0][0]
        conn.close()
        sql_pair_noaa = """
            select a.opr_date as recorddate,  
                a.icao as stationid, 
                a.tmax, 
                a.tmin 
            from WX1.WX_WEATHER_DAILY_CLEANED a 
            inner join (
                select * 
                from WX1.NOAA_WMO_WBAN
                where abs((lat) - """ + str(lat) + """) < """ + distance_threshold + """
                and abs((lon) - """ + str(lon) + """) < """ + distance_threshold + """
            ) b
            on a.icao=b.icao
            where a.opr_date>=date'""" + min_date + """'
        """
        conn = xdb.make_conn(db, stay_open=True)
        df_pair_noaa= conn.query(sql_pair_noaa)
        conn.close()
        arr_noaa = df_pair_noaa.values
        sql_pair_cwgf = """
            select effective_date as recorddate, 
                station as stationid, 
                max_temp as tmax, 
                min_temp as tmin 
            from WX1.CWG_CITY_OBS_F_V
            where abs((lat) - """ + str(lat) + """) < """ + distance_threshold + """
            and abs((lon) - """ + str(lon) + """) < """ + distance_threshold + """
            and effective_date>=date'""" + min_date + """'
        """
        conn = xdb.make_conn(db, stay_open=True)
        df_pair_cwgf= conn.query(sql_pair_cwgf)
        conn.close()
        arr_cwgf = df_pair_cwgf.values
        sql_pair_cwgc = """
            select effective_date as recorddate, 
                station as stationid, 
                max_temp as tmax, 
                min_temp as tmin 
            from WX1.CWG_CITY_OBS_C_V
            where abs((lat) - """ + str(lat) + """) < """ + distance_threshold + """
            and abs((lon) - """ + str(lon) + """) < """ + distance_threshold + """
            and effective_date>=date'""" + min_date + """'
        """
        conn = xdb.make_conn(db, stay_open=True)
        df_pair_cwgc= conn.query(sql_pair_cwgc)
        conn.close()
        arr_cwgc = df_pair_cwgc.values
        sql_pair_niwa = """
            select opr_date as recorddate,
                station as stationid,
                tmax,
                tmin
            from (
                select a.opr_date, a.station, a.tmax, a.tmin, b.lat, b.lon from WX1.NIWA_CLIFLOW_DATA_CLEANED a
                inner join WX1.NIWA_REF b on a.station=b.name
            ) c
            where abs((lat) - """ + str(lat) + """) < """ + distance_threshold + """
            and abs((lon) - """ + str(lon) + """) < """ + distance_threshold + """
            and opr_date>=date'""" + min_date + """'
        """
        conn = xdb.make_conn(db, stay_open=True)
        df_pair_niwa= conn.query(sql_pair_niwa)
        conn.close()
        arr_niwa = df_pair_niwa.values

    arr_con = arr_noaa
    #arr_con = np.concatenate((arr_noaa, arr_cwgf), axis=0)
    #arr_con = np.concatenate((arr_con, arr_cwgc), axis=0)
    #arr_con = np.concatenate((arr_con, arr_niwa), axis=0)
    df_pair_options = pd.DataFrame(arr_con)
    df_pair_options.rename(columns={0:'recorddate',1:'stationid',2:'tmax',3:'tmin'},inplace=True)
    return df_stations, df_pair_options

def select_pairs(db, df_stations, df_pair_options, date_col, tmax_col, tmin_col, table_name, id_type, min_date):
    #GET CORRELATIONS BETWEEN STATIONS & SELECT TOP 7 (OR LESS IF NOT ENOUGH STATATIONS MEET MINIMUM CORRELATION COEFF THRESHOLD)
    df_pair_ids = df_pair_options.stationid.unique()
    df_corrs = pd.DataFrame(columns = ['MainStation','PairStation','CorrMax','CorrMin'])
    #GET DATA FOR MAIN STATION. CURRENTLY ONLY BEEN TESTED FOR 1 MAIN STATION, BUT SHOULD WORK FOR MANY (MIGHT NEED TWEAKS)
    for i in df_stations.itertuples():
        sql_main_test = """
            select """ + date_col + """ as RecordDate,
                '""" + i.stationid + """' as MainStation,
                """ + tmax_col + """ as TMax_m, 
                """ + tmin_col + """ as TMin_m 
            from WX1.""" + table_name + """
            where """ + id_type + """ = '""" + i.stationid + """'
            and """ + date_col + """>=date'""" + min_date + """'
        """
        conn = xdb.make_conn(db, stay_open=True)
        df_main_test= conn.query(sql_main_test)
        df_main_test['RecordDate']= pd.to_datetime(df_main_test['RecordDate'])
        conn.close()
        #SAVE RAW DATA FOR FUTURE REFERENCE
        df_raw = df_main_test.copy()
        df_main_test = df_main_test[['RecordDate','TMax_m','TMin_m']]
        #GET DATA FOR EACH POTENTIAL PAIR
        temp_conn = sqlite3.connect(':memory:')
        df_pair_options.to_sql('df_pair', temp_conn, index=False)
        for j in df_pair_ids:
            query_pair_test = """
                select recorddate as RecordDate,
                    tmax  as TMax_p, 
                    tmin as TMin_p
                from df_pair
                where stationid = '""" + j + """'
            """
            df_pair_test = pd.read_sql_query(query_pair_test, temp_conn)
            df_pair_test['RecordDate']= pd.to_datetime(df_pair_test['RecordDate'])
            #MERGE MAIN AND PAIR STATION INTO 1 DF
            #!df_comb_test = df_main_test.join(df_pair_test.set_index('RecordDate'), on='RecordDate')
            df_comb_test = pd.merge(df_main_test, df_pair_test, how='left', on = ['RecordDate'])
            #CALCULATE CORRELATION COEFFS BETWEEN MAIN AND PAIR (SEPARATELY FOR TMIN AND TMAX)
            corr_max = df_comb_test['TMax_m'].corr(df_comb_test['TMax_p'])
            corr_min = df_comb_test['TMin_m'].corr(df_comb_test['TMin_p'])
            #CHECK IF COEFFS MEET THRESHOLD
            if corr_max>0.9 or corr_min>0.9:
                new_row = {'MainStation':i.stationid, 'PairStation':j, 'CorrMax':corr_max, 'CorrMin':corr_min}
                df_corrs = df_corrs.append(new_row, ignore_index=True)
        temp_conn.close()
    #SELECT TOP 7 PAIRS. KEEP IN MIND TOP PAIR WILL ALWAYS BE MAIN STATION ITSELF, SO REALLY WE WANT TOP 8 AND EXCLUDE 1ST
    #MAY BE LESS THAN 7 PAIRS IF CORRELATIONS ARE TOO WEAK. CODE WILL WORK AS LONG AS THERE IS AT LEAST 1, BUT MIGHT NOT LEAD TO RELIABLE RECALIBRATIONS
    df_corrs = df_corrs.sort_values(by=['CorrMax','CorrMin'], ascending=False)[1:8]
    return df_corrs, df_raw

def gen_deltas(db, df_stations, df_corrs, df_pair_options, date_col, id_type, tmax_col, tmin_col, table_name, main_station, min_date):
    #GET THE DELTA BETWEEN MAIN STATION AND EACH PAIR FOR EACH DAY IN RECALIBRATION PERIOD
    #DELTA IS CALCULATED SEPARATELY FOR TMIN AND TMAX
    #NEED TO ADD STEP TO ENSURE PAIRS HAVE SUFFICIENT DATA QUALITY

    for i in df_stations.itertuples():
        main_station = i.stationid
    pairs = []

    for j in df_corrs.itertuples():
        pairs.append(j.PairStation)

    #GET RAW DAILY  DATA FOR MAIN STATION INTO A DF
    sql_main = """
        select """ + date_col + """ as RecordDate, 
            """ + id_type + """ as StationID, 
            """ + tmax_col + """ as TMax,
            """ + tmin_col + """ as TMin,
            0 as Tier
        from WX1.""" + table_name + """
        where """ + id_type + """ = '""" + main_station + """'
        and """ + date_col + """>=date'""" + min_date + """'
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_main= conn.query(sql_main)
    df_main['RecordDate']= pd.to_datetime(df_main['RecordDate'])
    #GET RAW DAILY DATA FOR EACH PAIR INTO A DF
    df_pairs = pd.DataFrame(columns = ['RecordDate','StationID','TMax','Tmin', 'Tier'])
    temp_conn = sqlite3.connect(':memory:')
    df_pair_options.to_sql('df_pair', temp_conn, index=False)
    x=1
    for i in pairs:
        query_pair = """
            select recorddate as RecordDate,
                stationid as StationID,
                tmax  as TMax, 
                tmin as TMin,
                """ + str(x) + """ as Tier
            from df_pair
            where stationid = '""" + i + """'
            """
        df_pair_i = pd.read_sql_query(query_pair, temp_conn)
        df_pair_i['RecordDate']= pd.to_datetime(df_pair_i['RecordDate'])
        df_pairs = df_pairs.append(df_pair_i, ignore_index=True)
        x+=1
    conn.close()
    temp_conn.close()
    #CREATE SINGLE DF MERGING MAIN STATION AND PAIRS DATA
    for i in range(len(pairs)):
        df_main = df_main.join(df_pairs[df_pairs['Tier']==i+1].set_index('RecordDate').add_suffix('_Pair'+str(i+1)), on='RecordDate')
    #GET DAILY DELTAS FOR EACH PAIR. DELTA = MAIN STATION TEMP - PAIR STATION TEMP. CALC SEPARATELY FOR TMIN AND TMAX
    for i in range(len(pairs)):
        df_main.insert(0, 'p'+str(i+1)+'_tmax_delta', df_main['TMax'] - df_main['TMax_Pair'+str(i+1)])
        df_main.insert(0, 'p'+str(i+1)+'_tmin_delta', df_main['TMin'] - df_main['TMin_Pair'+str(i+1)])

    #ADD COLUMNS FOR FUTURE USE. INCLUDE c+=1 STEP AFTER EACH COLUMN SO THAT WE CAN TRACK NUMBER OF COLUMNS BEING ADDED.
    c=0
    df_main.insert(0,'Month',pd.DatetimeIndex(df_main['RecordDate']).month)
    c+=1
    df_main.insert(0,'Year',pd.DatetimeIndex(df_main['RecordDate']).year)
    c+=1
    df_main.insert(0,'StationID_Main',df_main['StationID'])
    c+=1
    #CREATE NEW DF CONTAINING ONLY RELEVANT COLS
    num_cols = (2*len(pairs)) + c
    df_main = df_main.iloc[:,:num_cols]
    tier1 = x
    return df_main, pairs, tier1

def get_temps(db, df_pair_options, pairs, tier1):
    #GET AVERAGE MONTHLY TEMPERATURE FOR EACH MONTH FOR MAIN STATION
    #DH = delta history
    #CURRENTLY sql_dh_main CALCULATES FOR EVERY STATIONS IN TABLE, BUT THIS COULD BE ALTERED TO JUST MAIN STATION & PAIRS
    df_dh_main = pd.DataFrame(columns = ['StationID','Month','Year', 'TMax', 'TMin', 'Tier'])
    temp_conn = sqlite3.connect(':memory:')
    df_pair_options.to_sql('df_dh_main', temp_conn, index=False)
    sql_dh_main = """
        select stationid as StationID,
            substr(recorddate,instr(recorddate,'-')+1,2) as Month,
            substr(recorddate,1,4) as Year,
            avg(tmax) as TMax,
            avg(tmin) as TMin,
            """ + str(tier1) + """ as Tier 
        from df_dh_main
        group by stationid, substr(recorddate,instr(recorddate,'-')+1,2), substr(recorddate,1,4)
    """
    df_dh_main = pd.read_sql_query(sql_dh_main, temp_conn)
    temp_conn.close()
    df_dh_main['Month'] = pd.to_numeric(df_dh_main['Month'])
    df_dh_main['Year'] = pd.to_numeric(df_dh_main['Year'])
    #GET AVERAGE MONTHLY TEMPERATURE FOR EACH MONTH FOR EACH PAIR STATION
    #x VARIABLE IS NEEDED JUST TO TRACK ORDER OF PAIRS
    df_dh_pairs = pd.DataFrame(columns = ['StationID','Month','Year', 'TMax', 'TMin', 'Tier'])
    x=1
    temp_conn = sqlite3.connect(':memory:')
    df_pair_options.to_sql('df_dh_pair', temp_conn, index=False)
    for i in pairs:
        sql_dh_pair = """
            select stationid as StationID,
                substr(recorddate,instr(recorddate,'-')+1,2) as Month,
                substr(recorddate,1,4) as Year,
                avg(tmax) as TMax,
                avg(tmin) as TMin,
                """ + str(x) + """ as Tier 
            from df_dh_pair 
            where stationid='""" + i + """'
            group by stationid, substr(recorddate,instr(recorddate,'-')+1,2), substr(recorddate,1,4)
        """
        df_dh_pair_i = pd.read_sql_query(sql_dh_pair, temp_conn)
        df_dh_pairs = df_dh_pairs.append(df_dh_pair_i, ignore_index=True)
        x+=1
    temp_conn.close()
    df_dh_pairs['Month'] = pd.to_numeric(df_dh_pairs['Month'])
    df_dh_pairs['Year'] = pd.to_numeric(df_dh_pairs['Year'])
    tier2 = x
    return df_dh_main, tier2

def agg_temps(df_dh_main, min_year):
    #QUERY_10YR calculates a rolling history of average tmin and tmax for a given month over previous 10 years
    #QUERY_FULL_HIST calculates an average tmin and tmax for a given month using all years from a preset minimum to present
    #query_full_hist is used by default. If changing to query_10yr make sure there is 10 years of hist available before earliest recalibration date
    temp_conn = sqlite3.connect(':memory:')
    df_dh_main.to_sql('df_dh_main', temp_conn, index=False)
    query_10yr = """
        select a.stationid,
            a.month,
            a.year,
            avg(b.TMax) as Avg_Month_10yr_Max,
            avg(b.TMin) as Avg_Month_10yr_Min
        from df_dh_main a
        inner join df_dh_main b 
        on a.stationid=b.stationid
            and a.month=b.month
            and a.year>b.year and a.year<b.year+11
        group by a.stationid, a.month, a.year
    """
    query_full_hist = """
        select a.stationid,
            a.month,
            a.year,
            avg(b.TMax) as Avg_Month_10yr_Max,
            avg(b.TMin) as Avg_Month_10yr_Min
        from df_dh_main a
        inner join df_dh_main b 
        on a.stationid=b.stationid
            and a.month=b.month
            and a.year>""" + min_year + """
        group by a.stationid, a.month, a.year
    """
    df_avg_hist = pd.read_sql_query(query_full_hist, temp_conn)
    temp_conn.close()
    return df_avg_hist

def gen_hist_deltas(df_avg_hist, main_station, pairs, tier2):
    #SELECT PART OF df_avg_hist RELEVANT FOR MAIN STATION
    temp_conn = sqlite3.connect(':memory:')
    df_avg_hist.to_sql('df_avg_hist', temp_conn, index=False)
    query = """
        select *,
        0 as Tier
        from df_avg_hist 
        where stationid='""" + main_station + """' 
    """
    df_avg_hist_main = pd.read_sql_query(query, temp_conn)

    #SELECT PART OF df_avg_hist RELEVANT FOR EACH PAIR STATION
    df_avg_hist_pairs = pd.DataFrame(columns = ['StationID','Month','Year', 'Avg_Month_10yr_Max', 'Avg_Month_10yr_Min', 'Tier'])
    x=1
    for i in pairs:
        query = """
            select StationID, 
                Month, 
                Year,
                Avg_Month_10yr_Max,
                Avg_Month_10yr_Min,
                """ + str(x) + """ as Tier
            from df_avg_hist
            where stationid='""" + i + """'
        """
        df_avg_hist_pair_i = pd.read_sql_query(query, temp_conn)
        df_avg_hist_pairs = df_avg_hist_pairs.append(df_avg_hist_pair_i, ignore_index=True)
        x+=1
    temp_conn.close()
    df_avg_hist_pairs['Month'] = pd.to_numeric(df_avg_hist_pairs['Month'])
    df_avg_hist_pairs['Year'] = pd.to_numeric(df_avg_hist_pairs['Year'])
    df_avg_hist_main['Month'] = pd.to_numeric(df_avg_hist_main['Month'])
    df_avg_hist_main['Year'] = pd.to_numeric(df_avg_hist_main['Year'])

    #CREATE DATAFRAME OF CONTAINING MONTHLY AVERAGES FOR MAIN STATION AND ALL PAIRS
    for i in range(len(pairs)):
        df_avg_hist_pair_i = df_avg_hist_pairs[df_avg_hist_pairs['Tier']==i+1]
        df_avg_hist_main = pd.merge(df_avg_hist_main, df_avg_hist_pair_i, how='left', on = ['Month','Year'],suffixes=('','_p'+str(i+1)))
    #CALCULATE HISTORICAL MONTHLY DELTA BETWEEN MAIN STATION AND EACH PAIR (SEPARATE CALC FOR TMIN AND TMAX)
    for i in range(len(pairs)):
        df_avg_hist_main.insert(0, 'Avg_Month_TMax_Delta_p'+str(i+1),  df_avg_hist_main['Avg_Month_10yr_Max'] - df_avg_hist_main['Avg_Month_10yr_Max_p'+str(i+1)])
        df_avg_hist_main.insert(0, 'Avg_Month_TMin_Delta_p'+str(i+1),  df_avg_hist_main['Avg_Month_10yr_Min'] - df_avg_hist_main['Avg_Month_10yr_Min_p'+str(i+1)])

    #ADD COLUMNS FOR READABILITY. INCLUDE c+=1 STEP AFTER EACH COLUMN SO THAT WE CAN TRACK NUMBER OF COLUMNS BEING ADDED.
    c=0
    df_avg_hist_main.insert(0, 'Comp_Year', df_avg_hist_main['Year'])
    c+=1
    df_avg_hist_main.insert(0,'Comp_Month',df_avg_hist_main['Month'])
    c+=1
    df_avg_hist_main.insert(0,'Main_StationID',df_avg_hist_main['StationID'])
    c+=1
    #CREATE NEW DF CONTAINING ONLY RELEVANT COLS
    num_cols = (2*len(pairs)) + c
    df_delta_hist = df_avg_hist_main.iloc[:,:num_cols]
    return df_delta_hist, num_cols

def format_final(df_main, df_delta_hist, pairs, num_cols):
    #CREATE NEW DF CONTAINING DAILY DELTAS MATCHED WITH MONTHLY AVERAGE DELTAS FOR EACH PAIR
    df_final = pd.merge(df_main, df_delta_hist, left_on = ['StationID_Main','Year','Month'], right_on = ['Main_StationID','Comp_Year','Comp_Month'], suffixes=['useless1','useless2'])
    #DETERMINE ANOMALIES (ANOMALY = DAILY DELTA - AVG DELTA FOR THAT MONTH). DO SEPARATE CALCS FOR TMIN AND TMAX
    for i in range(len(pairs)):
        df_final.insert(0, 'tmax_delta_comp_p' + str(i+1), df_final['p'+str(i+1)+'_tmax_delta'] - df_final['Avg_Month_TMax_Delta_p'+str(i+1)])
    for i in range(len(pairs)):
        df_final.insert(0, 'tmin_delta_comp_p' + str(i+1), df_final['p'+str(i+1)+'_tmin_delta'] - df_final['Avg_Month_TMin_Delta_p'+str(i+1)])
    #CREATE DF CONTAINING ONLY RELEVANT COLUMNS
    df_final = df_final.iloc[:,:num_cols]
    #AGGREGATE RESULTS TO MONTH. CHANGE COLUMNS IN groupby TO AGGREGATE DIFFERENTLY.
    df_final = df_final.groupby(['StationID_Main','Year','Month']).mean().round(2)
    #FIND POSITION OF RESULTS FOR TMIN AND TMAX
    tmin_cols_start=0
    tmin_cols_end=int((num_cols-3)/2)
    tmax_cols_start=int((num_cols-3)/2)
    tmax_cols_end=int(num_cols-3)
    #CALCULATE SOME HIGH LEVEL STATS.
    df_final['TMin_Min_Delta'] = df_final.iloc[:,tmin_cols_start:tmin_cols_end].min(axis=1)
    df_final['TMin_Max_Delta'] = df_final.iloc[:,tmin_cols_start:tmin_cols_end].max(axis=1)
    df_final['TMin_Median_Delta'] = df_final.iloc[:,tmin_cols_start:tmin_cols_end].median(axis=1)
    df_final['TMax_Min_Delta'] = df_final.iloc[:,tmax_cols_start:tmax_cols_end].min(axis=1)
    df_final['TMax_Max_Delta'] = df_final.iloc[:,tmax_cols_start:tmax_cols_end].max(axis=1)
    df_final['TMax_Median_Delta'] = df_final.iloc[:,tmax_cols_start:tmax_cols_end].median(axis=1)
    return df_final

def style_xlsx(current_file):
    #LOAD WORKSHEET
    wb = op.load_workbook(filename=current_file)
    for sheet in ['RECAL','RAW','CORRS']:
        for col in ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W']:
            ws = wb[sheet]
            ws.column_dimensions[col].width = 15
    wb.save(filename=current_file)