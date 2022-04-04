from xlib import xdb
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

#START OF FUNCTIONS FOR REGRESSION & DATA PROCESSING
def process_historical_data(db,stations,feature_contracts,years,hedge_time,notional,file_suffix):
    for as_of_year in years:
        max_year = as_of_year - 1
        min_year = as_of_year - 3
        for season in ['WINTER']:
            print(season)
            if season=='WINTER':
                month_list = [1,2,3,11,12]
            elif season=='SUMMER':
                month_list = [6,7,8]
            mon_str=''
            for mon in month_list:
                mon_str+='\''+str(mon)+'\','
            mon_str = mon_str[:-1]
            df_coeffs = pd.DataFrame(columns=['STATION','CONTRACT','COEFFS','INTERCEPT','R2','VOL'])
            df_payouts_final = pd.DataFrame(columns=['STATION','YEAR','MONTH','DD_PAYOUT','CONTRACT','HEDGE_PAYOUT','INTERCEPT'])
            df_price_features = get_price_features(db,feature_contracts,min_year,max_year)
            df_features, change_cons = format_features_df(df_price_features,min_year)
            df_vols = df_price_features.iloc[np.where(df_price_features['MONTH'].isin(month_list))]
            df_vols = df_vols[['CONTRACT','SETTLEMENT_PRICE']].iloc[np.where(df_vols['PROMPTNESS']==1)].groupby('CONTRACT').std()
            df_vols = df_vols.rename(columns={'SETTLEMENT_PRICE':'VOL'})
            for station in stations:
                print(station)
                df_obs = get_weath_obs(db,station,min_year,max_year,mon_str)
                df_combined = pd.merge(df_obs,df_features,how='inner',on=['MONTH','YEAR'])
                meta=['MONTH','YEAR','STRIP','PROMPTNESS','TAVG_OFF_10YR']
                feats=change_cons
                r2, coeffs, intercept = run_weather_lr(df_combined.iloc[np.where(df_combined['PROMPTNESS']==1)],meta,feats)
                print(r2)
                reg_contracts=[]
                for i in range(len(feats)):
                    reg_contracts.append(feats[i].replace('_CHANGE',''))
                df_coeffs_temp = pd.DataFrame(columns=['STATION','CONTRACT','COEFFS','INTERCEPT','R2'])
                df_coeffs_temp['CONTRACT'] = reg_contracts
                df_coeffs_temp['COEFFS'] = coeffs
                df_coeffs_temp['STATION'] = station
                df_coeffs_temp['INTERCEPT'] = intercept
                df_coeffs_temp['R2'] = r2
                df_coeffs_temp = pd.merge(df_coeffs_temp,df_vols,how='inner',on=['CONTRACT'])
                df_coeffs = df_coeffs.append(df_coeffs_temp)
                df_payouts = get_monthly_payouts(db,as_of_year,df_coeffs_temp,station,min_year,max_year,notional,mon_str,hedge_time)
                df_payouts['INTERCEPT'] = intercept
                df_payouts_final = df_payouts_final.append(df_payouts)
            df_coeffs.to_csv('/home/rday/data/df_coeffs_'+str(as_of_year)+'_'+season+'_'+mon_str+'_'+file_suffix+'.csv')
            df_payouts_final.to_csv('/home/rday/data/df_payouts_final_'+str(as_of_year)+'_'+season+'_'+mon_str+'_'+file_suffix+'.csv')

def get_ice_price_features(db,contracts,min_year,max_year):
    con_str=''
    for con in contracts:
        con_str+='\''+con+'\','
    con_str = con_str[:-1]
    sql_price = """
        select OPR_DATE,
            extract(month from OPR_DATE) as MONTH,
            extract(year from OPR_DATE) as YEAR,
            extract(month from date_sub(OPR_DATE,interval 1 month)) as PREV_MONTH,
            extract(year from date_sub(OPR_DATE,interval 1 month)) as PREV_YEAR,
            CONTRACT, 
            STRIP, 
            SETTLEMENT_PRICE, 
            NET_CHANGE 
        from WX1.MS_ICE_CLEARED_GAS
        where extract(year from OPR_DATE) between """+str(min_year)+"""-1 and """+str(max_year)+"""
        and CONTRACT in ("""+con_str+""")
        and STRIP<date_add(OPR_DATE,interval 13 month)
        and EXPIRATION_DATE>=OPR_DATE
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_price=conn.query(sql_price)
    conn.close()
    df_price['OPR_DATE'] = pd.to_datetime(df_price['OPR_DATE'])
    df_price['STRIP'] = pd.to_datetime(df_price['STRIP'])
    df_price = df_price.join(df_price[['OPR_DATE','CONTRACT','STRIP']].groupby(['OPR_DATE','CONTRACT']).rank().rename(columns={'STRIP':'PROMPTNESS'}))
    return df_price

def get_price_features(db,feature_contracts,min_year,max_year):
    df_price_features = get_ice_price_features(db,feature_contracts,min_year,max_year)
    #DETERMINE LAST TRADING DAY OF EACH MONTH AND REMOVE DATA FOR ALL OTHER DAYS
    df_price_features = pd.merge(df_price_features,df_price_features[['OPR_DATE','MONTH','YEAR']].groupby(['MONTH','YEAR']).max().reset_index(),how='inner',on=['MONTH','YEAR'],suffixes=('','_LAST'))
    df_price_features = df_price_features.iloc[np.where(df_price_features['OPR_DATE']==df_price_features['OPR_DATE_LAST'])]
    #MATCH PRICE ON LAST TRADING DAY OF EACH MONTH WITH PRICE FROM LAST TRADING DAY OF PRECEEDING MONTH
    df_price_features = pd.merge(df_price_features,df_price_features,how='inner',left_on=['PREV_MONTH','PREV_YEAR','STRIP','CONTRACT'],right_on=['MONTH','YEAR','STRIP','CONTRACT'],suffixes=('','_PREV'))
    #CALCULATE MONTH-OVER-MONTH PRICE CHANGE
    df_price_features['PRICE_CHANGE'] = df_price_features['SETTLEMENT_PRICE'] - df_price_features['SETTLEMENT_PRICE_PREV']
    df_price_features = df_price_features[['OPR_DATE','MONTH','YEAR','CONTRACT','STRIP','SETTLEMENT_PRICE','PROMPTNESS','PRICE_CHANGE']]
    return df_price_features

def format_features_df(df_price_features,min_year):
    df_features = df_price_features[['OPR_DATE','MONTH','YEAR','PROMPTNESS']].drop_duplicates().reset_index(drop=True)
    cons = []
    change_cons = []
    for con in df_price_features['CONTRACT'].drop_duplicates():
        start_date = df_price_features.iloc[np.where(df_price_features['CONTRACT']==con)]['OPR_DATE'].min()
        if start_date<=datetime.strptime(str(min_year)+'-1-1', '%Y-%m-%d'):
            cons.append(con)
            change_cons.append(con+'_CHANGE')
            df_features = pd.merge(df_features,df_price_features.iloc[np.where(df_price_features['CONTRACT']==con)],how='left',on=['OPR_DATE','PROMPTNESS'],suffixes=('','_NFG'))
            df_features = df_features.rename(columns={'SETTLEMENT_PRICE':con,'PRICE_CHANGE':con+'_CHANGE'})
    df_features = df_features[['OPR_DATE','MONTH','YEAR','STRIP','PROMPTNESS']+cons+change_cons]
    df_features['OPR_DATE'] = pd.to_datetime(df_features['OPR_DATE'])
    df_features = df_features.iloc[np.where(df_features['OPR_DATE']>=datetime.strptime(str(min_year)+'-1-1', '%Y-%m-%d'))].reset_index(drop=True)
    return df_features, change_cons

def get_weath_obs(db,station,min_year,max_year,mon_str):
    sql_obs = """
        select extract(year from c.OPR_DATE) as YEAR,
            extract(month from c.OPR_DATE) as MONTH,
            case when avg(c.10YR)<65
                then avg(c.10YR-c.TAVG) 
                else avg(c.TAVG-c.10YR) 
            end as TAVG_OFF_10YR,
            avg(c.TAVG) as TAVG,
            avg(c.10YR) as 10YR
        from (
            select a.OPR_DATE,
                a.TAVG,
                avg(b.TAVG) as 10YR
            from (
                select OPR_DATE, (tmin+tmax)/2 as TAVG from WX1.WX_WEATHER_DAILY_CLEANED
                where icao = '"""+station+"""'
                and extract(month from OPR_DATE) in ("""+mon_str+""")
            ) a
            inner join (
                select extract(year from OPR_DATE) as YEAR, 
                    extract(month from OPR_DATE) as MONTH,
                    avg((tmin+tmax)/2) as TAVG 
                from WX1.WX_WEATHER_DAILY_CLEANED
                where icao = '"""+station+"""'
                and extract(month from OPR_DATE) in ("""+mon_str+""")
                group by extract(year from OPR_DATE), extract(month from OPR_DATE)
            ) b
            on extract(year from a.OPR_DATE)>b.YEAR
            and extract(year from a.OPR_DATE)<b.YEAR + 11
            and extract(month from a.OPR_DATE)=b.MONTH
            where extract(year from a.opr_date) between """+str(min_year)+""" and """+str(max_year)+"""
            group by a.OPR_DATE, a.TAVG
        ) c
        group by extract(year from c.OPR_DATE), extract(month from c.OPR_DATE)
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_obs=conn.query(sql_obs)
    conn.close()
    return df_obs


def run_weather_lr(df_pair, meta_cols, feature_cols):
    cols = meta_cols + feature_cols
    num_features = len(feature_cols)
    df_lin = df_pair.copy()
    X = df_lin[cols].values
    X_train = X[:, -1 * num_features:]
    y = df_lin['TAVG_OFF_10YR'].values
    regressor = LinearRegression()
    train = regressor.fit(X_train.reshape(-1, num_features), y)
    r2 = train.score(X_train.reshape(-1, num_features), y)
    coeffs = train.coef_
    intercept = train.intercept_
    return r2, coeffs, intercept

#END OF FUNCTIONS FOR REGRESSION & DATA PROCESSING
#START OF FUNCTIONS FOR BACKTESTING

def get_hdd_cdd(db,station,min_year,max_year,mon_str):
    sql_dd = """
        select extract(year from OPR_DATE) as YEAR,
            extract(month from OPR_DATE) as MONTH,
            sum(HDD) - avg(HDD_10YR) as HDD_OFF_10YR,
            sum(CDD) - avg(CDD_10YR) as CDD_OFF_10YR,
            sum(HDD) as HDD,
            sum(CDD) as CDD,
            avg(HDD_10YR) as HDD_10YR,
            avg(CDD_10YR) as CDD_10YR
        from (
            select a.OPR_DATE,
                a.HDD,
                a.CDD,
                avg(b.HDD) as HDD_10YR,
                avg(b.CDD) as CDD_10YR
            from (
                select OPR_DATE, 
                    greatest(0,65-((tmin+tmax)/2)) as HDD,
                    greatest(0,((tmin+tmax)/2)-65) as CDD
                from WX1.WX_WEATHER_DAILY_CLEANED
                where icao = '"""+station+"""'
                and extract(month from OPR_DATE) in ("""+mon_str+""")
            ) a
            inner join (
                select extract(year from OPR_DATE) as YEAR, 
                    extract(month from OPR_DATE) as MONTH,
                    sum(HDD) as HDD,
                    sum(CDD) as CDD
                from (
                    select OPR_DATE, 
                        greatest(0,65-((tmin+tmax)/2)) as HDD,
                        greatest(0,((tmin+tmax)/2)-65) as CDD
                    from WX1.WX_WEATHER_DAILY_CLEANED
                    where icao = '"""+station+"""'
                    and extract(month from OPR_DATE) in ("""+mon_str+""")
                ) b1
                group by extract(year from OPR_DATE), extract(month from OPR_DATE)
            ) b
            on extract(year from a.OPR_DATE)>b.YEAR
            and extract(year from a.OPR_DATE)<b.YEAR + 11
            and extract(month from a.OPR_DATE)=b.MONTH
            where extract(year from a.OPR_DATE) between """+str(min_year)+""" and """+str(max_year)+"""
            group by a.OPR_DATE, a.HDD, a.CDD
        ) c
        group by extract(year from OPR_DATE), extract(month from OPR_DATE)
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_dd=conn.query(sql_dd)
    conn.close()
    return df_dd

def get_deal_payouts(df_dd,notional):
    hdd_payouts = df_dd[['YEAR','MONTH','HDD_OFF_10YR']]
    hdd_payouts['HDD_PAYOUT'] = notional * hdd_payouts['HDD_OFF_10YR']
    hdd_payouts = hdd_payouts[['YEAR','MONTH','HDD_PAYOUT']]
    cdd_payouts = df_dd[['YEAR','MONTH','CDD_OFF_10YR']]
    cdd_payouts['CDD_PAYOUT'] = notional * cdd_payouts['CDD_OFF_10YR']
    cdd_payouts = cdd_payouts[['YEAR','MONTH','CDD_PAYOUT']]
    return hdd_payouts, cdd_payouts

def get_ice_payouts(db,as_of_year,payout_cons,hedge_time):
    con_str=''
    for con in payout_cons:
        con_str+='\''+con+'\','
    con_str = con_str[:-1]
    sql_basis_payout = """
        select a.YEAR,
            a.MONTH,
            a.CONTRACT,
            a.STRIP,
            a.SETTLEMENT_PRICE as EXPIRY_PRICE,
            b.SETTLEMENT_PRICE as PURCHASE_PRICE
        from (
            select extract(year from date_sub(EXPIRATION_DATE,interval 7 day)) as YEAR,
                extract(month from date_sub(EXPIRATION_DATE,interval 7 day)) as MONTH,
                OPR_DATE, 
                CONTRACT, 
                STRIP, 
                SETTLEMENT_PRICE 
            from WX1.MS_ICE_CLEARED_GAS
            where extract(year from date_sub(EXPIRATION_DATE,interval 7 day)) = """+str(as_of_year)+"""
            and OPR_DATE=EXPIRATION_DATE
            and contract in ("""+con_str+""")
        ) a
        inner join (
            select OPR_DATE, 
                CONTRACT, 
                STRIP, 
                SETTLEMENT_PRICE 
            from WX1.MS_ICE_CLEARED_GAS
            where extract(year from date_sub(EXPIRATION_DATE,interval 7 day)) = """+str(as_of_year)+"""
            and OPR_DATE=date_sub(EXPIRATION_DATE, interval """+str(hedge_time)+""" day)
            and contract in ("""+con_str+""")
        ) b
        on a.CONTRACT=b.CONTRACT
        and a.STRIP=b.STRIP
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_basis_payout=conn.query(sql_basis_payout)
    conn.close()
    return df_basis_payout

def get_monthly_payouts(db,as_of_year,df_coeffs,station,min_year,max_year,notional,mon_str,hedge_time):
    df_dd = get_hdd_cdd(db,station,min_year,as_of_year,mon_str)
    hdd_payouts, cdd_payouts = get_deal_payouts(df_dd,notional)
    payout_cons = df_coeffs.iloc[np.where(df_coeffs['STATION']==station)]['CONTRACT'].drop_duplicates().to_list()
    df_basis_payout = get_ice_payouts(db,as_of_year,payout_cons,hedge_time)
    df_weather_payouts = pd.merge(hdd_payouts,cdd_payouts,how='inner',on=['MONTH','YEAR'])
    df_weather_payouts['DD_PAYOUT'] = np.where(abs(df_weather_payouts['HDD_PAYOUT'])>abs(df_weather_payouts['CDD_PAYOUT']),df_weather_payouts['HDD_PAYOUT'],df_weather_payouts['CDD_PAYOUT'])
    df_weather_payouts = df_weather_payouts[['YEAR','MONTH','DD_PAYOUT']]
    df_payouts = pd.merge(df_weather_payouts,df_basis_payout,how='inner',on=['MONTH','YEAR'])
    df_payouts = pd.merge(df_payouts,df_coeffs.iloc[np.where(df_coeffs['STATION']==station)][['CONTRACT','COEFFS','VOL']],how='inner',on=['CONTRACT'])
    df_payouts['HEDGE_PAYOUT'] = (df_payouts['EXPIRY_PRICE'] - df_payouts['PURCHASE_PRICE']) * df_payouts['COEFFS']
    df_payouts['IMPACT'] = df_payouts['COEFFS'] * df_payouts['VOL']
    df_payouts['STATION'] = station
    df_payouts = df_payouts[['STATION','YEAR','MONTH','DD_PAYOUT','CONTRACT','HEDGE_PAYOUT','IMPACT']]
    return df_payouts

#END OF MODEL
#BEGINNG OF VISUALIZATION

def get_adj_payouts_corr(long_short,years,season,mon_str,version1,version2):
    df_final = pd.DataFrame(columns=['STATION','YEAR','MONTH','DD_PAYOUT','CONTRACT','HEDGE_PAYOUT','COEFFS'])
    df_corrs = pd.DataFrame(columns=['STATION','CONTRACT','CORR','YEAR'])
    for year in years:
        df1 = pd.read_csv("/home/rday/data/df_payouts_final_"+str(year)+"_"+season+"_"+mon_str+"_"+version1+".csv")
        df1 = df1[['STATION','YEAR','MONTH','DD_PAYOUT','CONTRACT','HEDGE_PAYOUT']]
        df2 = pd.read_csv("/home/rday/data/df_payouts_final_"+str(year)+"_"+season+"_"+mon_str+"_"+version2+".csv")
        df2 = df2[['STATION','YEAR','MONTH','DD_PAYOUT','CONTRACT','HEDGE_PAYOUT']]
        df_coeffs1 = pd.read_csv("/home/rday/data/df_coeffs_"+str(year)+"_"+season+"_"+mon_str+"_"+version1+".csv")
        df_coeffs2 = pd.read_csv("/home/rday/data/df_coeffs_"+str(year)+"_"+season+"_"+mon_str+"_"+version2+".csv")
        df1 = pd.merge(df1,df_coeffs1[['STATION','CONTRACT','COEFFS']],how='inner',on=['STATION','CONTRACT'])
        df2 = pd.merge(df2,df_coeffs2[['STATION','CONTRACT','COEFFS']],how='inner',on=['STATION','CONTRACT'])
        df = df1.append(df2)
        df = df[['STATION','YEAR','MONTH','DD_PAYOUT','CONTRACT','HEDGE_PAYOUT','COEFFS']].groupby(['STATION','YEAR','MONTH','DD_PAYOUT','CONTRACT']).sum().reset_index()
        #THIS ADJUSTMENT TO COEFFS DOESN'T AFFECT ANYTHING BECAUSE THE ONLY ATTENTION WE PAY TO IT AFTER THIS POINT IS THE SIGN
        #HOWEVER, IT DOES MAKE IT MORE INTUITIVE IF THIS DATA IS EXTRACT FOR VALIDATION OR SANITY CHECKS
        df['COEFFS'] = df['COEFFS']/2
        df_final = df_final.append(df).reset_index(drop=True)
        corrs = df[['STATION','CONTRACT','HEDGE_PAYOUT','DD_PAYOUT']].groupby(['STATION','CONTRACT']).corr().reset_index()
        corrs = corrs[['STATION','CONTRACT','DD_PAYOUT']].iloc[np.where(corrs['level_2']=='HEDGE_PAYOUT')].rename(columns={'DD_PAYOUT':'CORR'})
        corrs['YEAR'] = year
        df_corrs = df_corrs.append(corrs)
    df_final = pd.merge(df_final,df_corrs,how='inner',on=['STATION','CONTRACT','YEAR'])
    ###CORRELATION THRESHOLD DOESN'T NEED TO BE 0. CONSIDER ADJUSTING THIS SO THAT WE CAN FORCE IT TO USE CONTRACTS WITH STRONGER CORRS
    ###THIS MIGHT NOT BE POSSIBLE FOR ALL LOCATIONS IF BASKET IS ALREADY SMALL
    if long_short==1:
        df_final['ADJ_HEDGE_PAYOUT'] = df_final['HEDGE_PAYOUT'] * np.where((df_final['COEFFS']>0) & (df_final['CORR']>0),1,0)
    elif long_short==-1:
        df_final['ADJ_HEDGE_PAYOUT'] = df_final['HEDGE_PAYOUT'] * np.where((df_final['COEFFS']<0) & (df_final['CORR']>0),1,0)
    df_final = df_final[['STATION','YEAR','MONTH','DD_PAYOUT','HEDGE_PAYOUT','ADJ_HEDGE_PAYOUT']].groupby(['STATION','YEAR','MONTH','DD_PAYOUT']).sum().reset_index()
    return df_final, df_corrs

def get_final_payouts(season,stations,years,mon_str,hedge_size,notional,long_short,version1,version2):
    #GET MONTHLY PAYOUTS FOR EACH LOCATION
    df_final, df_corrs = get_adj_payouts_corr(long_short,years,season,mon_str,version1,version2)
    #APPLY PARAMETERS
    df_final['UNHEDGED_PAYOUT'] = long_short*notional*df_final['DD_PAYOUT']
    df_final['HEDGED_PAYOUT'] = df_final['UNHEDGED_PAYOUT']-long_short*hedge_size*df_final['HEDGE_PAYOUT']
    df_final['ADJ_HEDGED_PAYOUT'] = long_short*df_final['ADJ_HEDGE_PAYOUT']
    df_final['PURE_GAS_PAYOUT'] = -1*df_final['ADJ_HEDGED_PAYOUT']
    #MODIFY HEDGE SIZE PER YEAR TO ACCOUNT FOR HIGHER VOL IN OLDER HISTORICAL DATA
    df_final['ADJ_HEDGED_PAYOUT'] = df_final['UNHEDGED_PAYOUT']-df_final['ADJ_HEDGED_PAYOUT']
    #ALL_YEARS
    df_final=df_final.iloc[np.where(df_final['YEAR'].isin(years))].reset_index(drop=True)
    df_final=df_final.iloc[np.where(df_final['STATION'].isin(stations))].reset_index(drop=True)
    #PRINT STATS
    print('UNHEDGED - MEAN:' + str(int(df_final['UNHEDGED_PAYOUT'].mean())) + ', STD:'+ str(int(df_final['UNHEDGED_PAYOUT'].std())))
    print('ADJ HEDGED - MEAN:' + str(int(df_final['ADJ_HEDGED_PAYOUT'].mean())) + ', STD:'+ str(int(df_final['ADJ_HEDGED_PAYOUT'].std())))
    print('PURE GAS - MEAN:' + str(int(df_final['PURE_GAS_PAYOUT'].mean())) + ', STD:'+ str(int(df_final['PURE_GAS_PAYOUT'].std())))
    return df_final