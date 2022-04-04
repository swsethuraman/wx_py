import pandas as pd
import numpy as np
from xlib import xdb
import json
import os
from datetime import datetime

def get_summary(trade_summary,df):
    df_parent = df.iloc[np.where(df['HIERARCHY']=='PARENT')]
    df_parent = df_parent[['FIELD','VALUE']].transpose().reset_index(drop=True)
    df_parent.columns = df_parent.iloc[0]
    df_parent = df_parent.drop(df_parent.index[0])
    df_parent.columns = [x.upper() for x in df_parent.columns]
    df_parent[['FILENAME']] = trade_summary
    df_parent = df_parent[['FILENAME','DEAL_NUMBER','COUNTERPARTY','RISK_REGION','RISK_SUB_REGION','AGGREGATE_DEDUCTIBLE','AGGREGATE_LIMIT_LC','AGGREGATE_LIMIT_CPTY','PAYOUT_FUNCTION','PAYOUT_FREQUENCY','CREATE_DATE_TIME','QUOTED_DATE_TIME','QUOTED_Y_N','TRADED_Y_N','TRADED_DATE_TIME']]
    return df_parent

def get_legs(leg,deal_number,df):
    df_leg = df.iloc[np.where(df['HIERARCHY']==leg)].reset_index(drop=True)
    df_leg = df_leg[['FIELD','VALUE']].transpose().reset_index(drop=True)
    df_leg.columns = df_leg.iloc[0]
    df_leg = df_leg.drop(df_leg.index[0])
    df_leg.columns = [x.upper() for x in df_leg.columns]
    df_leg = df_leg[['NAME','START_DATE','END_DATE','TYPE','NOTIONAL','WEATHER_STRIKE','COMMODITY_STRIKE','BUYSELL','LIMIT_LC','LIMIT_CPTY','WEATHER_BUMP','COMMODITY_BUMP']]
    df_leg['START_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_leg['START_DATE'])]
    df_leg['END_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_leg['END_DATE'])]
    df_leg['DEAL_NUMBER'] = deal_number
    df_leg['LEG'] = leg
    return df_leg

