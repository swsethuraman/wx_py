import GetData as gd
from xlib import xdb
import pandas as pd
import numpy as np
import datetime
from datetime import datetime
import dateutil.relativedelta
from wx.providers.common import Common_Functions as CF


def bloomberg_secmaster(db,start_date,end_date):
    sql_meta = """
        select distinct BB_ID, MODULE, WX_TABLE from WX1.BBG_MAPPING
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_meta=conn.query(sql_meta)
    conn.close()
    meta = df_meta.to_numpy()
    for i in meta:
        bb_id=i[0]
        module=i[1]
        wx_table=i[2]
        if module=='secmaster-price':
            df = gd.getData(module, source='BLOOMBERG', bbIds=[bb_id], startDate=int(start_date), endDate=int(end_date))
        if module=='economic-releases-bloomberg':
            df = gd.getData('economic-releases-bloomberg', startDate=int(start_date), endDate=int(end_date), symbols=[bb_id])
        sql_cols = """
            select BB_FIELD_NAME, WX_FIELD_NAME from WX1.BBG_MAPPING where BB_ID = '""" + bb_id + """' order by col_order
        """
        conn = xdb.make_conn(db, stay_open=True)
        df_cols=conn.query(sql_cols)
        conn.close()
        cols = df_cols.to_numpy()
        final_cols = []
        for j in cols:
            df[j[1]] = df[j[0]]
            final_cols.append(j[1])
        df = df[final_cols]
        CF.insert_update(db,wx_table,df,logging='Y')
        print('Inserted data for ' + i[0])