import pandas as pd
from datetime import datetime
from xlib import xdb
import numpy as np
import xlsxwriter

def gen_new_report():
    db = 'WX2-GC'
    conn = xdb.make_conn(db, stay_open=True)
    df=conn.query('select * from WX2.MTM_DAILY_REPORT_V')
    conn.close()
    return df

def format_new_report(df, yr, mon, day):
    writer = pd.ExcelWriter('/home/rday/data/MTM_Report/wx_mtm_report_'+yr+mon+day+'.xlsx', engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1',index=False)
    workbook  = writer.book
    worksheet = writer.sheets['Sheet1']
    for idx, col in enumerate(df):  # loop through all columns
        series = df[col]
        max_len = max((
            series.astype(str).map(len).max(),  # len of largest item
            len(str(series.name))  # len of column name/header
            )) + 5  # adding a little extra space
        worksheet.set_column(idx, idx, max_len)
    writer.save()