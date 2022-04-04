import sys
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from subprocess import Popen, PIPE
from xlib import xdb
import pandas as pd
import openpyxl
from openpyxl import load_workbook



def emailer(report,path,yr,mon,day,body=''):
    msg = MIMEMultipart()
    sql_recipients = """select recipient from WX2.EMAIL_RECIPIENTS where report='"""+report+"""'"""
    db = 'WX2-GC'
    conn = xdb.make_conn(db, stay_open=True)
    df_rec=conn.query(sql_recipients)
    conn.close()
    recipients = df_rec.recipient.tolist()
    msg["From"] = 'riley.day@laurioncap.com'
    msg["To"] = COMMASPACE.join(recipients)
    msg["Subject"] = 'Wx ' + report + ' '+yr+mon+day
    msg.attach(MIMEText(body))
    f=path
    with open(f, "rb") as fil:
        part = MIMEApplication(
            fil.read(),
            Name=basename(f)
        )
    # After the file is closed
    part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
    msg.attach(part)

    p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE)
    p.communicate(msg.as_bytes())
    print('email sent')

def insert_update(db,table,df,logging='Y'):
    if logging=='Y':
        df['DateUpdated'] = None
        df['UserUpdated'] = None
    conn = xdb.make_conn(db, stay_open=True)
    conn.bulkUpdateDf(df, table, local=True)
    conn.commit()
    conn.bulkInsertDf(df, table, local=True)
    conn.commit()
    conn.close()

def insert(db,table,df):
    conn = xdb.make_conn(db, stay_open=True)
    conn.bulkInsertDf(df, table, local=True)
    conn.commit()
    conn.close()

def df_to_xlsx(df_final,current_file,sheet_name,start_col,start_row):
    try:
        book = load_workbook(current_file)
        writer = pd.ExcelWriter(current_file, engine='openpyxl')
        writer.book = book
        df_final.to_excel(writer,sheet_name=sheet_name,index=False,startcol=start_col,startrow=start_row)
        writer.save()
        writer.close()
    except:
        writer = pd.ExcelWriter(current_file, engine='openpyxl')
        df_final.to_excel(writer,sheet_name=sheet_name,index=False,startcol=start_col,startrow=start_row)
        writer.save()
        writer.close()