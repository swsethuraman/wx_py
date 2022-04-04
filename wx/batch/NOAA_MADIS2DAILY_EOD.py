from wx.providers.market_data import Weather_Provider as WP
from flask_script import Command, Option
import datetime
from datetime import date, timedelta, datetime
import dateutil.relativedelta
from xlib import xdb
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import dateutil.relativedelta
from wx.providers.common import Common_Functions as CF


class NOAA_MADIS2Daily_Command(Command):
    """
        This is the command line interface for saving RT LBMP data from NYISO at the end of every day.
    """

    option_list = (
        Option('--as_of_date', '-a',
               default='today',
               dest='as_of_date', help='As of date for the EOD. E.g., "2020-09-10"'),
        Option('--sys', '-s',
               default='unix',
               dest='sys', help='System - windows or unix. Default is unix.'),
    )

    def run(self, as_of_date='today', sys='unix'):
        """
        :param as_of_date: as of date for the EOD being run. E.g., '2020-09-10'. Default is 'today'.
        :param sys: System being run on - windows or unix. Default is unix.
        :return:
        """
        db = 'WX1-GC'

        start = datetime.now()
        end = datetime.now()
        start = start - dateutil.relativedelta.relativedelta(days=30)
        end = end + dateutil.relativedelta.relativedelta(days=1)
        start = datetime.strftime(start, '%Y-%m-%d')
        end = datetime.strftime(end, '%Y-%m-%d')

        df = WP.madis2daily_getraw(start, end, db)
        df_ins = WP.madis2daily_formatdata(df)

        db = 'WX1-GC'
        table = 'NOAA_MADIS_DAILY_MINMAX'
        CF.insert_update(db,table,df_ins,'N')
        print('Updated NOAA_MADIS_DAILY_MINMAX')
        sqltext = """delete from WX1.MS_NOAA_MADIS_HOURLY where opr_date<date'""" + start + """'"""
        conn = xdb.make_conn(db, stay_open=True)
        conn.execute(sqltext)
        conn.commit()
        conn.close()
        print('Cleaned MS_NOAA_MADIS_HOURLY')
        pass
