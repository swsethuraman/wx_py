from wx.providers.market_data import Waterfall_Provider as WF
from flask_script import Command, Option
import datetime
from datetime import date, timedelta, datetime
import dateutil.relativedelta
from xlib import xdb
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import dateutil.relativedelta

class Waterfall_Command(Command):
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
        # SELECT STATION (USE GHCND CODE, USUALLY WBAN FOR US. NO NEED TO INCLUDE COUNTRY CODE AND PRECEDING 0s)
        sql_vals = """
            select distinct STATION, 
                'NOAA' as ORIGINAL_SOURCE 
            from WX1.MS_NOAA_GHCND 
            where opr_date>date_sub(curdate(),interval 90 day)
            and (station like 'CA%' or station like 'USW%')
            union
            select STATION, 
                ORIGINAL_SOURCE 
            from WX1.WX_WEATHER_STATIONS
        """
        # TO ADD OTHER SOURCES TO LOOP, UNION ONTO ABOVE QUERY
        db = 'WX1-GC'
        conn = xdb.make_conn(db, stay_open=True)
        values = conn.query(sql_vals)
        conn.close()
        location_list = values.to_numpy()
        start = datetime.now()
        end = datetime.now()
        start = start - dateutil.relativedelta.relativedelta(days=30)
        end = end + dateutil.relativedelta.relativedelta(days=1)
        start = datetime.strftime(start, '%Y-%m-%d')
        end = datetime.strftime(end, '%Y-%m-%d')
        for i in location_list:
            print(i[1])
            print(i[0])
            original_source = i[1]
            station = i[0]
            pairs = []
            try:
                WF.waterfall(original_source, station, pairs, start, end)
            except:
                print('Failed for ' + str(i))

        pass
