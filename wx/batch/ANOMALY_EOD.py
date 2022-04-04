from wx.providers.market_data import Weather_Anomaly_provider as WAP
from flask_script import Command, Option
import datetime
from datetime import date, timedelta, datetime
import dateutil.relativedelta
from xlib import xdb
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import dateutil.relativedelta

class Anomaly_Command(Command):
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
        #CLEAN OUT OLD DATA
        db = 'WX1-GC'
        conn = xdb.make_conn(db, stay_open=True)
        conn.deleteAllRows('WX1.WX_STATION_ANOMALIES_TIER2')
        conn.commit()
        conn.deleteAllRows('WX1.WX_STATION_ANOMALIES_TIER3')
        conn.commit()
        conn.close()

        distance_threshold = '1'
        min_date = '2000-1-1'
        min_dev_threshold = 2
        WAP.pre_processing(db,min_date,distance_threshold)
        WAP.process_anomalies(min_dev_threshold)
        WAP.create_send_report()

        pass
