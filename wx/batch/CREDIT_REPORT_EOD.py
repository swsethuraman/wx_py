from wx.providers.market_data import credit_data_provider as CDP
from wx.providers.common import Common_Functions as CF
from flask_script import Command, Option
import datetime
from datetime import date, timedelta, datetime
import dateutil.relativedelta
import pandas as pd
import numpy as np


class CREDIT_REPORT_Command(Command):
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
        today = datetime.today()
        mon = '0' + str(today.month)
        mon = mon[-2:]
        yr = str(today.year)
        day = '0' + str(today.day)
        day = day[-2:]
        CDP.load_new_data(yr, mon, day)
        df = CDP.gen_new_report()
        CDP.format_new_report(df, yr, mon, day)
        report = 'Credit Health Report'
        current_file = '/home/rday/data/Credit_Health_Report/wx_credit_health_report_'+yr+mon+day+'.xlsx'
        #CHECK IF REPORT HAS CHANGED
        df_changes = df[['rating_change_flag', 'cds1y_change_flag', 'cds5y_change_flag', 'spread_change_flag', 'market_cap_flag']]
        df_change1 = abs(df_changes[['rating_change_flag']])
        df_change2 = abs(df_changes[['cds1y_change_flag']])
        df_change3 = abs(df_changes[['cds5y_change_flag']])
        df_change4 = abs(df_changes[['spread_change_flag']])
        df_change5 = abs(df_changes[['market_cap_flag']])
        change_indicator = df_change1.sum()[0] + df_change2.sum()[0] + df_change3.sum()[0] + df_change4.sum()[0] + df_change5.sum()[0]
        if change_indicator > 1:
            status = ' - NEW CHANGES'
        else:
            status = ' - UNCHANGED'
        CF.emailer(report, current_file, yr, mon, day, status)

        pass
