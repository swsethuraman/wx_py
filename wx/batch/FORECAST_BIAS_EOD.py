from wx.providers.market_data import Forecast_Bias_Provider as FB
from wx.providers.common import Common_Functions as CF
from flask_script import Command, Option
import pandas as pd
from datetime import datetime
from xlib import xdb
import numpy as np
import os

class FORECAST_BIAS_Command(Command):
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

        locations = ['KATL', 'KCVG', 'KDFW', 'KLGA', 'KMSP', 'KORD', 'KPDX', 'KSAC', 'KLAS']
        fcst_days = ['D0', 'D1', 'D2', 'D3', 'D4', 'D5']
        today = datetime.today()
        mon = '0' + str(today.month)
        mon = mon[-2:]
        yr = str(today.year)
        day = '0' + str(today.day)
        day = day[-2:]
        current_file = '/home/rday/data//Forecast_Bias/wx_forecast_bias_' + yr + mon + day + '.xlsx'

        df, max_day = FB.get_data(locations, fcst_days)
        FB.separate_intervals(max_day, df, locations, fcst_days, current_file)
        report = 'Forecast Bias Report'
        CF.emailer(report,current_file,yr,mon,day)

        pass
