from wx.providers.trade import origination_provider as OP
from wx.providers.common import Common_Functions as CF
from flask_script import Command, Option
import pandas as pd
from datetime import datetime
from xlib import xdb
import numpy as np
import os


class ORIGINATION_REPORT_Command(Command):
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
        #regions = ['US', 'EU', 'UK', 'AU']
        regions = ['US']
        current_file = '/home/rday/data/MIS_Report/wx_otc_trade_mis_' + yr + mon + day + '.xlsx'
        try:
            os.remove(current_file)
        except:
            pass
        df = OP.read_deal_tracker()
        for region in regions:
            print(region)
            df_reg = df.iloc[np.where(df['REGION'] == region)].reset_index(drop=True)
            df_final = OP.format_stats(df_reg)
            CF.df_to_xlsx(df_final,current_file,region,1,4)
            OP.style_xlsx(current_file,region,mon,day,yr)
        report = 'OTC Trade MIS Report'
        CF.emailer(report,current_file,yr,mon,day)

        pass
