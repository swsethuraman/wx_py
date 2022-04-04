from wx.providers.trade import mtm_data_provider as MDP
from wx.providers.common import Common_Functions as CF
from flask_script import Command, Option
import pandas as pd
from datetime import datetime
from xlib import xdb
import numpy as np
import xlsxwriter


class MTM_REPORT_Command(Command):
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
        df = MDP.gen_new_report()
        MDP.format_new_report(df, yr, mon, day)
        report = 'MtM Report'
        path = '/home/rday/data/MTM_Report/wx_mtm_report_' + yr + mon + day + '.xlsx'
        CF.emailer(report,path,yr,mon,day)

        pass
