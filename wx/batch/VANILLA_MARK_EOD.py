from wx.providers.market_data import Vanilla_Mark_Provider as VM
from wx.providers.common import Common_Functions as CF
from flask_script import Command, Option
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from xlib import xdb
import numpy as np
import os

class VANILLA_MARK_Command(Command):
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
        locations = ['KATL', 'KCVG', 'KDFW', 'KLGA', 'KMSP', 'KORD', 'KPDX', 'KSAC', 'KLAS']
        loc_str = ''
        for i in locations:
            loc_str += '\'' + i + '\','
        loc_str = loc_str[:-1]

        df_real = VM.get_realized(db, loc_str)
        last_realized = df_real['OPR_DATE'].max()
        prompt = last_realized + relativedelta(months=1)
        prompt = datetime(year=prompt.year, month=prompt.month, day=1).date()
        fcst_date = VM.choose_fcst_date(last_realized)
        df_fcst1 = VM.get_cwg_fcst(db, loc_str, fcst_date, 'N')
        df_fcst_prompt = VM.get_cwg_fcst(db, loc_str, fcst_date, 'Y')
        df_fcst_format_hdd, df_fcst_format_cdd = VM.format_fcst(df_fcst1)
        df_fcst_format_hdd_prompt, df_fcst_format_cdd_prompt = VM.format_fcst(df_fcst_prompt)
        df_fcst_format_hdd = df_fcst_format_hdd.append(df_fcst_format_hdd_prompt)
        df_fcst_format_cdd = df_fcst_format_cdd.append(df_fcst_format_cdd_prompt)
        try:
            df_balmo1 = VM.get_balmo(db, last_realized, loc_str, 'N')
        except:
            df_balmo1 = pd.DataFrame(columns=['EMPTY'])
        try:
            df_balmo1_prompt = VM.get_balmo(db, prompt, loc_str, 'Y')
        except:
            df_balmo1_prompt = pd.DataFrame(columns=['EMPTY'])
        try:
            df_balmo1_format_hdds, df_balmo1_format_cdds = VM.format_balmo(df_balmo1)
        except:
            df_balmo1_format_hdds = pd.DataFrame(columns=['EMPTY'])
            df_balmo1_format_cdds = pd.DataFrame(columns=['EMPTY'])
        try:
            df_balmo1_format_hdds_prompt, df_balmo1_format_cdds_prompt = VM.format_balmo(df_balmo1_prompt)
        except:
            df_balmo1_format_hdds_prompt = pd.DataFrame(columns=['EMPTY'])
            df_balmo1_format_cdds_prompt = pd.DataFrame(columns=['EMPTY'])
        last_forecast = df_fcst1['END_DATE'].max()
        last_forecast_prompt = df_fcst_prompt['END_DATE'].max()
        try:
            df_balmo2 = VM.get_balmo_after_fcst(db, last_forecast, loc_str, 'N')
        except:
            df_balmo2 = pd.DataFrame(columns=['EMPTY'])
        try:
            df_balmo2_prompt = VM.get_balmo_after_fcst(db, last_forecast_prompt, loc_str, 'Y')
        except:
            df_balmo2_prompt = pd.DataFrame(columns=['EMPTY'])
        try:
            df_balmo2_format_hdds, df_balmo2_format_cdds = VM.format_balmo_after_fcst(df_balmo2)
        except:
            df_balmo2_format_hdds = pd.DataFrame(columns=['EMPTY'])
            df_balmo2_format_cdds = pd.DataFrame(columns=['EMPTY'])
        try:
            df_balmo2_format_hdds_prompt, df_balmo2_format_cdds_prompt = VM.format_balmo_after_fcst(df_balmo2_prompt)
        except:
            df_balmo2_format_hdds_prompt = pd.DataFrame(columns=['EMPTY'])
            df_balmo2_format_cdds_prompt = pd.DataFrame(columns=['EMPTY'])

        today = datetime.today()
        mon = '0' + str(today.month)
        mon = mon[-2:]
        yr = str(today.year)
        day = '0' + str(today.day)
        day = day[-2:]
        current_file = '/home/rday/data/Vanilla_Mark/wx_vanilla_mark_' + yr + mon + day + '.xlsx'
        try:
            os.remove(current_file)
        except:
            pass
        CF.df_to_xlsx(df_real.rename(columns={'OPR_DATE': 'MOST RECENT DATE'}), current_file, 'REALIZED', 0, 0)
        CF.df_to_xlsx(df_fcst_format_hdd, current_file, 'FORECAST (HDD)', 0, 0)
        CF.df_to_xlsx(df_fcst_format_cdd, current_file, 'FORECAST (CDD)', 0, 0)
        CF.df_to_xlsx(df_balmo1_format_hdds, current_file, 'BALMO (HDD)', 0, 0)
        CF.df_to_xlsx(df_balmo1_format_cdds, current_file, 'BALMO (CDD)', 0, 0)
        CF.df_to_xlsx(df_balmo2_format_hdds, current_file, 'BALMO AFTER FORECAST (HDD)', 0, 0)
        CF.df_to_xlsx(df_balmo2_format_cdds, current_file, 'BALMO AFTER FORECAST (CDD)', 0, 0)
        CF.df_to_xlsx(df_balmo1_format_hdds_prompt, current_file, 'PROMPT (HDD)', 0, 0)
        CF.df_to_xlsx(df_balmo1_format_cdds_prompt, current_file, 'PROMPT (CDD)', 0, 0)
        CF.df_to_xlsx(df_balmo2_format_hdds_prompt, current_file, 'PROMPT AFTER FORECAST (HDD)', 0, 0)
        CF.df_to_xlsx(df_balmo2_format_cdds_prompt, current_file, 'PROMPT AFTER FORECAST (CDD)', 0, 0)
        VM.style_xlsx(current_file, mon, day, yr)
        report = 'Vanilla Mark'
        CF.emailer(report,current_file,yr,mon,day)

        pass
