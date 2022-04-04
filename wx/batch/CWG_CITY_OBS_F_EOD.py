from wx.providers.market_data import Weather_Provider as WP
from flask_script import Command, Option
import datetime
from datetime import date, datetime, timedelta


class CWG_City_Obs_F_Command(Command):
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
        api_key = '887e65ef553dd9b359b076e8a2006560'
        urlBase = 'https://www.commoditywx.com/api-data/' + api_key
        start = date.today() - timedelta(days=3)
        end = date.today() - timedelta(days=0)
        WP.cwg_city_obs_f(start, end, ['northamerica','europe'], urlBase)

        pass
