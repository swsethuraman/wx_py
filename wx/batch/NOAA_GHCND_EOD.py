from wx.providers.market_data import Weather_Provider as WP
from flask_script import Command, Option
import datetime
from datetime import timedelta, datetime
import numpy as np

class NOAA_GHCND_Command(Command):
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
        end = datetime.today()
        start = datetime.today() - timedelta(days=10)
        start = np.datetime64("{0}-{1:0=2d}-{2:0=2d}".format(start.year, start.month, start.day))
        end = np.datetime64("{0}-{1:0=2d}-{2:0=2d}".format(end.year, end.month, end.day))
        WP.get_noaa_ghcnd(start, end)

        pass
