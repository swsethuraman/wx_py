from wx.providers.market_data import MS_provider as MS
from flask_script import Command, Option
import datetime
from datetime import date, timedelta, datetime
import dateutil.relativedelta


class NOAA_GHCND_MS_Command(Command):
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
        start = datetime.now()
        end = datetime.now()
        start = start - dateutil.relativedelta.relativedelta(days=2)
        end = end + dateutil.relativedelta.relativedelta(days=1)
        start = datetime.strftime(start, '%Y-%m-%d')
        end = datetime.strftime(end, '%Y-%m-%d')
        MS.noaa_ghcnd(start_date=start, end_date=end)

        pass
