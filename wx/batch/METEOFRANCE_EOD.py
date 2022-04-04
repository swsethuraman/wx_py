from wx.providers.market_data import Weather_Provider as WP
from flask_script import Command, Option
import datetime
from datetime import datetime, timedelta


class MeteoFrance_Command(Command):
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
        months = []
        for i in range(2):
            date = datetime.today() - timedelta(days=i)
            month = str(date.year) + str('0' + str(date.month))[-2:]
            if month not in months:
                months.append(month)
        for month in months:
            WP.get_meteofrance(month)

        pass
