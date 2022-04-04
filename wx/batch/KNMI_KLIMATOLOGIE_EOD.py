from wx.providers.market_data import Weather_Provider as WP
from flask_script import Command, Option
import datetime
from datetime import date, datetime, timedelta
import dateutil.relativedelta

class KNMI_Klimatologie_Command(Command):
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
        #start_date = date.today() - dateutil.relativedelta.relativedelta(months=1)
        #end_date = date.today()
        #WP.get_knmi_klimatologie(start_date,end_date)
        base_url = 'https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/daggegevens/etmgeg_'
        save_to = '/data-gc/wx/KNMI/'
        WP.knmi_download_files(base_url, save_to)
        WP.knmi_update_db(save_to)

        pass
