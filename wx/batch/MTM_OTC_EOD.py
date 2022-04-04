from wx.providers.trade import daily_mtm as mtm
from wx.providers.common import Common_Functions as CF
from flask_script import Command, Option
import datetime
from datetime import date, datetime, timedelta

class MTM_OTC_Command(Command):
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
        mtm_date = datetime.today()
        final_mtm = mtm.run_otc_mtm(mtm_date)
        db = 'WX2-GC'
        table = 'MTM_DAILY'
        CF.insert_update(db,table,final_mtm,'N')
        pass
