from wx.providers.trade import daily_mtm as mtm
from flask_script import Command, Option
import datetime

class MTM_CSV_UPDATE_Command(Command):

    def run(self, env='Test'):
        """
        :param as_of_date: as of date for the EOD being run. E.g., '2020-09-10'. Default is datetime.datetime.now().
        :param sys: System being run on - windows or unix. Default is unix.
        :return:
        """
        mtm.mtm_csv_update(run_date=datetime.datetime.now(), env=env)

        pass

