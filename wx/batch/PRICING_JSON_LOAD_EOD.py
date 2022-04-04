from wx.providers.trade import pricing_provider as pp
from flask_script import Command, Option
import datetime

class PRICING_JSON_LOAD_Command(Command):

    def run(self, env='Test'):
        """
        :param as_of_date: as of date for the EOD being run. E.g., '2020-09-10'. Default is datetime.datetime.now().
        :param sys: System being run on - windows or unix. Default is unix.
        :return:
        """
        pp.load_pricing_jsons()

        pass

