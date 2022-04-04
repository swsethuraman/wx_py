from wx.providers.market_data import Exchange_Provider as EP
from flask_script import Command, Option
import datetime
from datetime import date, timedelta, datetime
import dateutil.relativedelta
from xlib import xdb
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import dateutil.relativedelta

class MTM_EXCHANGE_EOD_Command(Command):

    def run(self, env='Test'):
        """
        :param as_of_date: as of date for the EOD being run. E.g., '2020-09-10'. Default is datetime.datetime.now().
        :param sys: System being run on - windows or unix. Default is unix.
        :return:
        """
        EP.load_exchange_marks()
        pass

