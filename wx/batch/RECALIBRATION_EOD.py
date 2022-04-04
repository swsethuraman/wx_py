from wx.providers.market_data import Recalibration_Provider as RP
from flask_script import Command, Option
from xlib import xdb
import pandas as pd
import numpy as np

class Recalibration_Command(Command):
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
        sql_stations = """select * from WX1.WX_RECAL_STATION_REF"""
        conn = xdb.make_conn(db, stay_open=True)
        df_station_list = conn.query(sql_stations)
        conn.close()
        for s in df_station_list.itertuples():
            data_source = s.DATA_SOURCE
            id_type = s.ID_TYPE
            main_station = s.MAIN_STATION
            pair_stations = []
            distance_threshold = str(s.DISTANCE_THRESHOLD)
            print(data_source, id_type, main_station, distance_threshold)
            try:
                RP.run_recal(db, data_source, id_type, main_station, pair_stations, distance_threshold)
            except:
                pass

        pass
