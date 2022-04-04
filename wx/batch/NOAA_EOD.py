from wx.providers.market_data import NOAA_provider as NOAA
from flask_script import Command, Option

class NOAA_EOD_GHCND_Command(Command):
    """
        This is the command line interface for saving weather data from NOAA at the end of every day.
    """

    option_list = (
        Option('--as_of_date', '-a',
               default='today',
               dest='as_of_date', help='As of date for the EOD. E.g., "2020-09-10"'),
        Option('--sys', '-s',
               default='unix',
               dest='sys', help='System - windows or unix. Default is unix.'),
        Option('--name', '-n',
               default='None',
               dest='name', help='Name of the station we want the data for.'),
        Option('--wban', '-w',
               default='None',
               dest='wban', help='WBAN of the station we want the data for.'),
        Option('--stationid', '-t',
               default='None',
               dest='stationid', help='StationID of the station we want the data for.'),
    )

    def __init__(self):
        self.mkt_data_provider = NOAA.NOAA_GHCND_v2()
        pass

    def run(self, as_of_date='today', sys='unix', name=None, wban=None, stationid=None):
        """

        :param as_of_date: as of date for the EOD being run. E.g., '2020-09-10'. Default is 'today'.
        :param sys: System being run on - windows or unix. Default is unix.
        :return:
        """
        station_info = {
            'name': name,
            'WBAN': wban,
            'WMO': None,
            'stationid': stationid,
            'datatypeid': ['TMAX', 'TMIN']
        }
        df = self.mkt_data_provider.get_historical_data([station_info], start_date='1970-01-01')
        self.mkt_data_provider.save_to_db(df)

        pass

