from .generic_api_provider import GenericApiProvider
import pandas as pd
import numpy as np
from mysql.connector import connect
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


class NOAA_GHCND_v2(GenericApiProvider):
    def __init__(self):
        super().__init__()
        self.url_base = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
        self.token = 'YznOUAOeEnTCIZKbNHAooghTSfbVdUMm'
        self.data_set_id = 'GHCND'
        conn_dict = dict(   drivername = 'mysql+pymysql',
                            host  = 'mdb-wx1.use4.gcp.laurion.corp',
                            database = 'WX1',
                            username ='ssethuraman',
                            password ='BTdaWJep,C!BUx-gHJZ#:b}Mp2vUz#?P')
        url = URL(**conn_dict)
        self.conn = create_engine(url)
    pass

    def construct_payload(self, expression, start_date=None, end_date=None, data_set_id='GHCND'):
        self.data_set_id = data_set_id
        self.station_id = self.get_stationid_from_expression(expression)
        payload = {
            'datasetid': self.data_set_id,
            'stationid': self.data_set_id + ':' + self.station_id,
            'datatypeid': expression['datatypeid'],
            'startdate': start_date.astype(str),
            'enddate': end_date.astype(str),
            'units': 'standard',
            'limit': '1000',
        }
        return payload

    def get_stationid_from_expression(self, expression):
        if 'WBAN' in expression.keys():
            if expression['WBAN'] is not None:
                return 'USW000' + str(expression['WBAN'])
        if 'stationid' in expression.keys():
            if expression['stationid'] is not None:
                return expression['stationid']
        else:
            pass

    def transform_response_df(self, response):
        df = pd.DataFrame(response['results'])
        df = df.pivot(index='date', columns='datatype')['value'].reset_index()
        df = df.rename(columns={'date': 'RecordDate', 'TMAX': 'TMax', 'TMIN': 'TMin'})
        df['RecordDate'] = pd.to_datetime(df['RecordDate'], unit='ns').astype(str)
        return df


    def post_process(self, df):
        df['DataSetID'] = self.data_set_id
        df['StationID'] = self.station_id
        df['DateUpdated'] = pd.Timestamp.now()
        return df

    def save_to_db(self, df):
        try:
            df.to_sql('NOAA_Daily_Tempr', con=self.conn, if_exists='append', index=False)
            print('Successfully saved to NOAA_Daily_Tempr.')
        except Exception as e:
            print('Failed to save to NOAA_Daily_Tempr.')

    def chunkify(self, start_date=None, end_date=None):
        chunky_tuple = []

        end_date_final = None
        if start_date is None:
            start = 1970
        else:
            start = pd.to_datetime(start_date).year

        if end_date is None:
            end_date_final = np.datetime64('today') - np.timedelta64(1, 'D')
            end = pd.to_datetime(end_date_final).year
        else:
            end_date_final = end_date
            end = end_date.year
        year_range = np.arange(start, end + 1)

        for year in year_range:
            if year == start and start_date is not None:
                st_ = start_date
            else:
                st_ = np.datetime64("{0}-{1:0=2d}-{2:0=2d}".format(year, 1, 1))
            if year == end and end_date_final is not None:
                end_ = end_date_final
            else:
                end_ = np.datetime64("{0}-{1:0=2d}-{2:0=2d}".format(year, 12, 31))
            chunky_tuple.append((st_, end_))
        return chunky_tuple



