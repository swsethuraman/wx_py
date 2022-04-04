from .generic_windows_db_provider import GenericWindowsDBProvider
import pandas as pd

class Wx1Provider(GenericWindowsDBProvider):
    def __init__(self):
        self.conn_dict = {
            'drivername': 'mysql+pymysql',
            'host': 'mdb-wx1.use4.gcp.laurion.corp',
            'database': 'WX1',
            'username': 'ssethuraman',
            'password': 'BTdaWJep,C!BUx-gHJZ#:b}Mp2vUz#?P'
        }
        #self.wx_daily_table = "NOAA_Daily_Tempr"
        self.wx_daily_table = "WX_WEATHER_DAILY_CLEANED"
        self.wx_forecast_table_na = "MS_CWG_FCST_NA_V"
        super().__init__()
        pass

    def get_wx_daily(self, constraints, columns):
        #if 'WBAN' in constraints.keys():
        #    constraints.update({'StationID': 'USW000' + constraints['WBAN']})
        #    constraints.pop('WBAN', None)
        if 'StartDate' in constraints.keys():
            pass
        if 'EndDate' in constraints.keys():
            pass
        #df1 = self.run_query_df(table='NOAA_Station_DateList_V', constraints=constraints, columns=['RecordDate'])
        #df2 = self.run_query_df(table=self.wx_daily_table, constraints=constraints, columns=columns)
        #df = pd.merge(df1,df2,how='left',on=['RecordDate'])
        #df = df.rename(columns={'RecordDate': 'Date'})
        df = self.run_query_df(table=self.wx_daily_table, constraints=constraints, columns=columns)
        df = df.rename(columns={'OPR_DATE': 'Date','TMIN': 'TMin','TMAX': 'TMax'})
        df['Date'] = df['Date'].apply(pd.to_datetime)
        df['Month'] = pd.DatetimeIndex(df['Date']).month
        df['Year'] = pd.DatetimeIndex(df['Date']).year
        df['Year-Month'] = df['Year'].astype('str') + '_' + df['Month'].astype('str')
        df['TAvg'] = df[['TMin', 'TMax']].astype(float).mean(axis=1)
        return df

    def get_wx_forecast_na (self, constraints, columns):
        #KEY COLUMNS ARE FORECAST_DATE, PUBLISHED_DATE, STATION, WMO, WBAN, FCST_MN, FCST_MX, FCST_AVG
        if 'StartDate' in constraints.keys():
            pass
        if 'EndDate' in constraints.keys():
            pass
        df = self.run_query_df(table=self.wx_forecast_table, constraints=constraints, columns=columns)
        df = df.rename(columns={'RecordDate': 'Date'})
        df['forecast_date'] = df['forecast_date'].apply(pd.to_datetime)
        df['published_date'] = df['published_date'].apply(pd.to_datetime)
        df['Month'] = pd.DatetimeIndex(df['forecast_date']).month
        df['Year'] = pd.DatetimeIndex(df['forecast_date']).year
        df['Year-Month'] = df['Year'].astype('str') + '_' + df['Month'].astype('str')
        return df