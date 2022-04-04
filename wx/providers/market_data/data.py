import requests
import pandas as pd
import numpy as np
import io
import os
import json

try:
    import config
except Exception as e:
    from . import config


def wx_fetch_station_array(station_info_array, start=2019, end=None, save_to_file=False):
    for station_info in station_info_array:
        wx_fetch_station(station_info, start=start, end=end, save_to_file=save_to_file)
    pass


def wx_fetch_station(station_info, start=1970, end=None, save_to_file=False):
    url_text = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    station_name = station_info['name']
    station_full_name = station_name + '_WBAN_' + str(station_info['WBAN'])
    stationid = 'USW000' + str(station_info['WBAN'])
    print('Fetching market_data for {0}'.format(station_full_name))

    payload = {
        'datasetid': 'GHCND',
        'stationid': 'GHCND:' + stationid,
        'datatypeid': ['TMAX', 'TMIN'],
        'units': 'standard',
        'limit': '1000',
    }
    headers = {
        'token': 'YznOUAOeEnTCIZKbNHAooghTSfbVdUMm'
    }

    temp_df = pd.DataFrame()

    end_date_final = None
    if end is None:
        end_date_final = np.datetime64('today') - np.timedelta64(1, 'D')
        end = pd.to_datetime(end_date_final).year
    year_range = np.arange(start, end+1)

    for year in year_range:
        start_date = np.datetime64("{0}-{1:0=2d}-{2:0=2d}".format(year, 1, 1))
        if year == end and end_date_final is not None:
            end_date = end_date_final
        else:
            end_date = np.datetime64("{0}-{1:0=2d}-{2:0=2d}".format(year, 12, 31))
        date_dict = {
            'startdate': start_date.astype(str),
            'enddate': end_date.astype(str),
        }
        payload.update(date_dict)
        url_data = requests.get(url_text, params=payload, headers=headers).content
        url_data = json.loads(url_data)
        temp_data = pd.DataFrame(url_data['results'])
        temp_data = temp_data.pivot(index='date', columns='datatype')['value'].reset_index()
        temp_data = temp_data.rename(columns={'date': 'Record date', 'TMAX': 'TMax', 'TMIN': 'TMin'})
        temp_data['Record date'] = pd.to_datetime(temp_data['Record date'], unit='ns').astype(str)
        temp_df = pd.concat([temp_df, temp_data], axis=0)
        print("Successfully queried market_data for year: {0}".format(year))

    if save_to_file:
        file_name = station_full_name + '.csv'
        full_name = os.path.join(config.TEMP_ROOT, file_name)
        temp_df.to_csv(full_name, index=False)
        print('Successfully saved wx market_data for {0}'.format(station_full_name))
    return temp_df


def getdata_csv(wx_location):
    file_name = wx_location + '.csv'
    df = pd.read_csv(os.path.join(config.TEMP_ROOT, file_name))
    df = df[['Record date', 'TMin', 'TMax']]
    df['Date'] = pd.to_datetime(df['Date'])
    # df['Date'] = pd.to_datetime(df['Date'], origin=pd.Timestamp('1899-12-30'), unit='D')
    df['Month'] = pd.DatetimeIndex(df['Date']).month
    df['Year'] = pd.DatetimeIndex(df['Date']).year
    df['Year-Month'] = df['Year'].astype('str') + '_' + df['Month'].astype('str')
    df['TAvg'] = df[['TMin', 'TMax']].astype(float).mean(axis=1)
    return df


def getfwd_csv(location, risk_start, risk_end, as_of_date):
    file_name = location + '.csv'
    df = pd.read_csv(os.path.join(config.FWD_ROOT, file_name))
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.set_index('Date')
    risk_start = pd.to_datetime(risk_start)
    risk_end = pd.to_datetime(risk_end)
    as_of_date = pd.to_datetime(as_of_date)
    spot_df = pd.read_csv(os.path.join(config.SPOT_ROOT, file_name)).fillna(method='bfill')
    spot_df['Date'] = spot_df.apply(lambda x: pd.to_datetime(x))
    spot_df = spot_df.set_index('Date')

    df_contracts = [pd.to_datetime(x) for x in df.columns if x != 'Date']
    season_start = np.min(df_contracts)
    ss_mth = season_start.month
    ss_year = season_start.year
    ss_season = 2012
    if ss_mth < 9:
        ss_season = ss_year - 1
    # season_final = np.max(df_contracts)
    se_mth = risk_end.month
    se_year = risk_end.year - 1

    se_season = 2019
    if se_mth < 9:
        se_season = se_year - 1
    season_arr = np.arange(ss_season, se_season + 1)



    risk_arr = pd.date_range(start=risk_start, end=risk_end, freq='MS')
    cmdty_devs = pd.DataFrame()

    for season in season_arr:
        yr = season + 1
        asof_season = pd.to_datetime(str(season) + '-' + str(as_of_date.month) + '-' + str(as_of_date.day))
        # print(season)
        for r in risk_arr:
            contract = pd.to_datetime(str(yr) + '-' + str(r.month) + '-' + '01')
            contract_series = df[contract.strftime('X%m/X%d/%Y').replace('X0','X').replace('X','')]
            season_asof_expiry = np.busday_offset(np.datetime64(contract, 'D'), -3, roll='forward', weekmask='1111100')
            fwd_season_inception = float(contract_series[contract_series.index == asof_season].iloc[0])
            fwd_season_expiry = float(contract_series[contract_series.index== season_asof_expiry].iloc[0])
            spot_season_r = spot_df.loc[pd.date_range(start=contract, periods=r.days_in_month)]
            spot_season_r_avg = spot_season_r[location].mean()
            cash_devs = np.log(np.array(spot_season_r[location]) / spot_season_r_avg)
            cmdty_devs = cmdty_devs.append({'Season': season,
                                        'Risk_Period': r.month,
                                        'Fwd_Inception': fwd_season_inception,
                                        'Fwd_Expiry': fwd_season_expiry,
                                        'Spot_Avg': spot_season_r_avg,
                                        'Fwd_Devs1': np.log(fwd_season_expiry/fwd_season_inception),
                                        'Fwd_Devs2': np.log(spot_season_r_avg/fwd_season_expiry),
                                        'Cash_Devs': cash_devs,
                                        'Antithetic': 1}, ignore_index=True)
            cmdty_devs = cmdty_devs.append({'Season': season,
                                        'Risk_Period': r.month,
                                        'Fwd_Inception': fwd_season_inception,
                                        'Fwd_Expiry': fwd_season_expiry,
                                        'Spot_Avg': spot_season_r_avg,
                                        'Fwd_Devs1': -1*np.log(fwd_season_expiry/fwd_season_inception),
                                        'Fwd_Devs2': -1*np.log(spot_season_r_avg/fwd_season_expiry),
                                        'Cash_Devs': -1*cash_devs,
                                        'Antithetic': -1}, ignore_index=True)

    # print(cmdty_devs)

    return cmdty_devs


if __name__ == '__main__':
    station_info_1 = {
        'name': 'Poughkeepsie',
        'WBAN': 14757,
        'WMO': None
    }

    station_info_2 = {
        'name': 'LGA',
        'WBAN': 14732,
        'WMO': 72503
    }

    station_info_3 = {
        'name': 'Dallas',
        'WBAN': '03927',
        'WMO': 72259
    }

    station_info_4 = {
        'name': 'McAllenMiller',
        'WBAN': 12959,
        'WMO': 722506
    }

    station_info_5 = {
        'name': 'MidlandInternational',
        'WBAN': 23023,
        'WMO': 72265
    }

    station_info_6 = {
        'name': 'NewarkLiberty',
        'WBAN': 14734,
        'WMO': 72502
    }


    # wx_fetch_station_array([station_info_6], start=1970, save_to_file=True)
    getfwd_csv('TetcoM3_New', '2021-01-01', '2021-02-28', '2020-09-01')