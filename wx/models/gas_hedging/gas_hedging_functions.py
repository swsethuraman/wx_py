import pandas as pd
from xlib import xdb
import numpy as np
db = 'WX1-GC'

###AGGREGATING GAS PRICES
def get_hedge_prices_no_options(db, ind_months,hedge_contract, hedge_in_advance):
    ###PRICE OF HEDGING PER YEAR
    #NOTE WILL NOT WORK IF HEDGING FOR WINTER BEFORE END OF JULY
    #ASSUMES HEDGING IS BEING DONE FOR NO MORE THAN 8 MONTHS AHEAD
    df_hedge_price = pd.DataFrame(columns=['hedge_month','winter','strip','future_price','option_price'])
    sql_hedge_price = """
                select extract(month from opr_date) as hedge_month,
                    extract(year from date_sub(opr_date, interval 180 day)) as winter,
                    strip,
                    avg(settlement_price) as future_price,
                    0 as option_price
                from WX1.MS_ICE_CLEARED_GAS
                where contract=""" + hedge_contract + """
                and extract(day from opr_date) between 14 and 20
                and opr_date>date_sub(strip, interval 250 day)
                group by extract(month from opr_date), 
                    extract(year from date_sub(opr_date, interval 180 day)), 
                    strip
        """
    conn = xdb.make_conn(db, stay_open=True)
    df_hedge_price_i=conn.query(sql_hedge_price)
    conn.close()
    df_hedge_price=df_hedge_price.append(df_hedge_price_i)
    df_hedge_price = df_hedge_price.reset_index(drop=True)
    return df_hedge_price

def get_henry_prices_no_options(db, ind_months, hedge_in_advance):
    ###PRICE OF HEDGING PER YEAR
    #WINTER SHOULD BE BASED ON STRIP NOT OPR_DATE?
    #NOTE WILL NOT WORK IF HEDGING FOR WINTER BEFORE END OF JULY
    #ASSUMES HEDGING IS BEING DONE FOR NO MORE THAN 8 MONTHS AHEAD
    df_henry_price = pd.DataFrame(columns=['hedge_month','winter','strip','henry_price'])
    sql_henry_price = """
        select extract(month from opr_date) as hedge_month,
            extract(year from date_sub(opr_date, interval 180 day)) as winter,
            strip,
            avg(settlement_price) as henry_price
        from WX1.MS_ICE_CLEARED_GAS
        where contract='H'
            and contract_type='F'
            and extract(day from opr_date) between 14 and 20
            and opr_date>date_sub(strip, interval 250 day)
        group by extract(month from opr_date), 
            extract(year from date_sub(opr_date, interval 180 day)), 
            strip
        """
    conn = xdb.make_conn(db, stay_open=True)
    df_henry_price_i=conn.query(sql_henry_price)
    conn.close()
    df_henry_price=df_henry_price.append(df_henry_price_i)
    df_henry_price = df_henry_price.reset_index(drop=True)
    return df_henry_price

def format_hedge_price(ind_months, df_hedge_price, df_henry_price, hedge_in_advance, hedge_initial):
    #GET PRICES IN AT TIME OF ORIGINAL HEDGING
    df_hedge_price_fin = df_hedge_price.iloc[np.where(df_hedge_price['hedge_month']==hedge_initial)]
    df_hedge_price_fin = df_hedge_price_fin.reset_index(drop=True)
    df_hedge_price_fin = pd.merge(df_hedge_price_fin,df_henry_price, how='left', on=['winter','strip','hedge_month'])
    df_hedge_price_fin.rename(columns={'hedge_month':'hedge_month_' + str(hedge_initial),'future_price':'future_price_' + str(hedge_initial),'option_price':'option_price_' + str(hedge_initial),'henry_price':'henry_price_' + str(hedge_initial)},inplace=True)
    #GET PRICES AT TIME OF EACH DELTA HEDGE IN RISK PERIOD
    for i in ind_months[1:]:
        if i == 1:
            j=12
        else:
            j = i-1
        df_hedge_temp = df_hedge_price.iloc[np.where(df_hedge_price['hedge_month']==j)]
        df_hedge_temp = df_hedge_temp.reset_index(drop=True)
        df_hedge_temp.rename(columns={'hedge_month':'hedge_month_'+str(i),'future_price':'future_price_'+str(i),'option_price':'option_price_'+str(i)},inplace=True)
        df_hedge_price_fin = pd.merge(df_hedge_price_fin,df_hedge_temp, how='left', on=['winter','strip'])
        df_henry_temp = df_henry_price.iloc[np.where(df_henry_price['hedge_month']==j)]
        df_henry_temp = df_henry_temp.reset_index(drop=True)
        df_henry_temp.rename(columns={'hedge_month':'hedge_month_'+str(i),'henry_price':'henry_price_'+str(i)},inplace=True)
        df_hedge_price_fin = pd.merge(df_hedge_price_fin,df_henry_temp, how='left', on=['winter','strip','hedge_month_'+str(i)])
    return df_hedge_price_fin

def get_prices_at_expiry(db, hedge_contract):
    sql_end_prices= """
        select b.contract as contract,
            extract(month from b.strip) as strip_month,
            b.strip as strip,
            b.settlement_price as settlement_price
        from (
            select contract, 
                strip, 
                max(trade_date) as trade_date
            from WX1.ICE_Gas_Settlement
            where contract in (""" + hedge_contract + """)
                and trade_date > date_sub(strip, interval 365 day)
            group by strip
        ) a
        inner join (
            select * 
            from WX1.ICE_Gas_Settlement 
            where contract in (""" + hedge_contract + """)
        ) b
        on a.trade_date=b.trade_date
        and a.strip=b.strip
        and a.contract=b.contract
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_end_price=conn.query(sql_end_prices)
    conn.close()
    return df_end_price

def get_henry_prices_at_expiry(db):
    sql_henry_end_prices= """
        select b.strip as strip,
            b.settlement_price as henry_expiry_price
        from (
            select strip, 
                max(trade_date) as trade_date
            from WX1.ICE_Gas_Settlement
            where contract='H'
                and contract_type='F'
                and trade_date > date_sub(strip, interval 365 day)
            group by strip
        ) a
        inner join (
            select * 
            from WX1.ICE_Gas_Settlement 
            where contract = 'H' 
            and contract_type='F'
        ) b
        on a.trade_date=b.trade_date
        and a.strip=b.strip
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_henry_end_price=conn.query(sql_henry_end_prices)
    conn.close()
    return df_henry_end_price


def format_prices(df_hedge_price_fin, df_end_price, df_henry_end_price, ind_months, current_season, risk,
                  hedge_initial):
    df_end_price_comb = pd.merge(df_end_price, df_henry_end_price, how='inner', on=['strip'])
    df_price_combined = pd.merge(df_hedge_price_fin, df_end_price_comb, how='inner', on=['strip'])
    df_price_combined = df_price_combined.loc[np.where(
        (df_price_combined['strip_month'] >= ind_months[0]) | (df_price_combined['strip_month'] <= ind_months[-1]))]
    df_price_combined = df_price_combined.reset_index(drop=True)
    df_price_final = pd.DataFrame(
        columns=['winter', 'strip', 'contract', 'strip_month', 'risk_months', 'option_price', 'hedge_month',
                 'basis_payoff', 'fixed_payoff', 'basis_put_payoff', 'fixed_put_payoff', 'basis_swap_payoff',
                 'fixed_swap_payoff'])

    #GET METADATA - USE THIS METADATA TO ADD NAMES TO CONTRACTS FOR CLARITY
    sql_meta = """
        select region,
            contract,
            full_name,
            basis
        from WX2.GAS_HEDGE_CONTRACTS
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_meta = conn.query(sql_meta)
    conn.close()
    contract = df_price_combined['contract'].drop_duplicates().to_numpy()[0]
    basis = df_meta.iloc[np.where(df_meta['contract'] == contract)]['basis'].to_numpy()[0]
    name = df_meta.iloc[np.where(df_meta['contract'] == contract)]['full_name'].to_numpy()[0]
    region = df_meta.iloc[np.where(df_meta['contract'] == contract)]['region'].to_numpy()[0]

    for i in ind_months:
        risk_month = i
        if i == ind_months[0]:
            i = hedge_initial
        df_price_final_temp = df_price_combined.copy().reset_index(drop=True)
        df_price_final_temp['risk_months'] = str(risk_month)
        df_price_final_temp['hedge_month'] = df_price_final_temp['hedge_month_' + str(i)]
        df_price_final_temp['basis_settlement_price'] = df_price_final_temp['settlement_price'] * basis
        df_price_final_temp['fixed_settlement_price'] = df_price_final_temp['settlement_price'] + (
                    (0 + basis) * df_price_final_temp['henry_expiry_price'])
        df_price_final_temp['basis_future_price'] = df_price_final_temp['future_price_' + str(i)] * basis
        df_price_final_temp['fixed_future_price'] = df_price_final_temp['future_price_' + str(i)] + (
                    (0 + basis) * df_price_final_temp['henry_price_' + str(i)])
        df_price_final_temp['basis_option_price'] = df_price_final_temp['option_price_' + str(i)]
        df_price_final = df_price_final.append(df_price_final_temp)

    df_price_final = df_price_final.reset_index(drop=True)
    df_price_final = df_price_final.iloc[np.where(df_price_final['winter'] < current_season)].reset_index(drop=True)

    if risk == 'strip':
        df_price_final['risk_months'] = str(ind_months[0]) + '-' + str(ind_months[-1])

    df_price_final = df_price_final[
        ['winter', 'strip', 'contract', 'risk_months', 'hedge_month', 'basis_settlement_price',
         'fixed_settlement_price', 'basis_future_price', 'fixed_future_price', 'basis_option_price']]
    df_price_final = df_price_final.iloc[np.where(df_price_final['hedge_month'].astype(float) > 0)].reset_index(
        drop=True)
    return df_price_final

###COMBINING WEATHER AND AGGREGATED GAS
def get_weather_data(db, risk_months, hedge_location):
    df_weather = pd.DataFrame(columns=['station','season','risk_period','season_hdds','rolling_hdds','coldness'])
    for i in risk_months:
        risk_period = i
        sql_weather = """
            select a.station as station,
                a.season as season,
                replace('""" + risk_period + """',',','_') as risk_period,
                round(a.hdds,2) as season_hdds,
                round(avg(b.hdds),2) as rolling_hdds,
                round(a.hdds,2) - round(avg(b.hdds),2) as coldness
            from (
                select station,
                    season,
                    sum(hdds) as hdds
                from (
                    select icao as station,
                        extract(year from date_sub(opr_date, interval 125 day)) as season,
                        extract(year from opr_date) as opr_year,
                        extract(month from opr_date) as opr_month,
                        opr_date, 
                        greatest(0,65 - ((tmin + tmax)/2)) as hdds
                    from WX1.WX_WEATHER_DAILY_CLEANED
                    where icao in ('""" + hedge_location + """')
                    and extract(month from opr_date) in (""" + risk_period + """)
                ) a1
                group by station, season
            ) a
            inner join (
                select station,
                    season,
                    sum(hdds) as hdds
                from (
                    select icao as station,
                        extract(year from date_sub(opr_date, interval 125 day)) as season,
                        extract(year from opr_date) as opr_year,
                        extract(month from opr_date) as opr_month,
                        opr_date, 
                        greatest(0,65 - ((tmin + tmax)/2)) as hdds
                    from WX1.WX_WEATHER_DAILY_CLEANED
                    where icao in ('""" + hedge_location + """')
                    and extract(month from opr_date) in (""" + risk_period + """)
                ) b1
                group by station, season
            ) b
            on a.season>b.season
                and a.season<b.season+11
                and a.station=b.station
            where a.season>2000
            group by a.station,a.season,a.hdds,replace('""" + risk_period + """',',','_')
        """
        conn = xdb.make_conn(db, stay_open=True)
        dfw=conn.query(sql_weather)
        conn.close()
        df_weather = df_weather.append(dfw)
    df_weather.rename(columns={'risk_period':'risk_months','season':'winter'},inplace=True)
    df_weather = df_weather[['risk_months','winter','station','coldness']]
    return df_weather

def format_weather(df_weather, hedge_location, risk):
    df_weather_temp = df_weather.iloc[np.where(df_weather['station']==hedge_location)].reset_index(drop=True)
    if risk=='monthly':
        df_weather_temp1 = df_weather_temp.iloc[np.where(df_weather_temp['risk_months']==0)]
        for i in ind_months:
            df_weather_temp1 = df_weather_temp1.append(df_weather_temp.iloc[np.where(df_weather_temp['risk_months']==str(i))])
        df_weather_temp = df_weather_temp1.copy()
    #TEST FOR STRIP LOGIC, CURRENTLY POINTLESS STEP
    if risk=='strip':
        df_weather_temp = df_weather_temp.copy()
    df_weather_temp = df_weather_temp.reset_index(drop=True)
    df_weather_temp['risk_months'] = df_weather_temp['risk_months'].astype(str)
    return df_weather_temp

#GET AGGREGATED GAS PRICES
def get_gas_hist(db,hedge_contract,hedge_in_advance,fc):
    sql_gas="""
        select winter,
            strip,
            contract,
            risk_months,
            hedge_month,
            basis_settlement_price,
            fixed_settlement_price,
            basis_future_price,
            fixed_future_price,
            basis_option_price,
            hedge_in_advance,
            fc
        from WX2.GAS_HEDGE_WINTER_SIMS_HEDGE_PRICES
        where CONTRACT = '""" + hedge_contract + """'
            and HEDGE_IN_ADVANCE = '""" + str(hedge_in_advance) + """'
            and FC = '""" + fc + """'
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_price_final=conn.query(sql_gas)
    conn.close()
    return df_price_final

def combine_price_weather(df_weather_temp,df_price_final,risk,hedge_in_advance,strip):
    ###FOR DELTA HEDGING STRIP, SHOULD WE BASE DECISION BASED ON TOTAL COLDNESS UP TO PRESENT INSTEAD OF PREV MONTH COLDNESS?
    df_weather_temp['risk_months'] = df_weather_temp['risk_months'].astype(str)
    df_price_final['hedge_month'] = df_price_final['hedge_month'].astype(str)
    df_price_final['actual_month'] = pd.DatetimeIndex(df_price_final['strip']).month.astype(str)
    df_price_final['effective_month'] = (pd.DatetimeIndex(df_price_final['strip']) - timedelta(days=1)).month.astype(str)
    #NEED REALIZED COLDNESS FOR EACH STRIP MONTH
    df_weather_temp['deal_strip'] = 'N/A'
    df_price_final['deal_strip'] = 'N/A'
    if risk=='monthly':
        #THESE VALUES USED FOR DETERMINING DELTA HEDGE SIZES
        df_combined = pd.merge(df_price_final,df_weather_temp[['winter','risk_months','station','coldness']],how='inner',left_on=['winter','risk_months'], right_on=['winter','risk_months'])
        df_combined = pd.merge(df_combined,df_weather_temp[['winter','risk_months','coldness']],how='left',left_on=['winter','hedge_month'], right_on=['winter','risk_months'])
        df_combined.rename(columns={'coldness_x':'coldness','coldness_y':'prev_month_coldness','risk_months_x':'risk_months'},inplace=True)
        #THESE COLUMNS USED TO DETERMINE CORRELATIONS BETWEEN WEATHER AND HEDGE PAYOUTS
        df_combined = pd.merge(df_combined,df_weather_temp[['winter','risk_months','coldness']],how='left',left_on=['winter','actual_month'], right_on=['winter','risk_months'])
        df_combined.rename(columns={'coldness_x':'coldness','coldness_y':'realized_coldness','risk_months_x':'risk_months'},inplace=True)
        df_combined = pd.merge(df_combined,df_weather_temp[['winter','risk_months','coldness']],how='left',left_on=['winter','effective_month'], right_on=['winter','risk_months'])
        df_combined.rename(columns={'coldness_x':'coldness','coldness_y':'pre_realized_coldness','risk_months_x':'risk_months'},inplace=True)
    if risk=='strip':
        df_price_final['deal_strip'] = strip[0]
        df_price_final['risk_months'] = df_price_final['winter'].astype(str) + '-' + df_price_final['hedge_month'] + '-01'
        df_price_final['risk_months'] = pd.to_datetime(df_price_final['risk_months']) + pd.DateOffset(months=int(hedge_in_advance))
        df_price_final['risk_months'] = pd.DatetimeIndex(df_price_final['risk_months']).month.astype(str)
        df_weather_temp['deal_strip'] =  df_weather_temp['risk_months']
        #THESE VALUES USED FOR DETERMINING DELTA HEDGE SIZES
        df_combined = pd.merge(df_price_final,df_weather_temp[['winter','risk_months','station','coldness']],how='inner',left_on=['winter','risk_months'], right_on=['winter','risk_months'])
        df_combined = pd.merge(df_combined,df_weather_temp[['winter','risk_months','coldness']],how='left',left_on=['winter','hedge_month'], right_on=['winter','risk_months'])
        df_combined.rename(columns={'coldness_x':'coldness','coldness_y':'prev_month_coldness','risk_months_x':'risk_months'},inplace=True)
        #THESE COLUMNS USED TO DETERMINE CORRELATIONS BETWEEN WEATHER AND HEDGE PAYOUTS
        df_combined = pd.merge(df_combined,df_weather_temp[['winter','risk_months','coldness']],how='left',left_on=['winter','actual_month'], right_on=['winter','risk_months'])
        df_combined.rename(columns={'coldness_x':'coldness','coldness_y':'realized_coldness','risk_months_x':'risk_months'},inplace=True)
        df_combined = pd.merge(df_combined,df_weather_temp[['winter','risk_months','coldness']],how='left',left_on=['winter','effective_month'], right_on=['winter','risk_months'])
        df_combined.rename(columns={'coldness_x':'coldness','coldness_y':'pre_realized_coldness','risk_months_x':'risk_months'},inplace=True)
    df_combined['basis_payout'] = df_combined['basis_future_price'] - df_combined['basis_settlement_price']
    df_combined['fixed_payout'] = df_combined['fixed_future_price'] - df_combined['fixed_settlement_price']
    df_combined=df_combined[['winter','strip','contract','risk_months','hedge_month','hedge_in_advance','fc','deal_strip','station','coldness','prev_month_coldness','realized_coldness','pre_realized_coldness','basis_payout','fixed_payout']]
    return df_combined

###THIS COULD BE SPLIT INTO MULTIPLE FUNCTIONS
def get_hedge_points(df_full, min_winter, hedge_contracts, basis_fixed):
    payout_col = basis_fixed + '_payout'
    hedge_points = []
    # CREATE DATAFRAME CONTAINING A COLUMN FOR EACH POTENTIAL HEDGE POINT
    # HEDGE_CONTRACTS HAS BEEN SORTED TO MAKE SURE FIRST IN LIST HAS FULL HISTORY (IMPORTANT FOR JOINS)
    df_corr_mtx = df_full[['winter', 'strip', 'hedge_month', payout_col]].iloc[
        np.where(df_full['contract'] == hedge_contracts['contract'][0])].reset_index(drop=True)
    df_corr_mtx = df_corr_mtx.iloc[np.where(df_corr_mtx['winter'] >= min_winter)].reset_index(drop=True)
    df_corr_mtx = df_corr_mtx.rename(columns={payout_col: payout_col + hedge_contracts['contract'][0]})
    for i in hedge_contracts['contract'][1:]:
        df_corr_mtx_temp = df_full[['winter', 'strip', 'hedge_month', payout_col]].iloc[
            np.where(df_full['contract'] == i)].reset_index(drop=True)
        df_corr_mtx_temp = df_corr_mtx_temp.iloc[np.where(df_corr_mtx_temp['winter'] >= min_winter)].reset_index(
            drop=True)
        df_corr_mtx = pd.merge(df_corr_mtx, df_corr_mtx_temp, how='left', on=['winter', 'strip', 'hedge_month'])
        df_corr_mtx = df_corr_mtx.rename(columns={payout_col: payout_col + i})
    df_intrastation_corrs = df_corr_mtx.corr().reset_index()
    # SELECT STATION WITH TOP CORRELATION TO PORTFOLIO PAYOUTS
    # !!!NEED TO SERIOUSLY CONSIDER THIS LOGIC
    # df_corrs = df_full[['winter','risk_months','station','contract','coldness','prev_month_coldness','realized_coldness','pre_realized_coldness','basis_payout','fixed_payout']].iloc[np.where(df_full['risk_months']=='3')].reset_index(drop=True)
    # df_corrs = df_corrs[['station','contract','coldness','prev_month_coldness','realized_coldness','pre_realized_coldness','basis_payout','fixed_payout']].iloc[np.where(df_corrs['winter']>=min_winter)].reset_index(drop=True)
    df_corrs = df_full[
        ['station', 'contract', 'coldness', 'prev_month_coldness', 'realized_coldness', 'pre_realized_coldness',
         'basis_payout', 'fixed_payout']].iloc[np.where(df_full['winter'] >= min_winter)].reset_index(drop=True)
    df_corrs = df_corrs.groupby(['station', 'contract']).corr().reset_index()
    df_corrs = df_corrs.rename(columns={'level_2': 'obs'})
    df_corrs = df_corrs.iloc[
        np.where(np.isin(df_corrs['obs'], ['realized_coldness', 'pre_realized_coldness']))].reset_index(drop=True)
    df_corrs = df_corrs[['station', 'contract', 'obs', 'basis_payout', 'fixed_payout']]
    # HANDLE NANS FOR CASES WHERE BASIS ISN'T RELEVANT
    df_corrs['basis_payout'] = np.where(df_corrs['basis_payout'].isnull(), df_corrs['fixed_payout'],
                                        df_corrs['basis_payout'])
    # CALCULATE MEAN OF CORRS WITH REALIZED AND PRE-REALIZED COLDNESS [CONTRACT MONTH AND MONTH BEFORE CONTRACT MONTH COLDNESS]
    df_corrs = df_corrs[['station', 'contract', 'basis_payout', 'fixed_payout']].groupby(
        ['station', 'contract']).mean().reset_index()
    # CALCULATE AVERAGE OF BASIS AND FIXED CORRS
    df_corrs['payout_corr'] = (df_corrs['basis_payout'] + df_corrs['fixed_payout']) / 2
    primary_hedge = \
    df_corrs['contract'].iloc[np.where(df_corrs['payout_corr'] == df_corrs['payout_corr'].min())].to_numpy()[0]
    hedge_points.append(primary_hedge)
    # !!!
    # !THIS SHOULD BE DONE IN LOOP UNTIL RESULTS BECOME UNHELPFUL
    # SELECT REMAINING STATIONS WITH LOW CORR TO PRIMARY
    rem_hedges = df_intrastation_corrs['index'].iloc[
        np.where(df_intrastation_corrs[payout_col + str(primary_hedge)].abs() < 0.5)].str.replace(payout_col,
                                                                                                  '').to_numpy()
    df_corr_rem = df_corrs.iloc[np.where(df_corrs['contract'].isin(rem_hedges))].reset_index(drop=True)
    # PICK OUT SECONDARY HEDGE
    try:
        secondary_hedge = df_corr_rem['contract'].iloc[
            np.where(df_corr_rem['payout_corr'] == df_corr_rem['payout_corr'].min())].to_numpy()[0]
        hedge_points.append(secondary_hedge)
    except:
        secondary_hedge = ''
    # !END OF "THIS SHOULD BE DONE IN LOOP UNTIL RESULTS BECOME UNHELPFUL"
    # CREATE DATAFRAME OF HEDGE_LOCATIONS
    primary_hedge = hedge_points[0]
    df_hedge = df_full.iloc[np.where(df_full['contract'] == primary_hedge)].reset_index(drop=True)
    df_hedge = df_hedge[
        ['winter', 'strip', 'risk_months', 'realized_coldness', 'pre_realized_coldness', payout_col]].rename(
        columns={payout_col: payout_col + primary_hedge})
    if len(hedge_points) > 1:
        for j in hedge_points[1:]:
            df_hedge_temp = df_full.iloc[np.where(df_full['contract'] == j)].reset_index(drop=True)
            df_hedge_temp = df_hedge_temp[['winter', 'strip', 'risk_months', payout_col]]
            df_hedge = pd.merge(df_hedge, df_hedge_temp, how='left', on=['winter', 'strip', 'risk_months']).rename(
                columns={payout_col: payout_col + j})
    # GET OPTIMAL WEIGHTS OF HEDGE POINTS
    hedge_weights = []
    for h in range(len(hedge_points)):
        hedge_weights.append(1 / len(hedge_points))

    def fun(hedge_weights, hedge_points):
        hedge_pay = 0
        for i in range(len(hedge_points)):
            hedge_pay += hedge_weights[i] * df_hedge[payout_col + hedge_points[i]]
        return df_hedge['pre_realized_coldness'].corr(hedge_pay)

    # !CONSIDER RUNNING A FEW METHODS AND TAKING AVERAGE OR MEDIAN
    weights = opt.minimize(fun, hedge_weights, hedge_points, method='Nelder-Mead').x
    weights = weights / weights.sum()
    df_hedge[payout_col] = 0
    # print('WEIGHTS')
    for z in range(len(hedge_points)):
        df_hedge[payout_col] += df_hedge[payout_col + hedge_points[z]] * weights[z]
    # TURN CONTRACTS AND WEIGHTS COLUMN INTO COMMA SEPARATE LIST COLUMNS
    cons = ''
    wei = ''
    for c in hedge_points:
        cons += c + ','
    for w in weights:
        wei += str(w.round(3)) + ','
    df_hedge_final = df_hedge[['winter', 'strip', 'risk_months', payout_col]]
    df_hedge_final[basis_fixed + '_contracts'] = cons[:-1]
    df_hedge_final[basis_fixed + '_weights'] = wei[:-1]
    return df_hedge_final

###THIS COULD BE SPLIT INTO MULTIPLE FUNCTIONS
def generate_summary(df_full,hedge_size_per_month,delta_hedge_multiplier,ind_months,curve_weighting,risk,bias,winter_exposure,lau_side):
    #THIS FUNCTION IS MEANT TO OPTIMIZE HEDGE SIZE, DELTA HEDGE MULTIPLIER, CURVE WEIGHTING
    df_summary = df_full.copy()
    df_summary = df_summary.reset_index(drop=True)
    #THIS STEP ASSUMES ATM CALLS AND PUTS ARE SAME PRICE
    df_summary['hedge_size_nominal'] = np.where(df_summary['prev_month_coldness'].isnull(),hedge_size_per_month,0.01 * hedge_size_per_month * delta_hedge_multiplier * df_summary['prev_month_coldness'].abs())
    df_summary['strip'] = pd.to_datetime(df_summary['strip'])
    df_summary['fc_order'] = df_summary.groupby(['winter','risk_months'])['strip'].rank(ascending=True)
    df_summary['fc_order'] = df_summary['fc_order'].astype(int)-1
    #ADD CURVE_WEIGHTING INFO
    df_summary['curve_weighting_matrix']=str(curve_weighting)
    df_curve_weighting = pd.DataFrame(columns=['risk_months','fc_order','curve_weighting'])
    for i in range(len(ind_months)):
        fco=0
        for j in range(len(curve_weighting[i])):
            #THIS HAS ONLY BEEN TESTED FOR MONTHLY RISK, NOT STRIP
            #CURVE WEIGHTING MIGHT BE DIFFERENT FOR STRIP
            rm=ind_months[i]
            new_row = {'risk_months':rm,'fc_order':fco,'curve_weighting':curve_weighting[i][j]}
            df_curve_weighting = df_curve_weighting.append(new_row, ignore_index=True)
            fco+=1
    df_curve_weighting['fc_order'] = df_curve_weighting['fc_order'].astype(int)
    df_summary['risk_months'] = df_summary['risk_months'].astype(int)
    df_summary['fc_order'] = df_summary['fc_order'].astype(int)
    df_summary = pd.merge(df_summary,df_curve_weighting,how='left',on=['risk_months','fc_order'])
    df_summary['hedge_size'] = df_summary['hedge_size_nominal'] * df_summary['curve_weighting']
    #ADD BIAS INFO
    df_summary['bias_matrix']=str(bias)
    df_bias = pd.DataFrame(columns=['risk_months','bias'])
    for j in range(len(df_summary.risk_months.unique())):
        new_row = {'risk_months':df_summary.risk_months.unique()[j],'bias':bias[j]}
        df_bias = df_bias.append(new_row, ignore_index=True)
    df_summary = pd.merge(df_summary,df_bias,how='left',left_on=pd.DatetimeIndex(df_summary['strip']).month, right_on='risk_months')
    df_summary = df_summary.drop(columns=['risk_months','risk_months_y'])
    df_summary = df_summary.rename(columns={'risk_months_x':'risk_months'})
    #CALCULATE WINTER COLDNESS
    df_final_winter = df_summary[['winter','station','coldness']].iloc[np.where(df_summary['fc_order']==0)]
    if risk=='monthly':
        df_final_winter = df_final_winter.groupby(['winter','station']).sum('coldness')
    if risk=='strip':
        df_final_winter = df_final_winter.groupby(['winter','station']).mean('coldness')
    df_summary = pd.merge(df_summary,df_final_winter,how='inner',on=['winter','station'])
    df_summary = df_summary.rename(columns={'coldness_x':'coldness_risk_month','coldness_y':'coldness_season'})
    #CALCULATE PORTFOLIO PAYOUTS
    df_summary['monthly_swap_portfolio_payout'] = np.where(df_summary['fc_order']==0,lau_side * (df_summary['coldness_risk_month'] + df_summary['bias']) * winter_exposure,0)
    total_bias = 0
    for i in bias:
        total_bias+=i
    df_summary['strip_swap_portfolio_payout'] = np.where((df_summary['fc_order']==0) & (df_summary['risk_months']==ind_months[0]),lau_side * (df_summary['coldness_season'] + total_bias) * winter_exposure,0)
    df_summary['monthly_option_portfolio_payout'] = np.where(df_summary['monthly_swap_portfolio_payout']>0,0,df_summary['monthly_swap_portfolio_payout'])
    df_summary['strip_option_portfolio_payout'] = np.where(df_summary['strip_swap_portfolio_payout']>0,0,df_summary['strip_swap_portfolio_payout'])
    #CALCULATE HEDGE PAYOUTS
    #df_summary['basis_swap_hedge_payout'] = -1 * lau_side * df_summary['basis_payout'] * df_summary['hedge_size']
    df_summary['basis_swap_hedge_payout'] =lau_side * df_summary['basis_payout'] * df_summary['hedge_size']
    df_summary['basis_option_hedge_payout'] = df_summary['basis_swap_hedge_payout']
    df_summary['basis_option_hedge_payout'] = np.where(df_summary['basis_option_hedge_payout']<0,0,df_summary['basis_option_hedge_payout'])
    #df_summary['fixed_swap_hedge_payout'] = -1 * lau_side * df_summary['fixed_payout'] * df_summary['hedge_size']
    df_summary['fixed_swap_hedge_payout'] =lau_side * df_summary['fixed_payout'] * df_summary['hedge_size']
    df_summary['fixed_option_hedge_payout'] = df_summary['fixed_swap_hedge_payout']
    df_summary['fixed_option_hedge_payout'] = np.where(df_summary['fixed_option_hedge_payout']<0,0,df_summary['fixed_option_hedge_payout'])
    #CALCULATE TOTAL DELTA HEDGED AND NON DELTA HEDGED PAYOUTS
    df_dh = df_summary[['winter','station','basis_swap_hedge_payout','basis_option_hedge_payout','fixed_swap_hedge_payout','fixed_option_hedge_payout']].copy()
    df_dh = df_dh.groupby(['winter','station']).sum().reset_index()
    df_dh = df_dh.rename(columns={'basis_swap_hedge_payout':'dh_basis_swap_hedge_payout','basis_option_hedge_payout':'dh_basis_option_hedge_payout','fixed_swap_hedge_payout':'dh_fixed_swap_hedge_payout','fixed_option_hedge_payout':'dh_fixed_option_hedge_payout'})
    df_dh['risk_months'] = ind_months[0]
    df_dh['fc_order'] = 0
    df_summary = pd.merge(df_summary,df_dh,how='left',on=['winter','station','risk_months','fc_order'])
    df_ndh = df_summary.iloc[np.where(df_summary['risk_months']==ind_months[0])].reset_index(drop=True)
    df_ndh = df_ndh[['winter','station','basis_swap_hedge_payout','basis_option_hedge_payout','fixed_swap_hedge_payout','fixed_option_hedge_payout']].copy()
    df_ndh = df_ndh.groupby(['winter','station']).sum().reset_index()
    df_ndh = df_ndh.rename(columns={'basis_swap_hedge_payout':'ndh_basis_swap_hedge_payout','basis_option_hedge_payout':'ndh_basis_option_hedge_payout','fixed_swap_hedge_payout':'ndh_fixed_swap_hedge_payout','fixed_option_hedge_payout':'ndh_fixed_option_hedge_payout'})
    df_ndh['risk_months'] = ind_months[0]
    df_ndh['fc_order'] = 0
    df_summary = pd.merge(df_summary,df_ndh,how='left',on=['winter','station','risk_months','fc_order'])
    #AGGREGATE INTO FINAL RESULT
    df_summary['hedge_size_per_month'] = hedge_size_per_month
    df_summary['delta_hedge_multiplier'] = delta_hedge_multiplier
    df_agg = df_summary[['winter','station','basis_contracts','basis_weights','fixed_contracts','fixed_weights','hedge_in_advance','fc','coldness_season','hedge_size_per_month','delta_hedge_multiplier','curve_weighting_matrix','bias_matrix','monthly_swap_portfolio_payout','strip_swap_portfolio_payout','monthly_option_portfolio_payout','strip_option_portfolio_payout','dh_basis_swap_hedge_payout','dh_basis_option_hedge_payout','dh_fixed_swap_hedge_payout','dh_fixed_option_hedge_payout','ndh_basis_swap_hedge_payout','ndh_basis_option_hedge_payout','ndh_fixed_swap_hedge_payout','ndh_fixed_option_hedge_payout']]
    df_agg = df_agg.groupby(['winter','station','basis_contracts','basis_weights','fixed_contracts','fixed_weights','hedge_in_advance','fc','coldness_season','hedge_size_per_month','delta_hedge_multiplier','curve_weighting_matrix','bias_matrix']).sum().reset_index()
    return df_agg

###OPTIMIZATION AND VISUALIZATION FUNCTIONS
def get_payout_data(station,deal_type,fc,min_winter,hedge_type,delta_hedge='Y'):
    if delta_hedge=='Y':
        prefix='dh'
    elif delta_hedge=='N':
        prefix='ndh'
    #GET ALL PAYOUT DATA
    sql="""
        select winter,
            station,
            fc,
            basis_contracts,
            basis_weights,
            fixed_contracts,
            fixed_weights,
            coldness_season,
            hedge_in_advance,
            hedge_size_per_month,
            delta_hedge_multiplier,
            curve_weighting_matrix,
            strip_"""+deal_type+"""_portfolio_payout as portfolio_payout,
            """+prefix+"""_basis_"""+hedge_type+"""_hedge_payout as basis_payout,
            """+prefix+"""_fixed_"""+hedge_type+"""_hedge_payout as fixed_payout
        from WX2.GAS_HEDGE_WINTER_SIMS_PAYOUTS
        where winter>="""+str(min_winter)+"""
            and station='"""+station+"""'
            and fc='"""+fc+"""'
    """
    conn = xdb.make_conn(db, stay_open=True)
    df=conn.query(sql)
    conn.close()
    return df

def select_optimal_parameters(df_payouts):
    hia_list = df_payouts['hedge_in_advance'].unique()
    dhm_list = df_payouts['delta_hedge_multiplier'].unique()
    cw_list  = df_payouts['curve_weighting_matrix'].unique()
    df_corr = pd.DataFrame(columns=['hedge_in_advance','delta_hedge_multiplier','curve_weighting_matrix','basis_correlation','fixed_correlation'])
    for i in hia_list:
        for j in dhm_list:
            for k in cw_list:
                df_temp=df_payouts.iloc[np.where(df_payouts['hedge_in_advance']==i)].reset_index(drop=True)
                df_temp=df_temp.iloc[np.where(df_temp['delta_hedge_multiplier']==j)].reset_index(drop=True)
                df_temp=df_temp.iloc[np.where(df_temp['curve_weighting_matrix']==str(k))].reset_index(drop=True)
                df_temp=df_temp[['portfolio_payout','basis_payout','fixed_payout']]
                df_temp=df_temp.reset_index(drop=True)
                corr=df_temp.corr().reset_index()
                corr=corr.iloc[np.where(corr['index']=='portfolio_payout')][['basis_payout','fixed_payout']]
                df_corr_temp=pd.DataFrame(columns=['hedge_in_advance','delta_hedge_multiplier','curve_weighting_matrix','basis_correlation','fixed_correlation'])
                new_row={'hedge_in_advance':i,'delta_hedge_multiplier':j,'curve_weighting_matrix':k, 'basis_correlation':corr['basis_payout'].to_numpy()[0], 'fixed_correlation':corr['fixed_payout'].to_numpy()[0]}
                df_corr_temp=df_corr_temp.append(new_row, ignore_index=True)
                df_corr=df_corr.append(df_corr_temp)
                df_corr=df_corr.reset_index(drop=True)
    #!!!RECONSIDER THIS LOGIC
    df_corr['combined_correlation']= df_corr['basis_correlation'] + df_corr['fixed_correlation']
    optimal_hia=df_corr.iloc[np.where(df_corr['combined_correlation']==df_corr['combined_correlation'].min())]
    optimal_hia=optimal_hia['hedge_in_advance'].unique()
    optimal_hia=optimal_hia[0]
    optimal_dhm=df_corr.iloc[np.where(df_corr['combined_correlation']==df_corr['combined_correlation'].min())]
    optimal_dhm=optimal_dhm['delta_hedge_multiplier'].unique()
    optimal_dhm=optimal_dhm[0]
    optimal_cw=df_corr.iloc[np.where(df_corr['combined_correlation']==df_corr['combined_correlation'].min())]
    optimal_cw=optimal_cw['curve_weighting_matrix'].unique()
    optimal_cw=optimal_cw[0]
    df_payouts = df_payouts.iloc[np.where(df_payouts['hedge_in_advance']==optimal_hia)].reset_index(drop=True)
    #IF THIS IS REMOVED THEN EVERYTHING REGARDGING DHM ABOVE CAN ALSO BE REMOVED
    df_payouts = df_payouts.iloc[np.where(df_payouts['delta_hedge_multiplier']==optimal_dhm)].reset_index(drop=True)
    df_payouts = df_payouts.iloc[np.where(df_payouts['curve_weighting_matrix']==optimal_cw)].reset_index(drop=True)
    return df_payouts

def get_hedge_size(df_payouts,percent_to_remove,basis_fixed):
    #FIND BOTTOM 20% OF PAYOUTS
    df_limit = df_payouts.sort_values(by=['portfolio_payout']).reset_index(drop=True)
    df_limit = df_limit[df_limit.index < df_limit.index.max()/(100/percent_to_remove)]
    max_allowable_payout = df_limit['portfolio_payout'].max()
    #GET MIN HEDGE SIZE TO ACCOMPLISH GOAL
    df_payouts['hedged_payout'] = df_payouts['portfolio_payout'] + df_payouts[basis_fixed+'_payout']
    hedge_size_per_month = df_payouts.iloc[np.where((df_payouts['hedged_payout']<=max_allowable_payout) & (df_payouts['portfolio_payout']<df_payouts['hedged_payout']))].reset_index(drop=True)
    hedge_size_per_month = hedge_size_per_month['hedge_size_per_month'].max()
    hedge_size_per_month = df_payouts.iloc[np.where(df_payouts['hedge_size_per_month']>hedge_size_per_month)].reset_index(drop=True)
    hedge_size_per_month = hedge_size_per_month['hedge_size_per_month'].min()
    df_payouts = df_payouts.iloc[np.where(df_payouts['hedge_size_per_month']==hedge_size_per_month)].reset_index(drop=True)
    return df_payouts
    #CONSIDER SCALING DOWN IF MEAN BASIS PAYOUT TOO LARGE RELATIVE TO PORTFOLIO PAYOUT

def get_histogram(df):
    import matplotlib
    df_hist = df[['winter', 'payout_unhedged', 'dh_swap_hedge_payout', 'denominator']]
    df_hist = df_hist.reset_index(drop=True)
    df_hist['payout_unhedged'] = df_hist['payout_unhedged'] / df_hist['denominator']
    df_hist['payout_unhedged'] = df_hist['payout_unhedged'] * 5
    df_hist['dh_swap_hedge_payout'] = df_hist['dh_swap_hedge_payout'] / df_hist['denominator']
    df_hist['dh_swap_hedge_payout'] = df_hist['dh_swap_hedge_payout'] * 5
    df_hist['hedged_payout'] = df_hist['payout_unhedged'] + df_hist['dh_swap_hedge_payout']
    df_hist = df_hist[['payout_unhedged', 'hedged_payout']]
    hist = df_hist.hist(bins=50, range=[-20000000, 20000000])
    return hist

###SIMPLE PYSPARK ML PIPELINE FOR FINAL HEDGE CONSTRUCTION AND PERFORMANCE EVALUATION
###LOGIC SHOULD BE IMPROVED BEFORE ACTUAL USE
def pipeline(db,start_year,end_year,station,hedge_size_per_month,hedge_in_advance,delta_hedge_multiplier):
    import findspark
    import pyspark
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession, SQLContext
    sc = SparkContext(conf=SparkConf())
    spark = SparkSession(sc)
    from pyspark.ml.stat import Correlation
    from pyspark.ml.feature import VectorAssembler, Normalizer, VectorIndexer
    from pyspark.mllib.stat import Statistics
    from pyspark.ml import Pipeline
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.regression import RandomForestRegressor
    from pyspark.ml.classification import NaiveBayes
    from sklearn.model_selection import train_test_split
    from pyspark.ml.evaluation import RegressionEvaluator
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    # THIS SQL SHOULD BE FILTERED DOWN TO ONLY INCLUDE OPTIMAL DHM, HEDGE SIZE, CURVE WEIGHTING, HEDGE IN ADVANCE
    sql_pay = """
        select dh_basis_swap_hedge_payout,
            -- dh_basis_option_hedge_payout,
            dh_fixed_swap_hedge_payout,
            -- dh_fixed_option_hedge_payout,
            ndh_basis_swap_hedge_payout,
            -- ndh_basis_option_hedge_payout,
            ndh_fixed_swap_hedge_payout,
            -- ndh_fixed_option_hedge_payout,
            strip_swap_portfolio_payout
            -- strip_option_portfolio_payout
        from WX2.GAS_HEDGE_WINTER_SIMS_PAYOUTS
        where winter<2020
        and winter>2012
        and station='KORD'
        -- and hedge_in_advance=3
        and hedge_size_per_month>6900
        -- and delta_hedge_multiplier<5
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_pay = conn.query(sql_pay)
    conn.close()

    df_pay['label'] = -1 * df_pay['strip_swap_portfolio_payout']

    train, test = train_test_split(df_pay, test_size=0.2, random_state=10)

    df_train = spark.createDataFrame(train)
    df_test = spark.createDataFrame(test)

    vectorAssembler = VectorAssembler(inputCols=df_train.columns[:-2], outputCol='features')
    normalizer = Normalizer(inputCol='features', outputCol='features_norm', p=1.0)
    lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
    rf = RandomForestRegressor(featuresCol='features_norm')
    pipeline = Pipeline(stages=[vectorAssembler, normalizer, lir])
    #pipeline = Pipeline(stages=[vectorAssembler, normalizer, rf])

    model_train = pipeline.fit(df_train)
    pred_train = model_train.transform(df_train)
    model_test = pipeline.fit(df_test)
    pred_test = model_test.transform(df_test)

    eval = RegressionEvaluator().setMetricName('r2').setLabelCol('label').setPredictionCol('prediction')
    print(eval.evaluate(pred_train))
    print(eval.evaluate(pred_test))
    print("Coefficients: " + str(model_train.stages[-1].coefficients))
    print("Intercept: " + str(model_train.stages[-1].intercept))