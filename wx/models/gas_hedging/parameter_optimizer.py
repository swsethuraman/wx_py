###NEED TO THINK OF BEST WAY TO SELECT REASONABLE HEDGE SIZE AND DELTA HEDGE MULTIPLIERS
###INSTEAD OF HEDGING AWAY BOTTOM X% OF ACTUAL LOSSES, WE SHOULD CREATE AN EXPECTED DISTRIBUTION BASED ON RESULTS
###THEN HEDGE AWAY BOTTOM X% OF THAT
##COULD LIKELY HAVE A HUGE IMPROVEMENT IF WE CAN SCALE HEDGE SIZE BY COMMODITY VOL
from wx.models.gas_hedging import gas_hedging_functions as ghf
import pandas as pd
from xlib import xdb
import numpy as np
db = 'WX1-GC'
#stations=[['KORD'],['KBOS'],['KIAH'],['KDFW'],['KCVG'],['KMSP'],['KDCA'],['KEWR'],['KLGA'],['KATL'],['KLAS'],['KLAX'],['KSAC']]
stations=[['KORD']]
deal_type='swap'
fc='11-3'
hedge_type='swap'
min_winter=2014
delta_hedge='Y'
percent_to_remove=20
basis_fixed = 'basis'
for i in stations:
    df_payouts = ghf.get_payout_data(i[0],deal_type,fc,min_winter,hedge_type,delta_hedge)
    df_payouts = ghf.select_optimal_parameters(df_payouts)
    df_payouts = ghf.get_hedge_size(df_payouts,percent_to_remove,basis_fixed)
    if i==stations[0]:
        df_total = df_payouts.copy()
    else:
        df_total = df_total.append(df_payouts)
df_total
