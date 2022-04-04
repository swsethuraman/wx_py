import flask
from flask_script import Manager, Command

from wx.batch.APX_HOURLY_EOD import APX_Hourly_Command
from wx.batch.BBG_SECMASTER_EOD import BBG_SECMASTER_Command
from wx.batch.BOC_EXCHANGE_RATES_EOD import BOC_EXCHANGE_RATES_Command
from wx.batch.CME_EOD import CME_Command
from wx.batch.CME_STLALT_EOD import CME_STLALT_Command
from wx.batch.CWG_CITY_FCST_EOD import CWG_City_Fcst_Command
from wx.batch.CWG_CITY_OBS_C_EOD import CWG_City_Obs_C_Command
from wx.batch.CWG_CITY_OBS_F_EOD import CWG_City_Obs_F_Command
from wx.batch.CWG_FCST_NA_EOD import CWG_FCST_NA_Command
from wx.batch.CWG_FCST_EU_EOD import CWG_FCST_EU_Command
from wx.batch.CWG_FCST_AP_EOD import CWG_FCST_AP_Command
from wx.batch.CWG_OBS_NA_F_EOD import CWG_OBS_NA_F_Command
from wx.batch.CWG_OBS_NA_C_EOD import CWG_OBS_NA_C_Command
from wx.batch.CWG_OBS_EU_EOD import CWG_OBS_EU_Command
from wx.batch.CWG_OBS_AP_EOD import CWG_OBS_AP_Command
from wx.batch.CWG_WDD_NATIONAL_EOD import CWG_WDD_NATIONAL_Command
from wx.batch.CWG_WDD_ISO_EOD import CWG_WDD_ISO_Command
from wx.batch.CWG_WDD_STATE_EOD import CWG_WDD_STATE_Command
from wx.batch.CWG_WDD_9REGION_EOD import CWG_WDD_9REGION_Command
from wx.batch.CWG_WDD_5REGION_EOD import CWG_WDD_5REGION_Command
from wx.batch.CWG_WDD_3REGION_EOD import CWG_WDD_3REGION_Command
from wx.batch.CWG_ELEC_CDD_EOD import CWG_ELEC_CDD_Command
from wx.batch.CWG_ELEC_CDD_NEXT_EOD import CWG_ELEC_CDD_NEXT_Command
from wx.batch.DWD_CLIMATE_EOD import DWD_Climate_Command
from wx.batch.ERCOT_DA_SPP_EOD import ERCOT_DAM_SPP_Command
from wx.batch.ERCOT_RT_SPP_EOD import ERCOT_RT_SPP_Command
from wx.batch.ERCOT_WIND_ACT_EOD import ERCOT_WIND_ACT_Command
from wx.batch.ERCOT_WIND_FCST_EOD import ERCOT_WIND_FCST_Command
from wx.batch.ERCOT_ST_SYS_ADEQ import ERCOT_ST_SYS_ADEQ_Command
from wx.batch.ICE_CLEARED_GAS_EOD import ICE_CLEARED_GAS_Command
from wx.batch.ICE_CLEARED_POWER_EOD import ICE_CLEARED_POWER_Command
from wx.batch.ICE_CLEARED_POWER_OPTIONS_EOD import ICE_CLEARED_POWER_OPTIONS_Command
from wx.batch.ISONE_DA_LMP_EOD import ISONE_DA_LMP_Command
from wx.batch.ISONE_RT_LMP_EOD import ISONE_RT_LMP_Command
from wx.batch.ISONE_RT_LMP_Prelim_EOD import ISONE_RT_LMP_Prelim_Command
from wx.batch.JMA_HOURLY_EOD import JMA_HOURLY_Command
from wx.batch.KNMI_KLIMATOLOGIE_EOD import KNMI_Klimatologie_Command
from wx.batch.METEOFRANCE_EOD import MeteoFrance_Command
from wx.batch.NOAA_GHCND_EOD import NOAA_GHCND_Command
from wx.batch.NOAA_GHCND_MS_EOD import NOAA_GHCND_MS_Command
from wx.batch.NOAA_GHCND_MS_EOW import NOAA_GHCND_MS_Hist_Command
from wx.batch.NOAA_MADIS_HOURLY_EOD import NOAA_MADIS_HOURLY_Command
from wx.batch.NOAA_MADIS2DAILY_EOD import NOAA_MADIS2Daily_Command
from wx.batch.NYISO_DA_LBMP_EOD import NYISO_DA_LBMP_Command
from wx.batch.NYISO_RT_LBMP_EOD import NYISO_RT_LBMP_Command
from wx.batch.PLATTS_GD_EOD import PLATTS_GD_Command
from wx.batch.PJM_DA_LMP_EOD import PJM_DA_LMP_Command
from wx.batch.PJM_RT_LMP_EOD import PJM_RT_LMP_Command
from wx.batch.PJM_RT_LMP_Prelim_EOD import PJM_RT_LMP_Prelim_Command
from wx.batch.PRICING_JSON_LOAD_EOD import PRICING_JSON_LOAD_Command
from wx.batch.WATERFALL_EOD import Waterfall_Command
from wx.batch.ANOMALY_EOD import Anomaly_Command
from wx.batch.RECALIBRATION_EOD import Recalibration_Command


from wx.batch.MTM_CSV_UPDATE import MTM_CSV_UPDATE_Command
from wx.batch.MTM_EXCHANGE_EOD import MTM_EXCHANGE_EOD_Command
from wx.batch.MTM_OTC_EOD import MTM_OTC_Command
from wx.batch.MTM_REPORT_EOD import MTM_REPORT_Command
from wx.batch.CREDIT_REPORT_EOD import CREDIT_REPORT_Command
from wx.batch.FORECAST_BIAS_EOD import FORECAST_BIAS_Command
from wx.batch.ORIGINATION_REPORT_EOD import ORIGINATION_REPORT_Command
from wx.batch.VANILLA_MARK_EOD import VANILLA_MARK_Command


app = flask.Flask(__name__)
manager = Manager(app)

env='Test'
# EOD Market Data Commands
manager.add_command("save_apx_hourly", APX_Hourly_Command)
manager.add_command("save_bbg_secmaster", BBG_SECMASTER_Command)
manager.add_command("save_boc_fx_rates", BOC_EXCHANGE_RATES_Command)
manager.add_command("save_cme", CME_Command)
manager.add_command("save_cme_stl_alt", CME_STLALT_Command)
manager.add_command("save_cwg_city_fcst", CWG_City_Fcst_Command)
manager.add_command("save_cwg_cit_obs_c", CWG_City_Obs_C_Command)
manager.add_command("save_cwg_city_obs_f", CWG_City_Obs_F_Command)
manager.add_command("save_cwg_fcst_na", CWG_FCST_NA_Command)
manager.add_command("save_cwg_fcst_eu", CWG_FCST_EU_Command)
manager.add_command("save_cwg_fcst_ap", CWG_FCST_AP_Command)
manager.add_command("save_cwg_obs_na_f", CWG_OBS_NA_F_Command)
manager.add_command("save_cwg_obs_na_c", CWG_OBS_NA_C_Command)
manager.add_command("save_cwg_obs_eu", CWG_OBS_EU_Command)
manager.add_command("save_cwg_obs_ap", CWG_OBS_AP_Command)
manager.add_command("save_cwg_wdd_national", CWG_WDD_NATIONAL_Command)
manager.add_command("save_cwg_wdd_iso", CWG_WDD_ISO_Command)
manager.add_command("save_cwg_wdd_state", CWG_WDD_STATE_Command)
manager.add_command("save_cwg_wdd_9region", CWG_WDD_9REGION_Command)
manager.add_command("save_cwg_wdd_5region", CWG_WDD_5REGION_Command)
manager.add_command("save_cwg_wdd_3region", CWG_WDD_3REGION_Command)
manager.add_command("save_cwg_elec_cdd", CWG_ELEC_CDD_Command)
manager.add_command("save_cwg_elec_cdd_next", CWG_ELEC_CDD_NEXT_Command)
manager.add_command("save_dwd_climate", DWD_Climate_Command)
manager.add_command("save_ercot_dam_spp", ERCOT_DAM_SPP_Command)
manager.add_command("save_ercot_rt_spp", ERCOT_RT_SPP_Command)
manager.add_command("save_ercot_wind_act", ERCOT_WIND_ACT_Command)
manager.add_command("save_ercot_wind_fcst", ERCOT_WIND_FCST_Command)
manager.add_command("save_ercot_st_sys_adeq", ERCOT_ST_SYS_ADEQ_Command)
manager.add_command("save_ice_cleared_gas", ICE_CLEARED_GAS_Command)
manager.add_command("save_ice_cleared_power", ICE_CLEARED_POWER_Command)
manager.add_command("save_ice_cleared_power_options", ICE_CLEARED_POWER_OPTIONS_Command)
manager.add_command("save_isone_da_lmp", ISONE_DA_LMP_Command)
manager.add_command("save_isone_rt_lmp", ISONE_RT_LMP_Command)
manager.add_command("save_isone_rt_prelim_lmp", ISONE_RT_LMP_Prelim_Command)
manager.add_command("save_jma_hourly", JMA_HOURLY_Command)
manager.add_command("save_knmi_klimatologie", KNMI_Klimatologie_Command)
manager.add_command("save_meteofrance", MeteoFrance_Command)
manager.add_command("save_noaa_ghcnd", NOAA_GHCND_Command)
manager.add_command("save_noaa_ghcnd_ms", NOAA_GHCND_MS_Command)
manager.add_command("save_noaa_ghcnd_ms_hist", NOAA_GHCND_MS_Hist_Command)
manager.add_command("save_noaa_madis_hourly", NOAA_MADIS_HOURLY_Command)
manager.add_command("save_nyiso_da_lbmp", NYISO_DA_LBMP_Command)
manager.add_command("save_nyiso_rt_lbmp", NYISO_RT_LBMP_Command)
manager.add_command("save_platts_gd", PLATTS_GD_Command)
manager.add_command("save_pjm_da_lmp", PJM_DA_LMP_Command)
manager.add_command("save_pjm_rt_lmp", PJM_RT_LMP_Command)
manager.add_command("save_pjm_rt_prelim_lmp", PJM_RT_LMP_Prelim_Command)

# EOD REPORT Commands
manager.add_command("send_credit_report", CREDIT_REPORT_Command)
manager.add_command("send_mtm_report", MTM_REPORT_Command)
manager.add_command("send_forecast_bias_report", FORECAST_BIAS_Command)
manager.add_command("send_origination_report", ORIGINATION_REPORT_Command)
manager.add_command("send_vanilla_mark", VANILLA_MARK_Command)

# EOD MTM Commands
manager.add_command("update_mtm_csv", MTM_CSV_UPDATE_Command)
manager.add_command("save_exchange_mtm", MTM_EXCHANGE_EOD_Command)
manager.add_command("save_mtm_otc", MTM_OTC_Command)

# OTHER Commands
manager.add_command("load_pricing_jsons", PRICING_JSON_LOAD_Command)
manager.add_command("update_madis2daily", NOAA_MADIS2Daily_Command)
manager.add_command("update_waterfall", Waterfall_Command)
manager.add_command("anomaly_report", Anomaly_Command)
manager.add_command("update_recalibrations", Recalibration_Command)


# Dev Commands

if __name__ == '__main__':
    manager.run()