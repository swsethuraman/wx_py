from wx.models import wx_models
from wx.providers.market_data.WX1_provider import Wx1Provider
import pandas as pd

vanilla_models = {
    'VanillaBurn': wx_models.VanillaBurnModel,
    'VanillaSingleLeg': wx_models.VanillaSingleLegModel
}

class VanillaPricer:
    def __init__(self, vanilla, pricer):
        self.data = pd.DataFrame()
        self.vanilla = vanilla
        self.pricer = pricer
        self.provider = Wx1Provider()

        temp = []
        for leg in vanilla.legs:
            for sub_index in leg.vanilla_index:
                temp.append(sub_index.risk_start)
                self.season_start = min(temp)
                self.get_data(sub_index=sub_index)

        self.model = vanilla_models[self.pricer](vanilla, data=self.data, season_start=self.season_start).get_results()

    def get_data(self, sub_index=None):
        df = pd.DataFrame()
        data_columns = ['RecordDate', 'TMax', 'TMin']
        check_name = ''.join([sub_index.location, '_', sub_index.underlying])
        if check_name not in self.data.columns:
            wban = sub_index.location.split('_')[-1]
            constraints = {
                'WBAN': wban
            }
            df = self.provider.get_wx_daily(constraints, data_columns)

            df.rename(columns={
                'TMin': ''.join([sub_index.location, '_', 'TMin']),
                'TMax': ''.join([sub_index.location, '_', 'TMax']),
                'TAvg': ''.join([sub_index.location, '_', 'TAvg']),
            }, inplace=True)

        if self.data.empty:
            self.data = df
        else:
            if not df.empty:
                self.data = pd.merge(self.data, df, on=['Date', 'Year', 'Month', 'Year-Month'])
        if sub_index.name not in self.data.columns:
            underlying_data = ''.join([sub_index.location, '_', sub_index.underlying])
            self.data[sub_index.name] = sub_index.transform(self.data[underlying_data])

