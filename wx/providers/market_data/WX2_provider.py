from wx.providers.market_data.generic_windows_db_provider import GenericWindowsDBProvider
import pandas as pd

class Wx2Provider(GenericWindowsDBProvider):
    def __init__(self):
        self.conn_dict = {
            'drivername': 'mysql+pymysql',
            'host': 'mdb-wx1.use4.gcp.laurion.corp',
            'database': 'WX2',
            'username': 'ssethuraman',
            'password': 'BTdaWJep,C!BUx-gHJZ#:b}Mp2vUz#?P'
        }
        super().__init__()
        pass