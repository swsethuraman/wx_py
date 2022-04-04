import pandas as pd
import numpy as np
import json
import requests

class GenericApiProvider:
    def __init__(self):
        self.url_base = None
        self.token = None
        pass

    def get_historical_data(self, expressions, start_date=None, end_date=None):
        df_final = pd.DataFrame()
        if start_date is not None:
            start_date = np.datetime64(start_date)
        if end_date is not None:
            end_date = np.datetime64(end_date)
        for expression in expressions:
            for (st, end) in self.chunkify(start_date, end_date):
                payload = self.construct_payload(expression, start_date=st, end_date=end)
                response = self.make_url_request(payload)
                df = self.transform_response_df(response)
                df_final = df_final.append(df)
                print("Successfully processed data for chunk: {0}".format((st, end)))
        return self.post_process(df_final)

    def construct_payload(self, expression, start_date=None, end_date=None):
        payload = expression
        return payload

    def make_url_request(self, payload):
        headers = {
            'token': self.token
        }
        url_data = requests.get(self.url_base, params=payload, headers=headers).content
        url_data = json.loads(url_data)
        return url_data

    def transform_response_df(self, response):
        df = pd.DataFrame(response['results'])
        return df

    def chunkify(self, start_date=None, end_date=None):
        return [(start_date, end_date)]

    def post_process(self, df):
        return df
