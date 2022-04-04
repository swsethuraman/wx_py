from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import pandas as pd

class GenericWindowsDBProvider:
    def __init__(self):
        if not hasattr(self, "conn_dict"):
            self.conn_dict = None
        if not hasattr(self, "conn"):
            self.conn = None
        self.connect_to_db()
        self.sql_select = "SELECT {2} FROM {0} WHERE {1}"
        pass

    def connect_to_db(self):
        url = URL(**self.conn_dict)
        self.conn = create_engine(url)
        pass

    def get_conn(self):
        return self.conn

    def run_query_df(self, query_sql=None, table=None, constraints=None, columns=None):
        if query_sql:
            df = pd.read_sql(query_sql, con=self.conn)
        else:
            final_constraints = ""
            for cons in constraints:
                val__ = self.quote(constraints[cons])
                final_constraints = final_constraints + cons + ' = ' + val__
            columns = ", ".join(columns)
            query_sql = self.sql_select.format(table, final_constraints, columns)
            df = pd.read_sql(query_sql, con=self.conn)
        return df

    def insert_to_db(self, df, table, db=None, columns=None):
        df.to_sql(str(table), con=self.conn, if_exists='append', index=False)

    def quote(self, string):
        return "'{}'".format(string)

