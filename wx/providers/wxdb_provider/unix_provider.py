try:
    import xlib.xdb as xdb
except Exception:
    pass

import pandas as pd

def WX1_Delete_BulkInsert(db, table, pk_cols, date_col, df):
    sec = df
    #Get list of columns in db table. Remove DateUpdated and UserUpdated as those will be handled separately
    #DateUpdated is auto-set to current timestamp
    #UserUpdated will be null for now
    conn = xdb.make_conn(db)
    table_cols = conn.getColumnNames(table)
    table_cols.remove('DateUpdated')
    table_cols.remove('UserUpdated')
    #Reorder dataframe columns to match db table
    sec = sec[table_cols]
    #Get distinct PK combinations (exluding effective date column)
    ids = sec[pk_cols].drop_duplicates()
    #Custom funtion -- same as conn.delete but with an extra line to support deleting within a date range
    def wx1_delete(table, criteria, field, date, startDate, endDate):
        sqltext = 'DELETE FROM %s WHERE 1=1' % table
        sqltext = conn.sqlAppendWhere(sqltext, criteria)
        sqltext = conn.sqlAppendWhereDate(sqltext, field, date, startDate, endDate)
        #print(sqltext)
        return conn.execute(sqltext)
    #Build wx1_delete parameters and then execute
    #wx1_delete will run in a loop for each distinct PK combination
    #For each loop, a smaller dataframe will be created containing only the data for that PK combination
    for i in range(ids.shape[0]):
        filter_str=''
        delete_dict={}
        #filter_str is used to create smaller datafrome
        #delete_dict is used to create where clause in delete statemend
        for j in range(len(pk_cols)):
            pk_val=ids.iloc[i][pk_cols[j]]
            filter_str += '(' + pk_cols[j] + '==\'' + pk_val + '\') & '
            delete_dict[pk_cols[j]] = pk_val
        filter_str=filter_str[:-3]
        sub_sec=sec.query(filter_str)
        #All data for a given PK combination between min_date and max_date will be deleted from db table
        min_date = min(sub_sec[date_col])
        max_date = max(sub_sec[date_col])
        #Create connection and run function. Very important to always close connection
        conn = xdb.make_conn(db, stay_open=True)
        wx1_delete(table,delete_dict, date_col, None, min_date, max_date)
        conn.commit()
        conn.close()
    #Create connection again and bulk insert entire dataframe. Very important to always close connection
    #Could put this into loop and do inserts by smaller dataframe if desired
    conn = xdb.make_conn(db, stay_open=True)
    conn.bulkInsertDf(sec, table, local=True)
    conn.commit()
    conn.close()

