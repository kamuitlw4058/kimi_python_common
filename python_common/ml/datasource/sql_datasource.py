from python_common.ml.datasource.base_datasource import DataSource



class SQLDataSource(DataSource):
    def __init__(table, date_col,start_date,end_date,features_cols,clk_col,imp_col):
        self._table= table
        self._data_col = date_col
        self._start_date = start_date
        self._end_date = end_date
        self._features_cols = features_cols
        self._clk_col = clk_col
        self._imp_col = imp_col

    def 

