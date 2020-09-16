
import json
import numpy as np
from datetime import datetime

from .logger import getLogger
logger = getLogger(__name__)

def _dup(col,duplicated_list):
    def is_dup(df):
        return df[col] in duplicated_list
    return is_dup
            

def get_col_duplicated(df,col):
    duplicated_list = df[df.duplicated(col)][col].unique()
    is_duplicated = df.apply(_dup(col,duplicated_list),axis=1)
    return df[is_duplicated]


def apply_if_col_not_exists(df,col_name,apply_func,overwrite=False):
    if col_name in df.columns and not overwrite:
        return df
    df[col_name] = df.apply(apply_func,axis=1)
    return df

def simple_group_agg(df,col,label_col='label',agg_funcs=['count','mean'],by='count'):
    if isinstance(col,str):
        col = [col]
    if isinstance(agg_funcs,dict):
        grouped_label = df.groupby(col)
        grouped_df = grouped_label.agg(**agg_funcs)
    else:
        grouped_label = df.groupby(col)[label_col]
        grouped_df = grouped_label.agg(agg_funcs)
    columns = {}
  

    if by is not None:
        ret = grouped_df.sort_values(by=by,ascending=False)
    else:
        ret = grouped_df.sort_index()
    
    for func_name in  agg_funcs:
        if isinstance(col,list):
            col = '_'.join(col)
        columns[func_name] = f'{col}_{label_col}_{func_name}'
    ret = ret.rename(columns=columns)
    return ret 


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)


class PandasStatics():
    def __init__(self,df,table_name=None):
        super().__init__()
        self.df = df 
        self.table_name = table_name
    
    def statics(self):
        df = self.df
        null_count = df.isna().sum().sum()
        rows_count = len(df)
        columns_count = len(df.columns)
        miss_rate = null_count / (rows_count * columns_count )
        if rows_count == 0:
            ret ={
            'rows_count':0, 
            'columns_count':columns_count,
            'miss_rate':0,
        }
        else:
            ret ={
                'rows_count':rows_count, 
                'columns_count':columns_count,
                'miss_rate':miss_rate,
            }
        return ret
    
    def missing_values(self):
        df = self.df
        columns = df.columns
        rows_count = len(df)
        df_na_sum = df.isna().sum()
        ret = {}
        for i in columns:
            miss_count = df_na_sum[i]
            if rows_count == 0:
                col_dict = {
                'miss_count':0,
                'rate': 0,
                'total_count':0
            }
            else:
                col_dict = {
                    'miss_count':miss_count,
                    'rate': miss_count / rows_count,
                    'total_count':rows_count
                }
            ret[i] = col_dict
        return  ret
    def variables_str(self,df):
        distribution = {}
        vc = df.value_counts()
        for i in list(vc.index):
            if not isinstance(i,dict):
                distribution[str(i)]= {
                    'count' : vc[i],
                    'rate' : vc[i] / len(df),
                }
        return {
            'distribution':distribution
        }

    def variables_number(self,df):
        distribution = {}
        vc = df.value_counts()
        for i in list(vc.index):
            distribution[str(i)]= {
                'count' : vc[i],
                'rate' : vc[i] / len(df),
            }
        statics =  dict(df.describe())
        return {
            'distribution':distribution,
            'statics':dict(statics)
        }
    
    def variables_datetime(self,df):
        distribution = {}
        date =  df.dt.date
        vc = date.value_counts()
        for i in list(vc.index):
            if isinstance(i,datetime):
                i = datetime.strftime('%Y-%m-%d %H:%M:%S')
            distribution[str(i)]= {
                'count' : vc[i],
                'rate' : vc[i] / len(df),
            }
        ret = {
            'distribution':distribution,
        }
        return ret

    def variables(self):
        df = self.df
        ret = {}
        cols = df.columns
        for col in cols:
            dtype = str(df[col].dtype)
            col_dict ={
                'dtype':dtype,
            }
            if dtype == "object":
                col_dict['string'] = self.variables_str(df[col])
            if dtype.startswith("float") or dtype.startswith("int"):
                col_dict['number'] = self.variables_number(df[col])
            if dtype.startswith("datetime"):
                col_dict['datetime'] = self.variables_datetime(df[col])
            ret[col] = col_dict
        return ret
        

    def to_json(self):
        df = self.df 
        ret ={
            'statics': self.statics(),
            'missing_values':self.missing_values(),
            'variables':self.variables()
        }
        return json.loads(json.dumps(ret,ensure_ascii=False,indent=4,cls=NpEncoder))



