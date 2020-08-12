
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