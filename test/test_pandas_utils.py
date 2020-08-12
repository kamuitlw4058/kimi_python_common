import pandas as pd
from zmc_common.utils.pd_utils import get_col_duplicated


rows = [
    {'a':1,'b':'123'},
     {'a':1,'b':'123'},
    {'a':2,'b':'233'},
    {'a':2,'b':'23323'},
]
df = pd.DataFrame(rows)


dump_df = get_col_duplicated(df,'b')
print(dump_df)