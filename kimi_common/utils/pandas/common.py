
def  row_one2many(df,col,keep_orig=True):
    keep_cols =  list(df.columns)
    if keep_orig:
        df[f'{col}_copy'] = df[col]
        keep_cols.append(f'{col}_copy')
    keep_cols.remove(col)
    print(keep_cols)
    if keep_cols is not None:
        df = df.set_index(keep_cols)
    df = df[col].str.split(" ", expand=True)
    print(df)
    df = df.stack().reset_index(drop=True, level=-1).reset_index()
    rename_dict = {
        0:col,
    }
    if keep_orig:
        rename_dict[f'{col}_copy']= col
        rename_dict[0]= f'{col}_split'
    else:
        rename_dict[0]= col
    df = df.rename(columns=rename_dict)
    return df
