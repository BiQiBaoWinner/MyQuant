import pandas as pd

def _long_to_wide_(long_data: pd.DataFrame, value_col: str) -> pd.Series:
    
    if 'code' not in long_data:
        raise ValueError('')

    wide_data = long_data.pivot(values = value_col, columns = 'code')
    
    return wide_data


# if __name__=='__main__':
    
    