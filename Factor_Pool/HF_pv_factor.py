import pandas as pd
import numpy as np
from Factor_Pool.utils.operators import _long_to_wide_

def Midpoint_Price_Change_single_day(Minute_df: pd.DataFrame) -> pd.Series:
    
    wide_close = _long_to_wide_(Minute_df, 'close')

    MPC = wide_close / wide_close.shift(1) - 1
    
    MPC_max = MPC.max()
    
    MPC_max.index.name = 'asset_id'
    MPC_max.name = Minute_df['date'].iloc[0]
    
    return MPC_max


class hf_pv_factor():
    
    def __init__(self):
        
        self.factor_pool = {
            'Midpoint_Price_Change': Midpoint_Price_Change_single_day
        }
        
        self.factor_register = {
            'Midpoint_Price_Change': ['date', 'close']
        }