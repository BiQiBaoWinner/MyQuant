import os 
import sys
import pandas as pd

from concurrent.futures import ThreadPoolExecutor
from fetch_data import DataFetcher, _get_long_data
from Factor_Pool.utils.operators import _long_to_wide_
from Factor_Pool.HF_pv_factor import hf_pv_factor

def Generate_daily_factor(
    start_date: str,
    end_date: str,
    universe: str='csi500',
    f_type: str='D',
    f_name: str='Midpoint_Price_Change'
    ):
    
    data_path = f"./download_data/{universe}_{start_date}_{end_date}_{f_type}.parquet"

    if not os.path.exists(data_path):
        daily_long_data = _get_long_data(start_date, end_date, universe, f_type)
        daily_long_data.to_parquet(f"/mnt/data_server/home/stu_zyb/MyQuant/download_data/{universe}_{start_date}_{end_date}_{f_type}.parquet")
    else:
        daily_long_data = pd.read_parquet(data_path)    
    
    
    def _calc_daily_factor_(data, date):
    
        if 'date' not in data or 'code' not in data:
            raise ValueError('')
        
        daily_df = data[data['date'] == date]
        
        if daily_df.empty:
            raise ValueError(f"No data for date {date}")
        
        MPC_max = hf_pv_factor().factor_pool[f_name](daily_df)
        
        return MPC_max
            
    daily_factors = []

    dates = daily_long_data['date'].unique()
    with ThreadPoolExecutor(max_workers=16) as pool:
        daily_factors = list(pool.map(lambda d: _calc_daily_factor_(daily_long_data, d), dates))
        
    return pd.concat(daily_factors, axis=1)

if __name__=='__main__':
    
    start_date = '2025-01-01'
    end_date = '2025-02-13'
    universe = 'csi300'
    data_type = 'M'
    
    daily_factors = Generate_daily_factor(start_date, end_date, universe, data_type)
    print(daily_factors)
    if not os.path.exists('./MyQuant/factors'):
        os.makedirs('./MyQuant/factors')
    daily_factors.to_parquet(f"./MyQuant/factors/{universe}_{start_date}_{end_date}_{data_type}_Midpoint_Price_Change.parquet")