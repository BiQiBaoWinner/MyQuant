import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

class DataFetcher:
    def __init__(self, start_date=None, end_date=None):
        self.start_date = pd.to_datetime(start_date, format='%Y-%m-%d') if start_date else None
        self.end_date = pd.to_datetime(end_date, format='%Y-%m-%d') if end_date else None

    def _login_baostock(self):
        # 登录baostock
        lg = bs.login()
        if lg.error_code != '0':
            print(f"登录失败: {lg.error_msg}")
            return False
        return True
    
    def _logout_baostock(self):
        bs.logout()

    def fetch_trading_dates(self):
        """
        获取指定日期范围内的交易日列表
        :param start_date: 起始日期，格式YYYY-MM-DD
        :param end_date: 结束日期，格式YYYY-MM-DD
        :return: 交易日列表
        """
        if self.end_date is None:
            self.end_date = datetime.now().strftime('%Y-%m-%d')
            print(f"End date not provided, using current date: {self.end_date}")

        trading_dates = bs.query_trade_dates(start_date=self.start_date, end_date=self.end_date)
        trading_dates_list = []
        
        while (trading_dates.error_code == '0') & trading_dates.next():
            # 获取一条记录，将记录合并在一起
            trading_dates_list.append(trading_dates.get_row_data())
        dates = pd.DataFrame(trading_dates_list, columns=trading_dates.fields)
        trading_dates = dates[dates['is_trading_day'] == '1']['calendar_date'].tolist()
        trading_dates = pd.to_datetime(trading_dates)
        
        return trading_dates

    def fetch_index_components(self, index_code):
        """
        获取指定指数的成分股列表
        :param index_code: 指数代码，如"csi300"（沪深300）, "csi500"（中证500）
        :return: 成分股列表
        """
        if index_code not in ['csi300', 'csi500']:
            raise ValueError("Unsupported index code. Supported codes are: 'csi300', 'csi500'.")
        
        if index_code == 'csi300':
            rs = bs.query_hs300_stocks()
        elif index_code == 'csi500':
            rs = bs.query_zz500_stocks()
            
        components = []
        while (rs.error_code == '0') & rs.next():
            components.append(rs.get_row_data()[1:])  # 获取股票代码和股票名称
        
        components = pd.DataFrame(components, columns=['code', 'name'])
        
        return components
    
    def fetch_main_board_codes(self, contain_300=False, contain_688=False):
        
        rs = bs.query_stock_basic()
        stock_list = rs.get_data()
        # 过滤沪深主板股票 (type == '1' 表示沪深主板)
        main_board = stock_list[stock_list['type'] == '1']
        
        # 剔除已退市的
        main_board = main_board[main_board['outDate'] <= self.end_date.strftime('%Y-%m-%d')].reset_index(drop=True)
        
        # 剔除ST
        main_board = main_board[~main_board['code_name'].str.contains('ST')].reset_index(drop=True)
        
        # 剔除上市不足60日的
        main_board = main_board[main_board['ipoDate'] < (self.start_date- timedelta(days=60)).strftime('%Y-%m-%d')].reset_index(drop=True)
        
        if not contain_300:
            main_board = main_board[~main_board['code'].str.startswith('sz.3')].reset_index(drop=True)
        
        if not contain_688:
            main_board = main_board[~main_board['code'].str.startswith('sh.688')].reset_index(drop=True)
        
        # 选择code列和name列
        main_board = main_board[['code', 'code_name']]
        main_board.rename(columns={'code_name': 'name'}, inplace=True)
        
        return main_board
    
    def fetch_daily_data(self, code):
        """
        获取指定股票的日线数据
        :param code: 股票代码
        :return: 日线数据DataFrame
        """

        trading_dates = self.fetch_trading_dates()

        rs = bs.query_history_k_data_plus(code, "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST", 
                                            start_date=self.start_date.strftime('%Y-%m-%d'), end_date=self.end_date.strftime('%Y-%m-%d'), frequency="d", adjustflag="2")
        daily_df = rs.get_data()
        if not daily_df.empty:
            daily_df['date'] = pd.to_datetime(daily_df['date'], format='%Y-%m-%d')
            daily_df.set_index('date', inplace=True)
            # 过滤掉不在交易日内的数据
            daily_df = daily_df[daily_df.index.isin(trading_dates)]
        else:
            return None
        
        daily_df['code'] = daily_df['code'].astype(str)
        daily_df[['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']] = daily_df[['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']].apply(pd.to_numeric, errors='coerce')
        daily_df['pctChg'] = daily_df['pctChg'] / 100.0  # 将百分比转换为小数
        daily_df[['adjustflag', 'tradestatus', 'isST']] = daily_df[['adjustflag', 'tradestatus', 'isST']].astype(int)
        
        return daily_df
    
    def fetch_minute_data(self, code, date):
        """
        获取指定股票的分钟数据，默认最近一个交易日
        :param codes: 股票代码列表
        :param date: 指定日期，格式YYYY-MM-DD，默认今天
        :return: dict {code: df}
        """
        if date is None:
            raise ValueError("Date must be provided in format YYYY-MM-DD.")

        rs = bs.query_history_k_data_plus(code, "date,time,code,open,high,low,close,volume", 
                                            start_date=date, end_date=date, frequency="5", adjustflag="2")
        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        M5_df = pd.DataFrame(data_list, columns=rs.fields)
        
        if not M5_df.empty:
            M5_df['time'] = pd.to_datetime(M5_df['time'], format='%Y%m%d%H%M%S000')
            M5_df.set_index('time', inplace=True)
        
        M5_df['code'] = M5_df['code'].astype(str)
        M5_df[['open', 'high', 'low', 'close', 'volume']] = M5_df[['open', 'high', 'low', 'close', 'volume']].apply(pd.to_numeric, errors='coerce')
        
        return M5_df

def _get_long_data(
    start_date: str,
    end_date: str,
    universe: str='csi500',
    D_or_M: str='D'
    ):
    
    DF = DataFetcher(start_date, end_date)
    DF._login_baostock()

    if universe == 'all':
        codes = DF.fetch_main_board_codes()['code'].tolist()
    elif universe == 'csi300' or 'csi500':
        codes = DF.fetch_index_components(universe)['code'].tolist()
    
    if D_or_M == 'D':
        
        def get_single_code(code):
            single_daily_df = DF.fetch_daily_data(code)
            return single_daily_df
        
        # with ThreadPoolExecutor(max_workers=16) as pool:
        #     results = list( pool.map(get_single_code, codes) )
        
        results = []
        for code in tqdm(codes, desc="Processing codes"):
            result = get_single_code(code)
            results.append(result)

        long_data = pd.concat(results, axis=0)
        
        DF._logout_baostock()
        
        return long_data
    
    elif D_or_M == 'M':
        
        def get_single_code_date(code, date):
            single_daily_df = DF.fetch_minute_data(code, date)
            return single_daily_df
        
        trading_dates = DF.fetch_trading_dates().strftime('%Y-%m-%d').tolist()
        results = []
        for code in tqdm(codes, desc='Calc for Code'):
            for date in tqdm(trading_dates, desc='Iteration of dates', leave=False):
                result = get_single_code_date(code, date)
                results.append(result)

        long_data = pd.concat(results, axis=0)
        
        DF._logout_baostock()
        
        return long_data
    
if __name__ == "__main__":
    
    start_date = '2025-01-05'
    end_date = '2025-01-08'
    universe = 'csi300'
    data_type = 'M'
    
    daily_long_data = _get_long_data(start_date, end_date, universe, data_type)
    print(daily_long_data)
    
    daily_long_data.to_parquet(f"/mnt/data_server/home/stu_zyb/MyQuant/download_data/{universe}_{start_date}_{end_date}_{data_type}.parquet")