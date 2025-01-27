import pandas as pd
import psycopg2
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
class SignalGenerator:
    def __init__(self):
        self.list_hk = ["2219.HK", "1093.HK", "6030.HK", "0291.HK", "9961.HK", "6690.HK", "2313.HK", "1928.HK", "0005.HK", "0384.HK",
         "1113.HK", "0981.HK", "0006.HK", "0011.HK", "0003.HK", "1109.HK", "9633.HK", "2899.HK", "1398.HK", "0939.HK",
         "0027.HK", "0823.HK", "0001.HK", "1088.HK", "0386.HK", "0016.HK", "3968.HK", "0002.HK", "0669.HK", "3988.HK",
         "1171.HK", "9868.HK", "2269.HK", "1810.HK", "0700.HK", "2382.HK", "2386.HK", "2318.HK", "0992.HK", "1024.HK",
         "9618.HK", "0883.HK", "0388.HK", "0175.HK", "0941.HK", "2628.HK", "1211.HK", "2388.HK", "9888.HK", "2020.HK", "9988.HK", "1299.HK"]
        self.today = datetime.now()
    
    def IC_HK_OnOpen(self, ln_, pct_calculating_profit, bm=0) -> list:
        ind_corrcoef = pd.Series()
        ln_cols = list(ln_.columns)
        ln_array = np.array(ln_.iloc[1:, :]).T
        pct_array = np.array(pct_calculating_profit.iloc[1:, :]).T
        for i in range(ln_array.shape[0]):
            valid_mask = ~np.isnan(ln_array[i]) & ~np.isnan(pct_array[i])
            if valid_mask.any():
                corr = np.corrcoef(ln_array[i][valid_mask], pct_array[i][valid_mask])[0, 1]
                ind_corrcoef[ln_cols[i]] = corr
            else:
                ind_corrcoef[ln_cols[i]] = np.nan  
        ind_corrcoef = ind_corrcoef.sort_values(ascending=True)
        mean_corrcoef = np.nanmean(ind_corrcoef)
        if bm == 0:  
            list_tradable = list(ind_corrcoef[ind_corrcoef < -0].index)
        else:
            list_tradable = list(ind_corrcoef[ind_corrcoef < mean_corrcoef].index)
            
        plt.figure(figsize=(12, 6))
        plt.bar(ind_corrcoef.index, ind_corrcoef)
        plt.axhline(mean_corrcoef)
        plt.xticks(rotation=90)
        return sorted(list_tradable)
    def Strategy_HK_onOpen(self) -> list:
        df = yf.download(self.list_hk, start='2022-01-01', rounding=2, progress=False);
        close = df['Close']
        open_ = df['Open']
        ln_ = np.log(open_/close.shift(1))
        pct_calculating_profit = close/open_-1 
        
        list_today = ln_.iloc[-1, :].sort_values(ascending=False)[:5]
        list_tradable = self.IC_HK_OnOpen(ln_, pct_calculating_profit)
        list_trade = [i for i in list(list_today.index) if i in list_tradable]
        list_trade_x_price = close.iloc[-1][list_trade]
        print(list_today)
        print("Strategy List for HK onOpen: ")
        print(list_trade_x_price)
        return list_trade_x_price

    def Strategy_HK_prevClose(self) -> list:
        df = yf.download(self.list_hk, start=self.today - timedelta(365), rounding=2, progress=False);
        close_ = df['Close']
        open_ = df['Open']
        ln_ = np.log(close_/open_) # index: 使用的時間
        pct_calculating_profit = close_/open_-1 # index: 當天收益
        list_today = ln_.iloc[-1, :].sort_values(ascending=False)[:5]
        list_tradable = self.IC_HK_OnOpen(ln_.shift(1), pct_calculating_profit, bm = 0)
        list_trade = [i for i in list(list_today.index) if i in list_tradable]
        list_trade_x_price = close_.iloc[-1][list_trade]
        print(list_today)
        print("Strategy List for HK prevClose: ")
        print(list_trade_x_price)
        return list_trade_x_price