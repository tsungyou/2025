import pandas as pd

class Factorization:

    def get_demean_weighting(factor):
        df1 = factor.dropna(axis='columns', how='all').copy()
        demean = df1.sub(df1.mean(axis=1), axis=0)
        weighting = demean.div(demean.abs().sum(axis=1), axis=0)
        return weighting
    def _reweighting_equal(weighting:pd.DataFrame):
        def equal_weight(row: pd.Series):
            count_larger_than_zero = (row != 0).sum()
            if count_larger_than_zero > 0:
                row = row.apply(lambda x: 1 / count_larger_than_zero if x > 0 else x)
            return row
        return weighting.apply(equal_weight, axis=1)
    
    def intraday_shifted_input_signal_pct_output_plot(ln_, pct_calculating_profit, quantile=10):
            
        time_length = len(ln_)
        quantile_column = [[None]*quantile]*time_length
        quantile_df = pd.DataFrame(quantile_column)
        for da in range(time_length):
            row = ln_.iloc[da, :]
            col = row.sort_values(ascending=False).index
            for i in range(0, quantile):
                quantile_df.iloc[da, i] = list(col[i*int(len(row)/quantile):(i+1)*int(len(row)/quantile)])

        # pct_close_w_corres = pct_calculating_profit[pct_calculating_profit.columns.intersection(ln_.columns)].shift(-2)
        holdings = []
        index = ln_.index
        quantiles = [None] * quantile
        for qt_iter in range(quantile):
            quantile_1 = quantile_df.iloc[:, qt_iter]
            if qt_iter == 0: holdings = quantile_1
            df_1 = ln_.copy()
            for i in range(len(quantile_1)):
                df_1.loc[index[i], ~df_1.columns.isin(quantile_1[i])] = 0
            weighting_1 = Factorization()._reweighting_equal(df_1)
            ret = pct_calculating_profit.loc[weighting_1.index[0]:] * weighting_1
            quantiles[qt_iter] = ret.sum(axis=1).cumsum()
        quantilized_cumsum = pd.DataFrame(quantiles).T
        quantilized_cumsum.iloc[:, :].plot(title="10 quantile cumsum distribution")
        return holdings
    
from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('st/combined_plot.html')  # Replace with your HTML file name

if __name__ == '__main__':
    app.run(debug=True)