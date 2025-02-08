from flask import Flask, render_template, request
import pandas as pd
import plotly.graph_objects as go

app = Flask(__name__)

# Load data only once
df = pd.read_csv("us30.csv", sep="\t")
df.columns = ['date', 'time', 'op', 'hi', 'lo', 'cl', "1", "2", "3"]

# Convert date & time to a single datetime column
df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])

# Get unique dates for navigation
unique_dates = sorted(df['date'].unique())  # Sorted list of unique dates
date_idx = {date: i for i, date in enumerate(unique_dates)}  # Mapping for previous/next navigation

@app.route('/', methods=['GET', 'POST'])
def index():
    start_date = request.form.get('start_date', unique_dates[0])  # Default to the first available date

    # Handle previous & next date
    prev_date = unique_dates[max(0, date_idx.get(start_date, 0) - 1)]
    next_date = unique_dates[min(len(unique_dates) - 1, date_idx.get(start_date, 0) + 1)]

    # Filter data
    filtered_df = df[df['date'] == start_date]

    if filtered_df.empty:
        return render_template('index.html', chart_html=None, start_date=start_date, prev_date=prev_date, next_date=next_date, no_data=True)

    # Create candlestick chart using Plotly
    fig = go.Figure(data=[go.Candlestick(
        x=filtered_df['datetime'],
        open=filtered_df['op'],
        high=filtered_df['hi'],
        low=filtered_df['lo'],
        close=filtered_df['cl']
    )])

    fig.update_layout(
        title="Stock Price Visualization",
        xaxis_title="Time",
        yaxis_title="Price",
        xaxis_rangeslider_visible=False
    )

    chart_html = fig.to_html(full_html=False)

    return render_template('index.html', chart_html=chart_html, start_date=start_date, prev_date=prev_date, next_date=next_date, no_data=False)

if __name__ == '__main__':
    app.run(debug=True)
