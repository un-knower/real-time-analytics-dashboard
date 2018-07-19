import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State, Event
from plotly.graph_objs import *
import pandas as pd
import os
import datetime as dt
from cassandra.cluster import Cluster
import time

app = dash.Dash('click-streaming-app')
server = app.server

app.layout = html.Div([
    html.Div([
        html.H2("Click Streaming"),
        html.Img(src="https://s3-us-west-1.amazonaws.com/plotly-tutorials/logo/new-branding/dash-logo-by-plotly-stripe-inverted.png"),
    ], className='banner'),
    html.Div([
        html.Div([
            html.H3("Click Count")
        ], className='Title'),
        html.Div([
            dcc.Graph(id='click-count'),
        ], className='click columns count'),
        dcc.Interval(id='click-count-update', interval=1000, n_intervals=0),
    ], className='row click-count-row'),
], style={'padding': '0px 10px 15px 10px',
          'marginLeft': 'auto', 'marginRight': 'auto', "width": "900px",
          'boxShadow': '0px 0px 5px 5px rgba(204,204,204,0.4)'})


def pandas_factory(colnames, rows):
    if rows:
        return pd.DataFrame(rows, columns=colnames)

    return pd.DataFrame([(0, 0)], columns=colnames)

cluster = Cluster(contact_points=['ec2-54-85-62-208.compute-1.amazonaws.com'])

session = cluster.connect()
session.set_keyspace('clickstream')
session.row_factory = pandas_factory
session.default_fetch_size = 10000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.


@app.callback(Output('click-count', 'figure'), [Input('click-count-update', 'n_intervals')])
def gen_wind_speed(interval):

    ts = time.time()
    st = dt.datetime.fromtimestamp(ts-259).strftime('%Y-%m-%d %H:%M:%S')
    et = dt.datetime.fromtimestamp(ts-59).strftime('%Y-%m-%d %H:%M:%S')

    query = """SELECT click_time, click_count FROM click_count_by_interval
           WHERE click_time > '{}' AND click_time < '{}'
           ALLOW FILTERING""".format(st, et)

    rows = session.execute(query)
    tmpDf = rows._current_rows
    df = tmpDf.sort('click_time')

    trace = Scatter(
        y=df['click_count'],
        line=Line(
            color='#42C4F7'
        ),
        hoverinfo='skip',
        mode='lines'
    )

    layout = Layout(
        height=450,
        xaxis=dict(
            range=[0, 200],
            showgrid=False,
            showline=False,
            zeroline=False,
            fixedrange=True,
            tickvals=[0, 50, 100, 150, 200],
            ticktext=['200', '150', '100', '50', '0'],
            title='Time Elapsed (sec)'
        ),
        yaxis=dict(
            range=[min(0, min(df['click_count'])),
                   max(45, max(df['click_count']))],
            showline=False,
            fixedrange=True,
            zeroline=False,
            nticks=max(6, round(df['click_count'].iloc[-1]/10))
        ),
        margin=Margin(
            t=45,
            l=50,
            r=50
        )
    )

    return Figure(data=[trace], layout=layout)


external_css = ["https://cdnjs.cloudflare.com/ajax/libs/skeleton/2.0.4/skeleton.min.css",
                "https://cdn.rawgit.com/plotly/dash-app-stylesheets/737dc4ab11f7a1a8d6b5645d26f69133d97062ae/dash-wind-streaming.css",
                "https://fonts.googleapis.com/css?family=Raleway:400,400i,700,700i",
                "https://fonts.googleapis.com/css?family=Product+Sans:400,400i,700,700i"]


for css in external_css:
    app.css.append_css({"external_url": css})

if 'DYNO' in os.environ:
    app.scripts.append_script({
        'external_url': 'https://cdn.rawgit.com/chriddyp/ca0d8f02a1659981a0ea7f013a378bbd/raw/e79f3f789517deec58f41251f7dbb6bee72c44ab/plotly_ga.js'
    })

if __name__ == '__main__':
    app.run_server()
