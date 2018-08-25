import sys
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import pandas as pd
from cassandra.cluster import Cluster
from dash.dependencies import Input, Output, State, Event

app = dash.Dash('sales-revenue-summary-streaming-app')

app.scripts.config.serve_locally=True

app.layout = html.Div([
    html.H4('DataTable'),
    dt.DataTable(
        rows=[{}], # initialise the rows
        row_selectable=True,
        filterable=True,
        sortable=True,
        selected_row_indices=[],
        id='datatable'
    ),
    dcc.Interval(id='sales-revenue-update', interval=1000, n_intervals=0),
])


def pandas_factory(colnames, rows):
    if rows:
        return pd.DataFrame(rows, columns=colnames)

    return pd.DataFrame([(0, 0)], columns=colnames)


cluster = Cluster(contact_points=['172.31.31.252','172.31.22.160','172.31.26.117','172.31.19.127'])

session = cluster.connect()
session.set_keyspace('adcentraldb')
session.row_factory = pandas_factory
session.default_fetch_size = 10000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.


@app.callback(Output('datatable', 'rows'), [Input('sales-revenue-update', 'n_intervals')])
def gen_revenue_data(interval):
    query = "SELECT * FROM sales_revenue_quota_summary_by_quarter WHERE year = 2018 and quarter = 3"

    rows = session.execute(query)
    df = rows._current_rows
    df['total_revenue'] = int(df['total_revenue']/100000)

    return df.to_dict('records')


if __name__ == '__main__':
    app.run_server(host='%s' %(sys.argv[1]))
