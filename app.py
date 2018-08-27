import sys
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import pandas as pd
from cassandra.cluster import Cluster
from dash.dependencies import Input, Output, State, Event
import plotly.graph_objs as go


app = dash.Dash('sales-revenue-summary-streaming-app')

app.scripts.config.serve_locally=True

app.layout = html.Div([
    html.H2('Real Time Revenue Dashboard'),

    html.Div([
        dcc.Graph(id='qtd-revenue'),
        dcc.Interval(id='sales-revenue-update', interval=1000, n_intervals=0),
    ]),

    html.Div([
        dcc.Graph(id='qtd-revenue-composition'),
        dcc.Interval(id='sales-revenue-update', interval=1000, n_intervals=0),
    ]),

    html.H4('Comment: The data shown above is calculated by performing ETL on real time streams of tblADCaccounts_salesrep_commissions, tblADCadvertiser_rep_revenues, tblCRMgeneric_product_credit, tbladvertiser, and tblADScurrency_rates.'),

    # html.Div([
    #     html.H4("Random Sales Representative's Revenue"),
    #     dt.DataTable(
    #         columns=['year', 'quarter', 'user_id', 'quota', 'total_revenue', '% Met'],
    #         rows=[{}], # initialise the rows
    #         row_selectable=True,
    #         filterable=True,
    #         sortable=True,
    #         selected_row_indices=[],
    #         id='datatable'
    #     ),
    #     dcc.Interval(id='sales-revenue-update', interval=1000, n_intervals=0),
    # ]),
])


def pandas_factory(colnames, rows):
    if rows:
        return pd.DataFrame(rows, columns=colnames)

    return pd.DataFrame([(0, 0, 0, 0, 0)], columns=colnames)

cluster = Cluster(contact_points=['172.31.31.252','172.31.22.160','172.31.26.117','172.31.19.127'])
# cluster = Cluster(contact_points=['34.230.53.208', '52.90.234.117', '52.90.234.213', '107.23.201.226'])

session = cluster.connect()
session.set_keyspace('adcentraldb')
session.row_factory = pandas_factory
session.default_fetch_size = 10000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.


# @app.callback(Output('datatable', 'rows'), [Input('sales-revenue-update', 'n_intervals')])
# def gen_revenue_data(interval):
#     query = "SELECT year, quarter, user_id, quota, total_revenue FROM sales_revenue_quota_summary_by_user_quarter WHERE year = 2018 and quarter = 3  and user_id IN (1001, 1277, 1962, 10889, 4442)"
#
#     rows = session.execute(query)
#     df = rows._current_rows
#     df = df.dropna().sort_values('total_revenue', ascending=False)
#     # df = df.head(10)
#     df['total_revenue'] = (df['total_revenue'].replace([pd.np.inf, pd.np.nan], 0)/100000).astype(int)
#     df['% Met'] = ((df['total_revenue']/df['quota'].replace(0, 1))*100).astype(float).round(2)
#
#     return df.to_dict('records')


@app.callback(Output('qtd-revenue', 'figure'), [Input('sales-revenue-update', 'n_intervals')])
def gen_revenue_data(interval):
    query = "SELECT SUM(total_revenue) AS total_revenue, SUM(quota) AS quota FROM sales_revenue_quota_summary_by_quarter WHERE year = 2018 and quarter = 3"
    rows = session.execute(query)
    df = rows._current_rows
    df = df.dropna()
    df['total_revenue'] = (df['total_revenue'].replace([pd.np.inf, pd.np.nan], 0)/100000).astype(int)
    df['quota'] = (df['quota']).astype(int)

    trace1 = go.Bar(
        y=['QTD Revenue'],
        x=df['quota']-df['total_revenue'],
        name='Quota to be met',
        text=(df['quota']-df['total_revenue']).map("${:,.0f}".format),
        textposition='auto',
        orientation = 'h',
        marker = dict(
            color = 'rgba(246, 78, 139, 0.6)',
            line = dict(
                color = 'rgba(246, 78, 139, 1.0)',
                width = 3)
        )
    )
    trace2 = go.Bar(
        y=['QTD Revenue'],
        x=df['total_revenue'],
        name='Revenue',
        text=df['total_revenue'].map("${:,.0f}".format),
        textposition='auto',
        orientation = 'h',
        marker = dict(
            color = 'rgba(58, 71, 80, 0.6)',
            line = dict(
                color = 'rgba(58, 71, 80, 1.0)',
                width = 3)
        )
    )

    data = [trace2, trace1]
    tot_rev = df['total_revenue'].map("${:,.0f}".format)[0]
    layout = go.Layout(
        barmode='stack',  title='Total Revenue for year 2018 and quarter 3: {}'.format(tot_rev)
    )

    return go.Figure(data=data, layout=layout)


@app.callback(Output('qtd-revenue-composition', 'figure'), [Input('sales-revenue-update', 'n_intervals')])
def gen_revenue_data(interval):
    query = "SELECT SUM(sales_revenue) AS sales_revenue, SUM(agency_revenue) AS agency_revenue, SUM(strategic_revenue) AS strategic_revenue, SUM(sales_new_revenue) AS sales_new_revenue FROM sales_revenue_summary_by_quarter WHERE year = 2018 and quarter = 3"
    rows = session.execute(query)
    df = rows._current_rows
    df = df.dropna()
    df['sales_revenue'] = (df['sales_revenue'].replace([pd.np.inf, pd.np.nan], 0)/100000).astype(int)[0]
    df['agency_revenue'] = (df['agency_revenue'].replace([pd.np.inf, pd.np.nan], 0)/100000).astype(int)[0]
    df['strategic_revenue'] = (df['strategic_revenue'].replace([pd.np.inf, pd.np.nan], 0)/100000).astype(int)[0]
    df['sales_new_revenue'] = (df['sales_new_revenue'].replace([pd.np.inf, pd.np.nan], 0)/100000).astype(int)[0]

    sales_revenue = df['sales_revenue'][0]
    agency_revenue = df['agency_revenue'][0]
    strategic_revenue = df['strategic_revenue'][0]
    sales_new_revenue = df['sales_new_revenue'][0]

    trace0 = go.Bar(
        x=['sales_revenue', 'agency_revenue', 'strategic_revenue', 'sales_new_revenue'],
        y=[sales_revenue, agency_revenue, strategic_revenue, sales_new_revenue],
        text=[df['sales_revenue'].map("${:,.0f}".format)[0], df['agency_revenue'].map("${:,.0f}".format)[0], df['strategic_revenue'].map("${:,.0f}".format)[0], df['sales_new_revenue'].map("${:,.0f}".format)[0]],
        textposition='auto',
        marker=dict(
            color='rgb(158,202,225)',
            line=dict(
                color='rgb(8,48,107)',
                width=1.5,
            )
        ),
        opacity=0.6
    )

    data = [trace0]
    layout = go.Layout(
        title='Revenue Composition for year 2018 and quarter 3'
    )

    return go.Figure(data=data, layout=layout)

if __name__ == '__main__':
    app.run_server(host='%s' %(sys.argv[1]))
