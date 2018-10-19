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

app.scripts.config.serve_locally = True

app.layout = html.Div([
    html.H2('Real Time Revenue Dashboard'),

    html.Div([
        dcc.Graph(id='qtd-revenue'),
        dcc.Interval(id='sales-revenue-update', interval=1000, n_intervals=0),
    ]),

    html.Div([
        dcc.Graph(id='qtd-revenue-composition'),
        dcc.Interval(id='revenue-composition-update', interval=1000, n_intervals=0),
    ]),

    html.Div([
        html.H4("Random Sales Representative's Revenue"),
        dt.DataTable(
            columns=['year', 'quarter', 'user_id', 'quota', 'total_revenue', '% Met'],
            rows=[{}],  # initialise the rows
            row_selectable=True,
            filterable=True,
            sortable=True,
            selected_row_indices=[],
            id='datatable'
        ),
        dcc.Interval(id='sales-rep-revenue-update', interval=3600000, n_intervals=0),
    ]),

    html.H4(
        'Comment: The data shown above is calculated by performing ETL on real time streams of tblADCaccounts_salesrep_commissions, tblADCadvertiser_rep_revenues, tblCRMgeneric_product_credit from adcentraldb and tbladvertiser, tblADScurrency_rates from adsystemdb to produce adcentraldb.tblADCsummary_sales_dashboard.'),

])


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


cluster = Cluster(contact_points=['172.31.10.148', '172.31.15.185', '172.31.15.43', '172.31.4.148'])
# cluster = Cluster(contact_points=['34.230.53.208', '52.90.234.117', '52.90.234.213', '107.23.201.226'])

session = cluster.connect()
session.set_keyspace('adcentraldb')
session.row_factory = pandas_factory
session.default_fetch_size = 10000000  # needed for large queries, otherwise driver will do pagination. Default is 50000.


@app.callback(Output('qtd-revenue', 'figure'), [Input('sales-revenue-update', 'n_intervals')])
def gen_revenue_data(interval):
    query = "SELECT SUM(total_revenue) AS total_revenue, SUM(quota) AS quota FROM sales_revenue_quota_summary_by_quarter WHERE year = 2018 and quarter = 4"
    rows = session.execute(query)
    df = rows._current_rows
    total_revenue = (df['total_revenue'] / 100000).astype(int)[0]
    quota = (df['quota']).astype(int)[0]

    revenue_met = True if total_revenue > quota else False

    trace0 = go.Bar(
        y=['Quota', 'Total Revenue', 'Revenue Exceeded By' if revenue_met else 'Revenue to be met'],
        x=[quota, total_revenue, total_revenue - quota if revenue_met else quota - total_revenue],
        text=["${:,.0f}".format(quota), "${:,.0f}".format(total_revenue),
              "${:,.0f}".format(total_revenue - quota) if revenue_met else "${:,.0f}".format(quota - total_revenue)],
        textposition='auto',
        orientation='h',
        marker=dict(
            color=['rgb(188, 107, 101)' if revenue_met else 'rgb(101, 130, 188)',
                   'rgb(24, 188, 101)' if revenue_met else 'rgb(201, 116, 119)', 'rgba(58, 71, 80, 0.6)'],
            line=dict(
                color='rgb(8,48,107)',
                width=1.5,
            )
        ),
        opacity=1.0
    )

    data = [trace0]
    layout = go.Layout(
        margin=go.layout.Margin(l=140),
        title='Total Revenue for year 2018 and quarter 4: {}'.format("${:,.0f}".format(total_revenue))
    )

    return go.Figure(data=data, layout=layout)

    # trace1 = go.Bar(
    #     y=['QTD Revenue'],
    #     x=[quota-total_revenue],
    #     name='Quota to be met',
    #     text="${:,.0f}".format(quota-total_revenue),
    #     textposition='auto',
    #     orientation = 'h',
    #     marker = dict(
    #         color = 'rgba(58, 71, 80, 0.6)',
    #         line = dict(
    #             color = 'rgba(226, 118, 118, 1.0)',
    #             width = 3)
    #     )
    # )
    # trace2 = go.Bar(
    #     y=['QTD Revenue'],
    #     x=[total_revenue],
    #     name='Revenue',
    #     text="${:,.0f}".format(total_revenue),
    #     textposition='auto',
    #     orientation = 'h',
    #     marker = dict(
    #         color = 'rgba(226, 118, 118, 0.6)',
    #         line = dict(
    #             color = 'rgba(58, 71, 80, 1.0)',
    #             width = 3)
    #     )
    # )
    #
    # data = [trace2, trace1]
    # layout = go.Layout(
    #     barmode='stack',  title='Total Revenue for year 2018 and quarter 4: {}'.format("${:,.0f}".format(total_revenue))
    # )
    #
    # return go.Figure(data=data, layout=layout)


@app.callback(Output('qtd-revenue-composition', 'figure'), [Input('revenue-composition-update', 'n_intervals')])
def gen_revenue_data(interval):
    query = "SELECT SUM(sales_revenue) AS sales_revenue, SUM(agency_revenue) AS agency_revenue, SUM(strategic_revenue) AS strategic_revenue, SUM(sales_new_revenue) AS sales_new_revenue, SUM(new_parent_revenue) AS new_parent_revenue FROM sales_revenue_summary_by_quarter WHERE year = 2018 and quarter = 4"
    rows = session.execute(query)
    df = rows._current_rows
    sales_revenue = (df['sales_revenue'] / 100000).astype(int)[0]
    agency_revenue = (df['agency_revenue'] / 100000).astype(int)[0]
    strategic_revenue = (df['strategic_revenue'] / 100000).astype(int)[0]
    sales_new_revenue = (df['sales_new_revenue'] / 100000).astype(int)[0]
    new_parent_revenue = (df['new_parent_revenue'] / 100000).astype(int)[0]

    trace0 = go.Bar(
        x=['sales_revenue', 'agency_revenue', 'strategic_revenue', 'sales_new_revenue', 'new_parent_revenue'],
        y=[sales_revenue, agency_revenue, strategic_revenue, sales_new_revenue, new_parent_revenue],
        text=["${:,.0f}".format(sales_revenue), "${:,.0f}".format(agency_revenue), "${:,.0f}".format(strategic_revenue),
              "${:,.0f}".format(sales_new_revenue), "${:,.0f}".format(new_parent_revenue)],
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
        title='Revenue Composition for year 2018 and quarter 4'
    )

    return go.Figure(data=data, layout=layout)


@app.callback(Output('datatable', 'rows'), [Input('sales-rep-revenue-update', 'n_intervals')])
def gen_revenue_data(interval):
    query = "SELECT year, quarter, user_id, quota, total_revenue FROM sales_revenue_quota_summary_by_user_quarter WHERE year = 2018 and quarter = 4  and user_id IN (1001, 1277, 1962, 10889, 4442)"

    rows = session.execute(query)
    df = rows._current_rows
    df['total_revenue'] = (df['total_revenue'] / 100000).astype(int)
    df['% Met'] = ((df['total_revenue'] / df['quota']) * 100).astype(float).round(2)

    return df.to_dict('records')


if __name__ == '__main__':
    app.config['SERVER_NAME'] = 'jaffa:8050'
    app.run_server(host='%s' % (sys.argv[1]),ssl_context='adhoc')

