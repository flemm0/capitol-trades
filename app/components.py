import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from utils import query_athena



def display_ten_rows():
    query = """
    SELECT *
    FROM "capitol-trades"."trades_cleaned" 
    LIMIT 10
    """
    df = query_athena(query)
    return df


def trades_by_state() -> go.Figure:
    query = """
    SELECT 
        state, 
        COUNT(*) AS total_trade_count,
        COUNT(DISTINCT politician) AS politician_count,
        COUNT(*) / COUNT(DISTINCT politician) AS trade_count_scaled
    FROM "capitol-trades"."trades_cleaned" 
    GROUP BY 1
    """
    df = query_athena(query)
    scale = st.toggle(label="Scale by State's No. of Representatives", value=True)
    fig = go.Figure(data=go.Choropleth(
        locations=df.get_column('state'), # Spatial coordinates
        z = df.get_column('trade_count_scaled') if scale else df.get_column('total_trade_count'), # Data to be color-coded
        locationmode = 'USA-states', # set of locations match entries in `locations`
        colorscale = 'GnBu',
        marker_line_color='grey',
        autocolorscale=False,
        colorbar_title = f"Trade Transactions{'<br><sup>(per representative)</sup>' if scale else ''}",
    ))
    fig.update_layout(
        title_text = f'Politician Trade Transactions by State of Representation{'<br><sup>(Scaled by Number of Representatives)</sup>' if scale else  ''}',
        geo = dict(
            scope='usa',
            projection=go.layout.geo.Projection(type = 'albers usa'),
            showlakes=True, # lakes
            lakecolor='rgb(255, 255, 255)',
            bgcolor='rgba(0,0,0,0)'
        ),
        height=500
    )
    return fig


def trades_by_chamber() -> go.Figure:
    query = """
    SELECT
        chamber,
        COUNT(*) AS total_trade_count,
        COUNT(*) / COUNT(DISTINCT politician) AS scaled_trade_count
    FROM "capitol-trades"."trades_cleaned"
    GROUP BY 1
    """
    df = query_athena(query)
    scale = st.toggle(label="Scale by Chamber's No. of Representatives", value=True)
    fig = go.Figure(data=[go.Pie(
        labels=df.get_column('chamber'), 
        values=df.get_column(f'{'scaled' if scale else 'total'}_trade_count')
        )])
    fig.update_layout(height=250)
    return fig


def trades_by_party() -> go.Figure:
    query = """
    SELECT
        party,
        COUNT(*) AS total_trade_count,
        COUNT(*) / COUNT(DISTINCT politician) AS scaled_trade_count
    FROM "capitol-trades"."trades_cleaned"
    GROUP BY 1
    """
    df = query_athena(query)
    scale = st.toggle(label="Scale by Party's No. of Representatives", value=True)
    fig = go.Figure(data=[go.Pie(
        labels=df.get_column('party'), 
        values=df.get_column(f'{'scaled' if scale else 'total'}_trade_count')
        )])
    fig.update_layout(height=250)
    return fig


def trades_by_chamber_and_party() -> go.Figure:
    chamber_query = """
    SELECT
        chamber,
        COUNT(*) AS total_trade_count,
        COUNT(*) / COUNT(DISTINCT politician) AS scaled_trade_count
    FROM "capitol-trades"."trades_cleaned"
    GROUP BY 1
    """
    party_query = """
    SELECT
        party,
        COUNT(*) AS total_trade_count,
        COUNT(*) / COUNT(DISTINCT politician) AS scaled_trade_count
    FROM "capitol-trades"."trades_cleaned"
    GROUP BY 1
    """
    chamber_df, party_df = query_athena(chamber_query), query_athena(party_query)
    scale = st.toggle(label="Scale by No. of Representatives", value=True)
    fig = make_subplots(rows=2, cols=1, specs=[[{'type':'domain'}], [{'type':'domain'}]])
    fig.add_trace(go.Pie(
            labels=chamber_df.get_column('chamber'),
            values=chamber_df.get_column(f'{'scaled' if scale else 'total'}_trade_count'),
            name=f"Trades by Chamber{'<br><sup>(Scaled by Number of Chamber Members)</sup>' if scale else ''}"), 
        1, 1)
    fig.add_trace(go.Pie(
            labels=party_df.get_column('party'),
            values=party_df.get_column(f'{'scaled' if scale else 'total'}_trade_count'),
            name=f"Trades by Party{'<br><sup>(Scaled by Number of Party Members)</sup>' if scale else ''}"), 
        2, 1)
    fig.update_traces(hole=.4, hoverinfo="label+percent+name")
    fig.update_layout(
        title=f'Trades by Chamber (House or Senate) and Party{'<br><sup>(Scaled by Number of Members)</sup>' if scale else ''}',
        height=500,
        annotations=[dict(text='By Party', x=1, y=0, font_size=20, showarrow=False),
                 dict(text='By Chamber', x=-.1, y=1, font_size=20, showarrow=False)],
        legend=dict(
            xanchor="right",
            yanchor="middle",
            y=.5
        )
    )
    return fig


def buys_and_sells_by_week() -> go.Figure:
    query = """
    SELECT 
        DATE_TRUNC('week', traded_date) AS trade_week_start,
        type,
        COUNT(*) AS "count"
    FROM "capitol-trades"."trades_cleaned"
    WHERE type IN ('buy', 'sell')
    GROUP BY 1, 2
    ORDER BY 1 DESC
    """
    df = query_athena(query)
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df.filter(pl.col('type') == 'buy').get_column('trade_week_start'),
        y=df.filter(pl.col('type') == 'buy').get_column('count'),
        name='Buy Transactions'
    ))
    fig.add_trace(go.Scatter(
        x=df.filter(pl.col('type') == 'sell').get_column('trade_week_start'),
        y=df.filter(pl.col('type') == 'sell').get_column('count'),
        name='Sell Transactions',
        line=dict(color='#36cf09')
    ))
    fig.update_layout(title='Weekly Buy and Sell Transactions Trendline',
                    xaxis_title='Date',
                    yaxis_title='Trade Transaction Count',
                    height=500)
    return fig


def trades_by_owner() -> go.Figure:
    query = """
    SELECT 
        owner, 
        COUNT(*) AS "count"
    FROM "capitol-trades"."trades_cleaned" 
    GROUP BY 1
    """
    df = query_athena(query)
    fig = go.Figure([go.Bar(
        x=df.get_column('owner'), 
        y=df.get_column('count')
    )])
    fig.update_layout(height=500, title='Trade Transaction Owner')
    return fig


def buy_and_sell_transactions_for_issuer() -> go.Figure:
    tickers = list(query_athena("""
    SELECT
        DISTINCT
        issuer_ticker AS ticker
    FROM "capitol-trades"."trades_cleaned";
    """).get_column('ticker').sort())
    ticker = st.selectbox(
        label='Select ticker to view trade activity for',
        options=tickers,
        index=tickers.index('AAPL:US')
    )
    query = f"""
    SELECT
        traded_date,
        "type",
        COUNT(*) AS "count"
    FROM "capitol-trades"."trades_cleaned"
    WHERE issuer_ticker = '{ticker}'
    GROUP BY 1, 2
    ORDER BY 1 DESC
    """
    df = query_athena(query)
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=df.filter(pl.col('type') == 'buy').get_column('traded_date'),
        y=df.filter(pl.col('type') == 'buy').get_column('count'),
        name='Buy Transactions',
        marker_color='#239e44'
    ))
    fig.add_trace(go.Histogram(
        x=df.filter(pl.col('type') == 'sell').get_column('traded_date'),
        y=df.filter(pl.col('type') == 'sell').get_column('count'),
        name='Sell Transactions',
        marker_color='#ed1818'
    ))
    fig.update_layout(title=f'Buy and Sell Transactions for {ticker}',
                    xaxis_title='Date',
                    yaxis_title='Trade Transaction Count',
                    height=500)
    return fig


def top_twenty_issuers() -> go.Figure:
    query = """
    SELECT
        issuer_name,
        type,
        COUNT(*) AS "count"
    FROM "capitol-trades"."trades_cleaned"
    WHERE issuer_name IN (
        SELECT
            issuer_name
        FROM (
            SELECT
                issuer_name,
                COUNT(*)
            FROM "capitol-trades"."trades_cleaned"
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 20
        )
    )
    GROUP BY 1, 2
    ORDER BY 1
    """
    df = query_athena(query)
    fig = px.scatter(
        data_frame=df,
        x='count',
        y='issuer_name',
        color='type',
        labels={
            "count": "Total Recorded Trade Transactions",
            "issuer_name": "Issuer"
        },
        color_discrete_sequence=px.colors.qualitative.G10
    )
    fig.update_traces(marker_size=10)
    fig.update_layout(height=600, title='Trade Activity for Top 20 Issuers')
    return fig
