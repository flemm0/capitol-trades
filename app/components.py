import streamlit as st
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
        colorscale = 'Reds',
        marker_line_color='grey',
        autocolorscale=False,
        colorbar_title = "Trade Transactions",
    ))
    title_text = 'Politician Trade Transactions by State of Representation'
    if scale:
        title_text = f'{title_text}<br><sup>(Scaled by Number of Representatives)</sup>'
    fig.update_layout(
        title_text = title_text,
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


def trades_by_chamber():
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


def trades_by_party():
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


def trades_by_chamber_and_party():
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
        title='Trades by Chamber (House or Senate) and Party',
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


def display_top_thirty_issuers():
    query = """
    SELECT
        issuer_name,
        type,
        COUNT(*) AS count
    FROM "capitol-trades"."trades_cleaned"
    WHERE issuer_name IN (
        SELECT issuer_name
        FROM (
            SELECT issuer_name, COUNT(*)
            FROM "capitol-trades"."trades_cleaned"
            GROUP BY 1
            ORDER BY COUNT(*) DESC
            LIMIT 30
        )
    )
    GROUP BY 1, 2
    """
    df = query_athena(query)
    return df
