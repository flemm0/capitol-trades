import streamlit as st
import polars as pl

from components import *


st.set_page_config(layout="wide")

st.markdown('## This dashboard displays summary reports of data scraped from the [Capitol Trades Website](https://www.capitoltrades.com/).')

st.markdown('Click [here](https://github.com/flemm0/capitol-trades) to see the source code for how the data was retrieved.')

st.write("""
CapitolTrades.com is an insightful and intuitive platform offering access to real-time politician trading data.
Harnessing 2iQ’s 20+ years of expertise in insider transaction data, CapitolTrades.com closes the information gap through a best-in-class data set that helps investors monitor U.S. politician stock market activity. This enriched data is built on an established and proven process combining automated and manual record collection.
CapitolTrades.com leads this sector of investor intelligence with the most amount of historical data available in the market, industry-leading filtering capabilities and the highest data volume for Senate and Congress representatives on Capitol Hill.
""")

st.divider()

with st.container(height=550, border=False):
    col1, col2 = st.columns([.65, .35], gap='small')
    with col1:
        # trades by state
        fig = trades_by_state()
        st.plotly_chart(
            fig,
            use_container_width=True
        )
    with col2:
        fig = trades_by_chamber_and_party()
        st.plotly_chart(
            fig,
            use_container_width=True
        )

with st.container(height=550, border=False):
    col1, col2 = st.columns([.2 ,.8], gap='small')
    with col1:
        fig = trades_by_owner()
        st.plotly_chart(
            fig,
            use_container_width=True
        )
    with col2:
        fig = buys_and_sells_by_week()
        st.plotly_chart(
            fig,
            use_container_width=True
        )

with st.container(height=600, border=False):
    fig = buy_and_sell_transactions_for_issuer()
    st.plotly_chart(
        fig,
        use_container_width=True
    )

with st.container(height=650, border=False):
    fig = top_twenty_issuers()
    st.plotly_chart(
        fig,
        use_container_width=True
    )

st.markdown('Made by Flemming Wu')

