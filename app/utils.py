from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

import streamlit as st
import polars as pl


@st.cache_resource
def get_athena_conn():
    '''Cached connection to Athena DB'''
    athena_connection = connect(
        s3_staging_dir=st.secrets['S3_STAGING_DIR'],
        region_name=st.secrets['AWS_REGION'],
        aws_access_key_id=st.secrets['AWS_ACCESS_KEY'],
        aws_secret_access_key=st.secrets['AWS_SECRET_ACCESS_KEY'],
        cursor_class=ArrowCursor
    )
    return athena_connection


@st.cache_data
def query_athena(query: str) -> pl.DataFrame:
    '''Executes a query against Athena DB and returns polars DataFrame'''
    cursor = get_athena_conn().cursor()
    arrow_table = cursor.execute(query).as_arrow()
    return pl.from_arrow(arrow_table)