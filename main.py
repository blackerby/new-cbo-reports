from datetime import date, timedelta
import os

import duckdb
import polars as pl
from pycapitol import url_for
import requests
import streamlit as st


CURRENT_CONGRESS = str((date.today().year - 1789) // 2 + 1)
CBO_URL = f"https://www.cbo.gov/rss/{CURRENT_CONGRESS}congress-cost-estimates.xml"
CDG_API_URL = "https://api.congress.gov/v3"
YESTERDAY = date.today() - timedelta(days=1)
API_KEY = os.getenv("CDG_API_KEY", "DEMO_KEY")


def create_duckdb_con():
    con = duckdb.connect()

    try:
        con.install_extension("webbed", repository="community")
        print("webbed extension installed successfully.")
    except Exception as e:
        print(f"Error installing extension: {e}")

    try:
        con.load_extension("webbed")
        print("webbed extension loaded successfully.")
    except Exception as e:
        print(f"Error loading extension: {e}")

    return con


def fetch_cdg_data(url):
    path = url.split(".gov")[1]
    url = CDG_API_URL + path
    response = requests.get(url, headers={"x-api-key": API_KEY})
    data = response.json()
    bill = data["bill"]
    estimates = bill["cboCostEstimates"]
    return [estimate["url"] for estimate in estimates][0]

@st.cache_data
def get_df():
    con = create_duckdb_con()
    rel = con.execute(f"""
    select Bill_Number, Link, Title, try_strptime(Date, '%a, %d %b %Y %H:%M:%S %z') as Date
    from '{CBO_URL}' where Bill_Number is not null;
    """)

    cite = pl.lit(CURRENT_CONGRESS) + pl.col("Bill_Number")
    df = (
        rel.pl()
        .filter(pl.col("Date").dt.date() == YESTERDAY)
        .with_columns(
            cite.map_elements(url_for, return_dtype=pl.String).alias("Bill URL")
        )
        .with_columns(
            pl.col("Bill URL")
            .map_elements(fetch_cdg_data, return_dtype=pl.String)
            .alias("cdg_api_cbo_url")
        )
        .with_columns(
            (pl.col("Link") == pl.col("cdg_api_cbo_url")).alias(
                "Cost Estimate Present on CDG?"
            )
        )
        .select("Date", "Title", "Bill URL", "Cost Estimate Present on CDG?")
    )

    return df


df = get_df()

st.set_page_config(layout="wide")
st.title("New CBO Reports")

st.dataframe(
    df,
    width="stretch",
    column_config={
        "Link": st.column_config.LinkColumn("Link"),
        "Bill URL": st.column_config.LinkColumn("Bill URL"),
    },
)

st.subheader(f"Total New: {len(df)}")

missing = df.filter(pl.col("Cost Estimate Present on CDG?") == False)

st.subheader(f"Total Missing: {len(missing)}")
