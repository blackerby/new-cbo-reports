from enum import StrEnum, auto
from datetime import date, timedelta
import os

import duckdb
import polars as pl
import requests
import streamlit as st

CURRENT_CONGRESS = str((date.today().year - 1789) // 2 + 1)
CBO_URL = f"https://www.cbo.gov/rss/{CURRENT_CONGRESS}congress-cost-estimates.xml"
CDG_API_URL = "https://api.congress.gov/v3"
CDG_URL = "https://www.congress.gov"
YESTERDAY = date.today() - timedelta(days=1)
API_KEY = os.getenv("CDG_API_KEY", "DEMO_KEY")


class Host(StrEnum):
    API = auto()
    WEB = auto()


def url_for(bill_number, host):
    bill_type, number = [
        s.replace(".", "").replace(" ", "").lower() for s in bill_number.rsplit(".", 1)
    ]

    if host == Host.WEB:
        url = CDG_URL
    if host == Host.API:
        url = CDG_API_URL

    return f"{url}/bill/{CURRENT_CONGRESS}/{bill_type}/{number}"


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
    response = requests.get(
        url, headers={"x-api-key": API_KEY}, params={"format": "json"}
    )
    data = response.json()
    bill = data["bill"]
    # ensure list is sorted by pubDate
    estimates = sorted(bill["cboCostEstimates"], key=lambda e: e.get("pubDate", ""))
    return [estimate["url"] for estimate in estimates][-1]


@st.cache_data
def get_df():
    con = create_duckdb_con()
    rel = con.execute(f"""
    select Bill_Number, Link, Title, try_strptime(Date, '%a, %d %b %Y %H:%M:%S %z') as Date
    from '{CBO_URL}' where Bill_Number is not null;
    """)

    df = (
        rel.pl()
        .filter(pl.col("Date").dt.date() == YESTERDAY)
        .with_columns(
            pl.col("Bill_Number")
            .map_elements(lambda n: url_for(n, Host.WEB), return_dtype=pl.String)
            .alias("Bill URL")
        )
        .with_columns(
            pl.col("Bill_Number")
            .map_elements(lambda n: url_for(n, Host.API), return_dtype=pl.String)
            .alias("Bill API URL")
        )
        .with_columns(
            pl.col("Bill API URL")
            .map_elements(fetch_cdg_data, return_dtype=pl.String)
            .alias("cdg_api_cbo_url")
        )
        .with_columns(
            (pl.col("Link") == pl.col("cdg_api_cbo_url")).alias(
                "Cost Estimate Present on CDG?"
            )
        )
        .select("Date", "Title", "Bill URL", "Bill API URL", "Cost Estimate Present on CDG?")
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
        "Bill API URL": st.column_config.LinkColumn("Bill API URL"),
    },
)

st.subheader(f"Total New: {len(df)}")

missing = df.filter(pl.col("Cost Estimate Present on CDG?") == False)

st.subheader(f"Total Missing: {len(missing)}")
