import os
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


DEFAULT_POSTGRES_TABLE = "marathon_results_2025"
DEFAULT_POSTGRES_CACHE_TTL_SECONDS = 300
TOP_N_MIN = 8
TOP_N_MAX = 50
TOP_N_DEFAULT = 24
GENDER_COLUMNS = ["Women", "Men", "Non-binary/Other", "Unspecified"]
GENDER_MAP = {"F": "Women", "M": "Men", "X": "Non-binary/Other"}
GENDER_COLORS = {
    "Women": "#facc15",
    "Men": "#1d4ed8",
    "Non-binary/Other": "#0f766e",
    "Unspecified": "#64748b",
}

ALPHA3_TO_ALPHA2 = {
    "ALG": "DZ",
    "ANG": "AO",
    "ARG": "AR",
    "ARU": "AW",
    "AUS": "AU",
    "AUT": "AT",
    "AZE": "AZ",
    "BAN": "BD",
    "BEL": "BE",
    "BER": "BM",
    "BIZ": "BZ",
    "BLR": "BY",
    "BOL": "BO",
    "BRA": "BR",
    "BRN": "BH",
    "BUL": "BG",
    "CAM": "KH",
    "CAN": "CA",
    "CAY": "KY",
    "CHI": "CL",
    "CHN": "CN",
    "COK": "CK",
    "COL": "CO",
    "CRC": "CR",
    "CRO": "HR",
    "CYP": "CY",
    "CZE": "CZ",
    "DEN": "DK",
    "DOM": "DO",
    "ECU": "EC",
    "EGY": "EG",
    "ESA": "SV",
    "ESP": "ES",
    "EST": "EE",
    "ETH": "ET",
    "FIN": "FI",
    "FRA": "FR",
    "FRO": "FO",
    "FSM": "FM",
    "GBR": "GB",
    "GEO": "GE",
    "GER": "DE",
    "GLP": "GP",
    "GRE": "GR",
    "GRN": "GD",
    "GUA": "GT",
    "HKG": "HK",
    "HON": "HN",
    "HUN": "HU",
    "IMN": "IM",
    "INA": "ID",
    "IND": "IN",
    "IRI": "IR",
    "IRL": "IE",
    "ISL": "IS",
    "ISR": "IL",
    "ITA": "IT",
    "JAM": "JM",
    "JER": "JE",
    "JPN": "JP",
    "KAZ": "KZ",
    "KEN": "KE",
    "KGZ": "KG",
    "KOR": "KR",
    "KOS": "",
    "KSA": "SA",
    "LAT": "LV",
    "LES": "LS",
    "LTU": "LT",
    "LUX": "LU",
    "MAC": "MO",
    "MAR": "MA",
    "MAS": "MY",
    "MDA": "MD",
    "MEX": "MX",
    "MGL": "MN",
    "MKD": "MK",
    "MLT": "MT",
    "MNE": "ME",
    "NCA": "NI",
    "NED": "NL",
    "NOR": "NO",
    "NZL": "NZ",
    "PAK": "PK",
    "PAN": "PA",
    "PAR": "PY",
    "PER": "PE",
    "PHI": "PH",
    "POL": "PL",
    "POR": "PT",
    "PRI": "PR",
    "ROU": "RO",
    "RSA": "ZA",
    "RUS": "RU",
    "SIN": "SG",
    "SLO": "SI",
    "SRB": "RS",
    "SUI": "CH",
    "SVK": "SK",
    "SWE": "SE",
    "TAN": "TZ",
    "THA": "TH",
    "TUN": "TN",
    "TUR": "TR",
    "TWN": "TW",
    "UAE": "AE",
    "UGA": "UG",
    "UKR": "UA",
    "URU": "UY",
    "USA": "US",
    "UZB": "UZ",
    "VEN": "VE",
    "VGB": "VG",
    "VIE": "VN",
    "ZIM": "ZW",
}


def flag_emoji_from_alpha2(alpha2: str) -> str:
    if len(alpha2) != 2 or not alpha2.isalpha():
        return "🌍"
    return chr(ord(alpha2[0].upper()) + 127397) + chr(ord(alpha2[1].upper()) + 127397)


def make_country_label(country: str, alpha3: str) -> str:
    alpha2 = ALPHA3_TO_ALPHA2.get(alpha3.upper(), "")
    return f"{flag_emoji_from_alpha2(alpha2)} {country}"


def get_config_value(key: str, default: Optional[str] = None) -> Optional[str]:
    if key in st.secrets:
        return str(st.secrets[key])
    return os.getenv(key, default)


def get_postgres_config() -> Tuple[str, Optional[str]]:
    database_url = get_config_value("SUPABASE_DB_URL") or get_config_value("DATABASE_URL")
    query = get_config_value("RESULTS_QUERY")
    table = get_config_value("RESULTS_TABLE", DEFAULT_POSTGRES_TABLE)
    if query:
        return database_url or "", query
    return database_url or "", f'SELECT * FROM "{table}"'


@st.cache_data(ttl=DEFAULT_POSTGRES_CACHE_TTL_SECONDS)
def load_postgres_data(database_url: str, query: str) -> Tuple[pd.DataFrame, str]:
    df = pd.read_sql_query(query, database_url)
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df, fetched_at


def load_data() -> Tuple[pd.DataFrame, Optional[str]]:
    database_url, query = get_postgres_config()
    if not database_url:
        raise ValueError(
            "No DB URL was provided. Set SUPABASE_DB_URL (or DATABASE_URL)."
        )
    df, fetched_at = load_postgres_data(database_url, query or "")
    return df, fetched_at


def build_country_counts(df: pd.DataFrame) -> pd.DataFrame:
    base = (
        df.assign(
            Country=df["CountryOfResName"].fillna("Unknown").astype(str).str.strip(),
            CountryCode=df["CountryOfResAbbrev"].fillna("").astype(str).str.strip().str.upper(),
            Gender=df["Gender"].fillna("").astype(str).str.strip().str.upper(),
        )
        .assign(
            Country=lambda d: d["Country"].replace("", "Unknown"),
            GenderGroup=lambda d: d["Gender"].map(GENDER_MAP).fillna("Unspecified"),
        )
    )
    gender_counts = (
        base.groupby(["Country", "CountryCode", "GenderGroup"], dropna=False)
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    for col in GENDER_COLUMNS:
        if col not in gender_counts.columns:
            gender_counts[col] = 0
    return (
        gender_counts.assign(Runners=lambda d: d[GENDER_COLUMNS].sum(axis=1))
        .sort_values("Runners", ascending=False, kind="mergesort")
        .assign(CountryLabel=lambda d: d.apply(lambda row: make_country_label(row["Country"], row["CountryCode"]), axis=1))
        .reset_index(drop=True)
    )


def render_styles() -> None:
    st.markdown(
        """
        <style>
            .stApp {
                background: linear-gradient(180deg, #e9edf3 0%, #dfe6ef 100%);
                color: #111111;
            }
            .block-container {
                max-width: 1060px;
                padding-top: 3.2rem;
                padding-bottom: 1.5rem;
            }
            .hero {
                border-left: 14px solid #facc15;
                padding: 0.35rem 1rem 0.65rem 1rem;
                margin-bottom: 1.2rem;
                background: rgba(255, 255, 255, 0.38);
            }
            .hero h1 {
                margin: 0;
                font-size: clamp(2rem, 5vw, 4rem);
                line-height: 0.95;
                letter-spacing: 0.5px;
                font-weight: 900;
                text-transform: uppercase;
                color: #0f172a;
            }
            .hero p {
                margin: 0.45rem 0 0 0;
                font-size: 1.05rem;
                color: #2c3a52;
                max-width: 850px;
            }
            [data-testid="stMetricValue"] {
                color: #0f172a;
                font-weight: 800;
            }
            [data-testid="stMetricLabel"] {
                color: #4b5f80;
                text-transform: uppercase;
                letter-spacing: 0.6px;
                font-size: 0.78rem;
            }
            [data-testid="stDataFrame"] {
                border: 1px solid #c7d2e3;
                border-radius: 10px;
                overflow: hidden;
            }
            [data-testid="stSidebar"] {
                border-right: 1px solid #c7d2e3;
                background: linear-gradient(180deg, #f8fafc 0%, #eef3f9 100%);
            }
            [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 {
                color: #0f172a;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar(
    df: pd.DataFrame,
    total_runners: int,
    total_countries: int,
    last_supabase_fetch: Optional[str],
) -> int:
    with st.sidebar:
        st.header("Control panel")
        st.caption("Adjust the ranking scope without modifying the base data.")
        top_n = st.slider(
            "Top countries shown",
            min_value=TOP_N_MIN,
            max_value=min(TOP_N_MAX, total_countries),
            value=min(TOP_N_DEFAULT, total_countries),
            step=1,
            help="Control how many countries appear in the horizontal bar chart.",
        )
        st.caption(f"Showing {top_n} of {total_countries} countries.")
        st.metric("Countries available", f"{total_countries:,}")
        st.metric("Total runners", f"{total_runners:,}")
        st.download_button(
            label="Download query results (CSV)",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="boston_marathon_results.csv",
            mime="text/csv",
            use_container_width=True,
        )
        st.caption(f"Supabase cache TTL: {DEFAULT_POSTGRES_CACHE_TTL_SECONDS} seconds")
        if last_supabase_fetch:
            st.caption(f"Last Supabase connection: {last_supabase_fetch}")
        else:
            st.caption("Last Supabase connection: unavailable")
    return top_n


def build_figure(chart_data: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for gender in GENDER_COLUMNS:
        fig.add_trace(
            go.Bar(
                x=chart_data[gender],
                y=chart_data["CountryLabel"],
                name=gender,
                orientation="h",
                marker=dict(color=GENDER_COLORS[gender]),
                hovertemplate="<b>%{y}</b><br>" + gender + ": %{x:,}<extra></extra>",
                cliponaxis=False,
            )
        )
    fig.add_trace(
        go.Scatter(
            x=chart_data["Runners"],
            y=chart_data["CountryLabel"],
            mode="text",
            text=chart_data["Runners"].map(lambda x: f"{x:,}"),
            textposition="middle right",
            textfont=dict(color="#0f172a", size=14),
            hoverinfo="skip",
            showlegend=False,
            cliponaxis=False,
        )
    )
    fig.update_layout(
        height=max(560, len(chart_data) * 34),
        margin=dict(l=15, r=110, t=8, b=90),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        barmode="stack",
        xaxis_title=None,
        xaxis=dict(
            title=dict(text=""),
            gridcolor="rgba(15, 23, 42, 0.08)",
            zeroline=False,
            showticklabels=False,
            ticks="",
        ),
        yaxis=dict(
            autorange="reversed",
            tickfont=dict(size=16, color="#0f172a"),
        ),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.12,
            xanchor="center",
            x=0.5,
        ),
    )
    return fig


def heatmap_cell_style(value: float, low: float, high: float, is_constant: bool = False) -> str:
    if pd.isna(value):
        return ""

    if is_constant:
        return "background-color: rgb(241, 245, 249); color: #475569;"

    intensity = max(0.0, min(1.0, (float(value) - low) / (high - low)))
    intensity = intensity**0.75

    light_rgb = (239, 246, 255)
    dark_rgb = (30, 64, 175)
    red = int(light_rgb[0] + (dark_rgb[0] - light_rgb[0]) * intensity)
    green = int(light_rgb[1] + (dark_rgb[1] - light_rgb[1]) * intensity)
    blue = int(light_rgb[2] + (dark_rgb[2] - light_rgb[2]) * intensity)
    text_color = "#0f172a" if intensity < 0.55 else "white"
    return f"background-color: rgb({red}, {green}, {blue}); color: {text_color};"


def column_scale_bounds(series: pd.Series) -> tuple[float, float, bool]:
    values = series.dropna().astype(float)
    if values.empty:
        return 0.0, 1.0, True

    vmin = float(values.min())
    vmax = float(values.max())
    if vmax <= vmin:
        return vmin, vmax, True

    low = float(values.quantile(0.05))
    high = float(values.quantile(0.95))
    if high <= low:
        low, high = vmin, vmax
    return low, high, False


def build_heatmap_table(chart_data: pd.DataFrame):
    table = chart_data[
        ["CountryLabel", "Runners", "Women", "Men", "Non-binary/Other"]
    ].rename(
        columns={
            "CountryLabel": "Country",
            "Runners": "Total runners",
        }
    )
    numeric_cols = ["Total runners", "Women", "Men", "Non-binary/Other"]
    styler = table.style.format({col: "{:,.0f}" for col in numeric_cols})
    for col in numeric_cols:
        low, high, is_constant = column_scale_bounds(table[col])
        styler = styler.applymap(
            lambda value, lo=low, hi=high, const=is_constant: heatmap_cell_style(value, lo, hi, const),
            subset=[col],
        )
    return styler.set_properties(subset=["Country"], **{"font-weight": "600"})


def main() -> None:
    st.set_page_config(page_title="Boston Marathon 2025", layout="centered")
    render_styles()

    try:
        df, last_supabase_fetch = load_data()
    except Exception as exc:
        st.error(f"Unable to load data source: {exc}")
        st.info(
            "Configure SUPABASE_DB_URL/DATABASE_URL and RESULTS_TABLE/RESULTS_QUERY for Postgres."
        )
        st.stop()

    country_counts = build_country_counts(df)

    top_country = country_counts.iloc[0]
    top_share = top_country["Runners"] / country_counts["Runners"].sum()

    st.markdown(
        f"""
        <div class="hero">
            <h1>Boston Marathon 2025</h1>
            <p>
                <strong>{top_country["CountryLabel"]}</strong> represents approximately
                <strong>{top_share:.1%}</strong> of all runners in the 2025 dataset.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    top_n = render_sidebar(
        df=df,
        total_runners=int(country_counts["Runners"].sum()),
        total_countries=country_counts.shape[0],
        last_supabase_fetch=last_supabase_fetch,
    )

    chart_data = country_counts.head(top_n)
    st.plotly_chart(build_figure(chart_data), use_container_width=True)

    st.caption("Source: 2025 Boston Marathon runners results")
    st.caption("Heatmap: darker blue means a higher value within each metric column.")
    st.dataframe(build_heatmap_table(chart_data), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
