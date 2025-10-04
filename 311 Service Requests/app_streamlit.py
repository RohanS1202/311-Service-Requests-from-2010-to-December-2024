# app_streamlit.py — neat single-page UI
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from datetime import timedelta, datetime
import os
import hashlib
try:
    import duckdb
    USE_DUCKDB = True
except Exception:
    duckdb = None
    USE_DUCKDB = False

# DuckDB-backed query helpers (fast, column/row projection + cached)
DATA_PATH_SINGLE = "data/processed/nyc311_clean.parquet"
DATA_PATH_PART = "data/processed_part/**/*.parquet"
SUMMARIES_DIR = "data/summaries"


@st.cache_resource
def get_con():
        con = duckdb.connect(":memory:")
        # prefer the partitioned dataset if present (faster scans)
        base = DATA_PATH_PART if os.path.exists("data/processed_part") else DATA_PATH_SINGLE
        con.execute(f"""
                CREATE OR REPLACE VIEW nyc AS
                SELECT * FROM parquet_scan('{base}');
        """)
        con.execute("PRAGMA threads=%d" % (os.cpu_count() or 1))
        return con


def _where_clause(start_date, end_date, boroughs, complaints):
        where = ["created_dt >= ?::TIMESTAMP", "created_dt < (?::DATE + INTERVAL 1 DAY)"]
        params = [pd.to_datetime(start_date), pd.to_datetime(end_date)]
        if boroughs:
                where.append("borough IN (" + ",".join(["?"]*len(boroughs)) + ")")
                params += boroughs
        if complaints:
                where.append("complaint_type IN (" + ",".join(["?"]*len(complaints)) + ")")
                params += complaints
        return " WHERE " + " AND ".join(where), params


@st.cache_data(show_spinner=False, ttl=300)
def kpi_query(start_date, end_date, boroughs, complaints, sla_hours):
    if USE_DUCKDB:
        con = get_con()
        where, params = _where_clause(start_date, end_date, boroughs, complaints)
        sql = f"""
            SELECT
                median(response_hours) AS med_hrs,
                avg(CASE WHEN response_hours <= ? THEN 1 ELSE 0 END) AS pct_within,
                count(*)::BIGINT AS tickets
            FROM nyc
            {where}
            AND response_hours IS NOT NULL
        """
        return con.execute(sql, [sla_hours, *params]).df().iloc[0]
    else:
        # pandas fallback (slower)
        df_local = pd.read_parquet(DATA_PATH_SINGLE)
        df_local['created_dt'] = pd.to_datetime(df_local['created_dt'])
        mask = (df_local['created_dt'] >= pd.to_datetime(start_date)) & (df_local['created_dt'] < (pd.to_datetime(end_date) + pd.Timedelta(days=1)))
        d = df_local.loc[mask]
        if boroughs:
            d = d[d['borough'].isin(boroughs)]
        if complaints:
            d = d[d['complaint_type'].isin(complaints)]
        d = d[d['response_hours'].notna()]
        return pd.Series({
            'med_hrs': d['response_hours'].median(),
            'pct_within': (d['response_hours'] <= sla_hours).mean() if len(d) else float('nan'),
            'tickets': int(len(d))
        })


@st.cache_data(show_spinner=False, ttl=300)
def top_types_query(start_date, end_date, boroughs, complaints, sla_hours, topn):
    if USE_DUCKDB:
        con = get_con()
        where, params = _where_clause(start_date, end_date, boroughs, complaints)
        sql = f"""
            SELECT
                complaint_type,
                count(*)::BIGINT AS tickets,
                avg(CASE WHEN response_hours > ? THEN 1 ELSE 0 END) AS breach_rate
            FROM nyc
            {where}
            GROUP BY 1
            ORDER BY tickets DESC
            LIMIT {int(topn)}
        """
        return con.execute(sql, [sla_hours, *params]).df()
    else:
        df_local = pd.read_parquet(DATA_PATH_SINGLE)
        df_local['created_dt'] = pd.to_datetime(df_local['created_dt'])
        mask = (df_local['created_dt'] >= pd.to_datetime(start_date)) & (df_local['created_dt'] < (pd.to_datetime(end_date) + pd.Timedelta(days=1)))
        d = df_local.loc[mask]
        if boroughs:
            d = d[d['borough'].isin(boroughs)]
        if complaints:
            d = d[d['complaint_type'].isin(complaints)]
        d = d[d['response_hours'].notna()]
        out = d.groupby('complaint_type', observed=True).agg(tickets=('unique_key','count'))
        out['breach_rate'] = d.groupby('complaint_type', observed=True).apply(lambda s: (s['response_hours'] > sla_hours).mean())
        out = out.reset_index().sort_values('tickets', ascending=False).head(int(topn))
        return out


@st.cache_data(show_spinner=False, ttl=300)
def seasonality_query(start_date, end_date, boroughs, complaints, sla_hours):
    if USE_DUCKDB:
        con = get_con()
        where, params = _where_clause(start_date, end_date, boroughs, complaints)
        sql = f"""
            SELECT
                strftime(created_dt, '%A') AS dow_name,
                extract(hour from created_dt)::INT AS hour,
                count(*)::BIGINT AS tickets,
                avg(CASE WHEN response_hours > ? THEN 1 ELSE 0 END) AS breach_rate,
                median(response_hours) AS median_response
            FROM nyc
            {where}
            AND response_hours IS NOT NULL
            GROUP BY 1,2
        """
        return con.execute(sql, [sla_hours, *params]).df()
    else:
        df_local = pd.read_parquet(DATA_PATH_SINGLE)
        df_local['created_dt'] = pd.to_datetime(df_local['created_dt'])
        mask = (df_local['created_dt'] >= pd.to_datetime(start_date)) & (df_local['created_dt'] < (pd.to_datetime(end_date) + pd.Timedelta(days=1)))
        d = df_local.loc[mask]
        if boroughs:
            d = d[d['borough'].isin(boroughs)]
        if complaints:
            d = d[d['complaint_type'].isin(complaints)]
        d = d[d['response_hours'].notna()]
        d['hour'] = d['created_dt'].dt.hour
        d['dow_name'] = pd.Categorical(d['created_dt'].dt.day_name(), categories=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"], ordered=True)
        out = d.groupby(['dow_name','hour'], observed=True).agg(tickets=('unique_key','count'), breach_rate=('response_hours', lambda s: (s > sla_hours).mean()), median_response=('response_hours','median')).reset_index()
        return out


@st.cache_data(show_spinner=False, ttl=300)
def dow_agg_query(start_date, end_date, boroughs, complaints, sla_hours):
    """Return per-day-of-week aggregates directly from DuckDB to avoid pandas groupby on large tables."""
    if USE_DUCKDB:
        con = get_con()
        where, params = _where_clause(start_date, end_date, boroughs, complaints)
        sql = f"""
            SELECT
                strftime(created_dt, '%A') AS dow_name,
                median(response_hours) AS median_response,
                avg(CASE WHEN response_hours > ? THEN 1 ELSE 0 END) AS breach_rate,
                count(*)::BIGINT AS tickets
            FROM nyc
            {where}
            AND response_hours IS NOT NULL
            GROUP BY 1
        """
        return con.execute(sql, [sla_hours, *params]).df()
    else:
        df_local = pd.read_parquet(DATA_PATH_SINGLE)
        df_local['created_dt'] = pd.to_datetime(df_local['created_dt'])
        mask = (df_local['created_dt'] >= pd.to_datetime(start_date)) & (df_local['created_dt'] < (pd.to_datetime(end_date) + pd.Timedelta(days=1)))
        d = df_local.loc[mask]
        if boroughs:
            d = d[d['borough'].isin(boroughs)]
        if complaints:
            d = d[d['complaint_type'].isin(complaints)]
        d = d[d['response_hours'].notna()]
        d['dow_name'] = pd.Categorical(d['created_dt'].dt.day_name(), categories=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"], ordered=True)
        out = d.groupby('dow_name', observed=True).agg(median_response=('response_hours','median'), breach_rate=('response_hours', lambda s: (s > sla_hours).mean()), tickets=('unique_key','count')).reset_index()
        return out

# ---------- page & theme ----------
st.set_page_config(page_title="NYC 311 Response Time & SLA Risk", layout="wide")
alt.data_transformers.disable_max_rows()

def _alt_neat():
    return {
        "config": {
            "view": {"stroke": "transparent"},
            "axis": {
                "domain": False, "tickColor": "#E5E7EB",
                "gridColor": "#F1F5F9", "labelColor": "#374151", "titleColor": "#111827",
                "labelLimit": 220
            },
            "axisX": {"grid": False},
            "legend": {"labelColor": "#374151", "titleColor": "#111827"},
            "font": "system-ui",
            "header": {"labelColor": "#111827", "titleColor": "#111827"},
            "range": {"category": ["#0A84FF"]},  # primary brand color for bars/lines
        }
    }
alt.themes.register("neat", _alt_neat)
alt.themes.enable("neat")

# ---------- light CSS polish ----------
st.markdown("""
<style>
:root {
  --bg: #FFFFFF; --muted: #F8FAFC; --border: #E5E7EB;
  --ink: #111827; --ink-2: #4B5563; --brand: #0A84FF;
}
header[data-testid="stHeader"] { border-bottom: 1px solid var(--border); }
.section-card {
  background: var(--bg); border: 1px solid var(--border);
  border-radius: 16px; padding: 18px 18px 8px 18px; margin: 6px 0 18px 0;
  box-shadow: 0 1px 0 rgba(0,0,0,.02);
}
.kpi {
  background: var(--muted); border: 1px solid var(--border);
  border-radius: 14px; padding: 14px 16px;
}
.kpi .label { font-size: .9rem; color: var(--ink-2); margin-bottom: 6px; }
.kpi .value { font-size: 1.9rem; font-weight: 800; color: var(--ink); line-height: 1.1; }
.section-title { font-size: 1.15rem; font-weight: 700; margin: 0 0 6px 2px; }
.section-note { color: var(--ink-2); font-size: .90rem; margin: -4px 0 6px 2px; }
.small { color: var(--ink-2); font-size: .88rem; }
hr{ margin: 12px 0 4px 0; border: none; height: 1px; background: var(--border); }
</style>
""", unsafe_allow_html=True)

# ---------- data ----------
@st.cache_data
def load_metadata():
    # use DuckDB to fetch min/max dates and distinct boroughs/complaint types (fast, no full table read)
    con = get_con()
    md = {}
    try:
        md['min_date'] = con.execute("select min(created_dt) as v from nyc").df()['v'].iloc[0]
        md['max_date'] = con.execute("select max(created_dt) as v from nyc").df()['v'].iloc[0]
        md['boroughs'] = sorted([r[0] for r in con.execute("select distinct borough from nyc where borough is not null and borough <> ''").fetchall()])
        md['complaints'] = sorted([r[0] for r in con.execute("select distinct complaint_type from nyc where complaint_type is not null and complaint_type <> ''").fetchall()])
    except Exception:
        # fallback: load small sample into pandas (older deployments)
        df_tmp = pd.read_parquet(DATA_PATH_SINGLE)
        df_tmp['created_dt'] = pd.to_datetime(df_tmp['created_dt'])
        md['min_date'] = df_tmp['created_dt'].min()
        md['max_date'] = df_tmp['created_dt'].max()
        md['boroughs'] = sorted(df_tmp['borough'].dropna().unique().tolist())
        md['complaints'] = sorted(df_tmp['complaint_type'].dropna().unique().tolist())
    # normalize dates to date objects for the UI
    md['min_date'] = pd.to_datetime(md['min_date']).date()
    md['max_date'] = pd.to_datetime(md['max_date']).date()
    return md

md = load_metadata()

def fmt_pct(x): return "—" if pd.isna(x) else f"{x*100:.1f}%"
def fmt_num(x): return "—" if pd.isna(x) else f"{x:,.0f}"

def render_kpi(col, label, value):
    with col:
        st.markdown(f"""<div class="kpi">
            <div class="label">{label}</div>
            <div class="value">{value}</div>
        </div>""", unsafe_allow_html=True)

# ---------- sidebar filters ----------
min_date = md['min_date']
max_date = md['max_date']
with st.sidebar:
    st.header("Filters")
    dr = st.date_input("Date range", (min_date, max_date), min_value=min_date, max_value=max_date)

    boroughs_all = md['boroughs']
    complaints_all = md['complaints']
    sel_b = st.multiselect("Borough", boroughs_all, default=[])
    sel_c = st.multiselect("Complaint type", complaints_all, default=[])

    st.markdown("**SLA (hours)**")
    preset = st.segmented_control("Quick set", options=[24, 48, 72], default=24)
    sla_hours = st.slider("Custom", 4, 96, int(preset), step=1)
    topn = st.slider("Top complaint types (by tickets)", 5, 25, 10, step=1)

# ---------- filtering ----------
start_date, end_date = dr
# Use DuckDB cached queries to compute KPIs and small aggregations
kpi = kpi_query(start_date, end_date, sel_b, sel_c, sla_hours)
med_resp = kpi["med_hrs"]
pct_within = kpi["pct_within"]
total_tickets = kpi["tickets"]

# We'll still construct a small dataframe for the data sample/download grid using a lightweight DuckDB query
def _sample_df(start_date, end_date, boroughs, complaints, limit=20000):
    con = get_con()
    where, params = _where_clause(start_date, end_date, boroughs, complaints)
    sql = f"SELECT * FROM nyc {where} LIMIT {int(limit)}"
    return con.execute(sql, params).df()


def _load_precomputed():
    """Return dict of precomputed DataFrames if they exist, else empty dict."""
    out = {}
    p = SUMMARIES_DIR
    try:
        import os
        if os.path.exists(p):
            import pandas as pd
            def _r(fn):
                fp = f"{p}/{fn}"
                try:
                    return pd.read_parquet(fp)
                except Exception:
                    return None
            out['daily'] = _r('daily_summary.parquet')
            out['complaint_type'] = _r('complaint_type_summary.parquet')
            out['dow_hour'] = _r('dow_hour_summary.parquet')
    except Exception:
        return {}
    return {k:v for k,v in out.items() if v is not None}

# load precomputed summaries once
PRECOMPUTED = _load_precomputed()

dff = _sample_df(start_date, end_date, sel_b, sel_c, limit=1200)
dff["within_sla"] = dff["response_hours"].le(sla_hours)
dff["breach"] = ~dff["within_sla"]
closed = dff["response_hours"].notna()

# ---------- header ----------
st.title("NYC 311 Response Time & SLA Risk")

# Dev indicator: small badge only when APP_ENV=dev (keeps UI calm by default)
import os, hashlib
sha = None
try:
    sha = hashlib.sha1(open(__file__, "rb").read()).hexdigest()[:8]
except Exception:
    sha = "unknown"

app_env = os.getenv("APP_ENV", "prod").lower()

# subtle chip style
st.markdown("""
<style>
.dev-badge {
  display:inline-block; padding:2px 8px; border:1px solid #E5E7EB;
  border-radius:999px; background:#F8FAFC; color:#475569; font-size:.8rem;
  margin-bottom:6px;
}
</style>
""", unsafe_allow_html=True)

if app_env == "dev":
    try:
        st.markdown(f"<span class='dev-badge'>DEV • {sha}</span>", unsafe_allow_html=True)
    except Exception:
        pass

st.caption(f"Dataset: NYC Open Data (erm2-nwe9). SLA = {sla_hours}h. Showing {fmt_num(len(dff))} tickets.")

# ---------- KPIs ----------
with st.container():
    c1, c2, c3 = st.columns(3)
    render_kpi(c1, "Median Response (hrs)", f"{med_resp:.1f}" if pd.notna(med_resp) else "—")
    render_kpi(c2, "% Within SLA", fmt_pct(pct_within))
    render_kpi(c3, "Tickets (filtered)", fmt_num(total_tickets))

# ---------- Section: Breach by complaint type ----------
st.subheader("Breach rate by Complaint Type")

if 'complaint_type' in PRECOMPUTED:
    # filter precomputed complaint_type summary down to current boroughs/complaints
    by_type = PRECOMPUTED['complaint_type']
    if sel_b:
        by_type = by_type[by_type['borough'].isin(sel_b)]
    if sel_c:
        by_type = by_type[by_type['complaint_type'].isin(sel_c)]
    # order and limit
    by_type = by_type.sort_values('tickets', ascending=False).head(int(topn))
else:
    by_type = top_types_query(start_date, end_date, sel_b, sel_c, sla_hours, topn)

if by_type.empty:
    st.info("No data for current filters (try expanding date range or lowering SLA).")
else:
    # Distinct colors + comfortable spacing + readable labels
    by_type["breach_rate"] = by_type["breach_rate"].astype(float).clip(0, 1)
    y_enc = alt.Y(
        "complaint_type:N",
        sort='-x',
        title=None,
        scale=alt.Scale(paddingInner=0.25, paddingOuter=0.1),   # <-- spacing between bars
        axis=alt.Axis(labelLimit=420)                           # avoid truncation
    )

    base = alt.Chart(by_type).encode(
        y=y_enc,
        x=alt.X("breach_rate:Q", axis=alt.Axis(format='%'), title="Breach rate"),
        tooltip=[
            alt.Tooltip("complaint_type:N", title="Complaint"),
            alt.Tooltip("tickets:Q", title="Tickets", format=",.0f"),
            alt.Tooltip("breach_rate:Q", title="Breach rate", format=".1%")
        ]
    )

    bars = base.mark_bar(size=26).encode(
        # DISTINCT COLORS PER TYPE (Tableau 10 palette)
        color=alt.Color("complaint_type:N", legend=None, scale=alt.Scale(scheme="tableau10"))
    )

    labels = base.mark_text(align="left", dx=6, dy=0, color="#0f172a").encode(
        text=alt.Text("breach_rate:Q", format=".0%")
    )

    # Height scales with number of categories for clear spacing
    chart_h = max(280, 28 * len(by_type))
    st.altair_chart((bars + labels).properties(height=chart_h), use_container_width=True)

# ---------- Section: Seasonality (stacked) ----------

st.subheader("Seasonality")

if 'dow_hour' in PRECOMPUTED:
    season = PRECOMPUTED['dow_hour']
    if sel_b:
        season = season[season['borough'].isin(sel_b)]
    if sel_c:
        season = season[season['complaint_type'].isin(sel_c)]
    # ensure types match expected
    season = season.rename(columns={'median_response':'median_response','breach_rate':'breach_rate'})
else:
    season = seasonality_query(start_date, end_date, sel_b, sel_c, sla_hours)

if season.empty:
    st.info("No seasonality data for current filters.")
else:
    # 1) Heatmap: Day x Hour (breach rate)
    season["breach_rate"] = season["breach_rate"].astype(float).clip(0, 1)
    heat = alt.Chart(season).mark_rect().encode(
        x=alt.X("hour:O", title="Hour of day"),
        y=alt.Y("dow_name:N", title=None),
        color=alt.Color("breach_rate:Q", title="Breach rate",
                        scale=alt.Scale(scheme="reds", domain=[0, 1])),
        tooltip=[
            "dow_name:N","hour:O",
            alt.Tooltip("tickets:Q", title="Tickets", format=",.0f"),
            alt.Tooltip("breach_rate:Q", title="Breach rate", format=".1%"),
            alt.Tooltip("median_response:Q", title="Median hrs", format=".1f"),
        ],
    ).properties(height=260)

    st.markdown("**Breach rate by Day & Hour**")
    st.altair_chart(heat, use_container_width=True)

    # Aggregate per day for lines (pull directly from DuckDB)
    dow = dow_agg_query(start_date, end_date, sel_b, sel_c, sla_hours)
    # convert breach_rate -> pct_within for the UI
    dow["pct_within"] = 1.0 - dow["breach_rate"].astype(float)

    # 2) Median response by day
    st.markdown("**Median response by Day**")
    ch1 = alt.Chart(dow).mark_line(point=True).encode(
        x=alt.X("dow_name:N", title=None),
        y=alt.Y("median_response:Q", title="Median hours"),
        tooltip=["dow_name","median_response","tickets"]
    ).properties(height=150)
    st.altair_chart(ch1, use_container_width=True)

    # 3) % within SLA by day
    st.markdown("**% within SLA by Day**")
    ch2 = alt.Chart(dow).mark_line(point=True).encode(
        x=alt.X("dow_name:N", title=None),
        y=alt.Y("pct_within:Q", axis=alt.Axis(format='%'), title="% within SLA"),
        tooltip=[alt.Tooltip("pct_within:Q", title="% within", format=".1%"), "dow_name","tickets"]
    ).properties(height=150)
    st.altair_chart(ch2, use_container_width=True)

# ---------- Section: Data ----------
with st.container():
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Data sample</div>', unsafe_allow_html=True)
    default_cols = [
        "unique_key","created_dt","borough","complaint_type","descriptor",
        "response_hours","within_sla","hour","dow_name","month_name"
    ]
    show_cols = st.multiselect("Columns to show", options=list(dff.columns), default=default_cols)
    st.dataframe(dff[show_cols].head(1200), use_container_width=True)
    csv = dff[show_cols].to_csv(index=False).encode("utf-8")
    st.download_button("Download current view (CSV)", csv, file_name="nyc311_current_view.csv", mime="text/csv")
    st.markdown('</div>', unsafe_allow_html=True)

st.caption("Notes: breach rate uses the current SLA threshold. KPIs exclude tickets without a close/resolution timestamp.")
