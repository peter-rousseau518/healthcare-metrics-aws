"""
Nursing Home Staffing & Occupancy Dashboard (AWS version)

Reads the gold layer in Amazon Athena (database cms_gold, workgroup
cms_lakehouse) using a read-only IAM user. Same pages and metric
definitions as the Snowflake version (healthcare-metrics-pipeline).

Run locally:  streamlit run dashboard/app.py
Credentials:  ~/.streamlit/secrets.toml locally; the app's Secrets on
              Streamlit Community Cloud. Never committed to the repository.
"""
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pyathena import connect
from pyathena.error import Error as AthenaError
from pyathena.pandas.cursor import PandasCursor

st.set_page_config(page_title="Nursing Home Staffing & Occupancy", page_icon="🏥", layout="wide")

REPO_URL = "https://github.com/peter-rousseau518/healthcare-metrics-aws"
TYPE_COLUMNS = {
    "Ownership type": "ownership_group",
    "Provider type": "provider_type",
    "Facility setting": "facility_setting",
}
STAFFING_MEASURES = {
    "Licensed nurses (RN + LPN)": "licensed_nurse_hprd",
    "Registered nurses (RN)": "rn_hprd",
    "Licensed practical nurses (LPN)": "lpn_hprd",
    "All nursing staff (incl. aides)": "total_nurse_hprd",
}


# --- Data access ---------------------------------------------------------------

@st.cache_resource
def get_connection():
    cfg = st.secrets["aws"]
    return connect(
        aws_access_key_id=cfg["access_key_id"],
        aws_secret_access_key=cfg["secret_access_key"],
        region_name=cfg.get("region", "us-east-1"),
        work_group=cfg.get("work_group", "cms_lakehouse"),
        schema_name=cfg.get("database", "cms_gold"),
        cursor_class=PandasCursor,
    )


def _run(sql: str) -> pd.DataFrame:
    df = get_connection().cursor().execute(sql).as_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df


@st.cache_data(ttl=60 * 60 * 24, show_spinner="Loading data from Athena...")
def query(sql: str) -> pd.DataFrame:
    """Cached for 24 hours: the data changes monthly, so page views
    don't each trigger an Athena query (and its per-scan charge)."""
    try:
        return _run(sql)
    except AthenaError:
        get_connection.clear()  # reconnect once, then retry
        return _run(sql)


facilities = query("""
    select ccn, provider_name, state, ownership_group, provider_type, facility_setting,
           is_chain_affiliated, calendar_quarter, certified_beds, avg_daily_census,
           occupancy_rate, rn_hprd, lpn_hprd, licensed_nurse_hprd, total_nurse_hprd,
           licensed_nurse_hours_per_week, licensed_nurse_fte_equivalent,
           contract_licensed_hours_per_week, contract_licensed_share,
           weekend_licensed_drop_pct, total_nursing_turnover_pct, rn_turnover_pct,
           contract_licensed_hours_rank, overall_rating
    from mart_facility_staffing
    where is_valid_for_analysis
""")
by_state_type = query("select * from mart_staffing_by_state_and_type")
daily = query("select * from mart_daily_staffing_trend")
quality = query("select * from mart_stay_type_quality_trend")
care = query("select * from mart_quality_of_care where is_valid_for_analysis")

daily["is_weekend"] = daily["is_weekend"].fillna(False).astype(bool)

for frame in (facilities, by_state_type, daily, quality, care):
    for col in frame.columns:
        if frame[col].dtype == object and col.endswith(("_hprd", "_rate", "_share", "_pct", "_score")):
            frame[col] = pd.to_numeric(frame[col], errors="coerce")


# --- Sidebar filters -----------------------------------------------------------

st.sidebar.header("Filters")
quarter = st.sidebar.selectbox("Quarter", sorted(facilities["calendar_quarter"].unique(), reverse=True))
selected_states = st.sidebar.multiselect("States (leave blank for all)", sorted(facilities["state"].unique()))
type_dimension = st.sidebar.radio("Define facility type by", list(TYPE_COLUMNS))
type_col = TYPE_COLUMNS[type_dimension]

f = facilities[facilities["calendar_quarter"] == quarter]
if selected_states:
    f = f[f["state"].isin(selected_states)]
scope_label = ", ".join(selected_states) if selected_states else "All states"

st.sidebar.markdown("---")
st.sidebar.caption(
    f"Sources: CMS Payroll-Based Journal daily nurse staffing ({quarter}) and "
    f"CMS Nursing Home Care Compare (October 2024). "
    f"[Pipeline and definitions]({REPO_URL})"
)


# --- Header --------------------------------------------------------------------

st.title("Nursing Home Staffing & Occupancy")
st.caption(
    f"{len(f):,} nursing homes · {scope_label} · {quarter}. "
    "Staffing is measured in hours per resident day (HPRD): total nurse hours ÷ total resident-days."
)

tab1, tab_hours, tab2, tab_turnover, tab3, tab4, tab5, tab_quality = st.tabs([
    "Staffing vs. occupancy",
    "Nurse hours",
    "Agency nurse hours",
    "Strain vs. turnover",
    "Staffing by state & type",
    "Short- vs. long-stay trends",
    "Daily trends",
    "Quality of care",
])

PBJ_SOURCE = f"Source: CMS Payroll-Based Journal daily nurse staffing, {quarter}."

# Weekly hours per facility, from PBJ: HPRD x average daily census x 7
f = f.assign(
    rn_hours_per_week=f["rn_hprd"] * f["avg_daily_census"] * 7,
    lpn_hours_per_week=f["lpn_hprd"] * f["avg_daily_census"] * 7,
    employee_licensed_hours_per_week=f["licensed_nurse_hours_per_week"] - f["contract_licensed_hours_per_week"],
    contract_licensed_hprd=f["contract_licensed_hours_per_week"] / (f["avg_daily_census"] * 7),
)


def describe_correlation(r: float) -> str:
    if pd.isna(r):
        return "not enough data"
    size = abs(r)
    strength = "negligible" if size < 0.1 else "weak" if size < 0.3 else "moderate" if size < 0.5 else "strong"
    direction = "" if size < 0.1 else (" positive" if r > 0 else " negative")
    return f"{strength}{direction}"


def correlation_bands(df: pd.DataFrame, x: str, y: str, x_label: str, y_label: str, key: str) -> None:
    """Rank correlation, Pearson, and the average of y across quintiles of x."""
    t = df[[x, y, type_col, "provider_name", "state"]].dropna()
    if len(t) < 10:
        st.warning("Too few facilities in the current selection to compare. Select more states or clear the filter.")
        return
    spearman = t[x].rank().corr(t[y].rank())
    pearson = t[x].corr(t[y])
    m1, m2, m3 = st.columns(3)
    m1.metric("Facilities with both measures", f"{len(t):,}")
    m2.metric("Rank correlation (Spearman)", f"{spearman:.3f}", describe_correlation(spearman), delta_color="off",
              help="Robust to skewed data and outliers; the primary measure here.")
    m3.metric("Linear correlation (Pearson)", f"{pearson:.3f}", describe_correlation(pearson), delta_color="off")
    t = t.assign(band=pd.qcut(t[x].rank(method="first"), 5,
                              labels=["Lowest 20%", "20-40%", "40-60%", "60-80%", "Highest 20%"]))
    bands = t.groupby("band", observed=True).agg(facilities=(y, "size"), avg=(y, "mean"),
                                                  x_low=(x, "min"), x_high=(x, "max")).reset_index()
    bands["range"] = bands["x_low"].map("{:.1f}".format) + " to " + bands["x_high"].map("{:.1f}".format)
    fig = px.bar(bands, x="band", y="avg", text_auto=".1f", hover_data=["facilities", "range"],
                 labels={"band": x_label, "avg": f"Average {y_label}", "range": x_label + " range"},
                 title=f"Average {y_label} by {x_label}", height=380)
    st.plotly_chart(fig, width="stretch", key=f"{key}_bands")
    low, high = bands.iloc[0], bands.iloc[-1]
    diff = high["avg"] - low["avg"]
    st.markdown(
        f"Facilities with the **highest 20%** {x_label} ({high['range']}) average **{high['avg']:.1f}** "
        f"{y_label}, versus **{low['avg']:.1f}** for the **lowest 20%** ({low['range']}): "
        f"**{abs(diff):.1f} {'higher' if diff > 0 else 'lower'}**."
    )
    with st.expander("Every facility"):
        cap = t[y].quantile(0.99)
        fig2 = px.scatter(t, x=x, y=y, color=type_col, opacity=0.35, render_mode="webgl",
                          hover_name="provider_name", hover_data={"state": True},
                          labels={x: x_label, y: y_label, type_col: type_dimension}, height=420)
        fig2.update_yaxes(range=[0, cap * 1.05])
        st.plotly_chart(fig2, width="stretch", key=f"{key}_scatter")
        st.caption("Y axis capped at the 99th percentile so outliers don't flatten the chart.")


# --- Tab 1: staffing vs occupancy ----------------------------------------------

with tab1:
    st.subheader("What is the relationship between nurse staffing and occupancy?")
    measure_label = st.radio("Staffing measure", list(STAFFING_MEASURES), horizontal=True, key="t1_measure")
    measure = STAFFING_MEASURES[measure_label]
    r = f["occupancy_rate"].corr(f[measure])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Facilities", f"{len(f):,}")
    c2.metric("Average occupancy", f"{f['occupancy_rate'].mean():.1%}")
    c3.metric(f"Average {measure_label.split(' (')[0].lower()} HPRD", f"{f[measure].mean():.2f}")
    c4.metric("Correlation (r)", f"{r:.3f}", describe_correlation(r), delta_color="off")

    fig = px.scatter(
        f, x="occupancy_rate", y=measure, color=type_col, opacity=0.45,
        hover_name="provider_name", hover_data={"state": True, "certified_beds": True},
        labels={"occupancy_rate": "Occupancy rate", measure: f"{measure_label} HPRD", type_col: type_dimension},
        render_mode="webgl", height=520,
    )
    clean = f[["occupancy_rate", measure]].dropna()
    if len(clean) > 2:
        slope, intercept = np.polyfit(clean["occupancy_rate"], clean[measure], 1)
        xs = np.linspace(clean["occupancy_rate"].min(), clean["occupancy_rate"].max(), 50)
        fig.add_trace(go.Scatter(x=xs, y=slope * xs + intercept, mode="lines",
                                 name="Trend (all facilities)", line=dict(color="black", width=2)))
    fig.update_xaxes(tickformat=".0%")
    st.plotly_chart(fig, width="stretch")
    st.caption(
        f"r = {r:.3f} ({describe_correlation(r)}). Values near 0 mean occupancy tells you little about "
        "staffing; a negative value means fuller facilities have fewer nurse hours per resident."
    )
    st.caption(PBJ_SOURCE)

    st.markdown("**Correlation by state** (states with at least 20 facilities)")
    by_state = (
        f.groupby("state")
         .apply(lambda g: pd.Series({"facilities": len(g), "r": g["occupancy_rate"].corr(g[measure])}),
                include_groups=False)
         .reset_index()
    )
    by_state = by_state[by_state["facilities"] >= 20].sort_values("r")
    fig2 = px.bar(by_state, x="state", y="r", color="r", color_continuous_scale="RdBu", range_color=(-1, 1),
                  labels={"r": "Correlation (r)", "state": "State"}, height=380)
    fig2.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig2, width="stretch")


# --- Tab 2: agency nurse hours (overtime proxy) --------------------------------

with tab2:
    st.subheader("Which facilities rely most on extra nurse hours?")
    st.info(
        "**Why this isn't labeled overtime:** overtime means hours over 40 per nurse per week, which requires "
        "hours for each individual nurse. Public CMS data reports hours per facility per day, with no headcount. "
        "The closest public signal is **contract (agency) RN + LPN hours**: hours a facility's own staff did not "
        "cover. Weekend staffing drop and turnover are shown alongside as further signs of strain.",
        icon="ℹ️",
    )
    top_n = st.slider("Number of facilities to show", 10, 50, 20, step=5)

    c1, c2, c3 = st.columns(3)
    c1.metric("Median contract share of licensed hours", f"{f['contract_licensed_share'].median():.1%}")
    c2.metric("Facilities using no contract nurses", f"{(f['contract_licensed_hours_per_week'] == 0).mean():.0%}")
    c3.metric("Average weekend staffing drop", f"{f['weekend_licensed_drop_pct'].mean():.1%}")

    ranked = f.sort_values("contract_licensed_hours_per_week", ascending=False).head(top_n).copy()
    ranked["facility"] = ranked["provider_name"].str.title() + " (" + ranked["state"] + ")"
    fig = px.bar(
        ranked.iloc[::-1], x="contract_licensed_hours_per_week", y="facility", orientation="h",
        color="contract_licensed_share", color_continuous_scale="Oranges",
        labels={"contract_licensed_hours_per_week": "Contract RN + LPN hours per week", "facility": "",
                "contract_licensed_share": "Contract share"},
        height=max(400, 24 * len(ranked)),
    )
    fig.update_coloraxes(colorbar_tickformat=".0%")
    st.plotly_chart(fig, width="stretch")

    table = ranked[[
        "contract_licensed_hours_rank", "provider_name", "state", type_col, "certified_beds",
        "contract_licensed_hours_per_week", "contract_licensed_share", "licensed_nurse_hours_per_week",
        "licensed_nurse_fte_equivalent", "weekend_licensed_drop_pct", "total_nursing_turnover_pct",
    ]]
    st.dataframe(
        table, hide_index=True, width="stretch",
        column_config={
            "contract_licensed_hours_rank": st.column_config.NumberColumn("National rank", format="%d"),
            "provider_name": "Facility",
            "state": "State",
            type_col: type_dimension,
            "certified_beds": st.column_config.NumberColumn("Beds", format="%d"),
            "contract_licensed_hours_per_week": st.column_config.NumberColumn("Contract hrs/week", format="%.0f"),
            "contract_licensed_share": st.column_config.NumberColumn("Contract share", format="percent"),
            "licensed_nurse_hours_per_week": st.column_config.NumberColumn("All licensed hrs/week", format="%.0f"),
            "licensed_nurse_fte_equivalent": st.column_config.NumberColumn("FTE equivalents", format="%.1f",
                help="Weekly hours ÷ 40. Full-time nurses' worth of hours, not a headcount."),
            "weekend_licensed_drop_pct": st.column_config.NumberColumn("Weekend drop", format="percent"),
            "total_nursing_turnover_pct": st.column_config.NumberColumn("Nurse turnover %", format="%.1f"),
        },
    )
    st.caption(PBJ_SOURCE + " Turnover: CMS Care Compare.")


# --- Tab: nurse hours ------------------------------------------------------------

with tab_hours:
    st.subheader("Nurse staffing hours")
    st.caption("Actual hours worked, from CMS Payroll-Based Journal (PBJ) daily payroll records. "
               "Weekly figures are the quarter's daily hours averaged to a 7-day week.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("RN hours per week", f"{f['rn_hours_per_week'].sum():,.0f}")
    c2.metric("LPN hours per week", f"{f['lpn_hours_per_week'].sum():,.0f}")
    c3.metric("Licensed nurse FTE equivalents", f"{f['licensed_nurse_fte_equivalent'].sum():,.0f}",
              help="Weekly RN + LPN hours ÷ 40. Full-time nurses' worth of hours, not a headcount.")
    total_licensed = f["licensed_nurse_hours_per_week"].sum()
    c4.metric("Worked by contract staff",
              f"{f['contract_licensed_hours_per_week'].sum() / total_licensed:.1%}" if total_licensed else "n/a")

    by_state_hours = (
        f.groupby("state")[["employee_licensed_hours_per_week", "contract_licensed_hours_per_week"]]
         .sum().reset_index()
         .melt(id_vars="state", var_name="staff", value_name="hours")
    )
    by_state_hours["staff"] = by_state_hours["staff"].map({
        "employee_licensed_hours_per_week": "Employee",
        "contract_licensed_hours_per_week": "Contract / agency",
    })
    order = (by_state_hours.groupby("state")["hours"].sum().sort_values(ascending=False).index.tolist())
    fig = px.bar(by_state_hours, x="state", y="hours", color="staff", category_orders={"state": order},
                 color_discrete_map={"Employee": "#4C78A8", "Contract / agency": "#F58518"},
                 labels={"hours": "RN + LPN hours per week", "state": "State", "staff": ""},
                 title="Licensed nurse hours per week by state: employee vs. contract", height=420)
    st.plotly_chart(fig, width="stretch")

    by_type_hours = (
        f.groupby(type_col)[["rn_hours_per_week", "lpn_hours_per_week"]].mean().reset_index()
         .melt(id_vars=type_col, var_name="role", value_name="hours")
    )
    by_type_hours["role"] = by_type_hours["role"].map({"rn_hours_per_week": "RN", "lpn_hours_per_week": "LPN"})
    fig2 = px.bar(by_type_hours, x=type_col, y="hours", color="role", barmode="group", text_auto=",.0f",
                  labels={type_col: type_dimension, "hours": "Average hours per week per facility", "role": ""},
                  title=f"Average weekly RN and LPN hours per facility, by {type_dimension.lower()}", height=380)
    st.plotly_chart(fig2, width="stretch")

    st.markdown("**Facility detail** (click a column header to sort)")
    st.dataframe(
        f[["provider_name", "state", type_col, "certified_beds", "avg_daily_census", "rn_hours_per_week",
           "lpn_hours_per_week", "licensed_nurse_hours_per_week", "employee_licensed_hours_per_week",
           "contract_licensed_hours_per_week", "licensed_nurse_fte_equivalent"]]
         .sort_values("licensed_nurse_hours_per_week", ascending=False),
        hide_index=True, width="stretch", height=420,
        column_config={
            "provider_name": "Facility",
            "state": "State",
            type_col: type_dimension,
            "certified_beds": st.column_config.NumberColumn("Beds", format="%d"),
            "avg_daily_census": st.column_config.NumberColumn("Avg residents", format="%.0f"),
            "rn_hours_per_week": st.column_config.NumberColumn("RN hrs/week", format="%.0f"),
            "lpn_hours_per_week": st.column_config.NumberColumn("LPN hrs/week", format="%.0f"),
            "licensed_nurse_hours_per_week": st.column_config.NumberColumn("RN + LPN hrs/week", format="%.0f"),
            "employee_licensed_hours_per_week": st.column_config.NumberColumn("Employee hrs/week", format="%.0f"),
            "contract_licensed_hours_per_week": st.column_config.NumberColumn("Contract hrs/week", format="%.0f"),
            "licensed_nurse_fte_equivalent": st.column_config.NumberColumn("FTE equivalents", format="%.1f"),
        },
    )
    st.caption(PBJ_SOURCE)


# --- Tab: strain vs turnover ---------------------------------------------------

with tab_turnover:
    st.subheader("Is nurse turnover higher where staffing is more strained?")
    st.info(
        "**What this compares:** overtime itself is not in public CMS data (hours are reported per facility, "
        "not per nurse). This view uses the strain measures that are available, chiefly **reliance on agency "
        "nurses**, and compares them with **nurse turnover** from Care Compare. Two cautions: turnover covers "
        "Care Compare's 12-month reporting window, not exactly the PBJ quarter; and correlation does not show "
        "direction. High turnover can *force* agency use as much as strain can cause turnover.",
        icon="ℹ️",
    )
    STRAIN = {
        "Contract share of licensed hours": "contract_licensed_share",
        "Contract hours per resident day": "contract_licensed_hprd",
        "Weekend staffing drop": "weekend_licensed_drop_pct",
    }
    TURNOVER = {"Total nursing staff turnover": "total_nursing_turnover_pct", "RN turnover": "rn_turnover_pct"}
    c1, c2 = st.columns(2)
    strain_label = c1.selectbox("Strain measure", list(STRAIN))
    turnover_label = c2.selectbox("Turnover measure", list(TURNOVER))
    x, y = STRAIN[strain_label], TURNOVER[turnover_label]

    t = f[[x, y, "provider_name", "state", type_col]].dropna().copy()
    # Care Compare reports turnover as percentage points (e.g. 45.2); scale if given as a fraction
    if len(t) and t[y].max() <= 1.5:
        t[y] = t[y] * 100

    if len(t) < 10:
        st.warning("Too few facilities in the current selection to compare. Select more states or clear the filter.")
    else:

        pearson = t[x].corr(t[y])
        spearman = t[x].rank().corr(t[y].rank())  # Spearman = Pearson on ranks
        m1, m2, m3 = st.columns(3)
        m1.metric("Facilities with both measures", f"{len(t):,}")
        m2.metric("Rank correlation (Spearman)", f"{spearman:.3f}", describe_correlation(spearman), delta_color="off",
                  help="Robust to skewed data and outliers; the primary measure here.")
        m3.metric("Linear correlation (Pearson)", f"{pearson:.3f}", describe_correlation(pearson), delta_color="off")

        # Bands give the plain-language answer: average turnover as strain increases
        if x == "contract_licensed_share":
            t["band"] = pd.cut(t[x], bins=[-0.001, 0, 0.05, 0.15, 1.0],
                               labels=["None (0%)", "Low (<5%)", "Moderate (5-15%)", "High (15%+)"])
        else:
            t["band"] = pd.qcut(t[x].rank(method="first"), 5,
                                labels=["Lowest 20%", "20-40%", "40-60%", "60-80%", "Highest 20%"])
        bands = t.groupby("band", observed=True).agg(facilities=(y, "size"), avg_turnover=(y, "mean")).reset_index()
        fig = px.bar(bands, x="band", y="avg_turnover", text_auto=".1f", hover_data=["facilities"],
                     labels={"band": strain_label, "avg_turnover": f"Average {turnover_label.lower()} (%)"},
                     title=f"Average {turnover_label.lower()} by level of {strain_label.lower()}", height=400)
        st.plotly_chart(fig, width="stretch")
        if len(bands) >= 2:
            low, high = bands.iloc[0], bands.iloc[-1]
            diff = high["avg_turnover"] - low["avg_turnover"]
            st.markdown(
                f"Facilities in the **{high['band']}** group average **{high['avg_turnover']:.1f}%** "
                f"{turnover_label.lower()}, versus **{low['avg_turnover']:.1f}%** in the **{low['band']}** group: "
                f"**{abs(diff):.1f} points {'higher' if diff > 0 else 'lower'}**."
            )

        by_type_band = (t.groupby([type_col, "band"], observed=True)[y].mean().reset_index())
        fig2 = px.bar(by_type_band, x="band", y=y, color=type_col, barmode="group",
                      labels={"band": strain_label, y: f"Average {turnover_label.lower()} (%)", type_col: type_dimension},
                      title=f"Same comparison, split by {type_dimension.lower()}", height=380)
        st.plotly_chart(fig2, width="stretch")

        fig3 = px.scatter(t, x=x, y=y, color=type_col, opacity=0.35, render_mode="webgl",
                          hover_name="provider_name", hover_data={"state": True},
                          labels={x: strain_label, y: f"{turnover_label} (%)", type_col: type_dimension},
                          title="Every facility", height=420)
        if x != "contract_licensed_hprd":
            fig3.update_xaxes(tickformat=".0%")
        st.plotly_chart(fig3, width="stretch")
        st.caption(PBJ_SOURCE + " Turnover: CMS Nursing Home Care Compare, October 2024.")


# --- Tab 3: staffing by state and type -----------------------------------------

with tab3:
    st.subheader("Average staffing levels by state and facility type")
    measure_label3 = st.radio("Staffing measure", list(STAFFING_MEASURES), horizontal=True, key="t3_measure")
    avg_col = "avg_" + STAFFING_MEASURES[measure_label3]

    s = by_state_type[(by_state_type["calendar_quarter"] == quarter)
                      & (by_state_type["facility_type_dimension"] == type_dimension)]

    national = s[s["state"] == "All states"].sort_values(avg_col, ascending=False)
    fig = px.bar(national, x="facility_type", y=avg_col, text_auto=".2f", hover_data=["facility_count"],
                 labels={"facility_type": type_dimension, avg_col: f"Average {measure_label3} HPRD"},
                 title=f"National average by {type_dimension.lower()}", height=380)
    st.plotly_chart(fig, width="stretch")

    states_only = s[s["state"] != "All states"]
    if selected_states:
        states_only = states_only[states_only["state"].isin(selected_states)]
    heat = states_only.pivot_table(index="state", columns="facility_type", values=avg_col)
    if not heat.empty:
        fig2 = px.imshow(heat, text_auto=".2f", aspect="auto", color_continuous_scale="Blues",
                         labels={"x": type_dimension, "y": "State", "color": "HPRD"},
                         title=f"Average {measure_label3} HPRD by state and {type_dimension.lower()}",
                         height=max(420, 22 * len(heat)))
        st.plotly_chart(fig2, width="stretch")
        st.caption("Blank cells: no valid facilities of that type in the state. "
                   "Averages count each facility once.")

    st.caption(PBJ_SOURCE)
    st.download_button(
        "Download this table (CSV)",
        s[["state", "facility_type", "facility_count", avg_col, "avg_occupancy_rate"]].to_csv(index=False),
        file_name=f"staffing_by_state_and_{type_col}_{quarter}.csv", mime="text/csv",
    )


# --- Tab 4: short- vs long-stay trends -----------------------------------------

with tab4:
    st.subheader("Short-stay vs. long-stay quality trends")
    st.info(
        "**About length of stay:** the CMS files do not include length of stay. This view trends quality "
        "measure scores for **short-stay** and **long-stay** residents across the four quarters of the "
        "measure period, the closest available data. Whether a higher score is better depends on the measure.",
        icon="ℹ️",
    )
    q = quality.copy()
    q["quarter"] = np.where(q["quarter_start"].notna(),
                            pd.to_datetime(q["quarter_start"]).dt.to_period("Q").astype(str),
                            "Q" + q["quarter_number"].astype(str))

    stay_types = sorted(q["resident_type"].dropna().unique())
    stay = st.radio("Resident type", stay_types, horizontal=True)
    qs = q[q["resident_type"] == stay]

    first = qs[qs["quarter_number"] == 1][["measure_code", "measure_description", "avg_score"]]
    last = qs[qs["quarter_number"] == 4][["measure_code", "avg_score"]]
    change = first.merge(last, on="measure_code", suffixes=("_q1", "_q4"))
    change["pct_change"] = (change["avg_score_q4"] - change["avg_score_q1"]) / change["avg_score_q1"].abs()
    change = change.sort_values("pct_change")
    fig = px.bar(change, x="pct_change", y="measure_description", orientation="h",
                 color="pct_change", color_continuous_scale="RdBu", range_color=(-0.5, 0.5),
                 labels={"pct_change": "Change, first to fourth quarter", "measure_description": ""},
                 title=f"{stay} measures: change across the measure period",
                 height=max(380, 40 * len(change)))
    fig.update_xaxes(tickformat=".0%")
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, width="stretch")

    measure_names = qs.drop_duplicates("measure_code").set_index("measure_code")["measure_description"]
    if len(measure_names):
        pick = st.selectbox("Measure detail", measure_names.index, format_func=lambda c: measure_names[c])
        detail = qs[qs["measure_code"] == pick].sort_values("quarter_number")
        fig2 = px.line(detail, x="quarter", y=["avg_score", "median_score"], markers=True,
                       labels={"value": "Score", "quarter": "Quarter", "variable": ""},
                       title=measure_names[pick], height=380)
        st.plotly_chart(fig2, width="stretch")


# --- Tab 5: daily trends -------------------------------------------------------

with tab5:
    st.subheader("Daily occupancy and staffing across the quarter")
    trend_state = selected_states[0] if len(selected_states) == 1 else "All states"
    if len(selected_states) > 1:
        st.caption("Several states selected: showing the national trend. Select one state to see its trend.")
    d = daily[daily["state"] == trend_state].sort_values("work_date")
    weekend = d[d["is_weekend"]]

    for col, label, fmt in (("occupancy_rate", "Occupancy rate", ".1%"),
                            ("licensed_nurse_hprd", "Licensed nurse (RN + LPN) HPRD", ".2f")):
        fig = px.line(d, x="work_date", y=col, labels={"work_date": "", col: label},
                      title=f"{label}: {trend_state}", height=320)
        fig.add_trace(go.Scatter(x=weekend["work_date"], y=weekend[col], mode="markers",
                                 name="Weekend", marker=dict(color="crimson", size=6)))
        fig.update_yaxes(tickformat=fmt)
        st.plotly_chart(fig, width="stretch")
    st.caption("Red points are weekends. Licensed nurse HPRD typically dips on weekends. " + PBJ_SOURCE)


# --- Tab: quality of care ------------------------------------------------------

with tab_quality:
    st.subheader("Quality of care: readmissions, complaints, and staffing")
    st.info(
        "**Time periods differ, so read these as associations:** staffing is PBJ "
        f"{quarter}; the claims rehospitalization measure covers its own 12-month window; the SNF VBP readmission "
        "rate covers fiscal year 2022; complaints are Care Compare's substantiated complaints from recent inspection "
        "cycles. Correlation does not show cause. Patient satisfaction is not shown because nursing home Care Compare "
        "has no satisfaction survey, and readmissions by diagnosis are not in public data.",
        icon="ℹ️",
    )
    READMIT = {
        "Short-stay rehospitalization (claims, risk-adjusted)": "short_stay_rehosp_pct",
        "SNF VBP 30-day readmission rate (FY 2022)": "vbp_readmission_rate_pct",
    }
    c = care[care["calendar_quarter"] == quarter]
    if selected_states:
        c = c[c["state"].isin(selected_states)]

    # 1. Readmissions by facility and state
    st.markdown("### 30-day readmissions")
    readmit_label = st.radio("Readmission measure", list(READMIT), horizontal=True, key="q_readmit")
    rcol = READMIT[readmit_label]
    has = c.dropna(subset=[rcol])
    k1, k2, k3 = st.columns(3)
    k1.metric("Facilities with a rate", f"{len(has):,}")
    k2.metric("Median rate", f"{has[rcol].median():.1f}%" if len(has) else "n/a")
    k3.metric("Middle 50% of facilities",
              f"{has[rcol].quantile(.25):.1f}% to {has[rcol].quantile(.75):.1f}%" if len(has) else "n/a")
    st.caption("Lower is better. Both measures are percentages of residents readmitted to a hospital.")

    by_state_r = (has.groupby("state").agg(facilities=(rcol, "size"), rate=(rcol, "mean")).reset_index()
                     .sort_values("rate"))
    fig = px.bar(by_state_r, x="state", y="rate", color="rate", color_continuous_scale="Reds",
                 hover_data=["facilities"], labels={"state": "State", "rate": "Average rate (%)"},
                 title=f"Average {readmit_label.split(' (')[0].lower()} by state", height=380)
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, width="stretch", key="q_state")

    top_n_q = st.slider("Facilities to list", 10, 50, 20, step=5, key="q_topn")
    st.markdown(f"**Highest {readmit_label.split(' (')[0].lower()} rates**")
    st.dataframe(
        has.sort_values(rcol, ascending=False).head(top_n_q)[
            ["provider_name", "state", type_col, "certified_beds", rcol, "licensed_nurse_hprd", "overall_rating"]],
        hide_index=True, width="stretch",
        column_config={
            "provider_name": "Facility", "state": "State", type_col: type_dimension,
            "certified_beds": st.column_config.NumberColumn("Beds", format="%d"),
            rcol: st.column_config.NumberColumn("Readmission rate (%)", format="%.1f"),
            "licensed_nurse_hprd": st.column_config.NumberColumn("Licensed nurse HPRD", format="%.2f"),
            "overall_rating": st.column_config.NumberColumn("Star rating", format="%d"),
        },
    )

    # 2. Residents per nurse vs complaints
    st.markdown("### Residents per nurse vs. complaint rate")
    st.caption("Residents per licensed nurse on duty = 24 ÷ licensed-nurse HPRD (the average number of residents "
               "each RN or LPN covers at any moment). Complaint rate = substantiated complaints per 100 residents, "
               "so large and small facilities compare fairly.")
    correlation_bands(c, "residents_per_licensed_nurse", "complaints_per_100_residents",
                      "residents per licensed nurse", "complaints per 100 residents", key="q_complaints")

    # 3. Staffing vs readmissions
    st.markdown("### Nurse staffing vs. readmissions")
    staff_label = st.radio("Staffing measure", list(STAFFING_MEASURES)[:2] + [list(STAFFING_MEASURES)[3]],
                           horizontal=True, key="q_staff")
    correlation_bands(c, STAFFING_MEASURES[staff_label], rcol,
                      f"{staff_label.split(' (')[0].lower()} HPRD", "readmission rate (%)", key="q_readmit_staff")
    st.caption("A negative correlation would mean facilities with more nurse hours per resident have fewer "
               f"readmissions. Readmission measure shown: {readmit_label} (change it at the top of this tab). "
               + PBJ_SOURCE + " Readmissions and complaints: CMS Care Compare and SNF VBP, October 2024 release.")


# --- Footer ----------------------------------------------------------------------

with st.expander("Definitions"):
    st.markdown(f"""
- **HPRD (hours per resident day):** total nurse hours ÷ total resident-days, the CMS method.
- **RN:** director of nursing + RN admin + direct-care RN. **LPN:** LPN admin + direct-care LPN.
  **Licensed nurses:** RN + LPN. **All nursing staff:** licensed nurses + CNAs, aides in training, and medication aides.
- **Occupancy rate:** average daily residents ÷ certified beds.
- **Contract hours:** hours worked by agency or contract staff rather than employees.
- **Weekly hours:** the quarter's daily PBJ hours averaged to a 7-day week (HPRD × average residents × 7).
- **Turnover:** Care Compare's nursing staff and RN turnover over its 12-month reporting period.
- **Facility type:** ownership (for profit / nonprofit / government), provider type (Medicare / Medicaid / both),
  or setting (hospital-based / freestanding).
- **Residents per licensed nurse:** 24 ÷ licensed-nurse HPRD, the residents each RN or LPN covers at any moment.
- **Complaint rate:** Care Compare substantiated complaints per 100 average daily residents.
- **Short-stay rehospitalization:** CMS claims measure 521, the risk-adjusted percentage of short-stay residents
  readmitted to a hospital after a nursing home admission. **SNF VBP readmission rate:** the risk-standardized
  30-day readmission rate from the SNF Value-Based Purchasing program, FY 2022 performance period.
- **Included facilities:** at least 60 reported days, occupancy 0-125%, total nurse HPRD 0.5-12
  (42 of about 14,500 excluded).

Full definitions and design decisions: [{REPO_URL}]({REPO_URL})
""")
