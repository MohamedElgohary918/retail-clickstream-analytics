import time
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session

# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Retail Clickstream Analytics",
    page_icon="🛒",
    layout="wide",
)

session = get_active_session()
DB_SCHEMA = "RETAIL_CLICKSTREAM_DB.MARTS"

# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------
st.sidebar.title("⚙️ Dashboard Controls")

st.sidebar.subheader("Live Refresh")
auto_refresh = st.sidebar.toggle("Live Auto-Refresh", value=False)
refresh_seconds = st.sidebar.select_slider(
    "Refresh interval (seconds)",
    options=[10, 30, 60, 120, 300],
    value=30,
    disabled=not auto_refresh,
)
if st.sidebar.button("🔄 Refresh now"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.divider()
st.sidebar.subheader("Data Quality Filter")
exclude_quality = st.sidebar.toggle(
    "Exclude sessions with data-quality issues",
    value=False,
    help="Removes sessions flagged for zero price, missing category, invalid price, "
         "invalid session ID or missing timestamps. Also removes inventory rows "
         "with invalid stock/reorder values or missing origin.",
)
exclude_bots = st.sidebar.toggle(
    "Exclude suspected bot sessions",
    value=False,
    help="Removes sessions with burst activity (25+ events in 5 seconds or less) "
         "or more than 100 events.",
)

st.session_state["exclude_quality"] = exclude_quality
st.session_state["exclude_bots"] = exclude_bots


def session_filter_sql(alias: str = "") -> str:
    """WHERE fragment for views exposing has_quality_issue / is_suspected_bot."""
    p = f"{alias}." if alias else ""
    clauses = ["1=1"]
    if st.session_state["exclude_quality"]:
        clauses.append(f"{p}has_quality_issue = FALSE")
    if st.session_state["exclude_bots"]:
        clauses.append(f"{p}is_suspected_bot = FALSE")
    return " AND ".join(clauses)


@st.cache_data(ttl=60, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    return session.sql(sql).to_pandas()


# ---------------------------------------------------------------------------
# KPI helpers (Step 3)
# ---------------------------------------------------------------------------
def num(row, col) -> float:
    """Safe numeric read from a one-row DataFrame (NULL -> 0)."""
    val = row[col]
    return 0.0 if pd.isna(val) else float(val)


def pct(numerator: float, denominator: float) -> float:
    return (numerator / denominator * 100) if denominator else 0.0


def compact(n: float, prefix: str = "") -> str:
    """Compact display so values never get truncated: 1.2K, 3.45M, 1.20B."""
    a = abs(n)
    if a >= 1e9:
        return f"{prefix}{n / 1e9:,.2f}B"
    if a >= 1e6:
        return f"{prefix}{n / 1e6:,.2f}M"
    if a >= 1e3:
        return f"{prefix}{n / 1e3:,.1f}K"
    return f"{prefix}{n:,.0f}"


def load_kpis() -> dict:
    where = session_filter_sql()

    # Session-based KPIs (respect quality + bot filters)
    s = run_query(f"""
        SELECT
            COUNT(*)                                                AS total_sessions,
            SUM(reached_cart)                                       AS cart_sessions,
            SUM(IFF(reached_cart = 1 AND reached_purchase = 0, 1, 0)) AS abandoned_sessions,
            SUM(reached_purchase)                                   AS purchase_sessions
        FROM {DB_SCHEMA}.VW_SESSION_SUMMARY
        WHERE {where}
    """).iloc[0]

    # Order / revenue KPIs
    r = run_query(f"""
        SELECT
            SUM(gross_revenue)            AS revenue,
            COUNT(DISTINCT order_id)      AS total_orders,
            SUM(total_cogs)               AS cogs,
            SUM(net_profit)               AS net_profit
        FROM {DB_SCHEMA}.VW_KPI_REVENUE_AND_ROI
    """).iloc[0]

    # Semi-additive: only the CURRENT SCD2 snapshot may be summed across products
    inv_where = "is_current = TRUE"
    if st.session_state["exclude_quality"]:
        inv_where += " AND has_inventory_quality_issue = FALSE"
    i = run_query(f"""
        SELECT SUM(stock_quantity) AS units_in_stock
        FROM {DB_SCHEMA}.VW_INVENTORY_SCD2_HEALTH
        WHERE {inv_where}
    """).iloc[0]

    total_sessions = num(s, "TOTAL_SESSIONS")
    cart_sessions = num(s, "CART_SESSIONS")
    abandoned = num(s, "ABANDONED_SESSIONS")
    purchases = num(s, "PURCHASE_SESSIONS")
    revenue = num(r, "REVENUE")
    cogs = num(r, "COGS")
    profit = num(r, "NET_PROFIT")

    return {
        # additive
        "revenue": revenue,
        "sessions": total_sessions,
        "orders": num(r, "TOTAL_ORDERS"),
        # semi-additive
        "stock": num(i, "UNITS_IN_STOCK"),
        # non-additive (always recomputed from summed components)
        "abandonment_pct": pct(abandoned, cart_sessions),
        "conversion_pct": pct(purchases, total_sessions),
        "roi_pct": pct(profit, cogs),
        "margin_pct": pct(profit, revenue),
    }


# ---------------------------------------------------------------------------
# Tab 1 renderers (Step 4)
# ---------------------------------------------------------------------------
def lower_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    return df


def render_funnel():
    st.markdown("#### Funnel Conversion & Drop-off")
    where = session_filter_sql()
    f = run_query(f"""
        SELECT
            COUNT_IF(reached_view = 1 OR reached_cart = 1 OR reached_purchase = 1) AS viewed,
            COUNT_IF(reached_cart = 1 OR reached_purchase = 1)                     AS carted,
            SUM(reached_purchase)                                                  AS purchased,
            SUM(reached_remove_from_cart)                                          AS removed,
            COUNT_IF(reached_purchase = 1 AND reached_cart = 0)                    AS purchase_no_cart,
            COUNT_IF(reached_purchase = 1 AND reached_view = 0)                    AS purchase_no_view
        FROM {DB_SCHEMA}.VW_SESSION_SUMMARY
        WHERE {where}
    """).iloc[0]

    stages = ["Viewed", "Cart or later", "Purchased"]
    values = [num(f, "VIEWED"), num(f, "CARTED"), num(f, "PURCHASED")]

    left, right = st.columns([1, 1])
    with left:
        fig = go.Figure(go.Funnel(
            y=stages,
            x=values,
            texttemplate="%{value:,.0f}<br>%{percentPrevious:.1%}",
            marker={"color": ["#4C78A8", "#F58518", "#54A24B"]},
        ))
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=340)
        st.plotly_chart(fig)
    with right:
        rows = []
        for idx, (stage, v) in enumerate(zip(stages, values)):
            prev = values[idx - 1] if idx else v
            rows.append({
                "Stage": stage,
                "Sessions": f"{v:,.0f}",
                "% prev": f"{pct(v, prev):.1f}%",
                "Drop-off": f"{100 - pct(v, prev):.1f}%" if idx else "—",
                "% total": f"{pct(v, values[0]):.1f}%",
            })
        st.dataframe(pd.DataFrame(rows), hide_index=True)
        removed = num(f, "REMOVED")
        st.caption(
            f"➖ {removed:,.0f} sessions removed an item from the cart. "
            f"⚠️ {num(f, 'PURCHASE_NO_CART'):,.0f} purchase sessions have no cart event "
            f"and {num(f, 'PURCHASE_NO_VIEW'):,.0f} have no view event."
        )
    st.caption(
        "A session counts at a stage if it reached that stage **or any later one** "
        "(a purchase implies the shopper passed through the cart), so the funnel can only shrink. "
        "The warning line shows how many purchase sessions are missing earlier events in the raw data."
    )


def render_abandonment():
    st.markdown("#### Cart Abandonment by Category / Brand")
    c1, c2, c3 = st.columns([1.3, 1, 1])
    group_by = c1.radio("Group by", ["Category", "Brand"], horizontal=True)
    top_n = c2.slider("Show top N", 5, 30, 10)
    min_carts = c3.select_slider(
        "Min cart sessions", options=[1, 5, 10, 20, 50, 100], value=10,
        help="Hides tiny groups where a % is statistically meaningless.",
    )
    col = "category_code_label" if group_by == "Category" else "brand_label"
    where = session_filter_sql()

    df = lower_cols(run_query(f"""
        SELECT {col} AS label,
               SUM(cart_sessions)            AS cart_sessions,
               SUM(abandoned_cart_sessions)  AS abandoned_sessions,
               SUM(purchase_sessions)        AS purchase_sessions
        FROM {DB_SCHEMA}.VW_KPI_CATEGORY_ABANDONMENT
        WHERE {where}
        GROUP BY 1
        HAVING SUM(cart_sessions) >= {int(min_carts)}
    """))
    if df.empty:
        st.info("No groups meet the minimum cart-session threshold with the current filters.")
        return

    for c in ["cart_sessions", "abandoned_sessions", "purchase_sessions"]:
        df[c] = df[c].astype(float)
    df["abandonment_pct"] = [pct(a, c) for a, c in zip(df.abandoned_sessions, df.cart_sessions)]
    overall = pct(df.abandoned_sessions.sum(), df.cart_sessions.sum())

    top = df.sort_values("abandonment_pct", ascending=False).head(top_n)
    fig = px.bar(
        top, x="abandonment_pct", y="label", orientation="h",
        color="abandonment_pct", color_continuous_scale="OrRd",
        text=top["abandonment_pct"].map(lambda v: f"{v:.1f}%"),
        hover_data={"cart_sessions": ":,.0f", "abandoned_sessions": ":,.0f",
                    "abandonment_pct": ":.1f"},
    )
    fig.update_yaxes(autorange="reversed", title=None, type="category")
    fig.update_xaxes(title="Cart abandonment %", range=[0, min(100, top.abandonment_pct.max() * 1.2 + 5)])
    fig.add_vline(x=overall, line_dash="dash",
                  annotation_text=f"Average {overall:.1f}%", annotation_position="top")
    fig.update_traces(textposition="outside")
    fig.update_layout(coloraxis_showscale=False, height=max(320, 34 * len(top) + 90),
                      margin=dict(l=10, r=10, t=30, b=10))
    st.plotly_chart(fig)
    st.caption(
        "Abandonment = sessions with a cart event but no purchase ÷ sessions with a cart event, "
        "per " + group_by.lower() + ". A session touching several groups is counted in each of them."
    )


def render_dwell():
    st.markdown("#### Session Dwell-Time Distribution")
    c1, c2, c3 = st.columns(3)
    bin_size = c1.selectbox("Bin width (seconds)", [5, 10, 30, 60], index=1)
    cap = c2.selectbox("Cap at (seconds)", [300, 600, 1800, 3600], index=1,
                       help="Sessions longer than this are grouped into the last bar.")
    drop_zero = c3.checkbox("Exclude single-event (0 s) sessions", value=True,
                            help="One-event sessions have 0 s dwell and would dwarf every other bar.")

    where = session_filter_sql()
    zero_clause = " AND session_dwell_seconds > 0" if drop_zero else ""

    hist = lower_cols(run_query(f"""
        SELECT FLOOR(LEAST(session_dwell_seconds, {int(cap)}) / {int(bin_size)}) * {int(bin_size)} AS bucket,
               COUNT(*) AS sessions
        FROM {DB_SCHEMA}.VW_SESSION_SUMMARY
        WHERE {where}{zero_clause}
        GROUP BY 1
        ORDER BY 1
    """))
    stats = run_query(f"""
        SELECT AVG(session_dwell_seconds)                                            AS avg_s,
               MEDIAN(session_dwell_seconds)                                         AS median_s,
               PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY session_dwell_seconds)    AS p90_s,
               COUNT(*)                                                              AS n_sessions
        FROM {DB_SCHEMA}.VW_SESSION_SUMMARY
        WHERE {where}{zero_clause}
    """).iloc[0]
    zero_n = num(run_query(f"""
        SELECT COUNT_IF(session_dwell_seconds = 0) AS z
        FROM {DB_SCHEMA}.VW_SESSION_SUMMARY WHERE {where}
    """).iloc[0], "Z")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Avg dwell", f"{num(stats, 'AVG_S'):,.0f}s")
    m2.metric("Median dwell", f"{num(stats, 'MEDIAN_S'):,.0f}s")
    m3.metric("90th percentile", f"{num(stats, 'P90_S'):,.0f}s")
    m4.metric("Single-event sessions", f"{zero_n:,.0f}")

    if hist.empty:
        st.info("No sessions match the current filters.")
        return
    fig = px.bar(hist, x="bucket", y="sessions")
    fig.update_traces(width=bin_size * 0.9, marker_color="#4C78A8")
    fig.update_xaxes(title=f"Session dwell time (seconds, {bin_size}s bins; last bar = {cap}s or more)")
    fig.update_yaxes(title="Sessions")
    fig.update_layout(height=340, margin=dict(l=10, r=10, t=10, b=10), bargap=0.05)
    st.plotly_chart(fig)
    st.caption(
        "Dwell time = last event time − first event time within a session. "
        "A spike in the first bins with a very high event count usually indicates bot traffic — "
        "toggle the bot filter in the sidebar to compare."
    )

# ---------------------------------------------------------------------------
# Tab 2 renderers (Step 5)
# ---------------------------------------------------------------------------
def render_revenue_trend():
    st.markdown("#### Revenue, Cost & Net Profit Over Time")

    statuses_df = lower_cols(run_query(f"""
        SELECT DISTINCT COALESCE(order_status, 'Unknown') AS status
        FROM {DB_SCHEMA}.VW_KPI_REVENUE_AND_ROI
        ORDER BY 1
    """))
    all_statuses = statuses_df["status"].tolist() if not statuses_df.empty else []

    c1, c2 = st.columns([1, 2])
    grain = c1.radio("Time grain", ["Daily", "Monthly"], horizontal=True)
    selected_statuses = c2.multiselect(
        "Filter by order status",
        options=all_statuses,
        default=all_statuses,
    )

    if not selected_statuses:
        st.warning("Select at least one order status to display revenue metrics.")
        return

    quoted_statuses = ", ".join(f"'{s.replace(chr(39), chr(39)*2)}'" for s in selected_statuses)
    date_expr = (
        "COALESCE(full_date, DATE_TRUNC('day', order_timestamp)::DATE)"
        if grain == "Daily"
        else "DATE_TRUNC('month', COALESCE(full_date, order_timestamp::DATE))::DATE"
    )

    df = lower_cols(run_query(f"""
        SELECT
            {date_expr}              AS period,
            SUM(gross_revenue)       AS revenue,
            SUM(total_cogs)          AS cogs,
            SUM(net_profit)          AS net_profit,
            COUNT(DISTINCT order_id) AS orders
        FROM {DB_SCHEMA}.VW_KPI_REVENUE_AND_ROI
        WHERE COALESCE(order_status, 'Unknown') IN ({quoted_statuses})
        GROUP BY 1
        HAVING period IS NOT NULL
        ORDER BY 1
    """))

    if df.empty:
        st.info("No order records found for the selected time grain and status.")
        return

    for c in ["revenue", "cogs", "net_profit", "orders"]:
        df[c] = df[c].astype(float)
    df["roi_pct"] = [pct(p, c) for p, c in zip(df.net_profit, df.cogs)]
    df["margin_pct"] = [pct(p, r) for p, r in zip(df.net_profit, df.revenue)]

    fig = go.Figure()
    fig.add_trace(go.Bar(x=df["period"], y=df["revenue"], name="Gross Revenue ($)", marker_color="#4C78A8"))
    fig.add_trace(go.Bar(x=df["period"], y=df["cogs"], name="Total COGS ($)", marker_color="#E45756"))
    fig.add_trace(go.Scatter(
        x=df["period"], y=df["net_profit"], name="Net Profit ($)",
        mode="lines+markers", line=dict(color="#54A24B", width=3),
    ))
    fig.update_layout(
        barmode="group",
        height=360,
        margin=dict(l=10, r=10, t=20, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_title="Period",
        yaxis_title="USD ($)",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "Gross Revenue (`SUM(line_total)`), Total COGS (`SUM(quantity * cost)`), and Net Profit are **additive** across time and dimensions."
    )


def render_category_brand_roi():
    st.markdown("#### Profitability & ROI by Category / Brand")

    c1, c2, c3 = st.columns([1.2, 1.2, 1])
    dim_choice = c1.radio("Analyze by", ["Category", "Brand"], horizontal=True, key="roi_dim")
    rank_metric = c2.selectbox(
        "Rank top groups by",
        ["Net Profit ($)", "Gross Revenue ($)", "ROI (%)", "Gross Margin (%)"],
    )
    top_n = c3.slider("Top N", 5, 25, 10, key="roi_top_n")

    col = "category_code_label" if dim_choice == "Category" else "brand_label"

    df = lower_cols(run_query(f"""
        SELECT
            {col}                    AS label,
            COUNT(DISTINCT order_id) AS orders,
            SUM(quantity)            AS units_sold,
            SUM(gross_revenue)       AS revenue,
            SUM(total_cogs)          AS cogs,
            SUM(net_profit)          AS net_profit
        FROM {DB_SCHEMA}.VW_KPI_REVENUE_AND_ROI
        GROUP BY 1
        HAVING SUM(gross_revenue) > 0
    """))

    if df.empty:
        st.info("No product revenue data available.")
        return

    for c in ["orders", "units_sold", "revenue", "cogs", "net_profit"]:
        df[c] = df[c].astype(float)

    df["roi_pct"] = [pct(p, c) for p, c in zip(df.net_profit, df.cogs)]
    df["margin_pct"] = [pct(p, r) for p, r in zip(df.net_profit, df.revenue)]

    sort_map = {
        "Net Profit ($)": "net_profit",
        "Gross Revenue ($)": "revenue",
        "ROI (%)": "roi_pct",
        "Gross Margin (%)": "margin_pct",
    }
    sort_col = sort_map[rank_metric]
    top = df.sort_values(sort_col, ascending=False).head(top_n)

    left, right = st.columns([1.3, 1])
    with left:
        is_pct = sort_col in ("roi_pct", "margin_pct")
        fig = px.bar(
            top,
            x=sort_col,
            y="label",
            orientation="h",
            color="roi_pct",
            color_continuous_scale="RdYlGn",
            text=top[sort_col].map(lambda v: f"{v:.1f}%" if is_pct else compact(v, "$")),
            hover_data={
                "revenue": ":$,.2f",
                "cogs": ":$,.2f",
                "net_profit": ":$,.2f",
                "roi_pct": ":.1f",
                "margin_pct": ":.1f",
            },
        )
        fig.update_yaxes(autorange="reversed", title=None, type="category")
        fig.update_xaxes(title=rank_metric, range=[0, top[sort_col].max() * 1.25])
        fig.update_traces(textposition="outside")
        fig.update_layout(
            height=max(320, 34 * len(top) + 80),
            margin=dict(l=10, r=10, t=20, b=10),
            coloraxis_colorbar=dict(title="ROI %"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with right:
        display_df = top[["label", "orders", "revenue", "cogs", "net_profit", "roi_pct", "margin_pct"]].copy()
        display_df.columns = [dim_choice, "Orders", "Revenue", "COGS", "Net Profit", "ROI %", "Margin %"]
        display_df["Orders"] = display_df["Orders"].map(lambda v: f"{v:,.0f}")
        display_df["Revenue"] = display_df["Revenue"].map(lambda v: compact(v, "$"))
        display_df["COGS"] = display_df["COGS"].map(lambda v: compact(v, "$"))
        display_df["Net Profit"] = display_df["Net Profit"].map(lambda v: compact(v, "$"))
        display_df["ROI %"] = display_df["ROI %"].map(lambda v: f"{v:.1f}%")
        display_df["Margin %"] = display_df["Margin %"].map(lambda v: f"{v:.1f}%")
        st.dataframe(display_df, hide_index=True, use_container_width=True)

    st.caption(
        "ROI % (`SUM(net_profit) ÷ SUM(total_cogs)`) and Gross Margin % (`SUM(net_profit) ÷ SUM(gross_revenue)`) "
        "are **non-additive** and are always recomputed from summed numerator and denominator per group."
    )


def render_geography_and_status():
    st.markdown("#### Customer Geography & Order Fulfillment Breakdown")

    left, right = st.columns([1.2, 1])

    with left:
        geo = lower_cols(run_query(f"""
            SELECT
                customer_country            AS country,
                COUNT(DISTINCT order_id)    AS orders,
                COUNT(DISTINCT customer_id) AS customers,
                SUM(gross_revenue)          AS revenue,
                SUM(net_profit)             AS net_profit
            FROM {DB_SCHEMA}.VW_KPI_REVENUE_AND_ROI
            GROUP BY 1
            ORDER BY revenue DESC
            LIMIT 15
        """))
        if geo.empty:
            st.info("No customer geography data available.")
        else:
            for c in ["orders", "customers", "revenue", "net_profit"]:
                geo[c] = geo[c].astype(float)
            fig_geo = px.bar(
                geo,
                x="revenue",
                y="country",
                orientation="h",
                color="net_profit",
                color_continuous_scale="Blues",
                text=geo["revenue"].map(lambda v: compact(v, "$")),
                title="Top Countries by Gross Revenue",
                hover_data={"orders": ":,.0f", "customers": ":,.0f", "net_profit": ":$,.2f"},
            )
            fig_geo.update_yaxes(autorange="reversed", title=None, type="category")
            fig_geo.update_xaxes(title="Gross Revenue ($)", range=[0, geo["revenue"].max() * 1.22])
            fig_geo.update_traces(textposition="outside")
            fig_geo.update_layout(
                coloraxis_showscale=False,
                height=360,
                margin=dict(l=10, r=10, t=40, b=10),
            )
            st.plotly_chart(fig_geo, use_container_width=True)

    with right:
        status_df = lower_cols(run_query(f"""
            SELECT
                COALESCE(order_status, 'Unknown') AS order_status,
                COUNT(DISTINCT order_id)          AS orders,
                SUM(gross_revenue)                AS revenue
            FROM {DB_SCHEMA}.VW_KPI_REVENUE_AND_ROI
            GROUP BY 1
            ORDER BY orders DESC
        """))
        if status_df.empty:
            st.info("No order status data available.")
        else:
            for c in ["orders", "revenue"]:
                status_df[c] = status_df[c].astype(float)
            fig_status = px.pie(
                status_df,
                names="order_status",
                values="orders",
                hole=0.45,
                title="Orders Share by Status",
            )
            fig_status.update_traces(textposition="inside", textinfo="percent+label")
            fig_status.update_layout(height=360, margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig_status, use_container_width=True)

# ---------------------------------------------------------------------------
# Tab 3 renderers (Step 6)
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Tab 3 renderers (Step 6 - Polished)
# ---------------------------------------------------------------------------
def render_inventory_scd2():
    st.markdown("#### Inventory Health & SCD Type 2 History")

    inv_where = "is_current = TRUE"
    if st.session_state["exclude_quality"]:
        inv_where += " AND has_inventory_quality_issue = FALSE"

    summary = run_query(f"""
        SELECT
            COUNT(DISTINCT product_id)                                               AS active_products,
            SUM(stock_quantity)                                                      AS total_stock_units,
            COUNT_IF(COALESCE(is_out_of_stock, FALSE))                               AS out_of_stock_cnt,
            COUNT_IF(COALESCE(is_at_or_below_reorder_level, FALSE)
                     AND NOT COALESCE(is_out_of_stock, FALSE))                       AS reorder_alert_cnt,
            (SELECT COUNT(*) FROM {DB_SCHEMA}.VW_INVENTORY_SCD2_HEALTH
             WHERE is_current = FALSE)                                               AS historical_scd2_rows
        FROM {DB_SCHEMA}.VW_INVENTORY_SCD2_HEALTH
        WHERE {inv_where}
    """).iloc[0]

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Active Products", compact(num(summary, "ACTIVE_PRODUCTS")))
    k2.metric("Current Stock (Semi-Additive)", compact(num(summary, "TOTAL_STOCK_UNITS")))
    k3.metric("Out of Stock", f"{num(summary, 'OUT_OF_STOCK_CNT'):,.0f}")
    k4.metric("At / Below Reorder", f"{num(summary, 'REORDER_ALERT_CNT'):,.0f}")
    k5.metric("SCD2 Historical Rows", f"{num(summary, 'HISTORICAL_SCD2_ROWS'):,.0f}")

    left, right = st.columns([1.1, 1.3])

    with left:
        st.markdown("**Current Stock Status by Inventory Origin**")
        origin_df = lower_cols(run_query(f"""
            SELECT
                COALESCE(inventory_origin, 'Missing Origin') AS origin,
                COUNT_IF(NOT COALESCE(is_out_of_stock, FALSE)
                         AND NOT COALESCE(is_at_or_below_reorder_level, FALSE)) AS healthy,
                COUNT_IF(COALESCE(is_at_or_below_reorder_level, FALSE)
                         AND NOT COALESCE(is_out_of_stock, FALSE))              AS reorder_needed,
                COUNT_IF(COALESCE(is_out_of_stock, FALSE))                      AS out_of_stock
            FROM {DB_SCHEMA}.VW_INVENTORY_SCD2_HEALTH
            WHERE {inv_where}
            GROUP BY 1
            ORDER BY (healthy + reorder_needed + out_of_stock) DESC
        """))
        if not origin_df.empty:
            fig_orig = go.Figure()
            fig_orig.add_trace(go.Bar(
                y=origin_df["origin"], x=origin_df["healthy"],
                name="Healthy", orientation="h", marker_color="#54A24B",
            ))
            fig_orig.add_trace(go.Bar(
                y=origin_df["origin"], x=origin_df["reorder_needed"],
                name="At/Below Reorder", orientation="h", marker_color="#F58518",
            ))
            fig_orig.add_trace(go.Bar(
                y=origin_df["origin"], x=origin_df["out_of_stock"],
                name="Out of Stock", orientation="h", marker_color="#E45756",
            ))
            fig_orig.update_layout(
                barmode="stack",
                height=340,
                margin=dict(l=10, r=10, t=35, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                xaxis_title="Products",
            )
            fig_orig.update_yaxes(autorange="reversed", type="category")
            st.plotly_chart(fig_orig, use_container_width=True)

    with right:
        st.markdown("**SCD Type 2 Version History Explorer (`DIM_INVENTORY`)**")
        scd_candidates = lower_cols(run_query(f"""
            SELECT product_id, COUNT(*) AS versions
            FROM {DB_SCHEMA}.VW_INVENTORY_SCD2_HEALTH
            GROUP BY product_id
            ORDER BY versions DESC, product_id
            LIMIT 25
        """))
        if not scd_candidates.empty:
            options = [
                f"Product {int(r.product_id)} ({int(r.versions)} version{'s' if r.versions > 1 else ''})"
                for _, r in scd_candidates.iterrows()
            ]
            selected_label = st.selectbox("Select a product to inspect SCD2 timeline", options)
            selected_pid = int(selected_label.split()[1])

            scd_rows = lower_cols(run_query(f"""
                SELECT
                    product_id,
                    brand_label,
                    stock_quantity,
                    reorder_level,
                    inventory_origin,
                    is_out_of_stock,
                    valid_from,
                    valid_to,
                    is_current
                FROM {DB_SCHEMA}.VW_INVENTORY_SCD2_HEALTH
                WHERE product_id = {selected_pid}
                ORDER BY valid_from DESC
            """))
            st.dataframe(scd_rows, hide_index=True, use_container_width=True)
    st.caption(
        "`DIM_INVENTORY` tracks historical changes using SCD Type 2 (`valid_from`, `valid_to`, `is_current`). "
        "Semi-additive stock totals strictly filter `is_current = TRUE` to prevent double-counting historical snapshots."
    )


def render_quality_gates():
    st.markdown("#### Data Quality Gate Audit (Clickstream & Inventory Flags)")

    cs_q = run_query(f"""
        SELECT
            COUNT(*)                                                  AS total_events,
            COUNT_IF(COALESCE(is_zero_price, FALSE))                  AS zero_price,
            COUNT_IF(COALESCE(is_missing_category_code, FALSE))       AS missing_category,
            COUNT_IF(COALESCE(has_invalid_price, FALSE))              AS invalid_price,
            COUNT_IF(COALESCE(has_invalid_session_id, FALSE))         AS invalid_session_id,
            COUNT_IF(COALESCE(has_missing_timestamps, FALSE))         AS missing_timestamps
        FROM {DB_SCHEMA}.FACT_CLICKSTREAM_EVENTS
    """).iloc[0]

    bot_q = run_query(f"""
        SELECT
            COUNT(*)                            AS total_sessions,
            COUNT_IF(is_suspected_bot = TRUE)   AS bot_sessions,
            COUNT_IF(has_quality_issue = TRUE)  AS dirty_sessions
        FROM {DB_SCHEMA}.VW_SESSION_SUMMARY
    """).iloc[0]

    inv_q = run_query(f"""
        SELECT
            COUNT(*)                                                  AS total_inv_rows,
            COUNT_IF(COALESCE(has_invalid_stock_quantity, FALSE))     AS invalid_stock_qty,
            COUNT_IF(COALESCE(has_invalid_reorder_level, FALSE))      AS invalid_reorder_lvl,
            COUNT_IF(COALESCE(is_missing_inventory_origin, FALSE))    AS missing_origin
        FROM {DB_SCHEMA}.DIM_INVENTORY
    """).iloc[0]

    total_ev = max(1.0, num(cs_q, "TOTAL_EVENTS"))
    total_sess = max(1.0, num(bot_q, "TOTAL_SESSIONS"))
    total_inv = max(1.0, num(inv_q, "TOTAL_INV_ROWS"))

    rules = [
        ("Clickstream", "Missing Category", "is_missing_category_code", num(cs_q, "MISSING_CATEGORY"), total_ev),
        ("Clickstream", "Zero Price", "is_zero_price", num(cs_q, "ZERO_PRICE"), total_ev),
        ("Clickstream", "Invalid Price", "has_invalid_price", num(cs_q, "INVALID_PRICE"), total_ev),
        ("Clickstream", "Invalid Session ID", "has_invalid_session_id", num(cs_q, "INVALID_SESSION_ID"), total_ev),
        ("Clickstream", "Missing Timestamps", "has_missing_timestamps", num(cs_q, "MISSING_TIMESTAMPS"), total_ev),
        ("Sessions", "Suspected Bot Burst", "is_suspected_bot", num(bot_q, "BOT_SESSIONS"), total_sess),
        ("Inventory", "Invalid Stock Qty", "has_invalid_stock_quantity", num(inv_q, "INVALID_STOCK_QTY"), total_inv),
        ("Inventory", "Invalid Reorder Level", "has_invalid_reorder_level", num(inv_q, "INVALID_REORDER_LVL"), total_inv),
        ("Inventory", "Missing Origin", "is_missing_inventory_origin", num(inv_q, "MISSING_ORIGIN"), total_inv),
    ]

    q_df = pd.DataFrame([
        {
            "Domain": domain,
            "Short Rule": short_label,
            "Quality Rule": f"{short_label} ({col_name})",
            "Flagged Rows": int(flagged),
            "Total Checked": int(total),
            "Flagged %": round(pct(flagged, total), 2),
            "Gate Status": "✅ PASS" if flagged == 0 else "⚠️ FLAGGED",
        }
        for domain, short_label, col_name, flagged, total in rules
    ])

    left, right = st.columns([1.15, 1.25])
    with left:
        st.markdown("**Anomaly & Quality Rule Violation Rate (%)**")
        fig_q = px.bar(
            q_df,
            x="Flagged %",
            y="Short Rule",
            color="Domain",
            orientation="h",
            text=q_df["Flagged %"].map(lambda v: f"{v:.2f}%"),
        )
        fig_q.update_yaxes(autorange="reversed", title=None)
        fig_q.update_xaxes(title="% of Records Flagged", range=[0, max(5.0, q_df["Flagged %"].max() * 1.28)])
        fig_q.update_traces(textposition="outside")
        fig_q.update_layout(
            height=360,
            margin=dict(l=10, r=20, t=35, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, title=None),
        )
        st.plotly_chart(fig_q, use_container_width=True)

    with right:
        display_q = q_df[["Domain", "Quality Rule", "Flagged Rows", "Total Checked", "Flagged %", "Gate Status"]]
        st.dataframe(display_q, hide_index=True, use_container_width=True)
        st.caption(
            "Use the **Data Quality Filter** and **Bot Filter** toggles in the left sidebar to dynamically "
            "exclude flagged records and observe how the Executive KPIs and Funnel metrics reconcile."
        )


def render_dag_monitor():
    st.markdown("#### Gold Layer Freshness & Snowflake Native Task DAG Monitor")

    left, right = st.columns([1, 1.3])

    with left:
        st.markdown("**Gold Marts Row Count & Timestamp Freshness**")
        freshness_df = lower_cols(run_query(f"""
            SELECT 'FACT_CLICKSTREAM_EVENTS' AS table_name, COUNT(*) AS row_count,
                   TO_VARCHAR(MAX(event_time), 'YYYY-MM-DD HH24:MI:SS') AS latest_record_ts
            FROM {DB_SCHEMA}.FACT_CLICKSTREAM_EVENTS
            UNION ALL
            SELECT 'FACT_ORDER_ITEMS', COUNT(*),
                   TO_VARCHAR(MAX(order_timestamp), 'YYYY-MM-DD HH24:MI:SS')
            FROM {DB_SCHEMA}.FACT_ORDER_ITEMS
            UNION ALL
            SELECT 'DIM_INVENTORY (Current + Hist)', COUNT(*),
                   TO_VARCHAR(MAX(valid_from), 'YYYY-MM-DD HH24:MI:SS')
            FROM {DB_SCHEMA}.DIM_INVENTORY
            UNION ALL
            SELECT 'DIM_PRODUCT', COUNT(*), 'Master Dimension'
            FROM {DB_SCHEMA}.DIM_PRODUCT
            UNION ALL
            SELECT 'DIM_CUSTOMER', COUNT(*),
                   TO_VARCHAR(MAX(registration_date), 'YYYY-MM-DD HH24:MI:SS')
            FROM {DB_SCHEMA}.DIM_CUSTOMER
        """))
        freshness_df["row_count"] = freshness_df["row_count"].map(lambda v: f"{int(v):,}")
        st.dataframe(freshness_df, hide_index=True, use_container_width=True)

    with right:
        st.markdown("**Snowflake Native Task Graph (DAG) Status & Execution History**")
        try:
            tasks_df = lower_cols(run_query("SHOW TASKS IN DATABASE RETAIL_CLICKSTREAM_DB"))
            if not tasks_df.empty:
                keep_cols = [c for c in ["name", "schema_name", "state", "schedule", "predecessors", "warehouse"] if c in tasks_df.columns]
                st.dataframe(tasks_df[keep_cols], hide_index=True, use_container_width=True)
            else:
                st.info(
                    "No Snowflake Native Tasks are registered yet in `RETAIL_CLICKSTREAM_DB`. "
                    "Once we create our Task Graph in the next phase, live DAG runs will appear here automatically."
                )
        except Exception as e:
            st.info(f"Task metadata view pending orchestration setup: {e}")

        try:
            hist_df = lower_cols(run_query("""
                SELECT
                    name AS task_name,
                    schema_name,
                    state,
                    scheduled_time,
                    completed_time,
                    DATEDIFF('second', query_start_time, completed_time) AS duration_s,
                    error_message
                FROM TABLE(RETAIL_CLICKSTREAM_DB.INFORMATION_SCHEMA.TASK_HISTORY(RESULT_LIMIT => 50))
                WHERE UPPER(database_name) = 'RETAIL_CLICKSTREAM_DB'
                ORDER BY scheduled_time DESC
                LIMIT 15
            """))
            if not hist_df.empty:
                st.dataframe(hist_df, hide_index=True, use_container_width=True)
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("🛒 Retail Clickstream Analytics Dashboard")
st.caption(
    f"Source: `{DB_SCHEMA}` (Gold layer) • "
    f"Auto-refresh: {'ON (' + str(refresh_seconds) + 's)' if auto_refresh else 'OFF'} • "
    f"Quality filter: {'ON' if exclude_quality else 'OFF'} • "
    f"Bot filter: {'ON' if exclude_bots else 'OFF'}"
)

st.divider()

# ---------------------------------------------------------------------------
# Executive KPI Banner (Step 3)
# ---------------------------------------------------------------------------
st.subheader("Executive KPI Banner")

try:
    k = load_kpis()

    # Row 1 - Additive
    with st.container(border=True):
        st.caption("➕ ADDITIVE — can be summed across any dimension")
        a1, a2, a3 = st.columns(3)
        a1.metric(
            "Revenue", compact(k["revenue"], "$"),
            help=f"SUM(line_total) from FACT_ORDER_ITEMS. Exact: ${k['revenue']:,.2f}",
        )
        a2.metric(
            "Sessions", compact(k["sessions"]),
            help=f"Sessions in VW_SESSION_SUMMARY (respects sidebar filters). Exact: {k['sessions']:,.0f}",
        )
        a3.metric(
            "Orders", compact(k["orders"]),
            help=f"COUNT(DISTINCT order_id) from FACT_ORDER_ITEMS. Exact: {k['orders']:,.0f}",
        )

    # Row 2 - Semi-additive | Non-additive
    col_semi, col_non = st.columns([1, 3])

    with col_semi:
        with st.container(border=True):
            st.caption("⏱️ SEMI-ADDITIVE")
            st.metric(
                "Stock Units", compact(k["stock"]),
                help=(
                    "SUM(stock_quantity) WHERE is_current = TRUE (latest SCD2 row per product). "
                    "Can be summed across products, but not across time. "
                    f"Exact: {k['stock']:,.0f}"
                ),
            )

    with col_non:
        with st.container(border=True):
            st.caption("➗ NON-ADDITIVE — ratios recomputed from summed components")
            n1, n2, n3 = st.columns(3)
            n1.metric(
                "Abandonment", f"{k['abandonment_pct']:.1f}%",
                help="Sessions with a cart event but no purchase ÷ sessions with a cart event.",
            )
            n2.metric(
                "Conversion", f"{k['conversion_pct']:.1f}%",
                help="Sessions that reached purchase ÷ total sessions.",
            )
            n3.metric(
                "ROI", f"{k['roi_pct']:.1f}%",
                delta=f"Margin {k['margin_pct']:.1f}%",
                delta_color="off",
                help="ROI = SUM(net_profit) ÷ SUM(COGS). Margin = SUM(net_profit) ÷ SUM(revenue).",
            )

    st.caption(
        "ℹ️ Session KPIs (sessions, abandonment, conversion) follow both sidebar filters. "
        "Inventory follows the quality filter. Revenue / orders / ROI are order-level "
        "and are not affected by session flags."
    )
except Exception as e:
    st.error(f"Could not load KPIs: {e}")

st.divider()

# ---------------------------------------------------------------------------
# Tabs (content filled in Steps 4-6)
# ---------------------------------------------------------------------------
tab1, tab2, tab3 = st.tabs([
    "🧭 Clickstream Journey",
    "💰 Revenue & ROI",
    "📦 Inventory, Quality & DAG Monitor",
])

with tab1:
    try:
        render_funnel()
        st.divider()
        render_abandonment()
        st.divider()
        render_dwell()
    except Exception as e:
        st.error(f"Could not render Clickstream tab: {e}")
with tab2:
    try:
        render_revenue_trend()
        st.divider()
        render_category_brand_roi()
        st.divider()
        render_geography_and_status()
    except Exception as e:
        st.error(f"Could not render Revenue & ROI tab: {e}")
with tab3:
    try:
        render_inventory_scd2()
        st.divider()
        render_quality_gates()
        st.divider()
        render_dag_monitor()
    except Exception as e:
        st.error(f"Could not render Inventory, Quality & DAG tab: {e}")
# ---------------------------------------------------------------------------
# Live auto-refresh (keep at the very bottom of the script)
# ---------------------------------------------------------------------------
if auto_refresh:
    time.sleep(refresh_seconds)
    st.cache_data.clear()
    st.rerun()