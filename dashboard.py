import os
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from google.cloud import bigquery
from dotenv import load_dotenv
import plotly.graph_objects as go
import math

# --- 1. COLOR CONFIGURATION ---
# 4-stop ramp: Dark → Light
RAMP_STOPS = [
    (0xF2, 0xDD, 0xE0),  # #F2DDE0 — lightest blush
    (0xE8, 0x84, 0x7A),  # #E8847A — peachy coral
    (0xD4, 0x73, 0x7E),  # #D4737E — coral rose
    (0xA0, 0x44, 0x55),  # #A04455 — darkest
]

def make_log_color_ramp(series):
    """Generates hex colors based on log values with safety for zero/empty series."""
    if series.empty or (series.max() == series.min()):
        return ['#D4737E'] * len(series)
        
    log_vals = series.apply(lambda x: math.log10(max(x, 1)))
    log_min, log_max = log_vals.min(), log_vals.max()
    n_segments = len(RAMP_STOPS) - 1

    def interpolate(val):
        # Calculate normalized position (0.0 to 1.0)
        t = (math.log10(max(val, 1)) - log_min) / (log_max - log_min)
        t = max(0, min(t, 1)) # Clamp values
        
        # Identify segment and local interpolation factor
        segment = min(int(t * n_segments), n_segments - 1)
        local_t = (t * n_segments) - segment
        
        r1, g1, b1 = RAMP_STOPS[segment]
        r2, g2, b2 = RAMP_STOPS[segment + 1]
        
        r = int(r1 + (r2 - r1) * local_t)
        g = int(g1 + (g2 - g1) * local_t)
        b = int(b1 + (b2 - b1) * local_t)
        return f'#{r:02x}{g:02x}{b:02x}'

    return series.apply(interpolate)

# --- 1. CONFIG & SYSTEM STYLE ---
load_dotenv()
st.set_page_config(page_title="Olist Executive Dashboard", layout="wide")

# Professional Sidebar and Metric Card Styling
st.markdown("""
    <style>
    [data-testid="stSidebar"] { min-width: 400px; max-width: 400px; }
    h1, h2, h3, h4, p, span, label { color: #400101 !important; }

    [data-testid="stMetric"] {
        background-color: #FC9A91!important;
        padding: 20px !important;
        border-radius: 12px !important;
        border: none !important;
    }

    [data-testid="stMetricValue"] {
        color: #400101 !important;
    }
    [data-testid="stMetricValue"] * {
        color: #400101 !important;
        font-weight: 700 !important;
        font-size: 1.5rem !important;
    }

    [data-testid="stMetricLabel"] {
        color: #5C1A1A !important;
    }
    [data-testid="stMetricLabel"] * {
        color: #5C1A1A !important;
    }

    .stAppViewMainContainer > div:first-child { 
        padding-top: 0rem !important; 
        margin-top: -30px !important; 
    }
    </style>
""", unsafe_allow_html=True)

PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
DATASET_ID = "olist"
client = bigquery.Client(project=PROJECT_ID)

# --- 2. INDEPENDENT OPTIMIZED LOADING (DISK PERSISTENCE) ---
@st.cache_data(persist="disk", show_spinner="Syncing Orders...")
def get_orders():
    cols = "order_id, order_status, order_purchase_timestamp, actual_amount_paid, lead_time_days, is_delivered_on_time, total_item_value, total_freight_value"
    return client.query(f"SELECT {cols} FROM `{PROJECT_ID}.{DATASET_ID}.dim_orders`").to_dataframe()

@st.cache_data(persist="disk", show_spinner="Syncing Customers...")
def get_customers():
    cols = "customer_id, customer_segment, lifetime_value, latitude, longitude, days_since_last_order, lifetime_frequency"
    return client.query(f"SELECT {cols} FROM `{PROJECT_ID}.{DATASET_ID}.dim_customers`").to_dataframe()

@st.cache_data(persist="disk", show_spinner="Syncing Sales...")
def get_sales():
    cols = "order_id, product_id, location_id, total_payment_value"
    return client.query(f"SELECT {cols} FROM `{PROJECT_ID}.{DATASET_ID}.fct_sales`").to_dataframe()

@st.cache_data(persist="disk", show_spinner="Syncing Products...")
def get_products():
    cols = "product_id, product_category_name, is_top_15_seller"
    return client.query(f"SELECT {cols} FROM `{PROJECT_ID}.{DATASET_ID}.dim_products`").to_dataframe()

@st.cache_data(persist="disk", show_spinner="Syncing Locations...")
def get_location():
    cols = "location_id, geolocation_state"
    return client.query(f"SELECT {cols} FROM `{PROJECT_ID}.{DATASET_ID}.dim_location`").to_dataframe()

# Execution
df_orders = get_orders()
df_customers = get_customers()
df_sales = get_sales()
df_products = get_products()
df_location = get_location()

# --- 1. CONFIG & SYSTEM STYLE ---
# Custom CSS to apply your specific colors
st.markdown(f"""
    <style>
    /* 1. Header & Label Colors */
    h1, h2, h3, h4, p, span, label {{
        color: #400101 !important;
    }}
    
    /* 2. Metric Card Styling (Background #D9C7C1) */
    div[data-testid="stMetric"] {{ 
        background-color: #F2DBAE; 
        padding: 20px; 
        border-radius: 12px; 
        border: none;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }}
    
    /* 3. Ensure the Metric Value (the big numbers)  */
    div[data-testid="stMetricValue"] > div {{
        color: #F25270 !important;
        font-weight: 700;
        font-size: 1.5rem !important; /* Slightly smaller to help it fit */
        overflow: visible !important; /* Removes the ".." */
        white-space: normal !important; /* Allows wrapping to a new line */
    }}
    
    /* 4. Metric Label (the small text above numbers) */
    div[data-testid="stMetricLabel"] > div > p {{
        color: #F25270 !important;
        opacity: 0.8;
        overflow: visible !important; /* Ensures label isn't cut off either */
    }}
            
    /* 5. Remove Top Padding / White Space */
    .stAppViewMainContainer > div:first-child {{
        padding-top: 0rem !important;
        margin-top: -30px !important; /* Pulls content upward */
    }}
    
    #root > div:nth-child(1) > div > div > div > div > section > div {{
        padding-top: 0rem !important;
    }}
            
    </style>
""", unsafe_allow_html=True)
# Expanded palette based on the Mondrian Foundation annual report image
ANNUAL_REPORT_PALETTE = [
    '#D4737E',  # coral rose — lead color, matches your nav pink
    '#7AAFC2',  # sky teal
    '#85B598',  # mint sage
    '#B087B8',  # soft violet
    '#D4A96A',  # golden sand
    '#E8847A',  # peachy coral
    '#8FAFC0',  # slate blue
    '#C49BB5',  # dusty pink
    '#A4C4A0',  # soft green
    '#D4A96A',  # warm amber
    '#E8847A',  # peachy coral (repeated for emphasis)
]

OTHERS_COLOR = '#D6CFC9'  # warm blush gray for "Others"

# Mappings & Palettes
STATE_MAP = {'SP': 'Sao Paulo', 'RJ': 'Rio de Janeiro', 'MG': 'Minas Gerais', 'RS': 'Rio Grande do Sul', 'PR': 'Parana', 'PE': 'Pernambuco', 'SC': 'Santa Catarina', 'BA': 'Bahia', 'DF': 'Distrito Federal'}

# --- 3. EXECUTIVE SUMMARY (KPIs) ---
st.title("📦 Olist E-commerce Performance")
# 1. CALCULATIONS (Must come first)
unique_clients = df_customers['customer_id'].nunique()
leads_mask = df_customers['lifetime_frequency'] == 0
total_rev = df_sales['total_payment_value'].sum()
order_count = df_orders['order_id'].nunique()
avg_ticket = total_rev / order_count if order_count > 0 else 0

# 2. DISPLAY (The KPI Row)
m1, m2, m3, m4, m5, m6 = st.columns(6)

m1.metric("Customers", f"{unique_clients/1000:.1f}K")
m2.metric("New Prospects", f"{len(df_customers[leads_mask]):,}")
m3.metric("Revenue", f"R$ {total_rev/1_000_000:.2f}M")
m4.metric("Order Volume", f"{order_count/1000:.2f}K")
m5.metric("Products Sold", f"{len(df_sales)/1000:.2f}K")
m6.metric("Average Order Value", f"R$ {avg_ticket:.2f}")

st.divider()

# --- ROW 2: SEGMENTATION & PRODUCTS ---
row1_col1, row1_col2 = st.columns(2)
with row1_col1:
    st.subheader("Monthly Sales Revenue")

    # 1. DATA PREPARATION
    df_orders['order_purchase_timestamp'] = pd.to_datetime(df_orders['order_purchase_timestamp'])
    monthly = (
        df_orders
        .assign(month_period=df_orders['order_purchase_timestamp'].dt.tz_localize(None).dt.to_period('M'))
        .groupby('month_period')['actual_amount_paid']
        .sum()
        .reset_index()
    )
    # Filter and Sort by time
    monthly = monthly[monthly['month_period'] < '2018-09'].sort_values('month_period')
    monthly['month_display'] = monthly['month_period'].dt.strftime('%b %Y')

    # 2. FORCED STEPPED GRADIENT (Fixes the "One Dark Patch" issue)
    # This creates a smooth transition from Light Blush (#F2DDE0) to Darkest (#A04455)
    import plotly.colors as pc
    MONTHLY_PALETTE = ['#F2DDE0', '#E8847A', '#D4737E', '#A04455']
    
    n_months = len(monthly)
    # Map each bar to a specific shade based on its index (time rank)
    monthly['bar_color'] = pc.sample_colorscale(MONTHLY_PALETTE, [i/(n_months-1) for i in range(n_months)])

    # 3. CREATE THE CHART
    fig_trend = px.bar(
        monthly,
        x='month_display',
        y='actual_amount_paid',
        color='bar_color',
        color_discrete_map='identity', # Use the colors we just generated
        log_y=True,
        text_auto='.3s',
        template='plotly_white',
        category_orders={"month_display": monthly['month_display'].tolist()}
    )

    # 4. STYLING & AXIS
    fig_trend.update_layout(
        showlegend=False,
        xaxis_tickangle=-45,
        height=550,
        plot_bgcolor='white',
        yaxis_title="Revenue (Log Scale)",
        xaxis_title=None,
        xaxis={'type': 'category'},
        margin=dict(l=10, r=10, t=10, b=10)
    )

    fig_trend.update_yaxes(
        type="log",
        tickvals=[250, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000],
        ticktext=['250', '500', '1k', '5k', '10k', '50k', '100k', '500k', '1M'],
        gridcolor='#F0F0F0',
        range=[2, 6.2]
    )

    fig_trend.update_traces(
        marker_line_color='white', # Add a thin white line between bars to break up "patches"
        marker_line_width=1,
        textposition="outside",
        cliponaxis=False,
        textfont_size=10
    )

    st.plotly_chart(fig_trend, use_container_width=True)

with row1_col2:
    st.subheader("Sales Revenue by States (Top 10)")

    # 1. DATA PREPARATION
    state_sales = df_sales.merge(df_location, on='location_id') \
                         .groupby('geolocation_state')['total_payment_value'].sum().reset_index()
    state_sales = state_sales.sort_values('total_payment_value', ascending=False)

    state_map = {
        'SP': 'Sao Paulo', 'RJ': 'Rio de Janeiro', 'MG': 'Minas Gerais',
        'RS': 'Rio Grande do Sul', 'PR': 'Parana', 'SC': 'Santa Catarina',
        'BA': 'Bahia', 'DF': 'Distrito Federal', 'ES': 'Espirito Santo',
        'GO': 'Goias', 'PE': 'Pernambuco', 'CE': 'Ceara', 'PA': 'Para',
        'MT': 'Mato Grosso', 'MA': 'Maranhao', 'MS': 'Mato Grosso do Sul',
        'PB': 'Paraiba', 'RN': 'Rio Grande do Norte', 'PI': 'Piaui',
        'AL': 'Alagoas', 'SE': 'Sergipe', 'TO': 'Tocantins', 'RO': 'Rondonia',
        'AM': 'Amazonas', 'AC': 'Acre', 'AP': 'Amapa', 'RR': 'Roraima'
    }
    state_sales['state_display'] = state_sales['geolocation_state'].map(state_map).fillna(state_sales['geolocation_state'])

    # 2. GROUP TOP 10 + OTHERS
    top_10_states = state_sales.head(10).copy()
    others_state_val = state_sales.iloc[10:]['total_payment_value'].sum()
    
    df_state_final = pd.concat([
        top_10_states,
        pd.DataFrame({
            'geolocation_state': ['Others'],
            'state_display': ['Others'],
            'total_payment_value': [others_state_val]
        })
    ], ignore_index=True)

    # 3. FORCED RANK-BASED COLORING (Fixes the "Ugly Patch" issue)
    # Using 11 steps for Top 10 + Others
    # Palette: Darkest (#A04455) down to Lightest Blush (#F2DDE0)
    STATE_PALETTE = [
        '#A04455', '#B34E5D', '#C15866', '#CF6371', '#D4737E', 
        '#DA848D', '#E0959C', '#E6A5AB', '#ECB6BA', '#F2C7C9', '#F2DDE0'
    ]
    
    # This assigns colors based on RANK (Position 1, 2, 3...) not the revenue value
    df_state_final['bar_color'] = STATE_PALETTE[:len(df_state_final)]

    # 4. CREATE CHART
    fig_state = px.bar(
        df_state_final,
        x='geolocation_state',
        y='total_payment_value',
        color='bar_color',
        color_discrete_map='identity', # Crucial: tells Plotly to use the hex codes
        log_y=True,
        text_auto='.2s',
        template='plotly_white',
        hover_name='state_display',       # shows full name on hover
        hover_data={'bar_color': False,   # hides the hex code from hover
                    'geolocation_state': False},
        # Ensure the order on the X-axis matches our sorted data
        category_orders={"geolocation_state": df_state_final['geolocation_state'].tolist()}
    )

    # 5. STYLING
    fig_state.update_layout(
        plot_bgcolor='white',
        height=550,
        showlegend=False,
        yaxis_title="Revenue (Log Scale)",
        xaxis_title=None,
        margin=dict(l=10, r=20, t=10, b=10),
    )
    
    # Map abbreviations → full names on x-axis ticks
    fig_state.update_xaxes(
        tickvals=df_state_final['geolocation_state'].tolist(),
        ticktext=df_state_final['state_display'].tolist(),
        tickangle=-35
    )
    
    fig_state.update_traces(
        marker_line_color='white', # Add a thin border to separate bars
        marker_line_width=1,
        textfont_size=10,
        textposition="outside",
        cliponaxis=False
    )

    st.plotly_chart(fig_state, use_container_width=True)

# --- ROW 2: SEGMENTATION & PRODUCTS ---
row2_col1, row2_col2 = st.columns(2)

with row2_col1:
    st.subheader("Customer Loyalty Mix")

    segment_rename = {
        'At Risk - Cannot Lose Them': 'At Risk',
        'New Customers - Promising':  'Promising Newcomers',
        'Hibernating - Lost':         'Dormant',
    }
    df_customers['customer_segment'] = df_customers['customer_segment'].replace(segment_rename)

    df_rfm = df_customers.groupby('customer_segment').size().reset_index(name='cnt').sort_values('cnt', ascending=False)
    total_cnt = df_rfm['cnt'].sum()
    df_rfm.loc[df_rfm['cnt'] < (total_cnt * 0.02), 'customer_segment'] = 'Others'
    df_rfm = df_rfm.groupby('customer_segment')['cnt'].sum().reset_index().sort_values('cnt', ascending=False)

    df_rfm['label'] = df_rfm.apply(
        lambda x: f"<b>{x['customer_segment']}</b><br>{x['cnt']} ({int(round(x['cnt']/total_cnt*100, 0))}%)",
        axis=1
    )

    segment_color_map = {
        'Potential Loyalists':  '#C4606C',
        'At Risk':              '#D4737E',
        'Loyal Customers':      '#E8847A',
        'Promising Newcomers':  '#D89CA1',
        'Dormant':              '#ECB6BA',
        'Others':               '#F2DDE0',
    }

    fig_rfm = go.Figure(go.Treemap(
        ids=df_rfm['customer_segment'].tolist(),
        labels=df_rfm['label'].tolist(),
        parents=[''] * len(df_rfm),
        values=df_rfm['cnt'].tolist(),
        marker=dict(
            colors=[segment_color_map.get(seg, '#F2DDE0') for seg in df_rfm['customer_segment']],
            line=dict(color='#F0EBE8', width=1),
            pad=dict(t=0, l=0, r=0, b=0)
        ),
        texttemplate="%{label}",
        hovertemplate='<b>%{label}</b><extra></extra>',
        branchvalues='total'
    ))

    fig_rfm.update_layout(
        height=500,
        margin=dict(t=30, l=10, r=10, b=10),
        showlegend=False,
        paper_bgcolor='white'
    )

    st.plotly_chart(fig_rfm, use_container_width=True)

with row2_col2:
    st.subheader("Top 12 Best Selling Product Categories")

    cat_summary = df_sales.merge(df_products[['product_id', 'product_category_name']], on='product_id') \
                          .groupby('product_category_name').size().reset_index(name='quantity')
    
    top_12_cats = cat_summary.nlargest(12, 'quantity').copy()
    top_12_cats['product_category_name'] = top_12_cats['product_category_name'].str.replace('_', ' ').str.title()
    others_qty = cat_summary[~cat_summary['product_category_name'].isin(
        cat_summary.nlargest(12, 'quantity')['product_category_name'])]['quantity'].sum()
    
    df_donut = pd.concat([
        top_12_cats,
        pd.DataFrame({'product_category_name': ['Others'], 'quantity': [others_qty]})
    ], ignore_index=True)

    # 13 stops: darkest → lightest, last stop reserved for Others
    DONUT_GRADIENT = [
        '#A04455', '#AA4B5A', '#B34E5D', '#BC5562',
        '#C15866', '#CF6371', '#D4737E', '#DA848D',
        '#DF9097', '#E0959C', '#E8A8AD', '#ECB6BA',
        '#F2DDE0'  # Others — always lightest
    ]

    total_qty = df_donut['quantity'].sum()
    custom_text = []
    for _, row in df_donut.iterrows():
        pct = row['quantity'] / total_qty * 100
        if pct >= 5:
            # Large slice: name + percent
            custom_text.append(f"{row['product_category_name']}<br>{pct:.1f}%")
        elif pct >= 2.5:
            # Medium slice: percent only
            custom_text.append(f"{pct:.1f}%")
        else:
            # Too small: hide
            custom_text.append("")

    fig_donut = px.pie(
        df_donut,
        values='quantity',
        names='product_category_name',
        hole=0.50,
        template='plotly_white',
        category_orders={"product_category_name": df_donut['product_category_name'].tolist()}
    )

    fig_donut.update_traces(
        text=custom_text,
        textposition='inside',
        textinfo='text',
        insidetextorientation='horizontal',
        marker=dict(colors=DONUT_GRADIENT, line=dict(color='#FFFFFF', width=2)),
        direction='clockwise',
        sort=False,
        showlegend=False,
        textfont=dict(size=10),
        hovertemplate='<b>%{label}</b><br>%{percent}<extra></extra>'
    )

    fig_donut.update_layout(
        height=580,
        margin=dict(l=10, r=10, t=10, b=10),
        uniformtext=dict(
            minsize=8,
            mode='hide'
        )
    )

    st.plotly_chart(fig_donut, use_container_width=True)

# --- ROW 3: MARKET SHARE ANALYSIS (FULL WIDTH) ---
st.divider()
st.subheader("State Market Share (Top 5 States & Top 8 Product Categories)")

import plotly.graph_objects as go

MONTHLY_PALETTE = ['#F2DDE0', '#E8847A', '#D4737E', '#A04455']

def interpolate_ramp(ramp, t):
    stops = [(int(c[1:3], 16), int(c[3:5], 16), int(c[5:7], 16)) for c in ramp]
    n = len(stops) - 1
    segment = min(int(t * n), n - 1)
    local_t = (t * n) - segment
    r1, g1, b1 = stops[segment]
    r2, g2, b2 = stops[segment + 1]
    r = int(r1 + (r2 - r1) * local_t)
    g = int(g1 + (g2 - g1) * local_t)
    b = int(b1 + (b2 - b1) * local_t)
    return f'#{r:02x}{g:02x}{b:02x}'

# Data prep
df_market = df_sales.merge(df_products[['product_id', 'product_category_name']], on='product_id') \
                    .merge(df_location, on='location_id')
df_market['total_payment_value'] = pd.to_numeric(df_market['total_payment_value'], errors='coerce').fillna(0)
df_market['state_full_name'] = df_market['geolocation_state'].map(STATE_MAP).fillna(df_market['geolocation_state'])

# Top 5 states + Others
top_5_states = df_market.groupby('state_full_name')['total_payment_value'] \
                        .sum().nlargest(5).index.tolist()
df_market['state_display'] = df_market['state_full_name'].apply(
    lambda x: x if x in top_5_states else 'Other States'
)

# Top 8 categories + Others
top_8_cats = df_market.groupby('product_category_name')['total_payment_value'] \
                      .sum().nlargest(8).index.tolist()
df_market['category_display'] = df_market['product_category_name'].apply(
    lambda x: str(x).replace('_', ' ').title() if x in top_8_cats else 'Others'
)

df_sunburst_data = df_market.groupby(['state_display', 'category_display'])['total_payment_value'] \
                            .sum().reset_index()

# State order: sorted by revenue descending, Others always last
state_totals = df_sunburst_data.groupby('state_display')['total_payment_value'].sum()
ordered_states = state_totals.drop('Other States', errors='ignore') \
                             .sort_values(ascending=False).index.tolist()
ordered_states = ordered_states + ['Other States']

# Inner ring: light→dark by revenue (smallest = lightest, largest = darkest)
# Exclude Other States from ramp — pin it to lightest
named_states = [s for s in ordered_states if s != 'Other States']
n_states = len(named_states)
state_color_map = {
    state: interpolate_ramp(MONTHLY_PALETTE, i / max(n_states - 1, 1))
    for i, state in enumerate(reversed(named_states))  # reversed: index 0 = largest = darkest
}
state_color_map['Other States'] = '#F2DDE0'  # always lightest

# Build nodes
ids, labels, parents, values, colors = [], [], [], [], []

# Inner ring: states
for state in ordered_states:
    ids.append(state)
    labels.append(state)
    parents.append('')
    values.append(float(state_totals[state]))
    colors.append(state_color_map[state])

# Outer ring: categories per state, light→dark by revenue within each state
for state in ordered_states:
    state_cats = df_sunburst_data[df_sunburst_data['state_display'] == state] \
                    .sort_values('total_payment_value', ascending=True)
    n = len(state_cats)
    for rank, (_, row) in enumerate(state_cats.iterrows()):
        t = rank / max(n - 1, 1)
        ids.append(state + ' > ' + row['category_display'])
        labels.append(row['category_display'])
        parents.append(state)
        values.append(float(row['total_payment_value']))
        colors.append(interpolate_ramp(MONTHLY_PALETTE, t))

fig_sunburst = go.Figure(go.Sunburst(
    ids=ids,
    labels=labels,
    parents=parents,
    values=values,
    marker=dict(colors=colors, line=dict(color='#FFFFFF', width=1)),
    branchvalues='total',
    insidetextorientation='radial',
    texttemplate="<b>%{label}</b><br>R$ %{value:,.0f}",
    hovertemplate='<b>%{label}</b><br>Revenue: R$ %{value:,.2f}<extra></extra>',
))

# Legend
for state in ordered_states:
    fig_sunburst.add_trace(go.Scatter(
        x=[None], y=[None], mode='markers',
        marker=dict(size=10, color=state_color_map[state], symbol='square'),
        name=state, showlegend=True
    ))

fig_sunburst.update_layout(
    margin=dict(t=20, l=10, r=150, b=20),
    height=700,
    showlegend=True,
    xaxis=dict(visible=False),
    yaxis=dict(visible=False),
    legend=dict(
        title='State',
        orientation='v',
        yanchor='middle',
        y=0.5,
        xanchor='left',
        x=1.02,
        font=dict(size=11)
    )
)

st.plotly_chart(fig_sunburst, use_container_width=True)
