import os
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from google.cloud import bigquery
from dotenv import load_dotenv

# --- 1. CONFIG & SYSTEM STYLE ---
load_dotenv()
st.set_page_config(page_title="Olist Executive Dashboard", layout="wide")

# Professional Sidebar (400px) and Metric Card Styling
st.markdown("""
    <style>
    [data-testid="stSidebar"] { min-width: 400px; max-width: 400px; }
    div[data-testid="stMetric"] { 
        background-color: #f8f9fa; 
        padding: 15px; border-radius: 10px; border: 1px solid #ececec; 
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
ANNUAL_REPORT_PALETTE = ['#F24BA7', '#32D97A', '#E1F25C', '#F27B35', '#F23838',
       '#5C88BF', '#9B7FB2', '#E89D61', '#4D9484', '#333333', '#E07A94', 
    '#D091C4', 
    '#E2EDE4', 
    '#FFF973', 
    '#C9E265']  

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


row1_col1, row1_col2 = st.columns(2)
# --- 2. VISUALIZATIONS ---
# --- ROW 1: GROWTH & GEOGRAPHY ---
with row1_col1:
    st.subheader("Monthly Sales Revenue")

    # --- 1. DATA PREPARATION ---
    df_orders['order_purchase_timestamp'] = pd.to_datetime(df_orders['order_purchase_timestamp'])
    
    # 1. Group by the new 'month_period' column
    monthly = (
        df_orders
        .assign(month_period=df_orders['order_purchase_timestamp'].dt.tz_localize(None).dt.to_period('M'))
        .groupby('month_period')['actual_amount_paid']
        .sum()
        .reset_index()
    )

    # 2. IMPORTANT: Sort by 'month_period' (the column created in step 1)
    monthly = monthly.sort_values('month_period')

    # 3. Use 'month_period' to create your display labels
    # We convert back to timestamp briefly just to use .strftime()
    monthly['month_display'] = monthly['month_period'].dt.strftime('%b %Y')
    monthly['month_name'] = monthly['month_period'].dt.strftime('%B')

    # 2. DEFINE MONTHLY COLOR MAP
    month_colors = {
        'January': '#F24BA7', 'February': '#32D97A', 'March': '#E1F25C', 
        'April': '#E2EDE4', 'May': '#F27B35', 'June': '#F23838',
        'July': '#9B7FB2', 'August': '#ADD8E6', 'September': '#DCAE96', 
        'October': '#8E8E8E', 'November': '#FFF973', 'December': '#719CF7'
    }

    # --- 3. CREATE THE CHART (THIS FIXES THE NAMEERROR) ---
    fig_trend = px.bar(
        monthly, 
        x='month_display',  
        y='actual_amount_paid',
        color='month_name',
        color_discrete_map=month_colors,
        log_y=True,
        text_auto='.3s',
        template='plotly_white',
        category_orders={"month_display": monthly['month_display'].tolist()}
    )

    # --- 4. STYLE UPDATES ---
    custom_tick_vals = [250, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000]
    custom_tick_text = ['250', '500', '1k', '5k', '10k', '50k', '100k', '500k', '1M']

    fig_trend.update_layout(
        showlegend=False, 
        xaxis_tickangle=-45, 
        height=550, 
        plot_bgcolor='white',
        yaxis_title="Revenue",
        xaxis_title=None,
        xaxis={'type': 'category'} 
    )

    fig_trend.update_yaxes(
        type="log",
        tickvals=custom_tick_vals,
        ticktext=custom_tick_text,
        gridcolor='#F0F0F0',
        # range=[2, 6.2] stretches the axis to start at 100 (10^2)
        # providing vertical gap so 250, 500, and 1k do not overlap.
        range=[2, 6.2], 
        dtick=1
    )

    fig_trend.update_traces(
        textposition="outside", 
        cliponaxis=False,
        textfont_size=10
    )

    st.plotly_chart(fig_trend, use_container_width=True)

with row1_col2:
    st.subheader("Sales Revenue by States")

    # 1. DATA PREPARATION (Ensure this is calculated)
    state_sales = df_sales.merge(df_location, on='location_id') \
                         .groupby('geolocation_state')['total_payment_value'].sum().reset_index()
    state_sales = state_sales.sort_values('total_payment_value', ascending=False)
    
    # ADD ALL STATES HERE, no accent version
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

    
    # This line applies the full names
    state_sales['state_name'] = state_sales['geolocation_state'].map(state_map).fillna(state_sales['geolocation_state'])

    # 2. CREATE THE LOGARITHMIC CHART
    # We use sample_colorscale to stretch your 15 colors across all 27 states
    import plotly.express as px
    extended_colors = px.colors.sample_colorscale(ANNUAL_REPORT_PALETTE, [i/26 for i in range(27)])

    fig_state = px.bar(
        state_sales, 
        x='geolocation_state', 
        y='total_payment_value',
        color='state_name',
        color_discrete_sequence=extended_colors, # Apply the full 15-color gradient
        log_y=True,            
        template='plotly_white',
        text_auto='.2s',       
        labels={'total_payment_value': 'revenue (log scale)', 'geolocation_state': 'State'}
    )

    # 3. STYLE UPDATES (Preserving your specific Log Scale settings)
    fig_state.update_layout(
        plot_bgcolor='white',
        # 1. Match the height of fig_trend (was 500, needs to be 550)
        height=550, 
        showlegend=True,
        yaxis_title="Revenue",
        # 2. Increase right margin (r=150) to prevent legend crowding
        margin=dict(l=10, r=150, t=10, b=10), 
        legend=dict(
            title="State Name", 
            orientation="v", 
            yanchor="middle", 
            y=0.5, 
            xanchor="left", 
            x=1.02,
            # 3. Force the legend to use the full height to avoid scrollbars
            traceorder="normal"
        )
    )


    # Customizing the Log Axis for clarity
    fig_state.update_yaxes(
        gridcolor='#F0F0F0',
        dtick=1, # Major gridlines at 10k, 100k, 1M, 10M
        minor=dict(showgrid=True, gridcolor='#F8F8F8') # Detailed sub-gridlines
    )

    fig_state.update_traces(
        textfont_size=10, 
        textangle=0, 
        textposition="outside",
        cliponaxis=False
    )

    st.plotly_chart(fig_state, use_container_width=True)

st.divider()

# --- ROW 2: SEGMENTATION & PRODUCTS ---
row2_col1, row2_col2 = st.columns(2)

with row2_col1:
    # 3. Clients by RFM Segment
    st.subheader("Customer Loyalty Mix")

    # Prepare the data
    df_rfm_counts = df_customers.groupby('customer_segment').size().reset_index(name='client_count')
    df_rfm_counts = df_rfm_counts.sort_values('client_count', ascending=False)

    # Calculate total for the percentage calculation in labels
    total_clients = df_rfm_counts['client_count'].sum()

    # Create the specific label format: "Segment <br> Count (Percentage%)"
    # Matches the image style: Bold title with count and % below it
    df_rfm_counts['label'] = df_rfm_counts.apply(
        lambda x: f"<b>{x['customer_segment']}</b><br>{x['client_count']} ({int(round(x['client_count']/total_clients*100, 0))}%)", 
        axis=1
    )

    # Your updated color palette (using the 6-digit pink)
    custom_colors = [ '#FFE858',"#D2FF8F", "#F2A2C0", '#F2AE30', '#D091C4', "#FF836C", "#80CCE5", '#CAD95B']
    
    # Generate Treemap
    fig_rfm = px.treemap(
        df_rfm_counts,
        path=[px.Constant("all"), 'label'], 
        values='client_count',
        color='customer_segment',
        color_discrete_sequence=custom_colors,
        template='plotly_white'
    )

    # Styling to match the image UI
    fig_rfm.update_traces(
        textinfo="label",
        texttemplate="%{label}", 
        hovertemplate='<b>%{label}</b>',
        # 2. Adjust tiling to remove the 'gaps' between the segments
        marker=dict(
            line=dict(color='#F2DBAE', width=1), # Thin gold borders between boxes
            pad=dict(t=0, l=0, r=0, b=0)         # Removes internal padding
        )
    )


    fig_rfm.update_layout(
        margin=dict(t=30, l=10, r=10, b=10), # Added top margin for the 'all' label
        height=500,
        showlegend=False,
        # Ensure the chart background matches the root
        paper_bgcolor='white' 
    )

    st.plotly_chart(fig_rfm, use_container_width=True)

with row2_col2:
    st.subheader("Top 15 Best Selling Product Categories")
    
    # 1. Prepare Data
    df_cat_qty = df_sales.merge(df_products[['product_id', 'product_category_name']], on='product_id')
    cat_summary = df_cat_qty.groupby('product_category_name').size().reset_index(name='quantity')
    
    # Get Top 15 and Others
    top_15_cats = cat_summary.nlargest(15, 'quantity').copy()
    others_qty = cat_summary[~cat_summary['product_category_name'].isin(top_15_cats['product_category_name'])]['quantity'].sum()
    
    # Clean up names for display
    top_15_cats['product_category_name'] = top_15_cats['product_category_name'].str.replace('_', ' ').str.title()
    
    # Combine: Top 15 FIRST, Others LAST
    df_others = pd.DataFrame({'product_category_name': ['Others'], 'quantity': [others_qty]})
    df_donut = pd.concat([top_15_cats, df_others], ignore_index=True)

    # 2. Create Figure
    fig_donut = px.pie(
        df_donut, 
        values='quantity', 
        names='product_category_name',
        hole=0.5, 
        template='plotly_white',
        # This is key: it prevents Plotly from re-sorting by size
        category_orders={"product_category_name": df_donut['product_category_name'].tolist()}
    )
    
    # 3. Handle Colors (Optional: Make 'Others' Grey)
    # If ANNUAL_REPORT_PALETTE has 15+ colors, we can force 'Others' to be light grey
    custom_colors = list(ANNUAL_REPORT_PALETTE[:15]) + ['#D3D3D3'] 
    fig_donut.update_traces(marker=dict(colors=custom_colors))

    fig_donut.update_traces(
        textposition='inside', 
        textinfo='percent', 
        marker=dict(line=dict(color='#FFFFFF', width=2)),
        # Start the first slice (Top 1) at 12 o'clock
        direction='clockwise',
        sort=False 
    )
    
    fig_donut.update_layout(
        height=500,
        legend=dict(
            title="Categories (Top to Bottom)", 
            orientation="v", 
            yanchor="middle", 
            y=0.5, 
            xanchor="left", 
            x=1.1,
            # Ensures legend follows our dataframe order, not percentage size
            traceorder="normal" 
        )
    )
    st.plotly_chart(fig_donut, use_container_width=True)

# --- ROW 3: MARKET SHARE ANALYSIS (FULL WIDTH) ---
st.divider()
st.subheader("State Market Share by Product Categories")

# 1. PREVENT LIGHT COLORS: Filter your palette for better contrast
# I've removed the very light ones like '#E2EDE4' and '#FFF973'
DARKER_PALETTE = ['#F24BA7', '#9B7FB2', '#F27B35', '#F23838', '#5C88BF', 
                    "#5DBF54", "#DD955B",  "#A2C357","#80CCE5", "#D4718A" ]

# 2. DATA PREPARATION (Strict grouping to ensure perfect alignment)
df_market = df_sales.merge(df_products[['product_id', 'product_category_name']], on='product_id') \
                    .merge(df_location, on='location_id')

df_market['state_full_name'] = df_market['geolocation_state'].map(state_map).fillna(df_market['geolocation_state'])

top_15_names = df_market.groupby('product_category_name').size().nlargest(15).index.tolist()
df_market['category_display'] = df_market['product_category_name'].apply(
    lambda x: str(x).replace('_', ' ').title() if x in top_15_names else 'Others'
)

# Aggregating ensures math is 1:1, which "snaps" the rings together
df_sunburst_data = df_market.groupby(['state_full_name', 'category_display'])['total_payment_value'].sum().reset_index()

# 3. CREATE SUNBURST
fig_sunburst = px.sunburst(
    df_sunburst_data,
    path=['state_full_name', 'category_display'],
    values='total_payment_value',
    color='state_full_name',
    color_discrete_sequence=DARKER_PALETTE,
    template='plotly_white',
    branchvalues="total" 
)

# 4. RESTORE LEGEND (Clean method)
# We add empty traces that don't have axes to keep the background clean
unique_states = sorted(df_sunburst_data['state_full_name'].unique())
for i, state in enumerate(unique_states):
    fig_sunburst.add_trace(pd.Series().pipe(lambda _: px.scatter(
        x=[None], y=[None], color=[state], 
        color_discrete_sequence=[DARKER_PALETTE[i % len(DARKER_PALETTE)]]
    )).data[0])

# 5. STYLE UPDATES
fig_sunburst.update_layout(
    margin=dict(t=20, l=10, r=10, b=10),
    height=850,
    showlegend=True,
    # Force background to be empty/no grid
    xaxis=dict(visible=False),
    yaxis=dict(visible=False),
    legend=dict(title="State Name", orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.05)
)

fig_sunburst.update_traces(
    insidetextorientation='radial', 
    # Wrap %{label} in <b> tags to bold the text on the chart segments
    texttemplate="<b>%{label}</b>, R$ %{value:,.0f}",
    hovertemplate='<b>%{label}</b><br>Revenue: R$ %{value:,.2f}',
    marker=dict(line=dict(color='#FFFFFF', width=1)),
    selector=dict(type='sunburst') 
)

st.plotly_chart(fig_sunburst, use_container_width=True)
