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

# --- 3. SIDEBAR & FILTERS ---
st.sidebar.header("Executive Filters")
if st.sidebar.button('🔄 Clear Disk Cache & Re-fetch'):
    st.cache_data.clear()
    st.rerun()

sel_status = st.sidebar.selectbox("Order Status", ["All"] + sorted(df_orders["order_status"].unique().tolist()))
sel_segment = st.sidebar.selectbox("Customer Segment (RFV)", ["All"] + sorted(df_customers["customer_segment"].unique().tolist()))

# Applying Filters
dff_orders = df_orders.copy()
if sel_status != "All": dff_orders = dff_orders[dff_orders["order_status"] == sel_status]
dff_customers = df_customers.copy()
if sel_segment != "All": dff_customers = dff_customers[dff_customers["customer_segment"] == sel_segment]

# --- 4. EXECUTIVE SUMMARY (KPIs) ---
st.title("📦 Olist E-commerce Performance")
st.caption(f"Project ID: {PROJECT_ID} | Stack: Meltano + dbt + Dagster")

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Total Customers", f"{len(df_customers):,}")
m2.metric("Leads", f"{len(df_customers[df_customers['customer_segment'] == 'Lead']):,}")
m3.metric("Total Revenue", f"R$ {df_orders['actual_amount_paid'].sum():,.0f}")
m4.metric("Transactions", f"{len(df_orders):,}")
m5.metric("Products Sold", f"{len(df_sales):,}")
st.divider()

# --- 5. CATEGORIZED TABS WITH PROFESSIONAL COLOR SCHEMES ---
tab_sales, tab_logistics, tab_others = st.tabs(["📊 Sales Performance", "🚚 Logistics Analysis", "🔍 Others & Deep Dives"])

with tab_sales:
    # 1. Bar Chart: Sales by State (Viridis)
    st.subheader("Sales by State")
    
    # 1. Data Prep: Join Sales with Location and aggregate Revenue
    state_sales = df_sales.merge(df_location, on='location_id').groupby('geolocation_state')['total_payment_value'].sum().reset_index()
    state_sales = state_sales.sort_values('total_payment_value', ascending=False)

    # 2. CREATE THE BAR CHART (Matching your Image)
    fig_state = px.bar(
        state_sales, 
        x='geolocation_state', 
        y='total_payment_value',
        color_discrete_sequence=['#E8E041'], # Exact yellow from your image
        template='plotly_white',            # FORCE WHITE BACKGROUND
        labels={'total_payment_value': 'revenue', 'geolocation_state': 'State'}
    )

    # 3. STYLE UPDATES (Matching the 15M Axis & Vertical Label)
    fig_state.update_layout(
        yaxis_range=[0, 15000000],           # Capped at 15M like your image
        yaxis_title="revenue",               # Vertical label as requested
        xaxis_title=None,
        plot_bgcolor='white',
        paper_bgcolor='white',
        margin=dict(l=0, r=0, t=20, b=0),
        height=500
    )

    # Add the specific horizontal gridlines (15M, 10M, 5M)
    fig_state.update_yaxes(
        showgrid=True, 
        gridcolor='LightGrey',
        tickvals=[5000000, 10000000, 15000000],
        ticktext=['5M', '10M', '15M']
    )

    st.plotly_chart(fig_state, use_container_width=True)
    
    # 2. Line Chart: Monthly Revenue (Dark Purple/Viridis end)
    st.subheader("Monthly Revenue Trend")
    
    # 1. Prepare data (Ensuring chronological order)
    df_orders['order_purchase_timestamp'] = pd.to_datetime(df_orders['order_purchase_timestamp'])
    monthly = df_orders.groupby(df_orders['order_purchase_timestamp'].dt.strftime('%b %Y'))['actual_amount_paid'].sum().reset_index()
    
    # Re-sorting chronologically because strftime breaks order
    monthly['sort_date'] = pd.to_datetime(monthly['order_purchase_timestamp'])
    monthly = monthly.sort_values('sort_date')

    # 2. CREATE THE BAR CHART (To match your image)
    fig_trend = px.bar(
        monthly, 
        x='order_purchase_timestamp', 
        y='actual_amount_paid',
        color='actual_amount_paid',       # Color bars by revenue
        color_continuous_scale='Viridis', # Exact color scheme from image
        text_auto='.3s',                  # Add labels on top (e.g., 1.5M)
        labels={'actual_amount_paid': 'Revenue', 'order_purchase_timestamp': 'Month'}
    )

    # 3. STYLE UPDATES (White Background & Capped Y-Axis)
    fig_trend.update_layout(
        plot_bgcolor='white',             # Set white background
        paper_bgcolor='white',            # Set container background
        yaxis_range=[0, 2000000],         # Cap at 2 Million as requested
        showlegend=False,
        coloraxis_showscale=True,         # Show the color bar on the right
        xaxis_tickangle=-45               # Slant dates like in the image
    )

    # Add gridlines for the "Professional" look
    fig_trend.update_yaxes(showgrid=True, gridcolor='LightGrey')
    fig_trend.update_xaxes(showgrid=False)

    st.plotly_chart(fig_trend, use_container_width=True)
   
    # 3. Treemap: Customer Value (Viridis)
    st.subheader("Clients by RFM Segment")

    # 1. Data Prep: Count customers per segment
    df_rfm_counts = df_customers.groupby('customer_segment').size().reset_index(name='client_count')
    df_rfm_counts = df_rfm_counts.sort_values('client_count', ascending=True) # Sort for horizontal bar

    # 2. CREATE THE HORIZONTAL BAR (Matching your Image)
    fig_rfm = px.bar(
        df_rfm_counts, 
        y='customer_segment', 
        x='client_count',
        orientation='h',              # Horizontal orientation
        color='client_count',          # Color based on count
        color_continuous_scale='Plasma', # High-contrast gradient like your image
        text_auto=',.0f',             # Add labels inside/on bars (e.g., 18,591)
        template='plotly_white',       # FORCE WHITE BACKGROUND
        labels={'client_count': 'Number of Clients', 'customer_segment': 'Segment'}
    )

    # 3. STYLE UPDATES (Jobseeker Aesthetic)
    fig_rfm.update_traces(
        textposition='inside', 
        textfont_size=14,
        marker_line_color='white',
        marker_line_width=1
    )

    fig_rfm.update_layout(
        xaxis_title=None,
        yaxis_title=None,
        coloraxis_showscale=True,      # Show the color bar on the right like your image
        height=500,
        margin=dict(l=0, r=50, t=20, b=0)
    )

    # Remove X-axis numbers to match the "clean" look of your image
    fig_rfm.update_xaxes(showticklabels=False, showgrid=False)
    fig_rfm.update_yaxes(showgrid=False)

    st.plotly_chart(fig_rfm, use_container_width=True)

with tab_logistics:
    # 4. Violin Plot: Lead Time (Purple/Teal Duo)
    st.subheader("🚚 Delivery Performance Analysis")
    fig_violin = px.violin(
        dff_orders, 
        y="lead_time_days", 
        x="order_status", 
        color="is_delivered_on_time", 
        box=True, 
        template='plotly_white',
        color_discrete_sequence=['#440154', '#E8E041'], # Purple & Yellow theme
        labels={'lead_time_days': 'Lead Time (Days)', 'order_status': 'Status'}
    )
    fig_violin.update_layout(margin=dict(l=0, r=0, t=20, b=0), height=450)
    st.plotly_chart(fig_violin, use_container_width=True)
    
    # 2. Customer Geographic Spread (Map)
    st.subheader("📍 Customer Geographic Spread")
    # Using st.map for a clean, integrated look with the dashboard theme
    st.map(dff_customers[['latitude', 'longitude']].dropna(), size=2, color="#440154")

with tab_others:
    # 6. Scatter: Value vs Freight (Prism)
    st.subheader("💰 Value vs Freight Analysis")
    fig_scatter = px.scatter(
        dff_orders, 
        x="total_item_value", 
        y="total_freight_value", 
        color="is_delivered_on_time", 
        trendline="ols",
        template='plotly_white',
        color_discrete_sequence=['#440154', '#E8E041'],
        labels={'total_item_value': 'Item Value', 'total_freight_value': 'Freight Cost'}
    )
    fig_scatter.update_layout(plot_bgcolor='white', margin=dict(l=0, r=0, t=20, b=0))
    st.plotly_chart(fig_scatter, use_container_width=True)
    
    st.subheader("Top 15 Product Categories by Quantity")

    # 1. Data Preparation: Aggregate by Quantity Sold
    # We join sales with products to get category names
    df_cat_qty = df_sales.merge(df_products, on='product_id')
    cat_summary = df_cat_qty.groupby('product_category_name').size().reset_index(name='quantity')
    
    # 2. Get Top 15 and group the rest as 'OTHERS' to match your image
    top_15_cats = cat_summary.nlargest(15, 'quantity')
    others_qty = cat_summary[~cat_summary['product_category_name'].isin(top_15_cats['product_category_name'])]['quantity'].sum()
    
    # Combine Top 15 + Others row
    df_donut = pd.concat([
        top_15_cats, 
        pd.DataFrame({'product_category_name': ['OTHERS'], 'quantity': [others_qty]})
    ])

    # 3. Create the Donut Chart (Mimicking your Image)
    fig_donut = px.pie(
        df_donut, 
        values='quantity', 
        names='product_category_name',
        hole=0.5, # Creates the "Donut" hole
        template='plotly_white', # <--- Force white background
        color_discrete_sequence=px.colors.qualitative.Prism, # Professional multi-color
        title="Total Quantity Sum per Category"
    )

    # 4. Style to match your Image exactly
    fig_donut.update_traces(
        textposition='inside', 
        textinfo='percent', # Show % inside slices like your image
        hoverinfo='label+value',
        marker=dict(line=dict(color='#FFFFFF', width=2)) # White borders between slices
    )

    fig_donut.update_layout(
        showlegend=True,
        legend=dict(title="Product Categories", orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.1),
        margin=dict(t=50, b=20, l=20, r=100), # Extra right margin for the legend
        height=600
    )

    st.plotly_chart(fig_donut, use_container_width=True)

with st.expander("🔍 View Technical Data Audit"):
    st.dataframe(dff_orders.tail(10), use_container_width=True)
