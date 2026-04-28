import streamlit as st
import io
import re   
import pandas as pd
from azure.identity import ClientSecretCredential
import numpy as np
from datetime import date, datetime, timezone
import sqlalchemy as sa
import os
from dotenv import load_dotenv
import warnings
import time
from streamlit_cookies_manager import EncryptedCookieManager

warnings.filterwarnings("ignore")

# =============================
# ENV CONFIG
# =============================
load_dotenv()

APP_USERNAME = os.getenv("APP_USERNAME")
APP_PASSWORD = os.getenv("APP_PASSWORD")

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")

SQL_ENDPOINT = os.getenv("SQL_ENDPOINT")
DATABASE = "WT_LH_Gold"
SCOPE = os.getenv("SCOPE")

# Page configuration
st.set_page_config(
    page_title="Customer Analytics Dashboard", 
    layout="wide",
    initial_sidebar_state="expanded"
)

import logging
logging.getLogger("streamlit").setLevel(logging.ERROR)

cookies = EncryptedCookieManager(
    prefix="customer_analytics_",
    password=APP_PASSWORD  # uses your existing env var as encryption key
)

if not cookies.ready():
    st.stop()

# =============================
# CUSTOM CSS FOR LIGHT THEME UI
# =============================
st.markdown("""
<style>
    /* Main container styling */
    .main {
        padding: 0rem 1rem;
    }
    
    /* Header styling */
    .main-header {
        background: linear-gradient(135deg, #1a73e8 0%, #0d47a1 100%);
        padding: 1.5rem 2rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        color: white;
    }
    
    .main-header h1 {
        margin: 0;
        font-size: 2rem;
        font-weight: 600;
    }
    
    .main-header p {
        margin: 0.5rem 0 0 0;
        opacity: 0.9;
        font-size: 0.9rem;
    }
    
    /* Metric card — pure HTML, no Streamlit widget inside */
    .metric-card {
        background-color: white;
        border-radius: 12px;
        padding: 1.4rem 1.6rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.07);
        border: 1px solid #e0e0e0;
        transition: box-shadow 0.3s ease, transform 0.3s ease;
        min-height: 110px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        gap: 0.35rem;
    }

    .metric-card:hover {
        box-shadow: 0 4px 16px rgba(0,0,0,0.12);
        transform: translateY(-2px);
    }

    .metric-card .mc-label {
        font-size: 0.72rem;
        font-weight: 600;
        color: #6b7280;
        text-transform: uppercase;
        letter-spacing: 0.7px;
        margin: 0;
        line-height: 1;
    }

    .metric-card .mc-value {
        font-size: 1.85rem;
        font-weight: 700;
        color: #111827;
        margin: 0;
        line-height: 1.15;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    
    /* Divider styling */
    hr {
        margin: 1.5rem 0 !important;
        border: none !important;
        height: 1px !important;
        background: linear-gradient(to right, #e0e0e0, transparent) !important;
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
        background-color: #f8f9fa;
        padding: 0.5rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 0.5rem 1.5rem;
        font-weight: 500;
        transition: all 0.2s ease;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: white !important;
        color: #1a73e8 !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    /* Primary action button — blue */
    .stButton > button[kind="primary"],
    .stButton > button {
        background-color: #1a73e8;
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-weight: 500;
        font-size: 0.875rem;
        transition: background-color 0.2s ease, box-shadow 0.2s ease;
        width: 100%;
        height: 38px;
        line-height: 1;
    }
    
    .stButton > button:hover {
        background-color: #1558b0;
        box-shadow: 0 2px 8px rgba(26, 115, 232, 0.35);
    }

    /* Clear filter button — neutral gray */
    .stButton > button[data-testid*="clear"] {
        background-color: #6b7280 !important;
        color: white !important;
    }

    .stButton > button[data-testid*="clear"]:hover {
        background-color: #4b5563 !important;
    }
    
    /* Download / Export button — green */
    .stDownloadButton > button {
        background-color: #16a34a !important;
        color: white !important;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-weight: 500;
        font-size: 0.875rem;
        width: 100%;
        height: 38px;
        line-height: 1;
        transition: background-color 0.2s ease, box-shadow 0.2s ease;
    }

    .stDownloadButton > button:hover {
        background-color: #15803d !important;
        box-shadow: 0 2px 8px rgba(22, 163, 74, 0.35);
    }
    
    /* Search box styling */
    .stTextInput input {
        border-radius: 8px;
        border: 1px solid #e0e0e0;
        padding: 0.6rem 1rem;
        font-size: 0.9rem;
        transition: all 0.3s ease;
        height: 38px;
    }
    
    .stTextInput input:focus {
        border-color: #1a73e8;
        box-shadow: 0 0 0 2px rgba(26, 115, 232, 0.12);
    }
    
    /* Info box styling */
    .stAlert {
        border-radius: 8px;
        border-left: 4px solid #1a73e8;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #f8f9fa;
        border-right: 1px solid #e0e0e0;
    }
    
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stDateInput label,
    [data-testid="stSidebar"] .stTextInput label {
        font-weight: 500;
        color: #495057;
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Dataframe styling */
    [data-testid="stDataFrame"] {
        border-radius: 8px;
        border: 1px solid #e0e0e0;
        overflow: hidden;
    }
    
    /* Success message styling */
    .stSuccess {
        border-radius: 8px;
        background-color: #d4edda;
        color: #155724;
        border: 1px solid #c3e6cb;
    }
    
    /* Caption styling */
    .custom-caption {
        font-size: 0.95rem;
        color: #374151;
        margin-top: 0.25rem;
        display: inline-block;
    }
    
    /* Inline view selector row — tighten gap */
    .view-selector-row {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        margin-bottom: 1.25rem;
        flex-wrap: wrap;
    }

    .view-selector-label {
        font-weight: 600;
        color: #1a1a2e;
        font-size: 0.95rem;
        white-space: nowrap;
        margin: 0;
    }

    /* Push radio buttons inline with label */
    .view-selector-row .stRadio {
        margin: 0 !important;
    }

    .view-selector-row .stRadio > div {
        flex-direction: row !important;
        gap: 0.75rem !important;
        align-items: center;
    }

    .view-selector-row .stRadio [data-testid="stWidgetLabel"] {
        display: none !important;  /* hide the default label; we show our own */
    }

    /* Radio button items */
    .stRadio label {
        font-weight: 500;
        font-size: 0.875rem;
        padding: 0.35rem 0.85rem;
        cursor: pointer;
        transition: all 0.18s ease;
        color: #374151;
    }

    .stRadio label:hover {
        border-color: #1a73e8;
        color: #1a73e8;
    }

    /* Header row for table title + export */
    .header-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
        padding: 0 0.25rem;
    }

    /* Loading spinner */
    .stSpinner > div {
        border-top-color: #1a73e8 !important;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_engine():
    connection_string = (
        f"mssql+pyodbc://{CLIENT_ID}:{CLIENT_SECRET}"
        f"@{SQL_ENDPOINT}:1433/{DATABASE}"
        f"?driver=ODBC+Driver+17+for+SQL+Server"
        f"&authentication=ActiveDirectoryServicePrincipal"
        f"&Encrypt=yes"
        f"&TrustServerCertificate=no"
    )
    return sa.create_engine(connection_string)

engine = get_engine()

# --- AUTHENTICATION & QUERY HELPER ---
@st.cache_data(ttl=3000)
def get_access_token():
    try:
        credential = ClientSecretCredential(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
        token_obj = credential.get_token(SCOPE)
        return token_obj.token
    except Exception as e:
        st.error(f"Authentication Error: {e}")
        return None

def run_query(query, params=None):
    try:
        with engine.connect() as conn:
            return pd.read_sql(sa.text(query), conn, params=params)
    except Exception as e:
        st.error(f"SQL Error: {e}")
        print(e)
        return None
    
# --- SAFE METRIC VALUE FUNCTION ---
def safe_metric_value(value, format_type="number"):
    """Safely format metric values handling None and invalid values"""
    if value is None:
        return "0"
   
    try:
        if format_type == "currency":
            formatted_val = "{:.2f}".format(round(float(value), 2))
            main_part, decimal_part = formatted_val.split('.')
            
            if len(main_part) > 3:
                last_three = main_part[-3:]
                remaining = main_part[:-3]
                remaining = re.sub(r'(\d+?)(?=(\d{2})+(?!\d))', r'\1,', remaining)
                main_part = remaining + ',' + last_three
                
            return f"Rs.{main_part}.{decimal_part}"
        else:
            return f"{int(float(value)):,}"
    except (ValueError, TypeError):
        return "0"  

# --- FETCH DYNAMIC FILTER DATA ---
filter_query = """
WITH min_date_cte AS (
    SELECT MIN(order_date) AS min_table_date
    FROM Customer_Analytics.customers_app
)

SELECT DISTINCT
    c.customer_brand,
    c.store_location,
    c.utm_source,
    c.platform,
    m.min_table_date
FROM Customer_Analytics.customers_app c
CROSS JOIN min_date_cte m;
"""

@st.cache_data(ttl=600)
def get_filter_metadata():
    df = run_query(filter_query)

    if df is not None and not df.empty:
        m_date = date(2023, 1, 1)

        if 'min_table_date' in df.columns:
            try:
                m_date = pd.to_datetime(df['min_table_date'].min()).date()
            except:
                pass

        return df, m_date

    return pd.DataFrame(), date(2023, 1, 1)

df_filter_master, global_min_date = get_filter_metadata()

# Initialize session state
if "logged_in" not in st.session_state:
    st.session_state.logged_in = cookies.get("logged_in") == "true" 
if 'full_dataframe' not in st.session_state:
    st.session_state.full_dataframe = None
if 'filtered_dataframe' not in st.session_state:
    st.session_state.filtered_dataframe = None
if 'metrics_data' not in st.session_state:
    st.session_state.metrics_data = None
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'all_data_loaded' not in st.session_state:
    st.session_state.all_data_loaded = False
if 'total_records' not in st.session_state:
    st.session_state.total_records = 0
if 'fetch_in_progress' not in st.session_state:
    st.session_state.fetch_in_progress = False
if 'from_date' not in st.session_state:
    st.session_state.from_date = global_min_date
if 'to_date' not in st.session_state:
    st.session_state.to_date = None
if 'filters_applied' not in st.session_state:
    st.session_state.filters_applied = False
if 'error_message' not in st.session_state:
    st.session_state.error_message = None
if 'search_text' not in st.session_state:
    st.session_state.search_text = ""

# =============================
# LOGIN SECTION
# =============================
st.sidebar.header("Login")

if not st.session_state.logged_in:

    with st.sidebar.form("login_form"):
        username_input = st.text_input("Username")
        password_input = st.text_input("Password", type="password")
        login_btn = st.form_submit_button("Login", use_container_width=True)

    if login_btn:
        if username_input == APP_USERNAME and password_input == APP_PASSWORD:
            st.session_state.logged_in = True
            cookies["logged_in"] = "true"
            cookies.save()
            st.sidebar.success("Logged in successfully")
            st.rerun()
        else:
            st.sidebar.error("Invalid username or password")

    st.stop()

else:
    st.sidebar.success("Logged in")

    if st.sidebar.button("Logout", use_container_width=True):
        st.session_state.logged_in = False
        cookies["logged_in"] = "false"
        cookies.save()
        st.rerun()
        
# --- SIDEBAR FILTERS ---
st.sidebar.markdown("---")
st.sidebar.subheader("Filter Settings")
temp_filter_df = df_filter_master.copy()

available_cols = temp_filter_df.columns.tolist()

# 1. Brand Filter
brand = None
if 'customer_brand' in available_cols:
    brand_list = sorted(temp_filter_df['customer_brand'].dropna().unique().tolist())
    brand = st.sidebar.selectbox("Brand", [None] + brand_list, key="brand_filter")
    if brand:
        temp_filter_df = temp_filter_df[temp_filter_df['customer_brand'] == brand]
else:
    st.sidebar.warning("Column 'customer_brand' not found in filter data.")
    brand = st.sidebar.text_input("Brand (Enter manually)", placeholder="e.g. Prashanti", key="brand_input")

# 2. Store Location Filter
store_loc = None
if 'store_location' in available_cols:
    store_options = sorted(temp_filter_df['store_location'].dropna().unique().tolist())
    store_loc = st.sidebar.selectbox("Store Location", [None] + store_options, key="store_filter")
    if store_loc:
        temp_filter_df = temp_filter_df[temp_filter_df['store_location'] == store_loc]
else:
    store_loc = st.sidebar.text_input("Store Location (Enter manually)", placeholder="e.g. Mumbai Store", key="store_input")

# 3. UTM Source Filter
utm_source = None
if 'utm_source' in available_cols:
    cat_options = sorted(temp_filter_df['utm_source'].dropna().unique().tolist())
    utm_source = st.sidebar.selectbox("UTM Source", [None] + cat_options, key="utm_source_filter")
else:
    utm_source = st.sidebar.text_input("UTM Source (Enter manually)", placeholder="e.g. Facebook", key="utm_source_input")

# 4. Platform Filter
platform = None
if 'platform' in available_cols:
    pf_options = sorted(temp_filter_df['platform'].dropna().unique().tolist())
    platform = st.sidebar.selectbox("Platform", [None] + pf_options, key="platform_filter")
else:
    platform = st.sidebar.text_input("Platform (Enter manually)", placeholder="e.g. Shopify", key="platform_input")

st.sidebar.markdown("---")
st.sidebar.subheader("Transaction Period")

col_from, col_to = st.sidebar.columns(2)

with col_from:
    st.markdown("**From Date**")
    from_date = st.date_input(
        "Select From Date",
        value=st.session_state.from_date,
        min_value=global_min_date,
        max_value=date.today(),
        key="from_date_input",
        label_visibility="collapsed",
        help=f"Leave empty to fetch from earliest available date ({global_min_date})"
    )

with col_to:
    st.markdown("**To Date**")
    to_date = st.date_input(
        "Select To Date",
        value=None,
        min_value=global_min_date,
        max_value=date.today(),
        key="to_date_input",
        label_visibility="collapsed",
        help="Leave empty to fetch until today"
    )

st.sidebar.caption(f"Leave dates empty to fetch all data from {global_min_date} to today")

st.sidebar.markdown("---")
st.sidebar.subheader("Search Criteria")
city_input = st.sidebar.text_input("City", placeholder="e.g. Chennai", key="city_input")
country_input = st.sidebar.text_input("Country", placeholder="e.g. India", key="country_input")
province_input = st.sidebar.text_input("Province", placeholder="e.g. Tamil Nadu", key="province_input")
prod_category = st.sidebar.text_input("Product Category", placeholder="e.g. Silk Sarees", key="category_input")

st.sidebar.markdown("---")
fetch_data = st.sidebar.button("Fetch Analytics Data", type="primary", key="fetch_button", use_container_width=True)

# Metrics query
metrics_query = """
SELECT 
    COUNT(DISTINCT customer_id) AS total_customers,
    COUNT(DISTINCT CASE WHEN is_old_order = 0 THEN order_id END) AS total_orders,
    SUM(deduped.total_lineItem_amount)      
        AS total_spent
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id, lineItem_id
               ORDER BY email DESC
           ) AS rn
    FROM Customer_Analytics.customers_app
    WHERE 
        (:utm_source IS NULL OR utm_source = :utm_source)
        AND (:brand IS NULL OR customer_brand = :brand)
        AND (:store_location IS NULL OR store_location = :store_location)
        AND (:platform IS NULL OR platform = :platform)
        AND (:city IS NULL OR LOWER(city) LIKE '%' + LOWER(:city) + '%')
        AND (:country IS NULL OR LOWER(country) LIKE '%' + LOWER(:country) + '%')
        AND (:province IS NULL OR LOWER(province) LIKE '%' + LOWER(:province) + '%')
        AND (:category IS NULL OR LOWER(product_category) LIKE '%' + LOWER(:category) + '%')
        AND (:start_date IS NULL OR order_date >= :start_date)
        AND (:end_date IS NULL OR order_date <= :end_date)
    ) deduped
WHERE rn = 1
"""

# Customers query - PHONE LEVEL
customers_query_phone = """WITH base AS (
    SELECT *
    FROM Customer_Analytics.customers_app
    WHERE 
        (:utm_source IS NULL OR utm_source = :utm_source)
        AND (:brand IS NULL OR customer_brand = :brand)
        AND (:store_location IS NULL OR store_location = :store_location)
        AND (:platform IS NULL OR platform = :platform)
        AND (:city IS NULL OR LOWER(city) LIKE '%' + LOWER(:city) + '%')
        AND (:country IS NULL OR LOWER(country) LIKE '%' + LOWER(:country) + '%')
        AND (:province IS NULL OR LOWER(province) LIKE '%' + LOWER(:province) + '%')
        AND (:category IS NULL OR LOWER(product_category) LIKE '%' + LOWER(:category) + '%')
        AND (:start_date IS NULL OR order_date >= :start_date)
        AND (:end_date IS NULL OR order_date <= :end_date)
),

-- Rows WITH orders: deduplicate each line item to one phone
deduped_with_orders AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id, lineItem_id
               ORDER BY phone ASC
           ) AS rn
    FROM base
    WHERE phone IS NOT NULL
      AND order_id IS NOT NULL
),

-- Rows WITHOUT orders: one row per phone, no dedup needed
no_orders AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY phone
               ORDER BY customer_created_date DESC
           ) AS rn
    FROM base
    WHERE phone IS NOT NULL
      AND order_id IS NULL
),

clean AS (
    SELECT * FROM deduped_with_orders WHERE rn = 1
    UNION ALL
    SELECT * FROM no_orders WHERE rn = 1
),

agg_categories AS (
    SELECT phone,
           STRING_AGG(CAST(product_category AS VARCHAR(MAX)), ', ') AS product_categories
    FROM (SELECT DISTINCT phone, product_category FROM clean WHERE product_category IS NOT NULL) x
    GROUP BY phone
),

agg_utm AS (
    SELECT phone,
           STRING_AGG(CAST(utm_source AS VARCHAR(MAX)), ', ') AS utm_source
    FROM (SELECT DISTINCT phone, utm_source FROM clean WHERE utm_source IS NOT NULL) x
    GROUP BY phone
)

SELECT
    MAX(b.customer_name)                    AS customer_name,
    b.phone                                 AS phone,
    MAX(b.city)                             AS city,
    MAX(b.province)                         AS province,
    MAX(b.country)                          AS country,
    MAX(b.order_date)                       AS latest_order_date,

    COUNT(DISTINCT CASE WHEN b.is_old_order = 0 THEN b.order_id END)              AS total_orders,
    SUM(ISNULL(b.total_lineItem_amount, 0)) AS total_spent,
    SUM(ISNULL(b.quantity, 0))              AS total_qty,

    COUNT(DISTINCT CASE WHEN b.is_returned_lineItem = 1 THEN b.order_id END)                     AS return_orders,
    SUM(CASE WHEN b.is_returned_lineItem = 1 THEN ISNULL(b.total_lineItem_amount, 0) ELSE 0 END) AS return_amount,
    SUM(CASE WHEN b.is_returned_lineItem = 1 THEN ISNULL(b.quantity, 0) ELSE 0 END)              AS return_qty,

    ac.product_categories,
    au.utm_source

FROM clean b
LEFT JOIN agg_categories ac ON b.phone = ac.phone
LEFT JOIN agg_utm au        ON b.phone = au.phone
GROUP BY b.phone, ac.product_categories, au.utm_source
ORDER BY total_spent DESC
"""

# Customers query - EMAIL LEVEL
customers_query_email = """WITH base AS (
    SELECT *
    FROM Customer_Analytics.customers_app
    WHERE 
        (:utm_source IS NULL OR utm_source = :utm_source)
        AND (:brand IS NULL OR customer_brand = :brand)
        AND (:store_location IS NULL OR store_location = :store_location)
        AND (:platform IS NULL OR platform = :platform)
        AND (:city IS NULL OR LOWER(city) LIKE '%' + LOWER(:city) + '%')
        AND (:country IS NULL OR LOWER(country) LIKE '%' + LOWER(:country) + '%')
        AND (:province IS NULL OR LOWER(province) LIKE '%' + LOWER(:province) + '%')
        AND (:category IS NULL OR LOWER(product_category) LIKE '%' + LOWER(:category) + '%')
        AND (:start_date IS NULL OR order_date >= :start_date)
        AND (:end_date IS NULL OR order_date <= :end_date)
),

-- Rows WITH orders: deduplicate each line item to one email
deduped_with_orders AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id, lineItem_id
               ORDER BY email ASC
           ) AS rn
    FROM base
    WHERE email IS NOT NULL
      AND order_id IS NOT NULL          -- only rows that have an order
),

-- Rows WITHOUT orders: one row per email, no dedup needed
no_orders AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY email
               ORDER BY customer_created_date DESC   -- pick most recent profile row
           ) AS rn
    FROM base
    WHERE email IS NOT NULL
      AND order_id IS NULL
),

-- Combine both, keeping only the winning row from each
clean AS (
    SELECT * FROM deduped_with_orders WHERE rn = 1
    UNION ALL
    SELECT * FROM no_orders WHERE rn = 1
),

agg_categories AS (
    SELECT email,
           STRING_AGG(CAST(product_category AS VARCHAR(MAX)), ', ') AS product_categories
    FROM (SELECT DISTINCT email, product_category FROM clean WHERE product_category IS NOT NULL) x
    GROUP BY email
),

agg_utm AS (
    SELECT email,
           STRING_AGG(CAST(utm_source AS VARCHAR(MAX)), ', ') AS utm_source
    FROM (SELECT DISTINCT email, utm_source FROM clean WHERE utm_source IS NOT NULL) x
    GROUP BY email
)

SELECT
    MAX(b.customer_name)                    AS customer_name,
    b.email                                 AS email,
    MAX(b.city)                             AS city,
    MAX(b.province)                         AS province,
    MAX(b.country)                          AS country,
    MAX(b.order_date)                       AS latest_order_date,

    COUNT(DISTINCT CASE WHEN b.is_old_order = 0 THEN b.order_id END)   AS total_orders,
    SUM(ISNULL(b.total_lineItem_amount, 0)) AS total_spent,
    SUM(ISNULL(b.quantity, 0))              AS total_qty,

    COUNT(DISTINCT CASE WHEN b.is_returned_lineItem = 1 THEN b.order_id END)                     AS return_orders,
    SUM(CASE WHEN b.is_returned_lineItem = 1 THEN ISNULL(b.total_lineItem_amount, 0) ELSE 0 END) AS return_amount,
    SUM(CASE WHEN b.is_returned_lineItem = 1 THEN ISNULL(b.quantity, 0) ELSE 0 END)              AS return_qty,

    ac.product_categories,
    au.utm_source

FROM clean b
LEFT JOIN agg_categories ac ON b.email = ac.email
LEFT JOIN agg_utm au        ON b.email = au.email
GROUP BY b.email, ac.product_categories, au.utm_source
ORDER BY total_spent DESC
"""

def format_date(date_obj, is_start_date=True):
    if not date_obj:
        return None
   
    if is_start_date:
        dt = datetime.combine(date_obj, datetime.min.time())
    else:
        dt = datetime.combine(date_obj, datetime.max.time())
   
    dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def fetch_all_data(query, base_variables):
    result = run_query(query, base_variables)
    if result is not None and not result.empty:
        return result.to_dict(orient="records")
    return []
    
@st.cache_data(ttl=3600)
def generate_excel_file(df):
    """Generate Excel file from dataframe (cached for performance)"""
    output = io.BytesIO()
   
    export_df = df.copy()
    if 'S.No' not in export_df.columns:
        export_df.insert(0, 'S.No', range(1, len(export_df) + 1))
   
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        export_df.to_excel(writer, index=False, sheet_name='Customer Analytics')
        worksheet = writer.sheets['Customer Analytics']
        for i, column in enumerate(export_df.columns):
            if i < 15:
                try:
                    col_series = export_df[column]
                    col_series = col_series.apply(
                        lambda x: ", ".join(x) if isinstance(x, list) else str(x) if pd.notnull(x) else ""
                    )
                    max_len = max(col_series.map(len).max(), len(column)) + 2
                except Exception:
                    max_len = len(column) + 2
                worksheet.set_column(i, i, min(max_len, 40))
   
    return output.getvalue()

@st.cache_data(ttl=600)
def get_phone_data_cached(variables_tuple):
    variables = dict(variables_tuple)
    return fetch_all_data(customers_query_phone, variables)

@st.cache_data(ttl=600)
def get_email_data_cached(variables_tuple):
    variables = dict(variables_tuple)
    return fetch_all_data(customers_query_email, variables)

# --- DATA FETCHING & PROCESSING ---
if fetch_data and not st.session_state.fetch_in_progress:
    st.session_state.fetch_in_progress = True
    st.session_state.filters_applied = True
    st.session_state.data_loaded = False
    st.session_state.all_data_loaded = False
    st.session_state.search_text = ""
   
    st.session_state.full_dataframe = None
    st.session_state.filtered_dataframe = None
    st.session_state.metrics_data = None
    st.session_state.total_records = 0
    st.session_state.error_message = None
   
    formatted_start = format_date(from_date, is_start_date=True) if from_date else None
    formatted_end = format_date(to_date, is_start_date=False) if to_date else None
   
    variables = {
        "utm_source": utm_source if utm_source else None,
        "brand": brand if brand else None,
        "store_location": store_loc if store_loc else None,
        "platform": platform if platform else None,
        "city": city_input if city_input else None,
        "country": country_input if country_input else None,
        "province": province_input if province_input else None,
        "category": prod_category if prod_category else None,
        "start_date": formatted_start,
        "end_date": formatted_end
    }

    st.session_state.query_variables = variables
   
    with st.spinner('Fetching customer analytics data...'):
        metrics_result = run_query(metrics_query, variables)
       
        if metrics_result is not None and not metrics_result.empty:
            metrics_raw = metrics_result.to_dict(orient="records")
            if metrics_raw and len(metrics_raw) > 0:
                metrics_data = metrics_raw[0]
                for key in ['total_customers', 'total_orders', 'total_spent']:
                    if metrics_data.get(key) is None:
                        metrics_data[key] = 0
                st.session_state.metrics_data = metrics_data
       
        st.session_state.full_dataframe = None
        st.session_state.filtered_dataframe = None
        st.session_state.total_records = 0
        st.session_state.data_loaded = True
        st.session_state.all_data_loaded = True

        st.success("Filters applied successfully! Switch tabs to view data.")

    st.session_state.fetch_in_progress = False
    st.rerun()

# --- SEARCH FUNCTION ---
def apply_search():
    if st.session_state.full_dataframe is None or st.session_state.full_dataframe.empty:
        return
   
    search_text = st.session_state.search_input
    st.session_state.search_text = search_text
   
    if search_text:
        q = search_text.strip().lower()
        mask = pd.Series([False] * len(st.session_state.full_dataframe))
       
        if 'Customer Name' in st.session_state.full_dataframe.columns:
            mask |= st.session_state.full_dataframe['Customer Name'].str.lower().str.contains(q, na=False)
        if 'Email' in st.session_state.full_dataframe.columns:
            mask |= st.session_state.full_dataframe['Email'].str.lower().str.contains(q, na=False)
        if 'Phone' in st.session_state.full_dataframe.columns:
            mask |= st.session_state.full_dataframe['Phone'].astype(str).str.contains(q, na=False)
       
        st.session_state.filtered_dataframe = st.session_state.full_dataframe[mask].copy()
    else:
        st.session_state.filtered_dataframe = st.session_state.full_dataframe.copy()

def clear_search():
    st.session_state.search_text = ""
    st.session_state.filtered_dataframe = st.session_state.full_dataframe.copy()

# --- DISPLAY LOGIC ---
st.markdown("""
<div class="main-header">
    <h1>Customer Analytics Dashboard</h1>
    <p>Comprehensive customer insights and analytics platform</p>
</div>
""", unsafe_allow_html=True)

# Display metric KPI cards — pure HTML so values stay inside card boundaries
if st.session_state.metrics_data:
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <p class="mc-label">Total Customers</p>
            <p class="mc-value">{safe_metric_value(st.session_state.metrics_data.get('total_customers'))}</p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <p class="mc-label">Total Orders</p>
            <p class="mc-value">{safe_metric_value(st.session_state.metrics_data.get('total_orders'))}</p>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <p class="mc-label">Total Revenue</p>
            <p class="mc-value">{safe_metric_value(st.session_state.metrics_data.get('total_spent'), "currency")}</p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

# Main data display
if st.session_state.data_loaded:

    # --- Inline "Select View" label + radio on the same row ---
    label_col, radio_col = st.columns([0.4, 4])

    with label_col:
        st.markdown(
            "<p style='font-weight:600; color:#1a1a2e; font-size:0.95rem; "
            "padding-top:0.6rem; margin:0; white-space:nowrap;'>Select View</p>",
            unsafe_allow_html=True
        )

    with radio_col:
        view_option = st.radio(
            label="view_option",
            options=["Phone Level", "Email Level"],
            horizontal=True,
            index=0,
            label_visibility="collapsed"
        )

    variables = st.session_state.get("query_variables")

    if variables is None:
        st.info("Set filters and click 'Fetch Analytics Data'")
        st.stop()

    if view_option == "Phone Level":
        raw_data = get_phone_data_cached(tuple(variables.items()))
    else:
        raw_data = get_email_data_cached(tuple(variables.items()))

    if raw_data:
        df = pd.DataFrame(raw_data)

        num_cols = ['total_spent', 'total_qty', 'return_orders', 'return_amount', 'return_qty', 'total_orders']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        column_map = {
            'customer_name': 'Customer Name',
            'email': 'Email',
            'phone': 'Phone',
            'city': 'City',
            'province': 'Province',
            'country': 'Country',
            'latest_order_date': 'Latest Order Date',
            'total_orders': 'Total Orders',
            'total_spent': 'Total Spent',
            'total_qty': 'Total Qty',
            'return_orders': 'Return Orders',
            'return_amount': 'Return Amount',
            'return_qty': 'Return Qty',
            'product_categories': 'Product Categories',
            'utm_source': 'UTM Sources'
        }

        rename_map = {k: v for k, v in column_map.items() if k in df.columns}
        df = df.rename(columns=rename_map)

        if 'Latest Order Date' in df.columns:
            df['Latest Order Date'] = pd.to_datetime(df['Latest Order Date']).dt.date

        if 'Product Categories' in df.columns:
            df['Product Categories'] = df['Product Categories'].apply(
                lambda x: sorted(list(set([item.strip() for item in x.split(',') if item.strip()])))
                if isinstance(x, str) and x.strip() else []
            )

        if 'UTM Sources' in df.columns:
            df['UTM Sources'] = df['UTM Sources'].apply(
                lambda x: sorted(list(set([item.strip() for item in x.split(',') if item.strip()])))
                if isinstance(x, str) and x.strip() else []
            )

        # --- SEARCH BAR (same height as clear button) ---
        search_col1, search_col2 = st.columns([5, 1])

        with search_col1:
            search_input = st.text_input(
                "Search customers",
                placeholder="Search by Name, Email, or Phone...",
                key=f"search_{view_option}",
                label_visibility="collapsed"
            )

        with search_col2:
            clear_clicked = st.button(
                "Clear Filter",
                key=f"clear_{view_option}",
                use_container_width=True
            )

        # --- APPLY SEARCH FILTER ---
        if clear_clicked:
            search_input = ""

        if search_input:
            q = search_input.strip().lower()
            mask = pd.Series([False] * len(df))

            if 'Customer Name' in df.columns:
                mask |= df['Customer Name'].astype(str).str.lower().str.contains(q, na=False)
            if 'Email' in df.columns:
                mask |= df['Email'].astype(str).str.lower().str.contains(q, na=False)
            if 'Phone' in df.columns:
                mask |= df['Phone'].astype(str).str.contains(q, na=False)

            df = df[mask].copy()

        # --- S.NO ---
        df = df.reset_index(drop=True)
        df.insert(0, 'S.No', range(1, len(df) + 1))

        # --- FORMAT ---
        if 'Total Spent' in df.columns:
            df['Total Spent'] = df['Total Spent'].apply(lambda x: safe_metric_value(x, "currency"))

        if 'Return Amount' in df.columns:
            df['Return Amount'] = df['Return Amount'].apply(lambda x: safe_metric_value(x, "currency"))

        # --- HEADER ROW: title + export button same width as clear filter ---
        col1, col2 = st.columns([5, 1])

        with col1:
            record_count = len(df)
            display_name = "Phone Level" if view_option == "Phone Level" else "Email Level"
            st.markdown(f"### {display_name} Customer Data")
            st.markdown(f"<span class='custom-caption'>Showing {record_count:,} records</span>", unsafe_allow_html=True)

        with col2:
            excel_data = generate_excel_file(df)
            file_size = len(excel_data) / (1024 * 1024)
            file_suffix = "phone" if view_option == "Phone Level" else "email"

            st.download_button(
                label=f"Export ({file_size:.1f} MB)",
                data=excel_data,
                file_name=f"customer_{file_suffix}_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

        # --- TABLE ---
        st.dataframe(
            df,
            hide_index=True,
            use_container_width=True,
            height=500
        )

    else:
        st.info("No data available for selected filters")
