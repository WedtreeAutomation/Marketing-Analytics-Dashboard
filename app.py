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

warnings.filterwarnings("ignore")

# =============================s
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
st.set_page_config(page_title="Customer Analytics Dashboard", layout="wide")
st.title("📊 Customer Analytics Dashboard")

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
            # 1. Format as integer string (No decimals)
            formatted_val = "{:.2f}".format(round(float(value), 2))
            
            # Split into integer and decimal parts
            main_part, decimal_part = formatted_val.split('.')
            
            # 2. Logic for Indian Thousand Separator (Lakhs/Crores)
            if len(main_part) > 3:
                last_three = main_part[-3:]
                remaining = main_part[:-3]
                
                # Group the remaining digits in pairs (twos)
                remaining = re.sub(r'(\d+?)(?=(\d{2})+(?!\d))', r'\1,', remaining)
                main_part = remaining + ',' + last_three
                
            # 3. Join back with the decimal part
            return f"₹{main_part}.{decimal_part}"
        else:
            return f"{int(float(value)):,}"
    except (ValueError, TypeError):
        return "0"  

# --- FETCH DYNAMIC FILTER DATA ---
filter_query = """
SELECT 
    customer_brand,
    store_location,
    utm_source,
    platform,
    MIN(order_date) AS min_table_date
FROM Customer_Analytics.customers_app
GROUP BY customer_brand, store_location, utm_source, platform
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
    st.session_state.logged_in = False
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
    st.session_state.from_date = None
if 'to_date' not in st.session_state:
    st.session_state.to_date = None
if 'filters_applied' not in st.session_state:
    st.session_state.filters_applied = False
if 'error_message' not in st.session_state:
    st.session_state.error_message = None
if 'search_text' not in st.session_state:
    st.session_state.search_text = ""

# =============================
# LOGIN SECTION (ENTER TO SUBMIT)
# =============================
st.sidebar.header("🔐 Login")

if not st.session_state.logged_in:

    with st.sidebar.form("login_form"):
        username_input = st.text_input("Username")
        password_input = st.text_input("Password", type="password")

        login_btn = st.form_submit_button("Login")

    if login_btn:
        if username_input == APP_USERNAME and password_input == APP_PASSWORD:
            st.session_state.logged_in = True
            st.sidebar.success("✅ Logged in successfully")
            st.rerun()
        else:
            st.sidebar.error("❌ Invalid username or password")

    # 🚨 Stop rest of app until login
    st.stop()

else:
    st.sidebar.success("✅ Logged in")

    if st.sidebar.button("Logout"):
        st.session_state.logged_in = False
        st.rerun()
        
# --- SIDEBAR FILTERS ---
st.sidebar.header("Filter Settings")
temp_filter_df = df_filter_master.copy()

# Safety Check: Ensure columns exist to avoid KeyError
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
st.sidebar.subheader("📅 Transaction Period")

# Create two columns for From and To dates in sidebar
col_from, col_to = st.sidebar.columns(2)

with col_from:
    st.markdown("**From Date**")
    from_date = st.date_input(
        "Select From Date",
        value=None,
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

st.sidebar.caption(f"💡 Leave dates empty to fetch all data from {global_min_date} to today")

st.sidebar.markdown("---")
st.sidebar.subheader("🔍 Search Criteria")
city_input = st.sidebar.text_input("City", placeholder="e.g. Chennai", key="city_input")
country_input = st.sidebar.text_input("Country", placeholder="e.g. India", key="country_input")
province_input = st.sidebar.text_input("Province", placeholder="e.g. Tamil Nadu", key="province_input")
prod_category = st.sidebar.text_input("Product Category", placeholder="e.g. Silk Sarees", key="category_input")

# Fetch button
fetch_data = st.sidebar.button("Fetch Analytics Data", type="primary", key="fetch_button")

# --- GRAPHQL QUERIES ---
# Metrics query
metrics_query = """
SELECT 
    COUNT(DISTINCT customer_id) AS total_customers,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(ISNULL(total_lineItem_amount,0)) AS total_spent
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
"""

# Customers query with pagination parameters
customers_query = """WITH base AS (
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

agg_categories AS (
    SELECT customer_id,
           STRING_AGG(CAST(product_category AS VARCHAR(MAX)), ', ') AS product_categories
    FROM (
        SELECT DISTINCT customer_id, product_category
        FROM base
        WHERE product_category IS NOT NULL
    ) x
    GROUP BY customer_id
),

agg_utm AS (
    SELECT customer_id,
           STRING_AGG(CAST(utm_source AS VARCHAR(MAX)), ', ') AS utm_source
    FROM (
        SELECT DISTINCT customer_id, utm_source
        FROM base
        WHERE utm_source IS NOT NULL
    ) x
    GROUP BY customer_id
)

SELECT
    b.customer_id,
    MAX(b.customer_name) AS customer_name,
    MAX(b.email) AS email,
    MAX(b.phone) AS phone,
    MAX(b.city) AS city,
    MAX(b.province) AS province,
    MAX(b.country) AS country,
    MAX(b.order_date) AS latest_order_date,

    COUNT(DISTINCT b.order_id) AS total_orders,
    SUM(ISNULL(b.total_lineItem_amount, 0)) AS total_spent,
    SUM(ISNULL(b.quantity, 0)) AS total_qty,

    COUNT(DISTINCT CASE WHEN b.is_returned_lineItem = 1 THEN b.order_id END) AS return_orders,
    SUM(CASE WHEN b.is_returned_lineItem = 1 THEN ISNULL(b.total_lineItem_amount, 0) ELSE 0 END) AS return_amount,
    SUM(CASE WHEN b.is_returned_lineItem = 1 THEN ISNULL(b.quantity, 0) ELSE 0 END) AS return_qty,

    ac.product_categories,
    au.utm_source

FROM base b
LEFT JOIN agg_categories ac ON b.customer_id = ac.customer_id
LEFT JOIN agg_utm au ON b.customer_id = au.customer_id

GROUP BY b.customer_id, ac.product_categories, au.utm_source
ORDER BY total_spent DESC"""

# --- FUNCTION TO FORMAT DATE FOR GRAPHQL ---
def format_date(date_obj, is_start_date=True):
    if not date_obj:
        return None
   
    if is_start_date:
        dt = datetime.combine(date_obj, datetime.min.time())
    else:
        dt = datetime.combine(date_obj, datetime.max.time())
   
    dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def fetch_all_data(base_variables):
    status_text = st.empty()
    status_text.text("📥 Fetching customer data...")

    result = run_query(customers_query, base_variables)
    print(result)

    if result is not None and not result.empty:
        data = result.to_dict(orient="records")
        status_text.text(f"✅ Loaded {len(data):,} records")
        return data
    else:
        status_text.text("⚠️ No data returned from SQL")
        return []
    
# --- FUNCTION TO GENERATE EXCEL FILE ---
@st.cache_data(ttl=3600)
def generate_excel_file(df):
    """Generate Excel file from dataframe (cached for performance)"""
    output = io.BytesIO()
   
    # Add S.No column for export
    export_df = df.copy()
    export_df.insert(0, 'S.No', range(1, len(export_df) + 1))
   
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        export_df.to_excel(writer, index=False, sheet_name='Customer Analytics')
        worksheet = writer.sheets['Customer Analytics']
        for i, column in enumerate(export_df.columns):
            if i < 15:
                max_len = max(export_df[column].astype(str).map(len).max(), len(column)) + 2
                worksheet.set_column(i, i, min(max_len, 40))
   
    return output.getvalue()

# --- DATA FETCHING & PROCESSING ---
if fetch_data and not st.session_state.fetch_in_progress:
    st.session_state.fetch_in_progress = True
    st.session_state.filters_applied = True
    st.session_state.data_loaded = False
    st.session_state.all_data_loaded = False
    st.session_state.search_text = ""
   
    # Reset session state
    st.session_state.full_dataframe = None
    st.session_state.filtered_dataframe = None
    st.session_state.metrics_data = None
    st.session_state.total_records = 0
    st.session_state.error_message = None
   
    # Format dates
    formatted_start = format_date(from_date, is_start_date=True) if from_date else None
    formatted_end = format_date(to_date, is_start_date=False) if to_date else None
   
    # Variables for queries
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
   
    with st.spinner('🛰️ Fetching customer analytics data...'):
        # Fetch metrics
        metrics_result = run_query(metrics_query, variables)
       
        if metrics_result is not None and not metrics_result.empty:
            metrics_raw = metrics_result.to_dict(orient="records")
            if metrics_raw and len(metrics_raw) > 0:
                # Ensure all values are numbers, not None
                metrics_data = metrics_raw[0]
                for key in ['total_customers', 'total_orders', 'total_spent']:
                    if metrics_data.get(key) is None:
                        metrics_data[key] = 0
                st.session_state.metrics_data = metrics_data
       
        # Fetch all customer data with progress tracking
        all_raw_data = fetch_all_data(variables)
       
        if all_raw_data:
            # Convert to DataFrame
            df = pd.DataFrame(all_raw_data)
           
            # Numeric conversion
            num_cols = ['total_spent', 'total_qty', 'return_orders', 'return_amount', 'return_qty', 'total_orders']
            for col in num_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
           
            # Map SQL names to Display Names
            column_map = {
                'customer_id': 'Customer ID',
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
            if rename_map:
                df = df.rename(columns=rename_map)
           
            # Convert date column
            if 'Latest Order Date' in df.columns:
                df['Latest Order Date'] = pd.to_datetime(df['Latest Order Date']).dt.date
           
            # 1. Process Product Categories
            if 'Product Categories' in df.columns:
                df['Product Categories'] = df['Product Categories'].apply(
                    lambda x: sorted(list(set([item.strip() for item in x.split(',') if item.strip()]))) 
                    if isinstance(x, str) and x.strip() else []
                )

            # 2. Process UTM Sources
            if 'UTM Sources' in df.columns:
                df['UTM Sources'] = df['UTM Sources'].apply(
                    lambda x: sorted(list(set([item.strip() for item in x.split(',') if item.strip()]))) 
                    if isinstance(x, str) and x.strip() else []
                )
           
            st.session_state.full_dataframe = df
            st.session_state.filtered_dataframe = df.copy()
            st.session_state.total_records = len(df)
            st.session_state.data_loaded = True
            st.session_state.all_data_loaded = True
           
            st.success(f"✅ Successfully loaded all {len(df):,} customer records!")
           
            # Show summary of customers with/without orders
            if 'Total Orders' in df.columns:
                with_orders = len(df[df['Total Orders'] > 0])
                without_orders = len(df[df['Total Orders'] == 0])
                if without_orders > 0:
                    st.info(f"📊 {with_orders:,} customers with orders • {without_orders:,} customers with no orders")
        else:
            st.warning("No customer data available")
            st.session_state.full_dataframe = pd.DataFrame()
            st.session_state.filtered_dataframe = pd.DataFrame()
            st.session_state.data_loaded = True
            st.session_state.all_data_loaded = True
   
    st.session_state.fetch_in_progress = False
    st.rerun()

# --- SEARCH FUNCTION ---
def apply_search():
    """Apply search filter to the dataframe"""
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
    """Clear search and reset to full dataframe"""
    st.session_state.search_text = ""
    st.session_state.filtered_dataframe = st.session_state.full_dataframe.copy()

# --- CUSTOM CSS ---
st.markdown("""
<style>
    /* Smaller metric cards */
    [data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
        white-space: nowrap !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 1rem !important;
        white-space: nowrap !important;
    }
    /* Ensure long numbers don't wrap */
    div[data-testid="stMetricValue"] > div {
        white-space: nowrap !important;
        overflow: visible !important;
    }
    /* Header alignment */
    .header-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }
    .search-header {
        font-size: 1.2rem !important;
        font-weight: 500 !important;
        margin: 0 !important;
    }
    /* Export button styling */
    .export-button-container {
        display: flex;
        justify-content: flex-end;
        align-items: center;
        height: 100%;
    }
</style>
""", unsafe_allow_html=True)

# --- DISPLAY LOGIC ---
st.write("---")

# Display metrics with safe value handling
if st.session_state.metrics_data:
    st.write("### Summary Metrics")
    metrics = st.session_state.metrics_data
    col1, col2, col3 = st.columns(3)
   
    with col1:
        st.container(border=True).metric(
            "Total Customers",
            safe_metric_value(metrics.get('total_customers'))
        )
   
    with col2:
        st.container(border=True).metric(
            "Total Orders",
            safe_metric_value(metrics.get('total_orders'))
        )
   
    with col3:
        st.container(border=True).metric(
            "Total Revenue",
            safe_metric_value(metrics.get('total_spent'), "currency")
        )
   
    st.divider()

# Main data display
if st.session_state.data_loaded and st.session_state.full_dataframe is not None:
    if not st.session_state.full_dataframe.empty:
        # Header row with Search Customers and Export button
        col1, col2 = st.columns([3, 1])
        with col1:
            st.write("### 🔍 Search Customers")
        with col2:
            st.markdown('<div class="export-button-container">', unsafe_allow_html=True)
           
            # Generate Excel file data
            excel_data = generate_excel_file(st.session_state.full_dataframe)
            file_size = len(excel_data) / (1024 * 1024)
           
            st.download_button(
                label=f"📥 Export to Excel ({file_size:.1f} MB)",
                data=excel_data,
                file_name=f"customer_analytics_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width="stretch",
                key="download_excel"
            )
            st.markdown('</div>', unsafe_allow_html=True)
       
        # Search bar and clear button row
        col1, col2 = st.columns([5, 1])
        with col1:
            search_input = st.text_input(
                "Search",
                placeholder="Name, email or phone...",
                value=st.session_state.search_text,
                key="search_input",
                label_visibility="collapsed",
                on_change=apply_search
            )
        with col2:
            if st.button("Clear Filter", type="secondary", width="stretch"):
                clear_search()
                st.rerun()
       
        # Get the current dataframe to display (filtered or full)
        display_df = st.session_state.filtered_dataframe.copy()
       
        # Show search status
        if st.session_state.search_text:
            st.caption(f"🔍 Filtered by: '{st.session_state.search_text}' • Showing {len(display_df):,} of {len(st.session_state.full_dataframe):,} records")
        
        if not display_df.empty:
            # --- THE "STAY SEQUENTIAL" FIX ---
            # 1. Drop any existing index and reset it to 0, 1, 2...
            display_df = display_df.reset_index(drop=True)
            
            # 2. Create a clean S.No column starting from 1
            display_df.insert(0, 'S.No', range(1, len(display_df) + 1))

            # --- APPLY CUSTOM INDIAN CURRENCY FORMATTING ---
            # We apply your safe_metric_value function to the specific columns
            display_df['Total Spent'] = display_df['Total Spent'].apply(
                lambda x: safe_metric_value(x, format_type="currency")
            )
            display_df['Return Amount'] = display_df['Return Amount'].apply(
                lambda x: safe_metric_value(x, format_type="currency")
            )
            
            # Configure column display
            column_config = {
                "S.No": st.column_config.NumberColumn("S.No", width="small", help="Row number"),
                "Customer ID": None, 
                "Total Orders": st.column_config.NumberColumn("Orders", help="0 = Customer has no orders"),
                "Total Spent": st.column_config.TextColumn("Amount Spent"),
                "Total Qty": st.column_config.NumberColumn("Quantity Ordered"),
                "Return Orders": st.column_config.NumberColumn("Returns"),
                "Return Amount": st.column_config.TextColumn("Return Amount"),
                "Return Qty": st.column_config.NumberColumn("Return Quantity"),
                "Latest Order Date": st.column_config.DateColumn("Last Order Date"),
                "Product Categories": st.column_config.ListColumn("Product Categories"),
                "UTM Sources": st.column_config.ListColumn("UTM Sources")
            }
            
            # 3. Explicitly include "S.No" at the start of the order
            base_order = ["S.No", "Customer Name", "Email", "Phone", "City", "Province", "Country"]
            additional = ["Total Orders", "Total Spent", "Total Qty", "Return Orders", "Return Amount", "Return Qty", "UTM Sources", "Product Categories", "Latest Order Date"]
            
            column_order = [col for col in base_order + additional if col in display_df.columns]
            
            st.dataframe(
                display_df,
                column_config=column_config,
                column_order=column_order,
                hide_index=True, # Hide the actual pandas index
                width="stretch",
                height=500
            )
        else:
            st.info("No records match your search criteria.")
    else:
        st.warning("No customer data available")
else:
    if st.session_state.fetch_in_progress:
        st.info("⏳ Fetching data... Please wait.")
    else:
        st.info("👈 Set your filters and click 'Fetch Analytics Data' to load results")
