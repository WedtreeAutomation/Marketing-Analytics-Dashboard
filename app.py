import streamlit as st
import io
import pandas as pd
import numpy as np
from datetime import date, datetime, timezone, timedelta
import requests
from azure.identity import ClientSecretCredential
import time
import xlsxwriter
import os
from dotenv import load_dotenv
import warnings
import logging

warnings.filterwarnings("ignore")

load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
ENDPOINT = os.getenv("ENDPOINT")
SCOPE = os.getenv("SCOPE")

# Page configuration
st.set_page_config(page_title="Customer Analytics Dashboard", layout="wide")
st.title("📊 Customer Analytics Dashboard")

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

def run_query(query, variables=None):
    token = get_access_token()
    if not token:
        return None
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        response = requests.post(ENDPOINT, json={'query': query, 'variables': variables}, headers=headers, timeout=300)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"API Error: Status {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Query Error: {e}")
        return None

# --- SAFE METRIC VALUE FUNCTION ---
def safe_metric_value(value, format_type="number"):
    """Safely format metric values handling None and invalid values"""
    if value is None:
        return "0"
   
    try:
        if format_type == "currency":
            return f"₹{float(value):,.2f}"
        else:
            return f"{int(float(value)):,}"
    except (ValueError, TypeError):
        return "0"

# --- FETCH DYNAMIC FILTER DATA ---
filter_query = """query {
    executesp_filtersData {
        customer_brand
        store_location
        country_category
        min_table_date
    }
}"""

@st.cache_data(ttl=600)
def get_filter_metadata():
    data = run_query(filter_query)
    if data and 'data' in data and data['data'].get('executesp_filtersData'):
        df = pd.DataFrame(data['data']['executesp_filtersData'])
       
        # Extract global min date from the column
        m_date = date(2023, 1, 1)  # Default fallback
        if not df.empty and 'min_table_date' in df.columns:
            try:
                m_date = pd.to_datetime(df['min_table_date'].iloc[0]).date()
            except:
                pass
        return df, m_date
    return pd.DataFrame(), date(2023, 1, 1)

df_filter_master, global_min_date = get_filter_metadata()

# Initialize session state
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

# 3. Country Category Filter
country_cat = None
if 'country_category' in available_cols:
    cat_options = sorted(temp_filter_df['country_category'].dropna().unique().tolist())
    country_cat = st.sidebar.selectbox("Country Category", [None] + cat_options, key="country_cat_filter")
else:
    country_cat = st.sidebar.text_input("Country Category (Enter manually)", placeholder="e.g. DOMESTIC", key="country_cat_input")

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
metrics_query = """query GetCustomerMetrics(
    $country_category: String,
    $brand: String,
    $store_location: String,
    $city: String,
    $country: String,
    $province: String,
    $category: String,
    $start_date: DateTime,
    $end_date: DateTime
) {
    executesp_readCustomers_metrics(
        country_category: $country_category,
        brand: $brand,
        store_location: $store_location,
        city: $city,
        country: $country,
        province: $province,
        category: $category,
        start_date: $start_date,
        end_date: $end_date
    ) {
        total_customers
        total_orders
        total_spent
    }
}"""

# Customers query with pagination parameters
customers_query = """query GetCustomerDetails(
    $country_category: String,
    $brand: String,
    $store_location: String,
    $city: String,
    $country: String,
    $province: String,
    $category: String,
    $start_date: DateTime,
    $end_date: DateTime,
    $pageNumber: Int,
    $pageSize: Int
) {
    executesp_readCustomers(
        country_category: $country_category,
        brand: $brand,
        store_location: $store_location,
        city: $city,
        country: $country,
        province: $province,
        category: $category,
        start_date: $start_date,
        end_date: $end_date,
        PageNumber: $pageNumber,
        PageSize: $pageSize
    ) {
        customer_id
        customer_name
        email
        phone
        city
        province
        country
        latest_order_date
        total_orders
        total_spent
        total_qty
        return_orders
        return_amount
        return_qty
        product_categories
    }
}"""

# --- FUNCTION TO FORMAT DATE FOR GRAPHQL ---
def format_date_for_graphql(date_obj, is_start_date=True):
    if not date_obj:
        return None
   
    if is_start_date:
        dt = datetime.combine(date_obj, datetime.min.time())
    else:
        dt = datetime.combine(date_obj, datetime.max.time())
   
    dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

# --- FUNCTION TO FETCH ALL PAGES WITH PROGRESS INDICATION ---
def fetch_all_pages(base_variables):
    """Fetch all pages of data with detailed progress tracking"""
    all_data = []
    page_number = 1
    page_size = 10000
    max_pages = 200
   
    # Progress indicators
    progress_bar = st.progress(0)
    status_text = st.empty()
    page_info = st.empty()
   
    total_records_fetched = 0
    estimated_pages = 1
   
    # First, try to get an estimate from metrics
    if st.session_state.metrics_data and st.session_state.metrics_data.get('total_customers'):
        total_estimate = st.session_state.metrics_data.get('total_customers', 0)
        estimated_pages = max(1, (total_estimate + page_size - 1) // page_size)
   
    for page in range(1, max_pages + 1):
        status_text.text(f"📥 Fetching page {page}" + (f" of {estimated_pages}" if estimated_pages > 1 else ""))
       
        page_vars = base_variables.copy()
        page_vars["pageNumber"] = page
        page_vars["pageSize"] = page_size
       
        result = run_query(customers_query, page_vars)
       
        if result and 'data' in result:
            page_data = result['data'].get('executesp_readCustomers', [])
           
            if not page_data:
                status_text.text(f"✅ Completed! Fetched {total_records_fetched:,} records from {page-1} pages")
                break
               
            all_data.extend(page_data)
            total_records_fetched += len(page_data)
           
            # Update progress with better estimation
            if estimated_pages > 1:
                progress = min(page / estimated_pages, 1.0)
            else:
                progress = page / 20
           
            progress_bar.progress(min(progress, 1.0))
            status_text.text(f"✅ Page {page}: {len(page_data):,} records (Total: {total_records_fetched:,})")
           
            if len(page_data) < page_size:
                status_text.text(f"✅ Complete! Loaded {total_records_fetched:,} records from {page} pages")
                break
        else:
            st.error(f"❌ Error fetching page {page}")
            break
   
    progress_bar.empty()
    status_text.empty()
    page_info.empty()
    return all_data

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
    formatted_start = format_date_for_graphql(from_date, is_start_date=True) if from_date else None
    formatted_end = format_date_for_graphql(to_date, is_start_date=False) if to_date else None
   
    # Variables for queries
    variables = {
        "country_category": country_cat if country_cat else None,
        "brand": brand if brand else None,
        "store_location": store_loc if store_loc else None,
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
       
        if metrics_result and 'data' in metrics_result:
            metrics_raw = metrics_result['data'].get('executesp_readCustomers_metrics')
            if metrics_raw and len(metrics_raw) > 0:
                # Ensure all values are numbers, not None
                metrics_data = metrics_raw[0]
                for key in ['total_customers', 'total_orders', 'total_spent']:
                    if metrics_data.get(key) is None:
                        metrics_data[key] = 0
                st.session_state.metrics_data = metrics_data
       
        # Fetch all customer data with progress tracking
        all_raw_data = fetch_all_pages(variables)
       
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
                'product_categories': 'Product Categories'
            }
           
            rename_map = {k: v for k, v in column_map.items() if k in df.columns}
            if rename_map:
                df = df.rename(columns=rename_map)
           
            # Convert date column
            if 'Latest Order Date' in df.columns:
                df['Latest Order Date'] = pd.to_datetime(df['Latest Order Date']).dt.date
           
            # Process product categories
            if 'Product Categories' in df.columns:
                df['Product Categories'] = df['Product Categories'].apply(
                    lambda x: list(set(x.split(', '))) if isinstance(x, str) and x else []
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
            
            # Configure column display
            column_config = {
                "S.No": st.column_config.NumberColumn("S.No", width="small", help="Row number"),
                "Customer ID": None, 
                "Total Orders": st.column_config.NumberColumn("Orders", help="0 = Customer has no orders"),
                "Total Spent": st.column_config.NumberColumn("Spent", format="₹%.2f"),
                "Total Qty": st.column_config.NumberColumn("Quantity"),
                "Return Orders": st.column_config.NumberColumn("Returns"),
                "Return Amount": st.column_config.NumberColumn("Return Amt", format="₹%.2f"),
                "Return Qty": st.column_config.NumberColumn("Return Qty"),
                "Latest Order Date": st.column_config.DateColumn("Last Order"),
                "Product Categories": st.column_config.ListColumn("Categories")
            }
            
            # 3. Explicitly include "S.No" at the start of the order
            base_order = ["S.No", "Customer Name", "Email", "Phone", "City", "Province", "Country"]
            additional = ["Total Orders", "Total Spent", "Total Qty", "Return Orders", "Return Amount", "Return Qty", "Product Categories", "Latest Order Date"]
            
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
