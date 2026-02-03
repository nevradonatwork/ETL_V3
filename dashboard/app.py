"""
Banking ETL Dashboard
=====================
Interactive Streamlit dashboard for the Banking ETL System.

Usage:
    streamlit run dashboard/app.py

Author: Nevra Donat
"""

import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

# Page config
st.set_page_config(
    page_title="Banking ETL Dashboard",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

def get_project_root():
    """Get the project root directory (normalized for Windows)."""
    return os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))


# Database path
DB_PATH = os.path.normpath(os.path.join(get_project_root(), 'database', 'banking.db'))


# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================

@st.cache_resource
def get_connection():
    """Get database connection."""
    if not os.path.exists(DB_PATH):
        return None
    return sqlite3.connect(DB_PATH, check_same_thread=False)


@st.cache_data(ttl=60)
def run_query(query):
    """Run a SQL query and return DataFrame."""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


def table_exists(table_name):
    """Check if a table exists."""
    conn = get_connection()
    if conn is None:
        return False
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cursor.fetchone() is not None


def get_kpi_value(kpi_name, default=0):
    """Get a KPI value from rptDashboardKPIs."""
    if not table_exists('rptDashboardKPIs'):
        return default
    df = run_query(f"SELECT kpi_value FROM rptDashboardKPIs WHERE kpi_name = '{kpi_name}'")
    if len(df) > 0:
        return df.iloc[0]['kpi_value']
    return default


# ============================================================================
# UI COMPONENTS
# ============================================================================

def render_kpi_card(title, value, format_type='number', delta=None, icon=None):
    """Render a KPI metric card."""
    if format_type == 'currency':
        formatted = f"${value:,.2f}"
    elif format_type == 'percent':
        formatted = f"{value:.1f}%"
    else:
        formatted = f"{value:,.0f}"

    if icon:
        title = f"{icon} {title}"

    st.metric(label=title, value=formatted, delta=delta)


def render_section_header(title, icon=None):
    """Render a section header."""
    if icon:
        st.markdown(f"### {icon} {title}")
    else:
        st.markdown(f"### {title}")


# ============================================================================
# MAIN DASHBOARD
# ============================================================================

def main():
    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/color/96/000000/bank-building.png", width=80)
        st.title("Banking ETL")
        st.markdown("---")

        # Navigation
        page = st.radio(
            "Navigation",
            ["📊 Overview", "👥 Customers", "💳 Accounts", "💰 Loans", "🏢 Operations", "📈 Analytics"]
        )

        st.markdown("---")

        # Refresh button
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()

        # Database info
        st.markdown("---")
        st.markdown("**Database Info**")
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            table_count = cursor.fetchone()[0]
            st.caption(f"Tables: {table_count}")

            # Last update
            if table_exists('rptDashboardKPIs'):
                df = run_query("SELECT MAX(updated_at) as last_update FROM rptDashboardKPIs")
                if len(df) > 0 and df.iloc[0]['last_update']:
                    st.caption(f"Updated: {df.iloc[0]['last_update']}")
        else:
            st.warning("Database not found")

    # Main content
    if not os.path.exists(DB_PATH):
        st.error("⚠️ Database not found!")
        st.markdown("""
        Please run the following commands to set up the system:

        ```bash
        python scripts/generate_sample_data.py
        python scripts/database_creator.py
        python scripts/csv_importer.py --sample
        python scripts/raw_to_stg.py
        python scripts/stg_to_rpt.py
        ```
        """)
        return

    # Route to pages
    if page == "📊 Overview":
        render_overview_page()
    elif page == "👥 Customers":
        render_customers_page()
    elif page == "💳 Accounts":
        render_accounts_page()
    elif page == "💰 Loans":
        render_loans_page()
    elif page == "🏢 Operations":
        render_operations_page()
    elif page == "📈 Analytics":
        render_analytics_page()


# ============================================================================
# PAGE: OVERVIEW
# ============================================================================

def render_overview_page():
    st.title("🏦 Banking ETL Dashboard")
    st.markdown("Real-time overview of banking operations and metrics.")

    # KPI Row 1
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        value = get_kpi_value('total_customers', 0)
        render_kpi_card("Total Customers", value, icon="👥")

    with col2:
        value = get_kpi_value('total_accounts', 0)
        render_kpi_card("Total Accounts", value, icon="💳")

    with col3:
        value = get_kpi_value('total_balance', 0)
        render_kpi_card("Total Balance", value, format_type='currency', icon="💵")

    with col4:
        value = get_kpi_value('total_loans', 0)
        render_kpi_card("Active Loans", value, icon="📋")

    # KPI Row 2
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        value = get_kpi_value('total_branches', 0)
        render_kpi_card("Branches", value, icon="🏢")

    with col2:
        value = get_kpi_value('total_employees', 0)
        render_kpi_card("Employees", value, icon="👔")

    with col3:
        value = get_kpi_value('total_credit_cards', 0)
        render_kpi_card("Credit Cards", value, icon="💳")

    with col4:
        value = get_kpi_value('pending_transactions', 0)
        render_kpi_card("Pending Txns", value, icon="⏳")

    st.markdown("---")

    # Charts Row
    col1, col2 = st.columns(2)

    with col1:
        render_section_header("Account Distribution", "📊")
        if table_exists('rptAccountSummary'):
            df = run_query("""
                SELECT account_type, SUM(account_count) as count
                FROM rptAccountSummary
                GROUP BY account_type
            """)
            if len(df) > 0:
                fig = px.pie(df, values='count', names='account_type',
                            color_discrete_sequence=px.colors.qualitative.Set2)
                fig.update_layout(margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No account data available")
        else:
            st.info("Run stg_to_rpt.py to generate report tables")

    with col2:
        render_section_header("Customer Segments", "👥")
        if table_exists('rptCustomerSummary'):
            df = run_query("""
                SELECT segment, SUM(customer_count) as count
                FROM rptCustomerSummary
                GROUP BY segment
            """)
            if len(df) > 0:
                fig = px.pie(df, values='count', names='segment',
                            color_discrete_sequence=px.colors.qualitative.Pastel)
                fig.update_layout(margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No customer data available")
        else:
            st.info("Run stg_to_rpt.py to generate report tables")

    # Tables Row
    col1, col2 = st.columns(2)

    with col1:
        render_section_header("Recent Transactions", "💸")
        if table_exists('stgPendingTransactions'):
            df = run_query("""
                SELECT transaction_id, amount, status, created_date
                FROM stgPendingTransactions
                ORDER BY created_date DESC
                LIMIT 10
            """)
            if len(df) > 0:
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.info("No pending transactions")
        else:
            st.info("No transaction data available")

    with col2:
        render_section_header("Failed Transactions", "⚠️")
        if table_exists('stgFailedTransactions'):
            df = run_query("""
                SELECT transaction_id, amount, error_code, error_message
                FROM stgFailedTransactions
                ORDER BY attempted_date DESC
                LIMIT 10
            """)
            if len(df) > 0:
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.info("No failed transactions")
        else:
            st.info("No transaction data available")


# ============================================================================
# PAGE: CUSTOMERS
# ============================================================================

def render_customers_page():
    st.title("👥 Customer Analytics")

    # KPIs
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        value = get_kpi_value('total_customers', 0)
        render_kpi_card("Total Customers", value)

    with col2:
        value = get_kpi_value('active_customers', 0)
        render_kpi_card("Active Customers", value)

    with col3:
        if table_exists('stgCustomerProfiles'):
            df = run_query("SELECT COUNT(DISTINCT segment) as count FROM stgCustomerProfiles")
            value = df.iloc[0]['count'] if len(df) > 0 else 0
        else:
            value = 0
        render_kpi_card("Segments", value)

    with col4:
        if table_exists('rptCustomerSummary'):
            df = run_query("SELECT SUM(customer_count) as count FROM rptCustomerSummary WHERE risk_rating = 'high'")
            value = df.iloc[0]['count'] if len(df) > 0 and df.iloc[0]['count'] else 0
        else:
            value = 0
        render_kpi_card("High Risk", value)

    st.markdown("---")

    # Charts
    col1, col2 = st.columns(2)

    with col1:
        render_section_header("Customers by Segment")
        if table_exists('rptCustomerSummary'):
            df = run_query("""
                SELECT segment, SUM(customer_count) as count
                FROM rptCustomerSummary
                GROUP BY segment
                ORDER BY count DESC
            """)
            if len(df) > 0:
                fig = px.bar(df, x='segment', y='count',
                            color='segment',
                            color_discrete_sequence=px.colors.qualitative.Set2)
                fig.update_layout(showlegend=False, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig, use_container_width=True)

    with col2:
        render_section_header("Risk Rating Distribution")
        if table_exists('rptCustomerSummary'):
            df = run_query("""
                SELECT risk_rating, SUM(customer_count) as count
                FROM rptCustomerSummary
                GROUP BY risk_rating
            """)
            if len(df) > 0:
                colors = {'low': 'green', 'medium': 'orange', 'high': 'red', 'Unknown': 'gray'}
                fig = px.pie(df, values='count', names='risk_rating',
                            color='risk_rating',
                            color_discrete_map=colors)
                fig.update_layout(margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig, use_container_width=True)

    # Customer Table
    render_section_header("Customer List")
    if table_exists('stgCustomerProfiles'):
        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            segments = run_query("SELECT DISTINCT segment FROM stgCustomerProfiles")
            segment_filter = st.multiselect("Segment", segments['segment'].tolist() if len(segments) > 0 else [])
        with col2:
            ratings = run_query("SELECT DISTINCT risk_rating FROM stgCustomerProfiles")
            risk_filter = st.multiselect("Risk Rating", ratings['risk_rating'].tolist() if len(ratings) > 0 else [])
        with col3:
            search = st.text_input("Search by Name")

        # Build query
        query = "SELECT customer_id, name, email, city, segment, risk_rating, status FROM stgCustomerProfiles WHERE 1=1"
        if segment_filter:
            query += f" AND segment IN ({','.join([repr(s) for s in segment_filter])})"
        if risk_filter:
            query += f" AND risk_rating IN ({','.join([repr(r) for r in risk_filter])})"
        if search:
            query += f" AND name LIKE '%{search}%'"
        query += " LIMIT 100"

        df = run_query(query)
        st.dataframe(df, use_container_width=True, hide_index=True)


# ============================================================================
# PAGE: ACCOUNTS
# ============================================================================

def render_accounts_page():
    st.title("💳 Account Analytics")

    # KPIs
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        value = get_kpi_value('total_accounts', 0)
        render_kpi_card("Total Accounts", value)

    with col2:
        value = get_kpi_value('total_balance', 0)
        render_kpi_card("Total Balance", value, format_type='currency')

    with col3:
        value = get_kpi_value('avg_account_balance', 0)
        render_kpi_card("Avg Balance", value, format_type='currency')

    with col4:
        value = get_kpi_value('total_credit_cards', 0)
        render_kpi_card("Credit Cards", value)

    st.markdown("---")

    # Charts
    col1, col2 = st.columns(2)

    with col1:
        render_section_header("Accounts by Type")
        if table_exists('rptAccountSummary'):
            df = run_query("""
                SELECT account_type, SUM(account_count) as count, SUM(total_balance) as balance
                FROM rptAccountSummary
                GROUP BY account_type
            """)
            if len(df) > 0:
                fig = px.bar(df, x='account_type', y='count',
                            color='account_type',
                            color_discrete_sequence=px.colors.qualitative.Bold)
                fig.update_layout(showlegend=False, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig, use_container_width=True)

    with col2:
        render_section_header("Balance by Account Type")
        if table_exists('rptAccountSummary'):
            df = run_query("""
                SELECT account_type, SUM(total_balance) as balance
                FROM rptAccountSummary
                WHERE total_balance > 0
                GROUP BY account_type
            """)
            if len(df) > 0:
                fig = px.pie(df, values='balance', names='account_type',
                            color_discrete_sequence=px.colors.qualitative.Set3)
                fig.update_layout(margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig, use_container_width=True)

    # Account Table
    render_section_header("Account List")
    if table_exists('stgAccountProducts'):
        col1, col2 = st.columns(2)
        with col1:
            types = run_query("SELECT DISTINCT account_type FROM stgAccountProducts")
            type_filter = st.multiselect("Account Type", types['account_type'].tolist() if len(types) > 0 else [])
        with col2:
            status = run_query("SELECT DISTINCT status FROM stgAccountProducts")
            status_filter = st.multiselect("Status", status['status'].tolist() if len(status) > 0 else [])

        query = "SELECT account_id, customer_id, account_type, balance, currency, status FROM stgAccountProducts WHERE 1=1"
        if type_filter:
            query += f" AND account_type IN ({','.join([repr(t) for t in type_filter])})"
        if status_filter:
            query += f" AND status IN ({','.join([repr(s) for s in status_filter])})"
        query += " ORDER BY balance DESC LIMIT 100"

        df = run_query(query)
        st.dataframe(df, use_container_width=True, hide_index=True)


# ============================================================================
# PAGE: LOANS
# ============================================================================

def render_loans_page():
    st.title("💰 Loan Portfolio")

    # KPIs
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        value = get_kpi_value('total_loans', 0)
        render_kpi_card("Total Loans", value)

    with col2:
        value = get_kpi_value('active_loans', 0)
        render_kpi_card("Active Loans", value)

    with col3:
        value = get_kpi_value('total_loan_amount', 0)
        render_kpi_card("Total Principal", value, format_type='currency')

    with col4:
        if table_exists('stgLoans'):
            df = run_query("SELECT AVG(interest_rate) as avg_rate FROM stgLoans")
            value = df.iloc[0]['avg_rate'] if len(df) > 0 and df.iloc[0]['avg_rate'] else 0
        else:
            value = 0
        render_kpi_card("Avg Interest Rate", value, format_type='percent')

    st.markdown("---")

    # Charts
    col1, col2 = st.columns(2)

    with col1:
        render_section_header("Loans by Type")
        if table_exists('rptLoanSummary'):
            df = run_query("""
                SELECT loan_type, SUM(loan_count) as count
                FROM rptLoanSummary
                GROUP BY loan_type
            """)
            if len(df) > 0:
                fig = px.bar(df, x='loan_type', y='count',
                            color='loan_type',
                            color_discrete_sequence=px.colors.qualitative.Vivid)
                fig.update_layout(showlegend=False, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig, use_container_width=True)

    with col2:
        render_section_header("Principal by Loan Type")
        if table_exists('rptLoanSummary'):
            df = run_query("""
                SELECT loan_type, SUM(total_principal) as principal
                FROM rptLoanSummary
                GROUP BY loan_type
            """)
            if len(df) > 0:
                fig = px.pie(df, values='principal', names='loan_type',
                            color_discrete_sequence=px.colors.qualitative.Dark2)
                fig.update_layout(margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig, use_container_width=True)

    # Loan Table
    render_section_header("Loan Details")
    if table_exists('stgLoans'):
        df = run_query("""
            SELECT loan_id, loan_type, principal, interest_rate, term_months, outstanding, status
            FROM stgLoans
            ORDER BY principal DESC
            LIMIT 100
        """)
        st.dataframe(df, use_container_width=True, hide_index=True)


# ============================================================================
# PAGE: OPERATIONS
# ============================================================================

def render_operations_page():
    st.title("🏢 Operations")

    # KPIs
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        value = get_kpi_value('total_branches', 0)
        render_kpi_card("Branches", value, icon="🏢")

    with col2:
        value = get_kpi_value('total_employees', 0)
        render_kpi_card("Employees", value, icon="👔")

    with col3:
        value = get_kpi_value('total_atms', 0)
        render_kpi_card("ATMs", value, icon="🏧")

    with col4:
        if table_exists('stgAtmLocations'):
            df = run_query("SELECT COUNT(*) as count FROM stgAtmLocations WHERE status = 'operational'")
            value = df.iloc[0]['count'] if len(df) > 0 else 0
        else:
            value = 0
        render_kpi_card("ATMs Online", value, icon="✅")

    st.markdown("---")

    # Branches
    col1, col2 = st.columns(2)

    with col1:
        render_section_header("Branch List", "🏢")
        if table_exists('stgBranches'):
            df = run_query("""
                SELECT branch_id, branch_name, city, state, phone
                FROM stgBranches
                ORDER BY branch_name
                LIMIT 50
            """)
            st.dataframe(df, use_container_width=True, hide_index=True)

    with col2:
        render_section_header("Employee Distribution", "👥")
        if table_exists('stgEmployees'):
            df = run_query("""
                SELECT role, COUNT(*) as count
                FROM stgEmployees
                GROUP BY role
                ORDER BY count DESC
            """)
            if len(df) > 0:
                fig = px.bar(df, x='role', y='count',
                            color='role',
                            color_discrete_sequence=px.colors.qualitative.Pastel)
                fig.update_layout(showlegend=False, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# PAGE: ANALYTICS
# ============================================================================

def render_analytics_page():
    st.title("📈 Analytics & SQL")

    # SQL Query Interface
    render_section_header("SQL Query Interface", "🔍")
    st.markdown("Execute custom SQL queries against the database.")

    # Sample queries
    sample_queries = {
        "Select a sample query...": "",
        "Count all tables": "SELECT name, type FROM sqlite_master WHERE type='table' ORDER BY name",
        "Customer segments": "SELECT segment, COUNT(*) as count FROM stgCustomerProfiles GROUP BY segment",
        "Account balances": "SELECT account_type, SUM(balance) as total, AVG(balance) as avg FROM stgAccountProducts GROUP BY account_type",
        "Loan portfolio": "SELECT loan_type, COUNT(*) as loans, SUM(principal) as total FROM stgLoans GROUP BY loan_type",
        "Failed transactions": "SELECT error_code, COUNT(*) as count FROM stgFailedTransactions GROUP BY error_code ORDER BY count DESC",
        "ETL Log": "SELECT * FROM _etl_log ORDER BY id DESC LIMIT 20"
    }

    selected = st.selectbox("Sample Queries", list(sample_queries.keys()))

    query = st.text_area("SQL Query", value=sample_queries[selected], height=100)

    if st.button("▶️ Execute Query"):
        if query.strip():
            try:
                df = run_query(query)
                st.success(f"Query returned {len(df)} rows")
                st.dataframe(df, use_container_width=True)
            except Exception as e:
                st.error(f"Error: {e}")
        else:
            st.warning("Please enter a query")

    st.markdown("---")

    # Database Statistics
    render_section_header("Database Statistics", "📊")

    if get_connection():
        conn = get_connection()
        cursor = conn.cursor()

        # Get all tables with row counts
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()

        table_stats = []
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
            count = cursor.fetchone()[0]
            if table_name.startswith('raw'):
                layer = 'Raw'
            elif table_name.startswith('stg'):
                layer = 'Staging'
            elif table_name.startswith('rpt'):
                layer = 'Report'
            elif table_name.startswith('_'):
                layer = 'System'
            else:
                layer = 'Other'
            table_stats.append({'Table': table_name, 'Layer': layer, 'Rows': count})

        df_stats = pd.DataFrame(table_stats)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Tables by Layer**")
            layer_counts = df_stats.groupby('Layer')['Rows'].sum().reset_index()
            fig = px.pie(layer_counts, values='Rows', names='Layer',
                        color_discrete_sequence=px.colors.qualitative.Set1)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**All Tables**")
            st.dataframe(df_stats, use_container_width=True, hide_index=True)


# ============================================================================
# RUN APP
# ============================================================================

if __name__ == "__main__":
    main()
