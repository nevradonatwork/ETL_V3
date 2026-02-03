-- ============================================================================
-- BANKING ETL SYSTEM - SAMPLE SQL QUERIES
-- ============================================================================
-- This file demonstrates SQL skills for portfolio/interview purposes
-- ============================================================================

-- ============================================================================
-- 1. BASIC QUERIES
-- ============================================================================

-- Count customers by segment
SELECT
    segment,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM stgCustomerProfiles), 2) as percentage
FROM stgCustomerProfiles
GROUP BY segment
ORDER BY customer_count DESC;

-- Get top 10 customers by total account balance
SELECT
    c.customer_id,
    c.name,
    c.segment,
    COUNT(a.account_id) as account_count,
    SUM(a.balance) as total_balance
FROM stgCustomerProfiles c
JOIN stgAccountProducts a ON c.customer_id = a.customer_id
GROUP BY c.customer_id, c.name, c.segment
ORDER BY total_balance DESC
LIMIT 10;

-- ============================================================================
-- 2. AGGREGATION QUERIES
-- ============================================================================

-- Account statistics by type
SELECT
    account_type,
    COUNT(*) as count,
    ROUND(SUM(balance), 2) as total_balance,
    ROUND(AVG(balance), 2) as avg_balance,
    ROUND(MIN(balance), 2) as min_balance,
    ROUND(MAX(balance), 2) as max_balance
FROM stgAccountProducts
GROUP BY account_type
ORDER BY total_balance DESC;

-- Loan portfolio summary
SELECT
    loan_type,
    status,
    COUNT(*) as loan_count,
    ROUND(SUM(principal), 2) as total_principal,
    ROUND(AVG(interest_rate), 2) as avg_interest_rate,
    ROUND(SUM(outstanding), 2) as total_outstanding
FROM stgLoans
GROUP BY loan_type, status
ORDER BY loan_type, status;

-- ============================================================================
-- 3. JOIN QUERIES
-- ============================================================================

-- Customer with their accounts and loans
SELECT
    c.customer_id,
    c.name,
    c.segment,
    a.account_type,
    a.balance,
    l.loan_type,
    l.principal,
    l.outstanding
FROM stgCustomerProfiles c
LEFT JOIN stgAccountProducts a ON c.customer_id = a.customer_id
LEFT JOIN stgLoans l ON a.account_id = l.account_id
WHERE c.segment = 'premium'
LIMIT 50;

-- Branch performance with employee count
SELECT
    b.branch_id,
    b.branch_name,
    b.city,
    COUNT(DISTINCT e.employee_id) as employee_count,
    COUNT(DISTINCT a.account_id) as account_count
FROM stgBranches b
LEFT JOIN stgEmployees e ON b.branch_id = e.branch_id
LEFT JOIN stgAccountProducts a ON b.branch_id = a.branch_id
GROUP BY b.branch_id, b.branch_name, b.city
ORDER BY employee_count DESC;

-- ============================================================================
-- 4. SUBQUERIES
-- ============================================================================

-- Customers with above-average balance
SELECT
    c.customer_id,
    c.name,
    c.segment,
    (SELECT SUM(balance) FROM stgAccountProducts WHERE customer_id = c.customer_id) as total_balance
FROM stgCustomerProfiles c
WHERE (SELECT SUM(balance) FROM stgAccountProducts WHERE customer_id = c.customer_id) >
      (SELECT AVG(balance) FROM stgAccountProducts)
ORDER BY total_balance DESC
LIMIT 20;

-- Accounts with more balance than their type average
SELECT
    a.account_id,
    a.account_type,
    a.balance,
    (SELECT ROUND(AVG(balance), 2) FROM stgAccountProducts WHERE account_type = a.account_type) as type_avg
FROM stgAccountProducts a
WHERE a.balance > (SELECT AVG(balance) FROM stgAccountProducts WHERE account_type = a.account_type)
LIMIT 20;

-- ============================================================================
-- 5. WINDOW FUNCTIONS (SQLite 3.25+)
-- ============================================================================

-- Rank customers by balance within their segment
SELECT
    customer_id,
    name,
    segment,
    total_balance,
    RANK() OVER (PARTITION BY segment ORDER BY total_balance DESC) as rank_in_segment
FROM (
    SELECT
        c.customer_id,
        c.name,
        c.segment,
        COALESCE(SUM(a.balance), 0) as total_balance
    FROM stgCustomerProfiles c
    LEFT JOIN stgAccountProducts a ON c.customer_id = a.customer_id
    GROUP BY c.customer_id, c.name, c.segment
)
WHERE rank_in_segment <= 5
ORDER BY segment, rank_in_segment;

-- Running total of loans by type
SELECT
    loan_id,
    loan_type,
    principal,
    SUM(principal) OVER (PARTITION BY loan_type ORDER BY loan_id) as running_total
FROM stgLoans
ORDER BY loan_type, loan_id
LIMIT 30;

-- ============================================================================
-- 6. COMMON TABLE EXPRESSIONS (CTEs)
-- ============================================================================

-- Customer segmentation analysis with CTE
WITH customer_balances AS (
    SELECT
        c.customer_id,
        c.name,
        c.segment,
        c.risk_rating,
        COALESCE(SUM(a.balance), 0) as total_balance,
        COUNT(a.account_id) as account_count
    FROM stgCustomerProfiles c
    LEFT JOIN stgAccountProducts a ON c.customer_id = a.customer_id
    GROUP BY c.customer_id, c.name, c.segment, c.risk_rating
),
segment_stats AS (
    SELECT
        segment,
        COUNT(*) as customer_count,
        ROUND(AVG(total_balance), 2) as avg_balance,
        SUM(account_count) as total_accounts
    FROM customer_balances
    GROUP BY segment
)
SELECT * FROM segment_stats
ORDER BY avg_balance DESC;

-- ============================================================================
-- 7. DATA QUALITY CHECKS
-- ============================================================================

-- Find duplicate records in raw table
SELECT
    customer_id,
    name,
    email,
    COUNT(*) as duplicate_count
FROM rawCustomerProfiles
GROUP BY customer_id, name, email
HAVING COUNT(*) > 1;

-- Compare raw vs staging counts
SELECT
    'CustomerProfiles' as table_name,
    (SELECT COUNT(*) FROM rawCustomerProfiles) as raw_count,
    (SELECT COUNT(*) FROM stgCustomerProfiles) as stg_count,
    (SELECT COUNT(*) FROM rawCustomerProfiles) - (SELECT COUNT(*) FROM stgCustomerProfiles) as duplicates_removed
UNION ALL
SELECT
    'AccountProducts',
    (SELECT COUNT(*) FROM rawAccountProducts),
    (SELECT COUNT(*) FROM stgAccountProducts),
    (SELECT COUNT(*) FROM rawAccountProducts) - (SELECT COUNT(*) FROM stgAccountProducts)
UNION ALL
SELECT
    'Loans',
    (SELECT COUNT(*) FROM rawLoans),
    (SELECT COUNT(*) FROM stgLoans),
    (SELECT COUNT(*) FROM rawLoans) - (SELECT COUNT(*) FROM stgLoans);

-- ============================================================================
-- 8. ETL MONITORING QUERIES
-- ============================================================================

-- ETL log summary
SELECT
    DATE(started_at) as date,
    operation,
    COUNT(*) as operation_count,
    SUM(rows_affected) as total_rows
FROM _etl_log
GROUP BY DATE(started_at), operation
ORDER BY date DESC, operation;

-- Latest imports by table
SELECT
    table_name,
    MAX(completed_at) as last_import,
    SUM(rows_affected) as total_rows_imported
FROM _etl_log
WHERE operation = 'csv_import'
GROUP BY table_name
ORDER BY last_import DESC;

-- ============================================================================
-- 9. BUSINESS INTELLIGENCE QUERIES
-- ============================================================================

-- Customer lifetime value estimate
WITH customer_metrics AS (
    SELECT
        c.customer_id,
        c.name,
        c.segment,
        c.created_date,
        COALESCE(SUM(a.balance), 0) as total_deposits,
        COALESCE(SUM(l.principal), 0) as total_loans,
        COUNT(DISTINCT a.account_id) as products
    FROM stgCustomerProfiles c
    LEFT JOIN stgAccountProducts a ON c.customer_id = a.customer_id AND a.balance > 0
    LEFT JOIN stgLoans l ON a.account_id = l.account_id
    GROUP BY c.customer_id, c.name, c.segment, c.created_date
)
SELECT
    segment,
    COUNT(*) as customers,
    ROUND(AVG(total_deposits), 2) as avg_deposits,
    ROUND(AVG(total_loans), 2) as avg_loans,
    ROUND(AVG(products), 1) as avg_products
FROM customer_metrics
GROUP BY segment
ORDER BY avg_deposits DESC;

-- Risk analysis
SELECT
    c.risk_rating,
    COUNT(DISTINCT c.customer_id) as customer_count,
    COUNT(DISTINCT l.loan_id) as loan_count,
    ROUND(SUM(l.principal), 2) as total_exposure,
    ROUND(AVG(l.interest_rate), 2) as avg_rate
FROM stgCustomerProfiles c
LEFT JOIN stgAccountProducts a ON c.customer_id = a.customer_id
LEFT JOIN stgLoans l ON a.account_id = l.account_id
GROUP BY c.risk_rating
ORDER BY
    CASE risk_rating
        WHEN 'high' THEN 1
        WHEN 'medium' THEN 2
        WHEN 'low' THEN 3
        ELSE 4
    END;

-- ============================================================================
-- END OF SAMPLE QUERIES
-- ============================================================================
