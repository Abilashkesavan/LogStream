-- ===============================================
-- SOURCE (MySQL) COUNT
-- ===============================================
SELECT 'orders' AS table_name, COUNT(*) AS row_count FROM orders;
SELECT 'users' AS table_name, COUNT(*) AS row_count FROM users;

-- ===============================================
-- TARGET (Postgres / warehouse) COUNT
-- ===============================================
SELECT 'orders' AS table_name, COUNT(*) AS row_count FROM orders;
SELECT 'users' AS table_name, COUNT(*) AS row_count FROM users;

