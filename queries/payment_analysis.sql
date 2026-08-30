-- Payment Analysis

SELECT
    payment_method,
    COUNT(payment_id) AS total_transactions,
    SUM(payment_amount) AS total_payment_amount,
    AVG(payment_amount) AS average_payment_amount
FROM fact_payments
GROUP BY payment_method
ORDER BY total_payment_amount DESC;