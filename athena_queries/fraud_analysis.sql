----What percentage of transactions are fraudulent by merchant category?
SELECT 
    category,
    COUNT(*) AS total_transactions,
    ROUND(SUM(CASE WHEN fraud_label = 'fraud' THEN 1 ELSE 0 END) *100.0 / COUNT(*),2) AS fraud_rate
FROM "AwsDataCatalog"."fraud_db"."processedtransactions"
GROUP BY category
HAVING COUNT(*) > 100
ORDER BY fraud_rate DESC;

------Which customer age groups are most targeted?
SELECT 
    age_range,
    COUNT(*) AS total_transactions,
    ROUND(SUM(CASE WHEN fraud_label = 'fraud' THEN 1 ELSE 0 END) *100.0 / COUNT(*),2) AS fraud_rate
FROM "AwsDataCatalog"."fraud_db"."processedtransactions"
GROUP BY age_range
ORDER BY fraud_rate DESC;

------- Which gender has a higher fraud rate, and does it vary by age range?
SELECT 
    gender,
    age_range,
    COUNT(*) AS total_transactions,
    ROUND(SUM(CASE WHEN fraud_label = 'fraud' THEN 1 ELSE 0 END) *100.0 / COUNT(*),2) AS fraud_rate
FROM "AwsDataCatalog"."fraud_db"."processedtransactions"
GROUP BY gender,age_range
ORDER BY fraud_rate DESC;


-----What is the average transaction amount for fraudulent vs legitimate transactions?
SElECT 
    fraud_label,
    COUNT(*) AS total_transactions,
    CAST(AVG(amount) AS DECIMAL(15,2)) AS avg_transaction_amount
FROM "AwsDataCatalog"."fraud_db"."processedtransactions"
WHERE fraud_label IN ('fraud','legitimate')
GROUP BY fraud_label;


---------Which US states have the highest fraud rates?
SELECT 
    CASE WHEN merchant_state IS NULL OR merchant_state = '' THEN 'ONLINE'ELSE merchant_state END,
    COUNT(*) AS total_transactions,
    ROUND(SUM(CASE WHEN fraud_label = 'fraud' THEN 1 ELSE 0 END) *100.0 / COUNT(*),2) AS fraud_rate
FROM "AwsDataCatalog"."fraud_db"."processedtransactions"
GROUP BY CASE WHEN merchant_state IS NULL OR merchant_state = '' THEN 'ONLINE'ELSE merchant_state END
HAVING COUNT(*) > 100
ORDER BY fraud_rate DESC;


-----Which credit range has the highest fraud rate?
SELECT 
    credit_range,
    COUNT(*) AS total_transactions,
    ROUND(SUM(CASE WHEN fraud_label = 'fraud' THEN 1 ELSE 0 END) *100.0 / COUNT(*),2) AS fraud_rate
FROM "AwsDataCatalog"."fraud_db"."processedtransactions"
GROUP BY credit_range
ORDER BY fraud_rate DESC;

-----Do customers with more credit cards have higher fraud exposure?
SELECT 
    num_cards_issued,
    COUNT(*) AS total_transactions,
    ROUND(SUM(CASE WHEN fraud_label = 'fraud' THEN 1 ELSE 0 END) *100.0 / COUNT(*),2) AS fraud_rate
FROM "AwsDataCatalog"."fraud_db"."processedtransactions"
GROUP BY num_cards_issued
ORDER BY fraud_rate DESC;

----- Does having a chip card reduce fraud rate?
SELECT 
    has_chip,
    COUNT(*) AS total_transactions,
    ROUND(SUM(CASE WHEN fraud_label = 'fraud' THEN 1 ELSE 0 END) *100.0 / COUNT(*),2) AS fraud_rate
FROM "AwsDataCatalog"."fraud_db"."processedtransactions"
GROUP BY has_chip
ORDER BY fraud_rate DESC;


-------- Is online fraud higher than in-person fraud, and which merchant categories drive it?
SELECT 
    CASE WHEN merchant_state IS NULL OR merchant_state = '' THEN 'ONLINE'ELSE 'In_Person' END,
    category,
    COUNT(*) AS total_transactions,
    ROUND(SUM(CASE WHEN fraud_label = 'fraud' THEN 1 ELSE 0 END) *100.0 / COUNT(*),2) AS fraud_rate
FROM "AwsDataCatalog"."fraud_db"."processedtransactions"
GROUP BY CASE WHEN merchant_state IS NULL OR merchant_state = '' THEN 'ONLINE'ELSE 'In_Person' END,category
HAVING COUNT(*) > 100
ORDER BY fraud_rate DESC;


-----------What hours of the day see the highest fraud activity, and does this vary by transaction type?
SELECT 
    hour,
    COUNT(*) AS total_transactions,
    ROUND(SUM(CASE WHEN fraud_label = 'fraud' THEN 1 ELSE 0 END) *100.0 / COUNT(*),2) AS fraud_rate
FROM "AwsDataCatalog"."fraud_db"."processedtransactions"
WHERE transaction_type = 'purchase'
GROUP BY hour
ORDER BY hour;

-----How does fraud rate vary by month?
SELECT 
    month,
    COUNT(*) AS total_transactions,
    ROUND(SUM(CASE WHEN fraud_label = 'fraud' THEN 1 ELSE 0 END) *100.0 / COUNT(*),2) AS fraud_rate
FROM "AwsDataCatalog"."fraud_db"."processedtransactions"
GROUP BY month
ORDER BY CAST(month AS INT);
