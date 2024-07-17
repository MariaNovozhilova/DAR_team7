

SELECT e.id
, COALESCE(tc.total_certificates, 0) AS total_certificates
, RANK() OVER (ORDER BY total_certificates DESC) AS ranking
FROM dds.employee e
LEFT JOIN (SELECT ec.user_id
, COUNT(*) AS total_certificates
FROM dds.employee_certificate ec
GROUP BY ec.user_id) tc ON e.id = tc.user_id
ORDER BY 2 DESC;
