CREATE MATERIALIZED VIEW total_sales_per_country AS
SELECT country, SUM(price) AS total_sales
FROM softcart_factsales
LEFT JOIN softcart_dimcountry ON softcart_factsales.country_id = softcart_dimcountry.country_id
GROUP BY country;
