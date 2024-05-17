select country, year, avg(price) as "average_sales"
from softcart_factsales
left join softcart_dimcountry
on softcart_factsales.country_id = softcart_dimcountry.country_id
left join softcart_dimdate
on softcart_factsales.datei_d = softcart_dimdate.date_id
group by cube (year, country);