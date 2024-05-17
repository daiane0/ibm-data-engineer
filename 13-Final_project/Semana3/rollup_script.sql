select year, country, sum(price) as "total_sales"
from softcart_factsales
left join softcart_dimdate
on softcart_factsales.datei_d = softcart_dimdate.date_id
left join softcart_dimcountry
on softcart_factsales.country_id = softcart_dimcountry.country_id
group by rollup(year, country)
order by year, country