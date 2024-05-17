SELECT country, category, sum(price) as "Total_sales"
from softcart_factsales
left join softcart_dimcountry 
on softcart_factsales.country_id = softcart_dimcountry.country_id
left join softcart_dimcategory
on softcart_factsales.category_id = softcart_dimcategory.category_id
group by grouping sets(country, category)
order by country, category