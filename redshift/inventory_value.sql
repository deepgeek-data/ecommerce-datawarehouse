SELECT
  i.inventory_location,
  SUM(p.product_customer_price * i.inventory_quantity) AS total_value
FROM "dev"."public"."inventory" i
JOIN "dev"."public"."product" p ON i.inventory_product_id = p.product_id
GROUP BY i.inventory_location
ORDER BY total_value DESC;