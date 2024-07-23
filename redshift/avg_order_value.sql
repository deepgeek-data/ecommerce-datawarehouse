SELECT
  c.customer_id,
  c.customer_name,
  AVG(o.order_total_price) AS avg_order_value
FROM "dev"."public"."customer" c
JOIN "dev"."public"."order" o ON c.customer_id = o.order_customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY avg_order_value DESC;