SELECT
  c.customer_id,
  c.customer_name,
  SUM(o.order_total_price) AS total_revenue
FROM "dev"."public"."customer" c
JOIN "dev"."public"."order" o ON c.customer_id = o.order_customer_id
GROUP BY c.customer_id,c.customer_name
ORDER BY total_revenue DESC;