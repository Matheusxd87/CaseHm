--Considero neste código que o time de engenharia teve transaction_datetime e transaction_date preenchidos
--Sabendo que a hotmart é uma empresa global, posso converter o timestamp para ter uma precisão maior, mas vamos manter um código mais limpo e se necessário, a transformação é simples

create table gold.top_2_products
WITH purchase_latest AS (
  SELECT
    p.*,
    ROW_NUMBER() OVER (
      PARTITION BY p.purchase_id, p.purchase_partition
      ORDER BY p.transaction_datetime DESC
    ) AS rn
  FROM purchase p
  WHERE release_date IS NOT NULL
),
paid AS (
  SELECT
    purchase_id,
    purchase_partition,
    producer_id,
    prod_item_id,
    prod_item_partition
  FROM purchase_latest
  WHERE rn = 1
    AND purchase_status = 'APROVADA'
-- daqui pra cima, apenas manipulando o último registro de cada compra com status aprovado
),
product_item_latest AS (
  SELECT
    pi.*,
    ROW_NUMBER() OVER (
      PARTITION BY pi.prod_item_id, pi.prod_item_partition
      ORDER BY pi.transaction_datetime DESC
    ) AS rn
  FROM product_item pi
),

revenue_by_product AS (
  SELECT
    p.producer_id,
    p.prod_item_id,
    p.prod_item_partition,
    SUM(COALESCE(pi.purchase_value,0) * COALESCE(pi.item_quantity,0)) AS revenue
  FROM paid p
  LEFT JOIN product_item_latest pi
    ON pi.prod_item_id = p.prod_item_id
   AND pi.prod_item_partition = p.prod_item_partition
   AND pi.rn = 1 
  GROUP BY
    p.producer_id, p.prod_item_id, p.prod_item_partition
),

ranked AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER (
      PARTITION BY r.producer_id
      ORDER BY r.revenue DESC, r.prod_item_id
    ) AS rn_prod
  FROM revenue_by_product r
)
SELECT
  producer_id,
  prod_item_id,
  prod_item_partition,
  revenue
FROM ranked
WHERE rn_prod <= 2