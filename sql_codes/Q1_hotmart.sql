--Considero neste código que o time de engenharia teve transaction_datetime e transaction_date preenchidos
--Sabendo que a hotmart é uma empresa global, posso converter o timestamp para ter uma precisão maior, mas vamos manter um código mais limpo e se necessário, a transformação é simples
create table gold.top_50_creators
WITH purchase_latest AS (
  SELECT p.*,
         ROW_NUMBER() OVER (
           PARTITION BY p.purchase_id, p.purchase_partition
           ORDER BY p.transaction_datetime DESC
         ) AS rn
  FROM purchase p
  WHERE
	release_date >= DATE '2021-01-01'
    AND release_date <  DATE '2022-01-01'
    AND release_date IS NOT NULL --caso haja algum cancelamento, reembolso, considerar para ser também um último status da compra
),
paid_2021 AS (
  SELECT
    purchase_id,
    purchase_partition,
    producer_id,
    prod_item_id,
    prod_item_partition
  FROM purchase_latest
  WHERE -- este para capturar a última linha da compra aprovada no ano anterior 
    rn = 1
    AND purchase_status = 'APROVADA'
-- daqui pra cima, apenas manipulando o último registro de cada compra com status aprovado
),
product_item_latest AS (
  SELECT pi.*,
         ROW_NUMBER() OVER (
           PARTITION BY pi.prod_item_id, pi.prod_item_partition
           ORDER BY pi.transaction_datetime DESC --capturarmos a última inserção na product_item, prevendo um comportamento da tabela
         ) AS rn
  FROM product_item pi
)
SELECT
  p.producer_id,
  SUM(coalesce(pi.purchase_value,0) * coalesce(pi.item_quantity,0)) AS revenue_2021 
  /*usei o coalesce pois a product_item pode estar desatualizada, para além disso, multipliquei o valor pela quantidade, 
  suponho que o cálculo seja assim, depende da regra que definirem, pode ser apenas o purchase_value.
  */
FROM paid_2021 p
left JOIN product_item_latest pi 
/* usei o left aqui pois queremos mostrar apenas as aprovadas, com isso se a product_item estiver vazia, é usado o coalesce,
pois apesar de ser vazia, de qualquer forma não teríamos como consultar o valor.
*/
  ON pi.prod_item_id = p.prod_item_id  
 AND pi.prod_item_partition = p.prod_item_partition
 AND pi.rn = 1 -- isso para pegar a linha corrente da product_item(para cada produto).
GROUP BY p.producer_id
ORDER BY revenue_2021 DESC
LIMIT 50