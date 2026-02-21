SELECT
    gmv_date,
    subsidiary,
    gmv_value,
    purchases_cnt
FROM gold.gmv_diario_por_subsidiario
WHERE as_of_date = current_date() --data de exemplo apenas

/*Aqui é importantíssimo colocar o mesmo as_of_date que da última vez que foi consultada a tabela, 
pois o código que fiz garante granularidade diária, rastreabilidade e navegação.
Ou seja, se quiser o valor reportado para alguém em algum dia e quiser ter o mesmo, ele está aqui, mas precisa colocar o mesmo as_of_date
*/

AND data_quality_status = 'OK'