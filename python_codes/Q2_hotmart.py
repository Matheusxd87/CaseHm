"""
Antes de começarmos, é relevante frisar que como os status das transações podem mudar (update), se fecharmos o gmv do 
dia e no dia seguinte houver uma atualização, os dados podem variar,
mas como foi pedido estritamente para ser estático, vamos criar uma base append-only, que só cria naquele momento que foi processado o código
e que para o passado não sofre alterações, a não ser por linhas novas criadas
"""
#Considerando o que pyspark está instalado...

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# Parâmetros do job (D-1)

"""
Em produção: use o scheduler pra passar a data, aqui deixo genérico. o analista pode substituir o valor que desejar
exemplo: run_date = data de processamento (D-1)
"""
run_date = F.current_date()  
as_of_date = F.date_sub(run_date, 1)  #capturarmos ontem
cutoff_ts = F.to_timestamp(F.concat_ws(" ", as_of_date.cast("string"), F.lit("23:59:59"))) #capturarmos até o final do dia ontem

created_at = F.current_timestamp()

# Aqui buscamos a transação corrente para o conjunto especificado mais adinte de chaves
def latest_as_of(df, keys, cutoff_col="transaction_datetime"):

    w = Window.partitionBy(*keys).orderBy(F.col(cutoff_col).desc())
    return (df
            .where(F.col(cutoff_col) <= cutoff_ts)
            .withColumn("rn", F.row_number().over(w))
            .where(F.col("rn") == 1)
            .drop("rn"))


# Leitura das tabelas e transformação para spark (supondo que elas estão na silver pelo conversado em entrevistas)

purchase = spark.table("silver.purchase")  # eventos/cdc
product_item = spark.table("silver.product_item")
purchase_extra_info = spark.table("silver.purchase_extra_info")

"""
Fazemos isso por boas práticas de processamento, para capturarmos apenas os dados que vão ser efetivamente usados(d-1), 
não capturando os que estão chegando hoje, por exemplo, isso ajuda muito, inclusive, pensando que a tabela pode estar
sendo particionado pela data, visto que a cardinalidade é alta e os analistas buscarão por esta coluna
"""
purchase = purchase.where(F.col("transaction_date") <= as_of_date)
product_item = product_item.where(F.col("transaction_date") <= as_of_date)
purchase_extra_info = purchase_extra_info.where(F.col("transaction_date") <= as_of_date)


# Definimos as chaves que serão usadas na função latest_as_of de forma organizada
p_keys  = ["purchase_id", "purchase_partition"]
pi_keys = ["prod_item_id", "prod_item_partition"]
pei_keys = ["purchase_id", "purchase_partition"]

#Aplicamos a função as tabelas
purchase_cur = latest_as_of(purchase, p_keys)
product_item_cur = latest_as_of(product_item, pi_keys)
pei_cur = latest_as_of(purchase_extra_info, pei_keys)


# Join 
base = (purchase_cur.alias("p")
        .join(product_item_cur.alias("pi"),
              on=[
                  F.col("p.prod_item_id") == F.col("pi.prod_item_id"),
                  F.col("p.prod_item_partition") == F.col("pi.prod_item_partition")
              ],
              how="left") # left pois o fator determinante da linha válida é, para além do release_date ser não nulo, o purchase_status ser 'aprovado', que consta na purchase_cur
        .join(pei_cur.alias("pei"),
              on=[
                  F.col("p.purchase_id") == F.col("pei.purchase_id"),
                  F.col("p.purchase_partition") == F.col("pei.purchase_partition")
              ],
              how="left"))

""" 
Decidi fazer o Data Quality flags(faltou product_item / faltou extra_info) pois  como dito em vídeo e pdf
as bases podem não ter sido carregadas ainda e isso pode gerar algumas faltas, mas é importante vermos o que falta e o status

"""
dq_status = F.when(F.col("pei.subsidiary").isNull(), F.lit("subsidiario_faltante")) \
             .when(F.col("pi.prod_item_id").isNull(), F.lit("subsidiario_faltante")) \
             .otherwise(F.lit("OK"))

dq_notes = F.concat_ws(
    "; ",
    F.when(F.col("pei.subsidiary").isNull(), F.lit("faltando purchase_extra_info")).otherwise(F.lit(None)),
    F.when(F.col("pi.prod_item_id").isNull(), F.lit("faltando product_item")).otherwise(F.lit(None))
)


# Regra GMV
is_gmv = (
    F.col("p.release_date").isNotNull()
    & (F.col("p.purchase_status").isin("APROVADA"))
)

# Se subsidiária estiver faltando, temos duas opções:
# (A) excluir do GMV (mais correto para "GMV por subsidiária", mas gera undercount naquele snapshot)
# (B) mandar para 'UNKNOWN' (mantém total, mas polui a dimensão)
# Aqui vou fazer (A) e ainda deixar rastreável via dq_status/dq_notes.
gmv_rows = (base
            .withColumn("as_of_date", as_of_date)
            .withColumn("created_at", created_at)
            .withColumn("data_quality_status", dq_status)
            .withColumn("dq_notes", dq_notes)
            .where(is_gmv)
            .where(F.col("pei.subsidiary").isNotNull())  # exige subsidiária para agregação final
            .withColumn("gmv_date", F.col("p.release_date")) #por dia
            .withColumn("subsidiary", F.col("pei.subsidiary")) #por subsidiária
            .withColumn("gmv_amount", F.col("p.purchase_total_value").cast("decimal(18,2)"))) 

agg = (gmv_rows
       .groupBy("as_of_date", "gmv_date", "subsidiary")  #isso garante que estamos calculando o gmv diário e não histórico(acumulado)
       .agg(
           F.sum("gmv_amount").alias("gmv_value"),
           F.countDistinct(F.struct(F.col("p.purchase_id"), F.col("p.purchase_partition"))).alias("purchases_cnt"),
           F.max("created_at").alias("created_at"),
           F.lit("OK").alias("data_quality_status"),
           F.lit(None).cast("string").alias("dq_notes")
       ))


# Escrita append-only (imutável)
(agg
 .write
 .format("delta") #considerando que o ambiente a ser gravado é delta lake
 .mode("append")
 .partitionBy("as_of_date")
 .saveAsTable("gold.gmv_diario_por_subsidiario"))


