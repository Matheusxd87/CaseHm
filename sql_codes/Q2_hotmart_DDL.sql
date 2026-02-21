CREATE TABLE IF NOT EXISTS gold.gmv_diario_por_subsidiario (
  as_of_date DATE                             COMMENT 'Snapshot date (partition). Resultado fechado do dia de processamento (D-1).',
  gmv_date   DATE                             COMMENT 'Data do GMV (release_date).',
  subsidiary STRING                           COMMENT 'Subsidiária vinda de purchase_extra_info.',
  gmv_value  DECIMAL(18,2)                    COMMENT 'Soma do valor das compras pagas e não canceladas.',
  purchases_cnt BIGINT                        COMMENT 'Qtde de compras distintas no GMV.',
  created_at TIMESTAMP                        COMMENT 'Timestamp do job que gerou o snapshot.',
  data_quality_status STRING                  COMMENT 'Apresenta se existem campos faltantes.',
  dq_notes STRING                             COMMENT 'Mostra qual campo está faltante.'
)
USING DELTA --Considerando que estamos em ambiente delta lake
PARTITIONED BY (as_of_date);