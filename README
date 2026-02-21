Q1:
GMV Analytics

Projeto desenvolvido para resolver dois problemas de modelagem e análise de dados a partir de eventos CDC em um Data Lake.

Questão 1 — Análise SQL
Objetivo:
Identificar os Top 50 produtores em faturamento em 2021
Identificar os Top 2 produtos mais vendidos por produtor

Estratégia:
Tratamento de CDC utilizando ROW_NUMBER() por chave + partição
Seleção do último estado da compra

Técnicas utilizadas:

Window Functions
CTEs encadeadas
Deduplicação por evento mais recente
Agregações com GROUP BY
Ranking com ROW_NUMBER() particionado por produtor


Q2: GMV Daily Snapshot

Pipeline de dados para cálculo de GMV diário por subsidiária, com suporte a CDC assíncrono e geração de snapshots históricos imutáveis.

Problema:

Calcular o GMV (Gross Merchandise Value) considerando apenas compras:
Garantindo:
Processamento full diário (D-1)
Reprocessamento seguro
Comparação as-of (estado do dado em qualquer data passada)
Imutabilidade histórica

Arquitetura:
Modelo Medalhão (Lakehouse)
Bronze (CDC Events)
Silver (Latest-as-of)
Gold (gmv_diario_por_subsidiario)

Stack:
Apache Spark / PySpark
Delta Lake (ou Iceberg/Hudi)
SQL (Window Functions)
Lakehouse Architecture

Diferenciais:
Histórico imutável
Reprocessamento seguro
Compatível com auditoria financeira
Modelagem preparada para cenários bitemporais

