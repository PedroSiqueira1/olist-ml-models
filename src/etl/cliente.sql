-- Databricks notebook source
SELECT *
FROM silver.olist.pedido

-- COMMAND ----------



-- COMMAND ----------


WITH tb_join AS (
  SELECT 
      DISTINCT 
      t1.idPedido,
      t1.idCliente,
      t2.idVendedor,
      t3.descUF

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido as t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.cliente t3
    ON t1.idCliente = t3.idCliente

  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  
), tb_group AS (

  SELECT 
  idVendedor,
  count(distinct descUF) AS qtdUFsPedidos,
  count(CASE WHEN descUF = 'AC' then idPedido end)/count(distinct idPedido) as pct_PedidoAC,  
  count(CASE WHEN descUF = 'AL' then idPedido end)/count(distinct idPedido) as pct_PedidoAL,  
  count(CASE WHEN descUF = 'AM' then idPedido end)/count(distinct idPedido) as pct_PedidoAM,  
  count(CASE WHEN descUF = 'AP' then idPedido end)/count(distinct idPedido) as pct_PedidoAP,  
  count(CASE WHEN descUF = 'BA' then idPedido end)/count(distinct idPedido) as pct_PedidoBA,  
  count(CASE WHEN descUF = 'CE' then idPedido end)/count(distinct idPedido) as pct_PedidoCE,  
  count(CASE WHEN descUF = 'DF' then idPedido end)/count(distinct idPedido) as pct_PedidoDF,  
  count(CASE WHEN descUF = 'ES' then idPedido end)/count(distinct idPedido) as pct_PedidoES,  
  count(CASE WHEN descUF = 'GO' then idPedido end)/count(distinct idPedido) as pct_PedidoGO,  
  count(CASE WHEN descUF = 'MA' then idPedido end)/count(distinct idPedido) as pct_PedidoMA,  
  count(CASE WHEN descUF = 'MG' then idPedido end)/count(distinct idPedido) as pct_PedidoMG,  
  count(CASE WHEN descUF = 'MS' then idPedido end)/count(distinct idPedido) as pct_PedidoMS,  
  count(CASE WHEN descUF = 'MT' then idPedido end)/count(distinct idPedido) as pct_PedidoMT,  
  count(CASE WHEN descUF = 'PA' then idPedido end)/count(distinct idPedido) as pct_PedidoPA,  
  count(CASE WHEN descUF = 'PB' then idPedido end)/count(distinct idPedido) as pct_PedidoPB,  
  count(CASE WHEN descUF = 'PE' then idPedido end)/count(distinct idPedido) as pct_PedidoPE,  
  count(CASE WHEN descUF = 'PI' then idPedido end)/count(distinct idPedido) as pct_PedidoPI,  
  count(CASE WHEN descUF = 'PR' then idPedido end)/count(distinct idPedido) as pct_PedidoPR,  
  count(CASE WHEN descUF = 'RJ' then idPedido end)/count(distinct idPedido) as pct_PedidoRJ,  
  count(CASE WHEN descUF = 'RN' then idPedido end)/count(distinct idPedido) as pct_PedidoRN,  
  count(CASE WHEN descUF = 'RO' then idPedido end)/count(distinct idPedido) as pct_PedidoRO,  
  count(CASE WHEN descUF = 'RR' then idPedido end)/count(distinct idPedido) as pct_PedidoRR,  
  count(CASE WHEN descUF = 'RS' then idPedido end)/count(distinct idPedido) as pct_PedidoRS,  
  count(CASE WHEN descUF = 'SC' then idPedido end)/count(distinct idPedido) as pct_PedidoSC,  
  count(CASE WHEN descUF = 'SE' then idPedido end)/count(distinct idPedido) as pct_PedidoSE,  
  count(CASE WHEN descUF = 'SP' then idPedido end)/count(distinct idPedido) as pct_PedidoSP,  
  count(CASE WHEN descUF = 'TO' then idPedido end)/count(distinct idPedido) as pct_PedidoTO


FROM tb_join
GROUP BY idVendedor

)

SELECT 
    '2018-01-01' as dtReference,
    *   
       
FROM tb_group

-- COMMAND ----------


