-- Query para generar ENCABEZADO DE ARCHIVO  69 desde tabla delta 
WITH ED AS 
( 
SELECT 
  --REPLACE(TRIM(TOTAL), '.', '') AS CUANTOS 
  CAST(SPLIT(TOTAL, '\\.')[0] AS STRING) AS CUANTOS
  FROM #DELTA_TABLA_NAME1# D
  LIMIT 1
  )
SELECT 
1 AS POSICION,
CONCAT 
  (
    '01' --2 posiciones
    ,#FECHA_PROCESO#  -- 8 posiciones 
    ,NVL2(ED.CUANTOS,LPAD(CUANTOS,9,'0'),'000000000')  --9 posiciones 
    --,CONCAT('000000000',ED.CUANTOS)
    ,LPAD(' ',109)  --109 posiciones 
  ) AS DETALLE
  -- , #FECHA_PROCESO# AS FECHA 
  -- ,ED.* 
  FROM ED