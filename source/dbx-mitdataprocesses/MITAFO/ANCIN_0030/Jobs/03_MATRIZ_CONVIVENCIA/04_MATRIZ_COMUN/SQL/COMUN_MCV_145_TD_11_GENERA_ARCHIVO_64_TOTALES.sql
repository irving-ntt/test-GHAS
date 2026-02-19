-- Query para generar TOTALES para el arcbivo  64 desde tabla delta 
WITH ED AS 
( 
SELECT 
  COUNT(*) AS CUANTOS 
  FROM #DELTA_TABLA_NAME1# D
  )
  SELECT 
9 AS POSICION,
CONCAT 
  (
    '09'
    ,#FECHA_PROCESO#
    ,NVL2(ED.CUANTOS,LPAD(CUANTOS,9,'0'),'0000000000')   -- 10 POSICIONES
    ,'000000000000000000'
   -- ,LPAD('1',18)  
    ,LPAD(' ',91) 
  ) AS DETALLE
 -- , #FECHA_PROCESO# AS FECHA 
 -- ,ED.* 
  FROM ED

-- WHERE RN = 1


