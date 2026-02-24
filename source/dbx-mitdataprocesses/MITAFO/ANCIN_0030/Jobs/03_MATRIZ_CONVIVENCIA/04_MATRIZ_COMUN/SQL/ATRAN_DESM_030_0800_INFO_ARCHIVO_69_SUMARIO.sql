-- Query para generar TOTALES para el arcbivo  69 desde tabla delta 
WITH ED AS 
( 
SELECT 
  0 AS CUANTOS 
  )
  SELECT 
9 AS POSICION,
CONCAT 
  (
    '09'
   -- ,#FECHA_PROCESO#
    ,ED.CUANTOS,LPAD(CUANTOS,34,'0' )  -- 35 POSICIONES
    ,RPAD('', 91, ' ') 
  ) AS DETALLE
  FROM ED



