-- Query para el total de registros  y 02 MATRIZ del comun de matriz 
--  
-- SELECT 
--  COUNT(*) AS TOTAL
-- FROM  #DELTA_TABLA_NAME1#  
SELECT 
 DT.* 
FROM  
  #DELTA_TABLA_NAME1#  DT
LIMIT 1
   
