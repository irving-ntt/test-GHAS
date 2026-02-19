-- Query para remover duplicados que no conviven  
WITH RDK AS 
( 
SELECT 
         FTN_NUM_CTA_INVDUAL	
        ,FTN_ID_TIPO_SUBCTA	
        ,FCN_ID_SUBPROCESO_CON
        ,FCN_ID_TIPO_SUBCTA_CON
        ,FCN_ID_TIPO_MONTO_CON
        ,FTD_FECHA_BLOQ
        ,ESTATUS
        ,MONTO_ACCIONES_BAL
        ,MONTO_PESOS_BAL
        ,row_number() OVER(PARTITION BY 
          FTN_NUM_CTA_INVDUAL, 
          FTN_ID_TIPO_SUBCTA 
          ORDER BY 
          FTN_NUM_CTA_INVDUAL, 
          FTN_ID_TIPO_SUBCTA,
          ESTATUS ASC) AS RN
      FROM  		       
    #DELTA_TABLA_NAME1#
)
SELECT * FROM RDK
WHERE RN = 1


