-- Query para generar el dataset 02 (200) NO CONVIV 
--  
SELECT 
   -- D.* 
         D.FTN_NUM_CTA_INVDUAL
        ,D.FTN_ID_PROCESO     
        ,D.FTN_ID_SUBPROCESO  
        ,D.FTN_ID_SIEFORE     
        ,D.FTN_ID_TIPO_SUBCTA 
        ,D.FTN_ID_TIPO_MONTO  
        ,D.FTN_ID_SALDO_OPERA 
        ,D.FTN_ID_TIPO_MOV    
        ,D.FTN_MONTO_ACCIONES 
        ,D.FTN_MONTO_PESOS  
        ,D.FCN_ID_PROCESO_CON    
        ,D.FCN_ID_SUBPROCESO_CON 
        ,D.FCN_ID_TIPO_SUBCTA_CON
        ,D.FCN_ID_TIPO_MONTO_CON 
        ,D.FTD_FECHA_BLOQ        
        ,D.B_MATRIZ      
        ,D.FFB_CONVIVENCIA       
        ,D.B_CONVIV 
        ,421 AS ESTATUS
        ,0 AS MONTO_ACCIONES_BAL
        ,0 AS MONTO_PESOS_BAL            
FROM  #DELTA_TABLA_NAME1# D
WHERE 
    D.B_MATRIZ = 1 AND D.FFB_CONVIVENCIA = '0'