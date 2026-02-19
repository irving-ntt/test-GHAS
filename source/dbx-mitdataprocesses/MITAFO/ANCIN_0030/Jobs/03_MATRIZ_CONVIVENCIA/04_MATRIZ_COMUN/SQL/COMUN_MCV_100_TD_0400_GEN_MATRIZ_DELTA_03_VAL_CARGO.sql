-- Query para generar el dataset 03  (400) VAL CARGO 
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
FROM  #DELTA_TABLA_NAME1# D
WHERE 
    D.B_MATRIZ = 1 
    AND D.FFB_CONVIVENCIA = '1'
    AND D.FTN_ID_TIPO_MOV = '180'