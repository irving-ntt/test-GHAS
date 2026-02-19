-- Query para validar la informacion de VAL_SUF y el resultado sera escrito en NO_CONVIV 
--  
SELECT 
         B.FTN_NUM_CTA_INVDUAL
        ,B.FTN_ID_PROCESO     
        ,B.FTN_ID_SUBPROCESO  
        ,B.FTN_ID_SIEFORE     
        ,B.FTN_ID_TIPO_SUBCTA 
        ,B.FTN_ID_TIPO_MONTO  
        ,B.FTN_ID_SALDO_OPERA 
        ,B.FTN_ID_TIPO_MOV    
        ,B.FTN_MONTO_ACCIONES 
        ,B.FTN_MONTO_PESOS  
        ,B.FCN_ID_PROCESO_CON    
        ,B.FCN_ID_SUBPROCESO_CON 
        ,B.FCN_ID_TIPO_SUBCTA_CON
        ,B.FCN_ID_TIPO_MONTO_CON  
        ,B.FTD_FECHA_BLOQ         
        ,B.B_MATRIZ      
        ,B.FFB_CONVIVENCIA       
        ,B.B_CONVIV   
        ,423 AS ESTATUS
        ,0 AS MONTO_ACCIONES_BAL
        ,0 AS MONTO_PESOS_BAL            
FROM 
(
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
        ,CASE WHEN 
             (D.FTN_ID_SALDO_OPERA = 12 AND BM.MONTO_ACCIONES_BAL < FTN_MONTO_ACCIONES  AND BM.B_SALDO = 1) 
                OR (D.FTN_ID_SALDO_OPERA = 11 AND  BM.B_SALDO <> 1 and BM.B_SALDO is not null) 
                OR (D.FTN_ID_SALDO_OPERA = 12 AND  BM.B_SALDO <> 1) THEN 1 ELSE 0 END AS B_SALDOS
FROM  #DELTA_TABLA_NAME1# D    -- DELTA DE VAL SUF
LEFT JOIN #DELTA_TABLA_NAME2# BM  -- DELTA DE BALANCE MOVS
      ON D.FTN_NUM_CTA_INVDUAL = BM.FTN_NUM_CTA_INVDUAL
      AND D.FTN_ID_SIEFORE = BM.FTN_ID_SIEFORE
      AND D.FTN_ID_TIPO_SUBCTA = BM.FTN_ID_TIPO_SUBCTA
) AS B
WHERE
      B_SALDOS = 1        -- SOLO LOS QUE CUMPLEN LAS CONDICIONES DE SALDOS
     



