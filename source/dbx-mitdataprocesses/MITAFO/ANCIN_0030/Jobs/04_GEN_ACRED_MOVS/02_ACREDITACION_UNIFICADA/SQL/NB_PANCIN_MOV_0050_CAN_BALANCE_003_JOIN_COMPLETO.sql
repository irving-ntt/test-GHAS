-- NB_PANCIN_MOV_0050_CAN_BALANCE_003_JOIN_COMPLETO.sql
-- Propósito: Aplicar CG_110 (Column Generator) y JO_120 (Join) usando tablas Delta
-- Tipo: Delta (Databricks) - SELECT con JOIN y campo LLAVE desde tablas Delta
-- NOTA: Esta query implementa la lógica de DataStage CG_110 + JO_120 usando tablas Delta
-- OPTIMIZACIÓN: Broadcast join para tabla pequeña (máximo ID)
-- ZONA HORARIA: México (America/Mexico_City)

-- Aplicar lógica de Column Generator (LLAVE) y Join usando tablas Delta creadas
-- NOTA: Broadcast join optimizado para tabla pequeña (MAX_ID_BALANCE)
SELECT  M.FTC_FOLIO,
        M.FTC_FOLIO_REL,
        M.FTF_MONTO_PESOS,
        M.FTF_MONTO_ACCIONES,
        M.FTN_NUM_CTA_INVDUAL AS FTC_NUM_CTA_INVDUAL,
        M.FCN_ID_TIPO_SUBCTA,
        M.FCN_ID_SIEFORE,
        M.FCN_ID_VALOR_ACCION,
        M.FTD_FEH_LIQUIDACION,
        M.FCN_ID_TIPO_MOV,
        M.FCN_ID_CONCEPTO_MOV,
        M.FCC_TABLA_NCI_MOV,
        M.FCN_ID_PLAZO,
        M.FTN_DEDUCIBLE,
        1 AS LLAVE,  -- CG_110: Campo LLAVE agregado
        B.MAXIMO1
FROM    #TABLA_MOVIMIENTOS_ETL# M
        INNER JOIN #TABLA_MAX_ID_BALANCE# B ON 1 = 1  -- Join usando LLAVE = 1
-- NOTA: Broadcast join automático para tabla pequeña (MAX_ID_BALANCE) 