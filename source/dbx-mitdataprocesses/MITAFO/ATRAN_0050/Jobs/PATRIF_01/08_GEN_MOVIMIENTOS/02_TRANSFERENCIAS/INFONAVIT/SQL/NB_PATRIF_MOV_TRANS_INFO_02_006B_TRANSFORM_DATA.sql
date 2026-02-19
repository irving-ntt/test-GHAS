-- =====================================================================================
-- NB_PATRIF_MOV_TRANS_INFO_02_006B_TRANSFORM_DATA.sql
-- =====================================================================================
-- Transformación de datos para inserción (solo Delta)
-- Se ejecuta con db.sql_delta() - SOLO PARA DELTA
-- =====================================================================================

SELECT 
    -- FCN_ID_CAT_SUBCTA
    NVL(FCN_ID_CAT_SUBCTA, 0) AS FCN_ID_CAT_SUBCTA,
    
    -- FTC_FOLIO
    FTC_FOLIO,
    
    -- FTC_FOLIO_REL (string null)
    CAST(NULL AS STRING) AS FTC_FOLIO_REL,
    
    -- FTF_MONTO_PESOS
    NVL(FTN_MONTO_PESOS, 0) AS FTF_MONTO_PESOS,
    
    -- FTF_MONTO_ACCIONES
    NVL(FTN_SDO_AIVS, 0) AS FTF_MONTO_ACCIONES,
    
    -- FCD_FEH_ACCION
    FCD_FEH_ACCION,
    
    -- FTN_NUM_CTA_INVDUAL
    FTN_NUM_CTA_INVDUAL,
    
    -- FTN_ID_MARCA
    NVL(FTN_ID_MARCA, 0) AS FTN_ID_MARCA,
    
    -- FCN_ID_TIPO_MOV
    #FL_ID_TIPO_MOV# AS FCN_ID_TIPO_MOV,
    
    -- FCC_TABLA_NCI_MOV
    '#TABLA_NCI_VIV#' AS FCC_TABLA_NCI_MOV,
    
    -- FNN_ID_REFERENCIA
    FTN_CONTA_SERV AS FNN_ID_REFERENCIA,
    
    -- FTN_PRE_MOV_GENERADO
    0 AS FTN_PRE_MOV_GENERADO,
    
    -- FCN_ID_SUBPROCESO
    #SR_SUBPROCESO# AS FCN_ID_SUBPROCESO,
    
    -- FTF_MONTO_PESOS_SOL (reglas condicionales)
    CASE 
        WHEN #SR_SUBPROCESO# = 364 THEN FTN_MONTO_PESOS
        WHEN #SR_SUBPROCESO# = 365 THEN FTN_NUM_APLI_INTE_VIV * FTN_VALOR_AIVS
        WHEN #SR_SUBPROCESO# = 363 THEN FTN_SALDO_VIV
        WHEN #SR_SUBPROCESO# = 3286 THEN FTN_SALDO_VIV
        WHEN #SR_SUBPROCESO# = 368 THEN FTN_SALDO_VIV
        ELSE 0
    END AS FTF_MONTO_PESOS_SOL,
    
    -- FTF_MONTO_ACCIONES_SOL (reglas condicionales)
    CASE 
        WHEN #SR_SUBPROCESO# = 364 THEN FTN_SDO_AIVS
        WHEN #SR_SUBPROCESO# = 365 THEN FTN_NUM_APLI_INTE_VIV
        WHEN #SR_SUBPROCESO# = 363 THEN FTN_NUM_APLI_INTE_VIV
        WHEN #SR_SUBPROCESO# = 3286 THEN FTN_NUM_APLI_INTE_VIV
        WHEN #SR_SUBPROCESO# = 368 THEN FTN_NUM_APLI_INTE_VIV
        ELSE 0
    END AS FTF_MONTO_ACCIONES_SOL,
    
    -- FTN_DEDUCIBLE
    FTN_DEDUCIBLE,
    
    -- FCN_ID_PLAZO
    FCN_ID_PLAZO,
    
    -- FCD_FEH_CRE
    from_utc_timestamp(current_timestamp(), 'America/Mexico_City') AS FCD_FEH_CRE,
    
    -- FCC_USU_CRE
    '#CX_CRE_USUARIO#' AS FCC_USU_CRE,
    
    -- FCD_FEH_ACT (timestamp null)
    CAST(NULL AS TIMESTAMP) AS FCD_FEH_ACT,
    
    -- FCC_USU_ACT (string null)
    CAST(NULL AS STRING) AS FCC_USU_ACT,
    
    -- FCN_ID_CONCEPTO_MOV
    NVL(FFN_ID_CONCEPTO_MOV, 0) AS FCN_ID_CONCEPTO_MOV,
    
    -- FCN_ID_SIEFORE
    #SIEFORE# AS FCN_ID_SIEFORE
    
FROM #CATALOG_SCHEMA#.NB_PATRIF_MOV_TRANS_INFO_02_005_#SR_FOLIO# 