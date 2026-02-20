-- =============================================================================
-- QUERY SQL QUE REPLICA EL JOB DATASTAGE JP_PATRIF_MOV_TRANS_INFO_01
-- =============================================================================
-- 
-- Este query replica toda la lógica del job DataStage en una sola consulta:
-- 1. Lee datos de entrada (DS_700_SALDOS_PROC)
-- 2. Filtra por tipo de subcuenta (FI_100_TIPO_SUBCTA)
-- 3. Combina flujos (FU_300_REG)
-- 4. Genera campos adicionales (CG_200_SALDO)
-- 5. Maneja valores nulos (MO_400_NONULL)
-- 6. Guarda resultado (DS_500_SALDO)
--
-- Parámetros del job:
-- - p_tipo_subcta15: Tipo subcuenta 15 (ej: 15)
-- - p_tipo_subcta16: Tipo subcuenta 16 (ej: 16)
-- =============================================================================

WITH 
-- Paso 1: Filtrar datos por tipo de subcuenta (FI_100_TIPO_SUBCTA)
filtered_data AS (
    SELECT 
        -- Datos filtrados para tipo subcuenta 15 (VIV97)
        FTN_NUM_CTA_INVDUAL,
        FTN_IND_SDO_DISP_VIV97 AS FTN_IND_SDO_DISP,
        FTN_SDO_AIVS,
        FTN_VALOR_AIVS,
        FTN_MONTO_PESOS,
        FCN_ID_TIPO_SUBCTA_97 AS FCN_ID_TIPO_SUBCTA,
        FTN_NUM_APLI_INTE_VIVI97_SOL AS FTN_NUM_APLI_INTE_VIV,
        FTN_SALDO_VIVI97_SOLI AS FTN_SALDO_VIV,
        FTC_FOLIO_BITACORA AS FTC_FOLIO,
        FCN_ESTATUS,
        FCN_ID_REGIMEN,
        FTN_ID_SUBP,
        FCN_ID_VALOR_ACCION,
        FTN_CONTA_SERV,
        FTN_MOTI_RECH,
        FTD_FEH_CRE,
        FTC_USU_CRE,
        'FLOW_15' AS flow_type
    FROM #CATALOG_SCHEMA#.#DELTA_700_SALDOS_PROC#
    WHERE FCN_ID_TIPO_SUBCTA_97 = #p_tipo_subcta15#  -- p_tipo_subcta15
      AND FTN_IND_SDO_DISP_VIV97 = 1
    
    UNION ALL
    
    SELECT 
        -- Datos filtrados para tipo subcuenta 16 (92)
        FTN_NUM_CTA_INVDUAL,
        FTN_IND_SDO_DISP_92 AS FTN_IND_SDO_DISP,
        FTN_SDO_AIVS_92 AS FTN_SDO_AIVS,
        FTN_VALOR_AIVS,
        FTN_MONTO_PESOS_92 AS FTN_MONTO_PESOS,
        FCN_ID_TIPO_SUBCTA_92 AS FCN_ID_TIPO_SUBCTA,
        FTN_NUM_APLI_INTE_VIVI92_SOL AS FTN_NUM_APLI_INTE_VIV,
        0 AS FTN_SALDO_VIV,  -- Campo generado como en CG_200_SALDO
        FTC_FOLIO_BITACORA AS FTC_FOLIO,
        FCN_ESTATUS,
        FCN_ID_REGIMEN,
        FTN_ID_SUBP,
        FCN_ID_VALOR_ACCION,
        FTN_CONTA_SERV,
        FTN_MOTI_RECH,
        FTD_FEH_CRE,
        FTC_USU_CRE,
        'FLOW_16' AS flow_type
    FROM #CATALOG_SCHEMA#.#DELTA_700_SALDOS_PROC#
    WHERE FCN_ID_TIPO_SUBCTA_92 = #p_tipo_subcta16#  -- p_tipo_subcta16
      AND FTN_IND_SDO_DISP_92 = 1
),

-- Paso 2: Aplicar transformaciones finales y manejo de nulos (MO_400_NONULL)
final_data AS (
    SELECT 
        -- Manejo de valores nulos (equivalente a handle_null)
        COALESCE(FTN_NUM_CTA_INVDUAL, 0) AS FTN_NUM_CTA_INVDUAL,
        FTN_IND_SDO_DISP,
        FTN_SDO_AIVS,
        FTN_VALOR_AIVS,
        FTN_MONTO_PESOS,
        FCN_ID_TIPO_SUBCTA,
        FTN_NUM_APLI_INTE_VIV,
        FTN_SALDO_VIV,
        COALESCE(FTC_FOLIO, '0') AS FTC_FOLIO,
        FCN_ESTATUS,
        FCN_ID_REGIMEN,
        FTN_ID_SUBP,
        FCN_ID_VALOR_ACCION,
        FTN_CONTA_SERV,
        FTN_MOTI_RECH,
        FTD_FEH_CRE,
        FTC_USU_CRE,
        flow_type
    FROM filtered_data
)

-- Paso 3: Seleccionar datos finales (equivalente a DS_500_SALDO)
SELECT 
    FTN_NUM_CTA_INVDUAL,
    FTN_IND_SDO_DISP,
    FTN_SDO_AIVS,
    FTN_VALOR_AIVS,
    FTN_MONTO_PESOS,
    FCN_ID_TIPO_SUBCTA,
    FTN_NUM_APLI_INTE_VIV,
    FTN_SALDO_VIV,
    FTC_FOLIO,
    FCN_ESTATUS,
    FCN_ID_REGIMEN,
    FTN_ID_SUBP,
    FCN_ID_VALOR_ACCION,
    FTN_CONTA_SERV,
    FTN_MOTI_RECH,
    FTD_FEH_CRE,
    FTC_USU_CRE
FROM final_data
ORDER BY FTN_NUM_CTA_INVDUAL, FCN_ID_TIPO_SUBCTA; 