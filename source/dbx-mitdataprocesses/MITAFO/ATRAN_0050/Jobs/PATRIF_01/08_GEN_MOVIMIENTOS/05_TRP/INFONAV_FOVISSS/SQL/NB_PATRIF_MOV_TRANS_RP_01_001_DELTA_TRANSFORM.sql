-- =============================================================================
-- NB_PATRIF_MOV_TRANS_RP_01_001_DELTA_TRANSFORM.sql
-- =============================================================================
-- Propósito: Filtrar, homologar campos y preparar datos para generación de movimientos TRP
-- Tipo: DELTA (Databricks)
-- Stages Originales: FI_100_TIPO_SUBCTA, FU_200_REG, MO_300_NONULL
-- =============================================================================
--
-- Este query replica la siguiente lógica de DataStage:
-- 1. FI_100_TIPO_SUBCTA: Filtra por tipo subcuenta (Output 0: VIV97/VIV08, Output 1: VIV92)
-- 2. FU_200_REG: Combina ambos flujos con funnel (UNION ALL)
-- 3. MO_300_NONULL: Convierte campos nullable a NOT NULL con handle_null
-- =============================================================================

WITH 
-- Paso 1: Filter Output 0 - VIV97 (INFONAVIT) o VIV08 (FOVISSSTE)
flujo_viv97_viv08 AS (
    SELECT 
        CAST(FTN_CONTA_SERV AS DECIMAL(10,0)) AS FTN_CONTA_SERV,
        FTN_NUM_CTA_INVDUAL,
        FTC_FOLIO_BITACORA AS FTC_FOLIO,
        FTN_ID_SUBP,
        FCN_ID_REGIMEN,
        FCN_ID_VALOR_ACCION,
        FTN_VALOR_AIVS,
        FTN_IND_SDO_DISP_VIV97 AS FTN_IND_SDO_DISP,
        FTN_SDO_AIVS,
        FTN_MONTO_PESOS,
        CAST(FCN_ID_TIPO_SUBCTA_97 AS DECIMAL(10,0)) AS FCN_ID_TIPO_SUBCTA,
        FTN_TOTAL_AIVS_97 AS FTN_NUM_APLI_INTE_VIV,
        FTN_NUM_APLI_INTE_VIVI97_SOL AS FTN_SALDO_VIV,
        FCN_ESTATUS,
        FTN_MOTI_RECH,
        FTD_FEH_CRE,
        FTD_USU_CRE AS FTC_USU_CRE
    FROM #CATALOG_SCHEMA#.#DELTA_700_SALDOS_PROC_FOV#
    WHERE FTN_IND_SDO_DISP_VIV97 = 1 
      AND (FCN_ID_TIPO_SUBCTA_97 = #TIPO_SUBCTA_VIV97# OR FCN_ID_TIPO_SUBCTA_97 = #TIPO_SUBCTA_VIV08_FOV#)
),

-- Paso 2: Filter Output 1 - VIV92 (INFONAVIT o FOVISSSTE)
flujo_viv92 AS (
    SELECT 
        CAST(FTN_CONTA_SERV AS DECIMAL(10,0)) AS FTN_CONTA_SERV,
        FTN_NUM_CTA_INVDUAL,
        FTC_FOLIO_BITACORA AS FTC_FOLIO,
        FTN_ID_SUBP,
        FCN_ID_REGIMEN,
        FCN_ID_VALOR_ACCION,
        FTN_VALOR_AIVS,
        FTN_IND_SDO_DISP_92 AS FTN_IND_SDO_DISP,
        FTN_SDO_AIVS_92 AS FTN_SDO_AIVS,
        FTN_MONTO_PESOS_92 AS FTN_MONTO_PESOS,
        FCN_ID_TIPO_SUBCTA_92 AS FCN_ID_TIPO_SUBCTA,
        FTN_TOTAL_AIVS_08 AS FTN_NUM_APLI_INTE_VIV,
        FTN_NUM_APLI_INTE_VIVI92_SOL AS FTN_SALDO_VIV,
        FCN_ESTATUS,
        FTN_MOTI_RECH,
        FTD_FEH_CRE,
        FTD_USU_CRE AS FTC_USU_CRE
    FROM #CATALOG_SCHEMA#.#DELTA_700_SALDOS_PROC_FOV#
    WHERE FTN_IND_SDO_DISP_92 = 1 
      AND (FCN_ID_TIPO_SUBCTA_92 = #TIPO_SUBCTA_VIV92# OR FCN_ID_TIPO_SUBCTA_92 = #TIPO_SUBCTA_VIV92_FOV#)
),

-- Paso 3: Funnel - Combinar ambos flujos (FU_200_REG)
datos_combinados AS (
    SELECT * FROM flujo_viv97_viv08
    UNION ALL
    SELECT * FROM flujo_viv92
),

-- Paso 4: Modify - Handle null para campos críticos (MO_300_NONULL)
resultado_final AS (
    SELECT 
        COALESCE(FTN_NUM_CTA_INVDUAL, 0) AS FTN_NUM_CTA_INVDUAL,
        FTN_IND_SDO_DISP,
        FTN_SDO_AIVS,
        FTN_VALOR_AIVS,
        FTN_MONTO_PESOS,
        FCN_ID_TIPO_SUBCTA,
        FTN_NUM_APLI_INTE_VIV,
        FTN_SALDO_VIV,
        FCN_ESTATUS,
        COALESCE(FTC_FOLIO, '0') AS FTC_FOLIO,
        FCN_ID_REGIMEN,
        FTN_ID_SUBP,
        FCN_ID_VALOR_ACCION,
        FTN_CONTA_SERV,
        FTN_MOTI_RECH,
        FTD_FEH_CRE,
        FTC_USU_CRE
    FROM datos_combinados
)

SELECT * FROM resultado_final

