-- ==========================================
-- QUERY: NB_PATRIF_0800_GMO_TRANS_FOV_01_UNIFIED.sql
-- DESCRIPCIÓN: Query unificado que combina filtros 08, 92, UNION y manejo de nulos
-- AUTOR: Sistema
-- FECHA: 2024-01-15
-- VERSIÓN: 3.0.0
-- ==========================================
-- PARÁMETROS:
--   @DELTA_TABLE: Tabla Delta de entrada
--   @TIPO_SUBCTA_VIV08_FOV: Tipo subcuenta 08
--   @TIPO_SUBCTA_VIV92_FOV: Tipo subcuenta 92
--   @SR_FOLIO: Folio del proceso
--   @CATALOG_SCHEMA: Catálogo y esquema
-- ==========================================
-- DEPENDENCIAS:
--   - Tabla Delta: DS_700_SALDOS_PROC_{SR_FOLIO}
-- ==========================================
-- HISTORIAL DE CAMBIOS:
--   2024-01-15 Sistema - Creación inicial
--   2024-01-15 Sistema - Optimización para memoria: Query separado
--   2024-01-15 Sistema - Unificación de todos los queries en uno solo
-- ==========================================

WITH filtered_data AS (
    -- Filtro por tipo subcuenta 08 (VIV08_FOV)
    SELECT 
        FTN_CONTA_SERV,
        FTN_NUM_CTA_INVDUAL,
        FTC_FOLIO_BITACORA AS FTC_FOLIO,
        FCN_ID_SUBP AS FTN_ID_SUBP,
        FTN_IND_SDO_DISP_VIV97 AS FTN_IND_SDO_DISP,
        FCN_ID_TIPO_SUBCTA_97 AS FCN_ID_TIPO_SUBCTA,
        FTN_NUM_APLI_INTE_VIVI97_SOL AS FTN_NUM_APLI_INTE_VIV,
        FTN_SALDO_TOT_VIVI08 AS FTN_SALDO_VIV,
        FCN_ID_REGIMEN,
        FCN_ID_VALOR_ACCION,
        FTN_VALOR_AIVS,
        FTN_SDO_AIVS,
        FTN_MONTO_PESOS,
        FCN_ESTATUS,
        FTN_MOTI_RECH,
        FTD_FEH_CRE,
        FTC_USU_CRE
    FROM #CATALOG_SCHEMA#.#DELTA_TABLE#
    WHERE FTC_FOLIO_BITACORA = '#SR_FOLIO#'
      AND FCN_ID_TIPO_SUBCTA_97 = #TIPO_SUBCTA_VIV08_FOV#
      AND FTN_IND_SDO_DISP_VIV97 = 1

    UNION ALL

    -- Filtro por tipo subcuenta 92 (VIV92_FOV)
    SELECT 
        FTN_CONTA_SERV,
        FTN_NUM_CTA_INVDUAL,
        FTC_FOLIO_BITACORA AS FTC_FOLIO,
        FCN_ID_SUBP AS FTN_ID_SUBP,
        FTN_IND_SDO_DISP_92 AS FTN_IND_SDO_DISP,
        FCN_ID_TIPO_SUBCTA_92 AS FCN_ID_TIPO_SUBCTA,
        FTN_NUM_APLI_INTE_VIVI92_SOL AS FTN_NUM_APLI_INTE_VIV,
        FTN_SALDO_TOT_VIVI92 AS FTN_SALDO_VIV,
        FCN_ID_REGIMEN,
        FCN_ID_VALOR_ACCION,
        FTN_VALOR_AIVS,
        FTN_SDO_AIVS_92 AS FTN_SDO_AIVS,
        FTN_MONTO_PESOS_92 AS FTN_MONTO_PESOS,
        FCN_ESTATUS,
        FTN_MOTI_RECH,
        FTD_FEH_CRE,
        FTC_USU_CRE
    FROM #CATALOG_SCHEMA#.#DELTA_TABLE#
    WHERE FTC_FOLIO_BITACORA = '#SR_FOLIO#'
      AND FCN_ID_TIPO_SUBCTA_92 = #TIPO_SUBCTA_VIV92_FOV#
      AND FTN_IND_SDO_DISP_92 = 1
)
-- Manejo de valores nulos y selección final
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
FROM filtered_data
