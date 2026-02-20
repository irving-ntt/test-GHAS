-- =============================================================================
-- NB_PATRIF_GMO_0100_DEV_FOV_003_DELTA_TRANSFORM.sql
-- =============================================================================
-- Propósito: Aplicar toda la lógica de transformación sobre tablas Delta
-- Tipo: DELTA (Databricks)
-- Stages Originales: LO_300, CG_400, MO_401, PI_600, TF_800
-- =============================================================================
--
-- Este query replica la siguiente lógica de DataStage:
-- 1. LO_300_ID_VALOR: Lookup de valores de acción por fecha
-- 2. CG_400_GEN_COLUMNAS: Genera columnas de tipos de subcuenta (17 y 18)
-- 3. MO_401_STRING: Convierte FTC_FOLIO a string (implícito en Spark)
-- 4. PI_600_COLUMNAS: Pivotea columnas VIV08/VIV92
-- 5. TF_800_GEN_REGLAS: Aplica reglas de negocio finales
-- =============================================================================

WITH 
-- Paso 1: Lookup de valores de acción (LO_300_ID_VALOR)
dev_saldos_enriquecido AS (
    SELECT 
        ds.FTN_NUM_CTA_INVDUAL,
        ds.FTN_IND_SDO_DISP,
        ds.FTN_SDO_AIVS_08,
        ds.FTN_SDO_AIVS_92,
        ds.FTN_MONTO_PESOS_08,
        ds.FTN_MONTO_PESOS_92,
        ds.FTN_NUM_APLI_INTE_VIV_08,
        ds.FTN_NUM_APLI_INTE_VIV_92,
        ds.FTN_SALDO_VIV,
        ds.FCN_ESTATUS,
        ds.FTC_FOLIO,
        ds.FCN_ID_REGIMEN,
        ds.FCN_ID_SUBPROCESO,
        ds.FTN_CONTA_SERV,
        ds.FTN_MOTI_RECH,
        ds.FECHA_VALOR_AIVS,
        va.FCN_ID_VALOR_ACCION,
        va.FCN_VALOR_ACCION
    FROM #CATALOG_SCHEMA#.TEMP_DEV_SALDOS_FOV_#SR_FOLIO# ds
    INNER JOIN #CATALOG_SCHEMA#.TEMP_VALOR_ACCION_#SR_FOLIO# va 
        ON ds.FECHA_VALOR_AIVS = va.FCD_FEH_ACCION
),

-- Paso 2: Column Generator - Agregar tipos de subcuenta (CG_400_GEN_COLUMNAS)
con_tipos_subcuenta AS (
    SELECT 
        *,
        CAST(#TIPO_SUBCTA_VIV92_FOV# AS DECIMAL(10,0)) AS FTN_TIPO_SUBTCA_17,
        CAST(#TIPO_SUBCTA_VIV08_FOV# AS DECIMAL(10,0)) AS FTN_TIPO_SUBTCA_18
    FROM dev_saldos_enriquecido
),

-- Paso 3: Modify - Conversión de string (MO_401_STRING) ya implícito en Spark
-- Paso 4: Pivot - Generar filas por columnas VIV08 y VIV92 (PI_600_COLUMNAS)
datos_pivoteados AS (
    SELECT 
        FTN_NUM_CTA_INVDUAL,
        FTN_IND_SDO_DISP,
        FCN_VALOR_ACCION,
        FTN_SDO_AIVS_92 AS FTN_SDO_AIVS,
        FTN_MONTO_PESOS_92 AS FTN_MONTO_PESOS,
        FTN_NUM_APLI_INTE_VIV_92 AS FTN_NUM_APLI_INTE_VIV,
        FTN_SALDO_VIV,
        FCN_ESTATUS,
        FTC_FOLIO,
        FCN_ID_REGIMEN,
        FCN_ID_SUBPROCESO,
        FTN_CONTA_SERV,
        FTN_MOTI_RECH,
        FCN_ID_VALOR_ACCION,
        FTN_TIPO_SUBTCA_17 AS FTN_TIPO_SUBTCA,
        '92' AS PERIODO
    FROM con_tipos_subcuenta
    UNION ALL
    SELECT 
        FTN_NUM_CTA_INVDUAL,
        FTN_IND_SDO_DISP,
        FCN_VALOR_ACCION,
        FTN_SDO_AIVS_08 AS FTN_SDO_AIVS,
        FTN_MONTO_PESOS_08 AS FTN_MONTO_PESOS,
        FTN_NUM_APLI_INTE_VIV_08 AS FTN_NUM_APLI_INTE_VIV,
        FTN_SALDO_VIV,
        FCN_ESTATUS,
        FTC_FOLIO,
        FCN_ID_REGIMEN,
        FCN_ID_SUBPROCESO,
        FTN_CONTA_SERV,
        FTN_MOTI_RECH,
        FCN_ID_VALOR_ACCION,
        FTN_TIPO_SUBTCA_18 AS FTN_TIPO_SUBTCA,
        '08' AS PERIODO
    FROM con_tipos_subcuenta
),

-- Paso 5: Transformer - Aplicar reglas de negocio finales (TF_800_GEN_REGLAS)
resultado_final AS (
    SELECT 
        FTN_NUM_CTA_INVDUAL,
        FTN_IND_SDO_DISP,
        FTN_SDO_AIVS,
        FCN_VALOR_ACCION AS FTN_VALOR_AIVS,
        COALESCE(FTN_MONTO_PESOS, 0) + COALESCE(FTN_NUM_APLI_INTE_VIV, 0) AS FTN_MONTO_PESOS,
        FTN_TIPO_SUBTCA AS FCN_ID_TIPO_SUBCTA,
        FTN_SDO_AIVS AS FTN_NUM_APLI_INTE_VIV,
        COALESCE(FTN_MONTO_PESOS, 0) + COALESCE(FTN_NUM_APLI_INTE_VIV, 0) AS FTN_SALDO_VIV,
        FCN_ESTATUS,
        '#SR_FOLIO#' AS FTC_FOLIO,
        FCN_ID_REGIMEN,
        FCN_ID_SUBPROCESO AS FTN_ID_SUBP,
        FCN_ID_VALOR_ACCION,
        FTN_CONTA_SERV,
        FTN_MOTI_RECH,
        CURRENT_TIMESTAMP() AS FTD_FEH_CRE,
        '#CX_CRE_USUARIO#' AS FTC_USU_CRE,
        PERIODO
    FROM datos_pivoteados
    -- Constraint: Solo registros con al menos un valor diferente de 0
    WHERE COALESCE(FTN_MONTO_PESOS, 0) <> 0
       OR COALESCE(FTN_SDO_AIVS, 0) <> 0
       OR COALESCE(FTN_NUM_APLI_INTE_VIV, 0) <> 0
)

SELECT * FROM resultado_final

