-- =============================================================================
-- NB_PATRIF_MOV_DEV_PSJL_02_006_DELTA_PROCESS.sql
-- =============================================================================
-- Propósito: Procesamiento completo - Joins, Lookups y Transformaciones
-- Tipo: DELTA (Databricks)
-- Stages Originales: JO_200, JO_301, LO_401, LO_501, JO_701, TF_800
-- =============================================================================
--
-- Este query consolida la siguiente lógica de DataStage:
-- 1. JO_200_SALDO_SOL: Left Outer Join (Oracle + Dataset)
-- 2. JO_301_FECH_ACCION: Inner Join (+ Valor Acción)
-- 3. LO_401_SUBCTA: Lookup con fail (+ Tipo Subcuenta)
-- 4. LO_501_MOV_SUB: Lookup con fail (+ Mov Subcuenta)
-- 5. JO_701_ID_MARCA: Left Outer Join (+ ID Marca)
-- 6. TF_800_REGLAS: Transformer (reglas de negocio VIV97)
-- =============================================================================

WITH
-- CTE 1: Left Outer Join entre Oracle y Dataset (JO_200_SALDO_SOL)
saldo_sol AS (
    SELECT 
        oracle.FTC_FOLIO,
        oracle.FCN_ID_SUBPROCESO,
        oracle.FTN_CONSE_REG_LOTE,
        oracle.FTN_NUM_CTA_INVDUAL,
        oracle.FFN_ID_CONCEPTO_MOV,
        oracle.FCN_ID_SIEFORE,
        oracle.FTN_SDO_ACCIONES,
        oracle.FTN_SDO_PESOS,
        oracle.FCN_ID_TIPO_SUBCTA,
        oracle.FCN_ID_VALOR_ACCION,
        oracle.FTN_VALOR_AIVS,
        dataset.IMP_SOL
    FROM #CATALOG_SCHEMA#.TEMP_VAL_SALDOS_#SR_FOLIO# oracle
    LEFT OUTER JOIN #CATALOG_SCHEMA#.DELTA_600_GEN_MOV_#SR_FOLIO# dataset
        ON oracle.FTC_FOLIO = dataset.FTC_FOLIO
       AND oracle.FCN_ID_SUBPROCESO = dataset.FCN_ID_SUBPROCESO
       AND oracle.FTN_NUM_CTA_INVDUAL = dataset.FTN_NUM_CTA_INVDUAL
       AND oracle.FFN_ID_CONCEPTO_MOV = dataset.FFN_ID_CONCEPTO_MOV
       AND oracle.FTN_CONSE_REG_LOTE = dataset.FTN_CONSE_REG_LOTE
),

-- CTE 2: Inner Join con Valor Acción (JO_301_FECH_ACCION)
con_fecha AS (
    SELECT 
        s.*,
        v.FCD_FEH_ACCION
    FROM saldo_sol s
    INNER JOIN #CATALOG_SCHEMA#.TEMP_VALOR_ACCION v
        ON s.FCN_ID_VALOR_ACCION = v.FCN_ID_VALOR_ACCION
),

-- CTE 3: Lookup Tipo Subcuenta (LO_401_SUBCTA) - Inner Join porque ifNotFound=fail
con_subcta AS (
    SELECT 
        f.*,
        t.FCN_ID_CAT_SUBCTA,
        t.FCN_ID_PLAZO
    FROM con_fecha f
    INNER JOIN #CATALOG_SCHEMA#.TEMP_TIPO_SUBCTA t
        ON f.FCN_ID_TIPO_SUBCTA = t.FCN_ID_TIPO_SUBCTA
),

-- CTE 4: Lookup Movimiento Subcuenta (LO_501_MOV_SUB) - Inner Join porque ifNotFound=fail
con_mov AS (
    SELECT 
        c.*,
        m.FRN_ID_MOV_SUBCTA,
        m.FTN_DEDUCIBLE
    FROM con_subcta c
    INNER JOIN #CATALOG_SCHEMA#.TEMP_MOV_SUBCTA_#SR_SUBPROCESO# m
        ON c.FCN_ID_TIPO_SUBCTA = m.FCN_ID_TIPO_SUBCTA
),

-- CTE 5: Left Outer Join con Matriz Convivencia (JO_701_ID_MARCA)
con_marca AS (
    SELECT 
        mv.*,
        mc.FTN_ID_MARCA
    FROM con_mov mv
    LEFT OUTER JOIN #CATALOG_SCHEMA#.TEMP_MATRIZ_CONV_#SR_FOLIO# mc
        ON mv.FTC_FOLIO = mc.FTC_FOLIO
       AND mv.FTN_NUM_CTA_INVDUAL = mc.FTN_NUM_CTA_INVDUAL
       AND mv.FRN_ID_MOV_SUBCTA = mc.FRN_ID_MOV_SUBCTA
),

-- CTE 6: Aplicar Transformaciones (TF_800_REGLAS)
reglas AS (
    SELECT
        -- Campo 1: FCN_ID_CAT_SUBCTA (convertir NULL a 0)
        COALESCE(FCN_ID_CAT_SUBCTA, CAST(0 AS DECIMAL(10,0))) AS FCN_ID_CAT_SUBCTA,
        
        -- Campo 2: FTC_FOLIO
        FTC_FOLIO,
        
        -- Campo 3: FTC_FOLIO_REL (siempre NULL)
        CAST(NULL AS STRING) AS FTC_FOLIO_REL,
        
        -- Campo 4: FTF_MONTO_PESOS (convertir NULL a 0, escala 2)
        CAST(COALESCE(FTN_SDO_PESOS, 0) AS DECIMAL(18,2)) AS FTF_MONTO_PESOS,
        
        -- Campo 5: FTF_MONTO_ACCIONES (convertir NULL a 0)
        COALESCE(FTN_SDO_ACCIONES, CAST(0 AS DECIMAL(18,6))) AS FTF_MONTO_ACCIONES,
        
        -- Campo 6: FCD_FEH_ACCION
        FCD_FEH_ACCION,
        
        -- Campo 7: FTN_NUM_CTA_INVDUAL
        FTN_NUM_CTA_INVDUAL,
        
        -- Campo 8: FTN_ID_MARCA (convertir NULL a 0)
        COALESCE(FTN_ID_MARCA, CAST(0 AS DECIMAL(10,0))) AS FTN_ID_MARCA,
        
        -- Campo 9: FCN_ID_TIPO_MOV (constante parámetro)
        CAST(#FL_ID_TIPO_MOV# AS DECIMAL(10,0)) AS FCN_ID_TIPO_MOV,
        
        -- Campo 10: FCC_TABLA_NCI_MOV (lógica condicional VIV97)
        CASE 
            WHEN FFN_ID_CONCEPTO_MOV = #CON_MOV_VIV97# THEN '#TABLA_NCI_VIV#'
            ELSE '#TABLA_NCI_RCV#'
        END AS FCC_TABLA_NCI_MOV,
        
        -- Campo 11: FNN_ID_REFERENCIA (renombrado de FTN_CONSE_REG_LOTE)
        FTN_CONSE_REG_LOTE AS FNN_ID_REFERENCIA,
        
        -- Campo 12: FTN_PRE_MOV_GENERADO (constante 0 = Insertado)
        CAST(0 AS DECIMAL(1,0)) AS FTN_PRE_MOV_GENERADO,
        
        -- Campo 13: FCN_ID_SUBPROCESO (del parámetro)
        CAST(#SR_SUBPROCESO# AS DECIMAL(10,0)) AS FCN_ID_SUBPROCESO,
        
        -- Campo 14: FTF_MONTO_PESOS_SOL (lógica VIV97 - escala 2)
        CAST(
            CASE 
                WHEN FFN_ID_CONCEPTO_MOV = #CON_MOV_VIV97# THEN IMP_SOL * FTN_VALOR_AIVS
                ELSE IMP_SOL
            END AS DECIMAL(18,2)
        ) AS FTF_MONTO_PESOS_SOL,
        
        -- Campo 15: FTF_MONTO_ACCIONES_SOL (lógica VIV97 inversa)
        CASE 
            WHEN FFN_ID_CONCEPTO_MOV = #CON_MOV_VIV97# THEN IMP_SOL
            ELSE IMP_SOL / FTN_VALOR_AIVS
        END AS FTF_MONTO_ACCIONES_SOL,
        
        -- Campo 16: FTN_DEDUCIBLE
        FTN_DEDUCIBLE,
        
        -- Campo 17: FCN_ID_PLAZO
        FCN_ID_PLAZO,
        
        -- Campo 18: FCD_FEH_CRE (timestamp actual)
        CURRENT_TIMESTAMP() AS FCD_FEH_CRE,
        
        -- Campo 19: FCC_USU_CRE (usuario ETL)
        '#CX_CRE_USUARIO#' AS FCC_USU_CRE,
        
        -- Campo 20: FCD_FEH_ACT (siempre NULL)
        CAST(NULL AS TIMESTAMP) AS FCD_FEH_ACT,
        
        -- Campo 21: FCC_USU_ACT (siempre NULL)
        CAST(NULL AS STRING) AS FCC_USU_ACT,
        
        -- Campo 22: FCN_ID_CONCEPTO_MOV (convertir NULL a 0)
        COALESCE(FFN_ID_CONCEPTO_MOV, CAST(0 AS DECIMAL(10,0))) AS FCN_ID_CONCEPTO_MOV,
        
        -- Campo 23: FCN_ID_SIEFORE
        FCN_ID_SIEFORE
        
    FROM con_marca
)

-- Resultado final con todos los campos en el orden correcto
SELECT 
    FCN_ID_CAT_SUBCTA,
    FTC_FOLIO,
    FTC_FOLIO_REL,
    FTF_MONTO_PESOS,
    FTF_MONTO_ACCIONES,
    FCD_FEH_ACCION,
    FTN_NUM_CTA_INVDUAL,
    FTN_ID_MARCA,
    FCN_ID_TIPO_MOV,
    FCC_TABLA_NCI_MOV,
    FNN_ID_REFERENCIA,
    FTN_PRE_MOV_GENERADO,
    FCN_ID_SUBPROCESO,
    FTF_MONTO_PESOS_SOL,
    FTF_MONTO_ACCIONES_SOL,
    FTN_DEDUCIBLE,
    FCN_ID_PLAZO,
    FCD_FEH_CRE,
    FCC_USU_CRE,
    FCD_FEH_ACT,
    FCC_USU_ACT,
    FCN_ID_CONCEPTO_MOV,
    FCN_ID_SIEFORE
FROM reglas

