-- =============================================================================
-- NB_PATRIF_MOV_DEV_PSJL_01_002_DELTA_UNPIVOT.sql
-- =============================================================================
-- Propósito: Aplicar unpivot de conceptos de movimiento y filtrar registros
-- Tipo: DELTA (Databricks)
-- Stages Originales: CG_GEN_CAMPOS, PI_300_GEN_REGISTROS, FI_400_ELIMINA_CEROS, CG_GEN_FOLIO
-- =============================================================================
--
-- Este query replica la siguiente lógica de DataStage:
-- 1. CG_GEN_CAMPOS: Genera 11 columnas de conceptos de movimiento
-- 2. PI_300_GEN_REGISTROS: Hace unpivot de 11 columnas a 11 filas por registro
-- 3. FI_400_ELIMINA_CEROS: Filtra registros con IMP_SOL <> 0
-- 4. CG_GEN_FOLIO: Agrega FTC_FOLIO y renombra CON_MOV a FFN_ID_CONCEPTO_MOV
-- =============================================================================

SELECT 
    '#SR_FOLIO#' AS FTC_FOLIO,
    FCN_ID_SUBPROCESO,
    FTN_CONSE_REG_LOTE,
    FTN_NUM_CTA_INVDUAL,
    CON_MOV AS FFN_ID_CONCEPTO_MOV,
    IMP_SOL
FROM (
    SELECT 
        FCN_ID_SUBPROCESO,
        FTN_CONSE_REG_LOTE,
        FTN_NUM_CTA_INVDUAL,
        STACK(11,
            #CON_MOV_IMP_PESOS_RET#,          IMP_PESOS_RET,
            #CON_MOV_ACT_PESOS_RET#,          ACT_PESOS_RET,
            #CON_MOV_REN_PESOS_RET#,          REN_PESOS_RET,
            #CON_MOV_IMP_PESOS_CES_VEJ_PAT#,  IMP_PESOS_CES_VEJ_PAT,
            #CON_MOV_IMP_PESOS_CES_VEJ_TRA#,  IMP_PESOS_CES_VEJ_TRA,
            #CON_MOV_ACT_PESOS_CES_VEJ_PAT#,  ACT_PESOS_CES_VEJ_PAT,
            #CON_MOV_ACT_PESOS_CES_VEJ_TRA#,  ACT_PESOS_CES_VEJ_TRA,
            #CON_MOV_REN_PESOS_CES_VEJ_PAT#,  REN_PESOS_CES_VEJ_PAT,
            #CON_MOV_REN_PESOS_CES_VEJ_TRA#,  REN_PESOS_CES_VEJ_TRA,
            #CON_MOV_CS_IMP#,                 IMP_PESOS_CUO_SOCIAL,
            #CON_MOV_VIV97#,                  ACT_AIVS_VIV
        ) AS (CON_MOV, IMP_SOL)
    FROM #CATALOG_SCHEMA#.TEMP_DEV_PAG_SJL_#SR_FOLIO#
)
WHERE IMP_SOL <> 0

