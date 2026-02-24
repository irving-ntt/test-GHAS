-- ==========================================
-- QUERY: NB_PANCIN_MOV_0010_EXT_PREMOVS_OCI_003.sql
-- DESCRIPCIÓN: Extraer datos de concepto de movimiento desde OCI (DB_600 en DataStage)
-- AUTOR: Sistema
-- FECHA: 2024-01-15
-- VERSIÓN: 1.0.0
-- ==========================================
-- PARÁMETROS:
--   @CX_CRN_ESQUEMA: Esquema de CIERREN
--   @TL_CRN_CONFIG_CONCEP_MOV: Tabla de configuración de conceptos de movimiento
-- ==========================================
-- TIPO: OCI
-- USO: db.sql_oci()
-- ==========================================

SELECT 
    FFN_ID_CONCEPTO_MOV as FCN_ID_CONCEPTO_MOV,
    fcn_id_tipo_monto,
    frn_id_mov_subcta,
    ftn_origen as FTN_ORIGEN_APORTACION,
    NVL(FTN_DEDUCIBLE,0) FTN_DEDUCIBLE 
FROM #CX_CRN_ESQUEMA#.#TL_CRN_CONFIG_CONCEP_MOV#