-- ==========================================
-- QUERY: NB_PANCIN_MOV_0010_EXT_PREMOVS_OCI_001.sql
-- DESCRIPCIÓN: Extraer datos de tipo de subcuenta desde OCI (DB_300 en DataStage)
-- AUTOR: Sistema
-- FECHA: 2024-01-15
-- VERSIÓN: 1.0.0
-- ==========================================
-- PARÁMETROS:
--   @CX_CRN_ESQUEMA: Esquema de CIERREN
--   @TL_CRN_TIPO_SUBCTA: Tabla de tipo de subcuenta
-- ==========================================
-- TIPO: OCI
-- USO: db.sql_oci()
-- ==========================================

SELECT 
    fcn_id_cat_subcta,
    fcn_id_grupo,
    fcn_id_tipo_subcta,
    FCN_ID_REGIMEN,
    FCN_ID_PLAZO 
FROM #CX_CRN_ESQUEMA#.#TL_CRN_TIPO_SUBCTA# 