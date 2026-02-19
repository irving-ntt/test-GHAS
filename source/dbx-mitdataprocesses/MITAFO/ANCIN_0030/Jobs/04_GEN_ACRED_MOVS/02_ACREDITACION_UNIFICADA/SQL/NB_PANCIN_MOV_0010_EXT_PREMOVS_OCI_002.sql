-- ==========================================
-- QUERY: NB_PANCIN_MOV_0010_EXT_PREMOVS_OCI_002.sql
-- DESCRIPCIÓN: Extraer datos de movimientos de subcuenta desde OCI (DB_500 en DataStage)
-- AUTOR: Sistema
-- FECHA: 2024-01-15
-- VERSIÓN: 1.0.0
-- ==========================================
-- PARÁMETROS:
--   @CX_CRN_ESQUEMA: Esquema de CIERREN
--   @TL_CRN_MOV_SUBCTA: Tabla de movimientos de subcuenta
-- ==========================================
-- TIPO: OCI
-- USO: db.sql_oci()
-- ==========================================

SELECT 
    fcn_id_subproceso,
    fcn_id_tipo_subcta,
    frn_id_mov_subcta,
    fcn_id_tipo_mov 
FROM #CX_CRN_ESQUEMA#.#TL_CRN_MOV_SUBCTA# 