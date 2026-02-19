-- ==========================================
-- QUERY: NB_PANCIN_MOV_0010_EXT_PREMOVS_OCI_004.sql
-- DESCRIPCIÓN: Extraer datos de valor de acción desde OCI (DB_700 en DataStage)
-- AUTOR: Sistema
-- FECHA: 2024-01-15
-- VERSIÓN: 1.0.0
-- ==========================================
-- PARÁMETROS:
--   @CX_CRN_ESQUEMA: Esquema de CIERREN
--   @TL_CRN_VALOR_ACCION: Tabla de valor de acción
-- ==========================================
-- TIPO: OCI
-- USO: db.sql_oci()
-- ==========================================

SELECT 
    FCN_ID_SIEFORE,
    fcn_id_regimen,
    FCN_ID_VALOR_ACCION,
    FCN_VALOR_ACCION,
    TRUNC(FCD_FEH_ACCION) FCD_FEH_ACCION 
FROM #CX_CRN_ESQUEMA#.#TL_CRN_VALOR_ACCION# 
WHERE TRUNC(FCD_FEH_ACCION) >= SYSDATE - 365 