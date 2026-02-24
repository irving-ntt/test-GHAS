-- COMUN_MCV_160_TD_08_DB_EXTRAE_INFO_ARCHIVO_CSV.sql
-- 20251013
WITH DATOS AS (
	SELECT 
			1 AS ORDEN
		   ,'Cuenta' AS Cuenta	
	       ,'NSS' AS NSS
	       ,'CURP' AS CURP
	       ,'Nombre_del_trabajador' AS Nombre_del_trabajador
	       ,'Validación' AS Validación
	       ,'Motivo_de_Rechazo' AS Motivo_de_Rechazo
	       ,'Fecha_de_bloqueo' AS Fecha_de_bloqueo
	FROM DUAL 
UNION 
	SELECT DISTINCT 
		   2 AS ORDEN,
		   TO_CHAR(MAT.FTN_NUM_CTA_INVDUAL) AS Cuenta,
	       TO_CHAR(BUC.FTC_NSS) AS NSS,
	       BUC.FTC_CURP AS CURP,
	       BUC.FTC_NOMBRE || ' ' || BUC.FTC_AP_PATERNO || ' ' || BUC.FTC_AP_MATERNO AS Nombre_del_trabajador,
	       CASE 
	           WHEN MAT.FTN_ID_ERROR_VAL = '283' THEN 'SUFICIENCIA DE SALDO'
	           ELSE ERR.FCC_VALOR 
	       END AS Validación,
	       CASE 
	           WHEN MAT.FTN_ID_ERROR_VAL = '283' THEN ERR.FCC_VALOR
	           ELSE SUB.FCC_VALOR
	       END AS Motivo_de_Rechazo,
	 --      MAT.FTD_FECHA_BLOQ AS Fecha_de_bloqueo,
	--       TO_CHAR(MAT.FTD_FECHA_BLOQ, 'YYYY-MM-DD') AS Fecha_de_bloqueo 
			TO_CHAR(MAT.FTD_FECHA_BLOQ, 'YYYY-MM-DD HH24:MI') AS Fecha_de_bloqueo   
	FROM CIERREN_ETL.TLSISGRAL_ETL_VAL_MATRIZ_CONV MAT
	LEFT JOIN CIERREN_ETL.TLSISGRAL_ETL_BUC BUC ON BUC.FTN_NUM_CTA_INVDUAL = MAT.FTN_NUM_CTA_INVDUAL
	LEFT JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO ERR ON ERR.FCN_ID_CAT_CATALOGO = MAT.FTN_ID_ERROR_VAL
	LEFT JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO SUB ON SUB.FCN_ID_CAT_CATALOGO = MAT.FTN_ID_SUBPROC_NO_CONV
	WHERE (MAT.FTC_FOLIO_REL = '#SR_FOLIO#' OR MAT.FTC_FOLIO = '#SR_FOLIO#')  
	AND (MAT.FTB_ESTATUS_MARCA = '0' OR MAT.FCN_CONV_INGTY = 0)
)
SELECT 
	Cuenta	
	,NSS
	,CURP
	,Nombre_del_trabajador
	,Validación
	,Motivo_de_Rechazo
	,Fecha_de_bloqueo
FROM DATOS
ORDER BY ORDEN

