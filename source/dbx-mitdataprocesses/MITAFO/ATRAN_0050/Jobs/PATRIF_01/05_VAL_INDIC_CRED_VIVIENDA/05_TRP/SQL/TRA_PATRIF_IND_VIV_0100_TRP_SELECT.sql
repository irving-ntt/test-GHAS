WITH TTRP AS (
SELECT DISTINCT
C.FTN_NUM_CTA_INVDUAL,
C.FFN_ID_CONFIG_INDI,
C.FCC_VALOR_IND
FROM PROCESOS.TTAFOTRAS_TRANS_REC_PORTA M
INNER JOIN CIERREN.TTAFOGRAL_IND_CTA_INDV C ON M.ftn_num_cta_invdual=c.ftn_num_cta_invdual 
AND M.FTC_FOLIO = '#sr_folio#'
AND m.FTC_ESTATUS_OPE = 1
AND C.FFN_ID_CONFIG_INDI = '#config_indi#'
AND FTC_VIGENCIA = '#ind_vigencia#'
) 
SELECT
	M.FTN_NUM_CTA_INVDUAL,
	M.FTC_NSS_IMSS,
	M.FTC_INSTITUTO_ORIGEN,
	M.FTC_FOLIO,
	CASE 
	    WHEN FFN_ID_CONFIG_INDI = 25 AND (FCC_VALOR_IND = 1 OR FCC_VALOR_IND = 0) 
	    THEN '01' 
	    ELSE '01' 
	END AS FTC_ESTATUS_OPE,
	CASE 
	    WHEN FFN_ID_CONFIG_INDI = 25 AND FCC_VALOR_IND = 2 
	    THEN NULL --103 
	    WHEN FFN_ID_CONFIG_INDI = 25 AND (FCC_VALOR_IND = 0 OR FCC_VALOR_IND = 1) 
	    THEN NULL 
	    ELSE NULL --21 
	END AS FTC_MOTIVO_REC,
	(FROM_TZ(CAST(SYSTIMESTAMP AS TIMESTAMP), 'UTC') AT TIME ZONE 'America/Mexico_City') AS FTD_FEH_ACT,
	'DATABRICKS' AS FTC_USU_ACT,
	999 AS FTN_ID_ARCHIVO, --Valor dummy para insertar en tabla temporal, no se inserta en la tabla final
	'.DAT' AS FTC_MASCARA_RECEP, --Valor dummy para insertar en tabla temporal, no se inserta en la tabla final
	CURRENT_TIMESTAMP as FTD_FEH_CRE, --Valor dummy para insertar en tabla temporal, no se inserta en la tabla final
	'DUMMY' AS FTC_USU_CRE, --Valor dummy para insertar en tabla temporal, no se inserta en la tabla final
	1 AS FTN_NO_LINEA --Valor dummy para insertar en tabla temporal, no se inserta en la tabla final
FROM  PROCESOS.TTAFOTRAS_TRANS_REC_PORTA M
LEFT JOIN
	TTRP T
ON
	T.FTN_NUM_CTA_INVDUAL = M.FTN_NUM_CTA_INVDUAL
where FTC_FOLIO = '#sr_folio#'
AND FTC_ESTATUS_OPE= 1
AND FTC_INSTITUTO_ORIGEN = '001'
AND (T.FFN_ID_CONFIG_INDI = '#config_indi#' AND T.FCC_VALOR_IND <> 1 OR T.FCC_VALOR_IND <> 0)
