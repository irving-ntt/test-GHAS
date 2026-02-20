WITH TB_MARCA_DESMARCA_INFO AS (
	SELECT 
	FTC_NSS_AFORE ,
	FTC_NSS_AFORE  AS FTC_NSS_AFORE_ORIG,
	FTC_FOLIO_BITACORA AS FTN_FOLIO_BITACORA_ORIG
	from PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO
	where 
	  FTC_FOLIO_BITACORA = '#sr_folio#' --sr_folio_original
	  AND  FTC_ESTA_REGI = '1' --'#sr_id_est_reg#'
	  AND   FTC_TIPO_ARCH = '01' --'#sr_tipo_arch_original#'
) 
SELECT
		DISTINCT
		M.FTN_ID_ARCHIVO AS ID_ARCHIVO,
		M.FTD_FEH_CRE AS FECHA_CARGA,
		'02' AS DETALLE,
		M.FTN_CONTA_SERV AS NO_LINEA,
		M.FTC_NSS_AFORE AS NSS,
		M.FTC_CURP AS CURP,
		CASE 
				WHEN (CASE WHEN INFO.FTC_NSS_AFORE_ORIG IS NULL THEN 0 ELSE 1 END) = 0 
						THEN CONCAT('Registro No Coincidentes contra el archivo de Recepcion ', '#sr_folio#') --sr_folio_rechazado
				ELSE CONCAT('Registro Coincidentes contra el archivo de Recepción ', '#sr_folio#') --sr_folio_rechazado
		END AS VALIDACION,
		'3857' AS COD_ERROR,
		CASE WHEN CASE WHEN INFO.FTC_NSS_AFORE_ORIG IS NULL THEN 0 ELSE 1 END = 0 THEN 'Registro no coincidente' ELSE '' END DESC_ERROR
FROM 
	CIERREN_ETL.TTAFOTRAS_ETL_MARCA_DESMARCA M
LEFT JOIN
	TB_MARCA_DESMARCA_INFO INFO
ON
	INFO.FTC_NSS_AFORE = M.FTC_NSS_AFORE
WHERE 
    FTN_ID_ARCHIVO = '#sr_id_archivo#' --sr_id_archivo_rechazado
    AND FTC_FOLIO_BITACORA = '#sr_folio#' --sr_folio_rechazado
    AND FTC_TIPO_ARCH = '#sr_tipo_archivo#' --sr_tipo_arch_rechazado

