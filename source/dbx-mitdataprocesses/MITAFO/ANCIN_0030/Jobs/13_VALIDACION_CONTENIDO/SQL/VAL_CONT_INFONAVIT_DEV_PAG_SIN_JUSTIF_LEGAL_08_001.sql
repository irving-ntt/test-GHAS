--Plantilla: Layout de Respuesta Devolución de Pagos sin Justificacion Legal A-P v1.0 1
--Subproceso: 3832
--Ejemplo máscara nombre: PREFTD_20250828_72942_NOTDEV.TXT
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '08' then NULL else substr(FTC_LINEA,1,2) end Error_Tipo_registro
	,CASE WHEN substr(FTC_LINEA,3,2) = '03' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,3,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,3,2) end Error_Identificador_servicio
	,CASE WHEN substr(FTC_LINEA,5,16) != REPEAT(' ', 16) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,5,16))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,5,16) end Error_Identificador_pago
	,CASE WHEN try_cast(substr(FTC_LINEA,21,15) as bigint) > 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,21,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,21,15) end Error_Importe_solicitado_Instituto
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,36,8), 'yyyyMMdd') IS NOT NULL then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,36,8))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,36,8) end Error_Fecha_liq
	--,COMO SE CALCULA
	,CASE WHEN substr(FTC_LINEA,59,15) = REPEAT('0', 15) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,59,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,59,15) END Error_Imp_aport_pend_rcv_admin
	--,COMO SE CALCULA
	,CASE WHEN try_cast(substr(FTC_LINEA,89,18) as bigint) > 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,89,18))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,89,18) end a_Error_Num_AIVS_solicitadas_Inst
	--,COMO SE CALCULA
	,CASE WHEN substr(FTC_LINEA,125,18) = REPEAT('0', 18) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,125,18))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,125,18) END Error_Num_aplic_int_viv_pend_admin
	--,COMO SE CALCULA
	--,COMO SE CALCULA
	,CASE WHEN substr(FTC_LINEA,176,15) = REPEAT('0', 15) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,176,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,176,15) END Error_Imp_viv_pend_dev_admin
	--,COMO SE CALCULA
	,CASE WHEN TRY_CAST(substr(FTC_LINEA,5,16) as BIGINT) = '1' and substr(FTC_LINEA,206,8) in ("PROF-BP ","PROF-B1 ","PROF-B2 ","PROF-B3 ","PROF-B4 ","        ") THEN NULL WHEN TRY_CAST(substr(FTC_LINEA,5,16) as BIGINT) = '3' and substr(FTC_LINEA,206,8) in ("        ") THEN NULL ELSE substr(FTC_LINEA,206,8) END Indicador_Siefore
	,CASE WHEN substr(FTC_LINEA,206,8) in ("PROF-BP ") AND substr(FTC_LINEA,214,3) = "001" THEN NULL
	WHEN substr(FTC_LINEA,206,8) in ("PROF-BP ") AND substr(FTC_LINEA,214,3) = "001" THEN NULL
	WHEN substr(FTC_LINEA,206,8) in ("PROF-B1 ") AND substr(FTC_LINEA,214,3) = "002" THEN NULL
	WHEN substr(FTC_LINEA,206,8) in ("PROF-B2 ") AND substr(FTC_LINEA,214,3) = "003" THEN NULL
	WHEN substr(FTC_LINEA,206,8) in ("PROF-B3 ") AND substr(FTC_LINEA,214,3) = "004" THEN NULL
	WHEN substr(FTC_LINEA,206,8) in ("PROF-B4 ") AND substr(FTC_LINEA,214,3) = "090" THEN NULL
	WHEN substr(FTC_LINEA,206,8) in ("        ") AND substr(FTC_LINEA,214,3) = "000" THEN NULL ELSE substr(FTC_LINEA,214,3) END Tipo_siefore
	--,Resultado_de_la_Operacion_1
	--,Diagnostico_1
	--,Diagnostico_2
	--,Diagnostico_3
	--,Filler
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '08'