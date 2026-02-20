--Plantilla:LayoutRespuestaPorta
--TRANSFERENCIA DE RECURSOS POR PORTABILIDAD
--Subproceso: 3286
--Ejemplo máscara nombre: PTCFT.DP.A01534.PORTATRA.GDG
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Tipo_registro
	--,Folio_Tramite_ProceSAR
	--,NSS_IMSS
	--,Apellido_Paterno_
	--,Apellido_Materno_
	--,Nombre_
	--,CURP
	--,Tipo_operacion
	--,Instituto_Origen
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,207,10), '[0-9 ]*', '')) = 0 then NULL else substr(FTC_LINEA,207,10) end Identificador_de_credito_INFONAVIT
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,217,12), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,217,12))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,217,12) end Monto_en_Pesos_INFONAVIT_97
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,229,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,229,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,229,15) end Valor_AIV_INFONAVIT_97
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,244,8), 'yyyyMMdd') IS NOT NULL then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,244,8))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,244,8) end Fecha_de_Valor_AIVs_INFONAVIT_97
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,252,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,252,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,252,15) end Total_AIVs_de_Vivienda_INFONAVIT_97
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,267,10), '[0-9 ]*', '')) = 0 then NULL else substr(FTC_LINEA,267,10) end Identificador_de_credito_FOVISSSTE
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,277,12), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,277,12))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,277,12) end Monto_en_Pesos_FOVISSSTE_2008
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,289,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,289,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,289,15) end Valor_AIV_FOVISSSTE_2008
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,304,8), 'yyyyMMdd') IS NOT NULL then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,304,8))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,304,8) end Fecha_de_Valor_AIVs_FOVISSSTE_2008
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,312,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,312,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,312,15) end Total_AIVs_de_Vivienda_FOVISSSTE_2008
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,327,8), 'yyyyMMdd') IS NOT NULL then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,327,8))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,327,8) end Fecha_del_credito
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,335,8), 'yyyyMMdd') IS NOT NULL then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,335,8))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,335,8) end Fecha_valor_de_la_transferencia
	--,Diagnostico_
	--,Motivo_rechazo
	--,Resultado_de_la_operacion
	--,Filler
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'