SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_registro_diferente_a_02
	,CASE WHEN substr(FTC_LINEA,3,2) = '03' then NULL else substr(FTC_LINEA,3,2) end Error_en_campo_Identificador_de_servicio_diferente_a_03
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,5,8), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,5,8) end Error_en_campo_Consecutivo_de_registro_del_lote_diferente_a_dato_numerico
	,CASE WHEN LAG(substr(FTC_LINEA,5,8)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,5,8) AS INT) - 1 = LAG(substr(FTC_LINEA,5,8)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,5,8) end
	ELSE NULL
	END Error_en_formato_de_Consecutivo_de_registro_dentro_del_lote_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,13,11), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,13,11) end Error_en_campo_Numero_de_seguridad_social_NSS_diferente_a_dato_numerico
	,CASE WHEN substr(FTC_LINEA,24,3) in ('001','002') then NULL else substr(FTC_LINEA,24,3) end Error_en_campo_Clave_de_entidad_origen_diferente_a_clave_valida
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,158,9), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,158,9) end Error_en_campo_Importe_registro_a_devolver_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,167,9), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,167,9) end Error_en_campo_Importe_cesantia_y_vejes_cuota_patron_a_devolver_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,176,9), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,176,9) end Error_en_campo_Importe_cesantia_y_vejes_cuota_trabajador_a_devolver_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,185,9), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,185,9) end Error_en_campo_Importe_de_cuota_social_a_devolver_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,194,9), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,194,9) end Error_en_campo_Importe_de_actualizaciones_y_recargos_de_retiro_a_devolver_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,203,9), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,203,9) end Error_en_campo_Importe_actualizaciones_y_recargos_de_ceantia_y_vejex_a_devolver_cuota_patron_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,212,9), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,212,9) end Error_en_campo_Importe_actualizaciones_y_recargos_de_ceantia_y_vejex_a_devolver_cuota_trabajador_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,221,9), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,221,9) end Error_en_campo_Importe_de_aportacion_patronal_de_vivienda_a_devolver_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,230,2), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,230,2) end Error_en_campo_dias_cotizados_en_el_bimestre_a_devolver_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,232,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,232,15) end Error_en_campo_Numero_de_apliaciones_de_vivienda_por_aportacion_patronal_a_devolver_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,247,9), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,247,9) end Error_en_campo_Importe_de_actualizaciones_de_retiro_por_rendimientos_en_cuenta_individual_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,256,9), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,256,9) end Error_en_campo_Importe_de_actualizaciones_de_cesantia_y_vejez_por_rendimientos_en_cuenta_individual_cuota_patron_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,265,9), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,265,9) end Error_en_campo_Importe_de_actualizaciones_de_cesantia_y_vejez_por_rendimientos_en_cuenta_individual_cuota_trabajador_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,274,8), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,274,8) end Error_en_campo_Fecha_de_envio_origen_de_la_solicitud_a_PROCESAR_diferente_a_dato_numerico
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '08'
	AND substr(FTC_LINEA,1,2) != '09'