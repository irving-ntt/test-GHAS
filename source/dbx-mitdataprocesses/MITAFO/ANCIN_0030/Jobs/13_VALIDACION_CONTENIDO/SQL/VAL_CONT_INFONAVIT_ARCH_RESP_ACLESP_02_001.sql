SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Error_en_Tipo_de_registro_diferente_a_02
	,CASE WHEN substr(FTC_LINEA,3,2) = '03' then NULL else substr(FTC_LINEA,3,2) end Error_en_Identificador_del_servicio_diferente_a_03
	,CASE WHEN LAG(substr(FTC_LINEA,5,8)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,5,8) AS INT) - 1 = LAG(substr(FTC_LINEA,5,8)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,5,8) end
	ELSE NULL
	END Error_en_formato_de_Consecutivo_de_registro_dentro_del_lote_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,13,11), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,13,11) end Error_en_NSS_no_valido
	,CASE WHEN substr(FTC_LINEA,55,50) != '' then NULL else substr(FTC_LINEA,55,50) end Error_no_presenta_Nombre_de_Trabajador
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,85,6), 'yyyyMM') IS NOT NULL and try_to_timestamp(substr(FTC_LINEA,85,6), 'yyyyMM') >= '199701' then NULL else substr(FTC_LINEA,91,8) end Error_en_Periodo_de_pago_invalido
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,111,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,111,8) end Error_en_Formato_de_fecha_de_pago_erroneo
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,119,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,119,8) end Error_en_Formato_de_fecha_valor_RCV_erroneo
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,127,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,127,8) end Error_en_Formato_de_fecha_Fecha_valor_Cuatro_Seguros_IMSS_ACV_y_Vivienda_erroneo
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,135,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,135,7) end Error_en_ultimo_salario_diario_integrado_del_periodo_no_numerico
	,CASE WHEN substr(FTC_LINEA,142,6) != '' then NULL else substr(FTC_LINEA,142,6) end Error_Folio_de_pago_SUA_vacio
	,CASE WHEN substr(FTC_LINEA,148,11) != '' then NULL else substr(FTC_LINEA,148,11) end Error_Registro_patronal_IMSS_NRP_vacio
	,CASE WHEN substr(FTC_LINEA,172,3) != '' then NULL else substr(FTC_LINEA,172,3) end Error_no_presenta_Nombre_de_Trabajador_2
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,175,2), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,175,2) end Error_en_Dias_cotizados_en_el_bimestre_No_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,177,2), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,177,2) end Error_en_Dias_de_incapacidad_en_el_bimestre_No_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,179,2), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,179,2) end Error_en_Dias_de_ausentismo_en_el_bimestre_No_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,181,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,181,7) end Error_en_Aportacion_Retiro_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,188,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,188,7) end Error_en_Actualizacion_Retiro_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,195,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,195,7) end Error_en_Aportacion_Cesantia_y_Vejez_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,202,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,202,7) end Error_en_Actualizacion_Cesantia_y_Vejez_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,209,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,209,7) end Error_en_Aportacion_Voluntaria_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,216,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,216,7) end Error_en_Aportacion_ACR_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,223,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,223,7) end Error_en_Aportacion_Vivienda_con_formato_invalido
	,CASE WHEN substr(FTC_LINEA,230,7) = '0000000' then NULL else substr(FTC_LINEA,230,7) end Error_en_Aportacion_Cuota_Social_invalido
	,CASE WHEN substr(FTC_LINEA,237,7) = '0000000' then NULL else substr(FTC_LINEA,237,7) end Error_en_Aportacion_Estatal_Social_invalido
	,CASE WHEN substr(FTC_LINEA,244,7) = '0000000' then NULL else substr(FTC_LINEA,244,7) end Error_en_Aportacion_Especial_Social_invalido
	,CASE WHEN substr(FTC_LINEA,251,7) = '0000000' then NULL else substr(FTC_LINEA,251,7) end Error_en_Actualizacion_Cuota_Social_invalido
	,CASE WHEN substr(FTC_LINEA,258,7) = '0000000' then NULL else substr(FTC_LINEA,258,7) end Error_en_Actualizacion_Estatal_Social_invalido
	,CASE WHEN substr(FTC_LINEA,265,7) = '0000000' then NULL else substr(FTC_LINEA,265,7) end Error_en_Actualizacion_Especial_Social_invalido
	,CASE WHEN substr(FTC_LINEA,272,8) = '00010101' then NULL else substr(FTC_LINEA,272,8) end Error_en_Fecha_pago_Gubernamental_invalida
	,CASE WHEN substr(FTC_LINEA,280,1) in ('0','2') then NULL else substr(FTC_LINEA,280,1) end Identificador_de_Vivienda_en_Garantia
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,281,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,281,15) end Error_en_AVIS_Vivienda_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,296,11), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,296,11) end Error_en_Formato_Valor_de_la_Aplicacion_de_Intereses_de_Vivienda_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,307,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,307,7) end Error_en_Importe_Remanente_de_Vivienda_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,314,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,314,15) end Error_en_AIVS_Remanente_de_Vivienda_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,329,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,329,7) end Error_en_Importe_Extemporaneo_de_Vivienda_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,336,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,336,15) end Error_en_AIVS_Extemporaneo_de_Vivienda_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,351,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,351,7) end Error_en_Aportacion_ALP_invalido
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'