SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Error_en_Tipo_de_registro_diferente_a_02
	,CASE WHEN substr(FTC_LINEA,3,2) = '09' then NULL else substr(FTC_LINEA,3,2) end Error_en_Identificador_del_servicio_diferente_a_09
	,CASE WHEN LAG(substr(FTC_LINEA,5,8)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,5,8) AS INT) - 1 = LAG(substr(FTC_LINEA,5,8)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,5,8) end
	ELSE NULL
	END Error_en_formato_de_Consecutivo_de_registro_dentro_del_lote_invalido
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,85,6), 'yyyyMM') IS NOT NULL and try_to_timestamp(substr(FTC_LINEA,85,6), 'yyyyMM') >= '199701' then NULL else substr(FTC_LINEA,91,8) end Error_en_Periodo_de_pago_invalido
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,91,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,91,8) end Error_en_Formato_de_fecha_de_pago_erroneo
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,99,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,99,8) end Error_en_Formato_de_fecha_valor_RCV_erroneo
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,107,11), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,107,11) end Error_en_NSS_no_valido_IMSS
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,118,11), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,118,11) end Error_en_NSS_no_valido_ISSSTE
	,CASE WHEN substr(FTC_LINEA,280,1) in ('0','1','2',' ') then NULL else substr(FTC_LINEA,280,1) end Error_en_Indicador_de_Trabajador_con_BONO
	,CASE WHEN substr(FTC_LINEA,281,2) in ('01','02') then NULL else substr(FTC_LINEA,281,2) end Error_en_identificacion_sobre_el_tipo_de_Aportacion
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,283,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,283,7) end Error_en_Sueldo_Basico_de_Cotizacion_al_RCV_no_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,290,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,290,12) end Error_en_Importe_de_SAR_ISSSTE_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,302,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,302,12) end Error_en_Importe_de_Retiro_ISSSTE_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,314,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,314,12) end Error_en_Importe_de_CV_Aportacion_Patron_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,326,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,326,12) end Error_en_Importe_de_CV_Cuota_Trabajador_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,338,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,338,12) end Error_en_Intereses_de_Pagos_Extemporaneos_de_Retiro_SAR_ISSSTE_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,350,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,350,12) end Error_en_Intereses_de_Pagos_Extemporaneos_de_Retiro_ISSSTE_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,362,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,362,12) end Error_en_Intereses_de_Pagos_Extemporaneos_de_CV_Aportacion_Patron_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,374,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,374,12) end Error_en_Intereses_de_Pagos_Extemporaneos_de_CV_Cuota_Trabajador_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,386,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,386,7) end Error_en_Importe_de_Ahorro_Solidario_Aportacion_Trabajador_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,393,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,393,7) end Error_en_Importe_de_Ahorro_Solidario_Aportacion_Dependencia_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,400,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,400,12) end Error_en_Importe_de_Intereses_de_Pagos_Extemporaneos_de_Ahorro_Solidario_Aportacion_Trabajador_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,412,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,412,12) end Error_en_Importe_de_Intereses_de_Pagos_Extemporaneos_de_Ahorro_Solidario_Aportacion_Dependencia_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,424,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,424,12) end Error_en_Importe_de_Intereses_en_Transito_de_Retiro_SAR_ISSSTE_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,436,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,436,12) end Error_en_Importe_de_Intereses_en_Transito_de_Retiro_ISSSTE_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,448,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,448,12) end Error_en_Importe_de_Intereses_en_Transito_de_CV_Aportacion_Patron_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,460,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,460,12) end Error_en_Importe_de_Intereses_en_Transito_de_CV_Cuota_Trabajador_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,472,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,472,12) end Error_en_Importe_de_Intereses_en_Transito_de_Pagos_extemporaneos_de_Retiro_SAR_ISSSTE_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,484,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,484,12) end Error_en_Importe_de_Intereses_en_Transito_de_Pagos_extemporaneos_de_Retiro_ISSSTE_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,496,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,496,12) end Error_en_Importe_de_Intereses_en_Transito_de_Pagos_extemporaneos_de_CV_Aportacion_Patron_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,508,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,508,12) end Error_en_Importe_de_Intereses_en_Transito_de_Pagos_extemporaneos_de_CV_Cuota_Trabajador_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,520,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,520,12) end Error_en_Importe_de_Intereses_en_Transito_de_Ahorro_Solidario_Aportacion_Trabajador_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,532,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,532,12) end Error_en_Importe_de_Intereses_en_Transito_de_Ahorro_Solidario_Aportacion_Dependencia_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,544,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,544,12) end Error_en_Importe_de_Intereses_en_Transito_de_Pagos_Extemporaneos_de_Ahorro_Solidario_Aportacion_Trabajador_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,556,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,556,12) end Error_en_Importe_de_Intereses_en_Transito_de_Pagos_Extemporaneos_de_Ahorro_Solidario_Aportacion_Dependencia_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,568,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,568,12) end Error_en_Importe_Total_de_Intereses_en_Transito_con_formato_invalido
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'